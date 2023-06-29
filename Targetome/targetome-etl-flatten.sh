#!/bin/bash

usage () {
    echo "Usage: $(basename "$0")"
    echo "  --export_dir => Directory to put the exported TSV files"
    echo "  --help => Display this help message"
}

# Parse Command-Line Arguments
getopt --test > /dev/null
if [ $? -ne 4 ]; then
    echo "`getopt --test` failed in this environment."
    exit 1
fi

## Command line options should match usage description
OPTIONS=
LONGOPTIONS=help,export_dir:,

# -temporarily store output to be able to check for errors
# -e.g. use "--options" parameter by name to activate quoting/enhanced mode
# -pass arguments only via   -- "$@"   to separate them correctly
PARSED=$(\
    getopt --options=$OPTIONS --longoptions=$LONGOPTIONS --name "$0" -- "$@"\
)
if [ $? -ne 0 ]; then
    # e.g. $? == 1
    #  then getopt has complained about wrong arguments to stdout
    usage
    exit 2
fi

# read getopt's output this way to handle the quoting right:
eval set -- "$PARSED"

# default export directory
EXPORT_DIR=export

## Handle each command line option. Lower-case variables, e.g., ${file}, only
## exist if they are set as environment variables before script execution.
## Environment variables are used by Agave. If the environment variable is not
## set, the Upper-case variable, e.g., ${FILE}, is assigned from the command
## line parameter.
while true; do
    case "$1" in
        --help)
            usage
            exit 0
            ;;
        --export_dir)
            if [ -z "${export_dir}" ]; then
                EXPORT_DIR=$2
            else
                EXPORT_DIR=${export_dir}
            fi
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Invalid option"
            usage
            exit 3
            ;;
    esac
done

if [ -z "${EXPORT_DIR}" ]; then
    echo "Export directory required"
    echo
    usage
    exit 1
fi
EXPORT_DIR_FULL=$(readlink -f "${EXPORT_DIR}")
EXPORT_DIR_DIR=$(dirname "${EXPORT_DIR_FULL}")
EXPORT_DIR_BASE=$(basename "${EXPORT_DIR_FULL}")

# create export dir if it doesn't already exist
mkdir -p ${EXPORT_DIR_FULL}

# Delete existing exported TSV files 
sudo rm -rf /tmp/targetome_*

# Create temporary tables - these are flattened joined views of original tables
cat <<SQL | mysql -N -u user -D targetome
DROP TABLE IF EXISTS TargetSynonyms;
CREATE TABLE TargetSynonyms
SELECT
  targetID,
  synonym
FROM TargetSyn_SET;
SQL

cat << SQL | mysql -N -u user -D targetome
DROP TABLE IF EXISTS DrugSynonyms;
CREATE TABLE DrugSynonyms
SELECT
  drugID,
  synonym
FROM DrugSyn_SET;
SQL

cat << SQL | mysql -N -u user targetome
DROP TABLE IF EXISTS Sources;
CREATE TABLE Sources
SELECT
  src.sourceID,
  lite.PubMedID,
  db.databaseName,
  db.version AS db_version,
  db.downloadURL AS db_downloadURL,
  db.downloadDate AS db_downloadDate
FROM Source AS src
JOIN LitEvidence AS lite
  ON src.litID = lite.litID
JOIN DatabaseRef AS db
  ON src.databaseID = db.databaseID;
SQL

cat << SQL | mysql -N -u user targetome
DROP TABLE IF EXISTS Experiments;
CREATE TABLE Experiments
SELECT
  exp.expID,
  exp.assayType as exp_assayType,
  exp.assayValueLow as exp_assayValueLow,
  exp.assayValueMedian as exp_assayValueMedian,
  exp.assayValueHigh as exp_assayValueHigh,
  exp.assayUnits as exp_assayUnits,
  exp.assayRelation as exp_assayRelation,
  exp.assayDescription as exp_assayDescription,
  exp.assaySpecies as exp_assaySpecies,
  exp.parentSource as exp_parentSource,
  Sources.*
FROM ExpEvidence as exp
JOIN expEvidenceSource_Set as expSrc
  ON exp.expID = expSrc.expID
LEFT JOIN Sources
  on expSrc.sourceID = Sources.sourceID;
SQL

cat << SQL | mysql -N -u user targetome
DROP TABLE IF EXISTS Interactions;
CREATE TABLE Interactions
SELECT
  Interaction.*,
  Drug.drugName,
  Drug.approvalDate as drug_approvalDate,
  Drug.atcClassID as drug_atcClassID,
  Drug.atcClassName as drug_atcClassName,
  Drug.atcClassStatus as drug_atcClassStatus,
  Drug.epcClassID as drug_epcClassID,
  Drug.epcClassName as drug_epcClassName,
  Target.targetName,
  Target.targetType,
  Target.uniprotID as target_uniprotID,
  Target.targetSpecies,
  interactionSources_Set.sourceID,
  interactionExpEvidence_Set.expID
FROM Interaction
JOIN Target
  ON Interaction.targetID = Target.targetID
JOIN Drug
  ON Interaction.drugID = Drug.drugID
LEFT JOIN interactionSources_Set
  ON Interaction.interactionID = interactionSources_Set.interactionID
LEFT JOIN interactionExpEvidence_Set
  ON Interaction.interactionID = interactionExpEvidence_Set.interactionID;
SQL

# List of tables in database to iterate
TABLE_LIST=(TargetSynonyms DrugSynonyms Sources Experiments Interactions)

for table in ${TABLE_LIST[@]}; do
    echo "${table}"

    # Get list of column names in table
    COL_NAMES=$(
cat <<SQL | mysql -N -u user targetome
SELECT
  GROUP_CONCAT(CONCAT("'", column_name, "'"))
FROM information_schema.columns
WHERE
  table_name = '${table}'
  AND table_schema = 'targetome'
ORDER BY ordinal_position;
SQL
    )

    # Export each Targetome table to TSV file, include column names as header
cat <<SQL | mysql -u user targetome
SELECT ${COL_NAMES}
UNION ALL
SELECT
  *
INTO OUTFILE '/tmp/targetome_${table}.tsv'
CHARACTER SET 'utf8'
FIELDS TERMINATED by '\t'
OPTIONALLY ENCLOSED BY ''
FROM ${table};
SQL

done

# Copy TSV files to specified folder
sudo mv /tmp/targetome_* ${EXPORT_DIR_FULL}

# Generate default schema
#for tsv in $(ls ${EXPORT_DIR_FULL}/*.tsv); do
#    python3 ~/extlib/createSchemaP3.py ${tsv} 1
#done



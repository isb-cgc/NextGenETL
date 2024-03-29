#!/usr/bin/env bash

# Copyright 2019, Institute for Systems Biology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.



REL_ROOT=${HOME}/GDC-metadata
SCRATCH_DIR=${REL_ROOT}/scratch
export PATH=${REL_ROOT}/scripts/:${PATH}

#
# Per release configuration values are changed in this file, not here. Plus, put the
# customized config file up in the home directory so it can be retained long-term even if
# code is changing.
#

source ${HOME}/setEnvVarsGDCMetadata.sh

#
# These flags tell the script what to do. This allows the user to do the workflow step-by-step as desired.
# Actual values are set in the setEnvVarsGDCMetadata.sh file, **not here**. This script should, in practice,
# not need to be edited as things change from release to release!
#
#
#BUILD_DIR=skip
#API_PULL_LEGACY=skip
#API_PULL_CURRENT=skip
#HEX_EXTRACT=run
#CURR_FILE_CHECK=run
#LEG_FILE_CHECK=run
#QC_CHECK=run
#GEN_CUT_LISTS=run
#BQ_PREP_CASES=run
#BQ_PREP_OTHER=run
#BUILD_NORM_TSVS=run
#COMPARE_TO_LAST=run
#DETAILED_DIFFS=run
#RAW_SCHEMA_CHECK=run
#OUTPUT_LOOK_FILES=run
#COPY_ANNOT_SCHEMA=run
#LOAD_BQ=run
#DESC_AND_LABELS=run
#PUBLISH_TABLES=run
#ARCHIVE_TARS=run

#
# REALLY cannot run both phases at once. The API pull takes days and exits immediately with nohup set:
#

PHASE_1=false
PHASE_2=false

if [ "${API_PULL_LEGACY}" == "run" ] || [ "${API_PULL_CURRENT}" == "run" ] || [ "${BUILD_DIR}" == "run" ]; then
    PHASE_1=true
fi

if [ "${QC_CHECK}" == "run" ] || \
   [ "${HEX_EXTRACT}" == "run" ] || \
   [ "${CURR_FILE_CHECK}" == "run" ] || \
   [ "${LEG_FILE_CHECK}" == "run" ] || \
   [ "${GEN_CUT_LISTS}" == "run" ] || \
   [ "${BQ_PREP_CASES}" == "run" ] || \
   [ "${BQ_PREP_OTHER}" == "run" ] || \
   [ "${RAW_SCHEMA_CHECK}" == "run" ] || \
   [ "${OUTPUT_LOOK_FILES}" == "run" ] || \
   [ "${BUILD_NORM_TSVS}" == "run" ] || \
   [ "${COMPARE_TO_LAST}" == "run" ] || \
   [ "${DETAILED_DIFFS}" == "run" ] || \
   [ "${COPY_ANNOT_SCHEMA}" == "run" ] || \
   [ "${LOAD_BQ}" == "run" ] || \
   [ "${DESC_AND_LABELS}" == "run" ] || \
   [ "${PUBLISH_TABLES}" == "run" ] || \
   [ "${ARCHIVE_TARS}" == "run" ]; then
    PHASE_2=true
fi

if [ "${PHASE_1}" = true ] && [ "${PHASE_2}" = true ]; then
    echo "CANNOT RUN PHASES 1 AND 2 TOGETHER"
    exit
fi

#
# Make the three directories!
#

if [ "${BUILD_DIR}" == "run" ]; then
    mkdir -p ${REL_ROOT}
    mkdir -p ${SCRATCH_DIR}
    cd ${REL_ROOT}
    mkdir -p ${RELNAME}-current
    mkdir -p ${RELNAME}-legacy
    mkdir -p ${RELNAME}-forBQ
fi

#
# Query the GDC APIs. This takes a **long** time, and should be done separately and allowed to complete before
# doing phase II operations:
#

if [ "${API_PULL_LEGACY}" == "run" ]; then
    source ~/pyVenvForThree/bin/activate
    cd ${REL_ROOT}/${RELNAME}-legacy
    run_try_legacy.sh
    deactivate
fi

if [ "${API_PULL_CURRENT}" == "run" ]; then
    source ~/pyVenvForThree/bin/activate
    cd ${REL_ROOT}/${RELNAME}-current
    run_try_active.sh
    deactivate
fi

if [ "${API_PULL_CURRENT}" == "run" ] || [ "${API_PULL_LEGACY}" == "run" ]; then
    echo "Go away for a day or two..."
    exit
fi


#
# Extract out the file hex codes needed by downstream scripts. API scripts tag outputs with random hex
# codes (but not guaranteed unique?) to allow multiple runs to be sorted out.
#
# WJRL-08-03-19: This is the hex code for the case-API load in current. Note (see rel15-current) that
# there are no "caseless" files in current (file-API and case-API fileData pulls are exactly the same
# size). SO FOR CURRENT FILEDATA PULLS WE CAN USE THE CASE-API HEX FOR FILEDATA:
#CURRENT_CASE_API_HEX=2a88c9c6
# WJRL-08-03-19: This is the hex code for the case-API load in legacy:
#LEGACY_CASE_API_HEX=5a036efc
# WJRL-08-03-19: This is the hex code for the file-API load in legacy. Since there are caseless files
# in legacy, we need to use the file-API hex to get all the files:
#LEGACY_FILE_API_HEX=5e71e3b0
#

if [ "${HEX_EXTRACT}" == "run" ]; then
    echo "Running HEX_EXTRACT"

    #
    # Extract from current:
    #

    cd ${REL_ROOT}/${RELNAME}-current
    echo "now in" `pwd`
    CURRENT_CASE_API_HEX=`ls -1 caseData.bq.*.t* | awk -F . '{print $3}' | sort | uniq`

    #
    # *_FILE_API_HEX is tricker, since there are two fileData.bq sets: one from the case
    # data run and one from the file data run. We take the one that is *NOT* matched to the
    # case data hex:
    #

    TWO_FILE_DATA_HEX=`ls -1 fileData.bq.*.t* | awk -F . '{print $3}' | sort | uniq`
    CURRENT_FILE_API_HEX=`echo ${CURRENT_CASE_API_HEX} ${TWO_FILE_DATA_HEX} | tr " " "\n" | \
                         sort | uniq -c | grep "^ *1 " | awk '{print $2}'`

    #
    # Save to file so we can reuse if we run other later steps but not this one:
    #

    echo ${CURRENT_CASE_API_HEX} > current_case_api_hex.txt
    echo ${CURRENT_FILE_API_HEX} > current_file_api_hex.txt

    #
    # Do the same for legacy:
    #

    cd ${REL_ROOT}/${RELNAME}-legacy
    LEGACY_CASE_API_HEX=`ls -1 caseData.bq.*.t* | awk -F . '{print $3}' | sort | uniq`

    TWO_FILE_DATA_HEX=`ls -1 fileData.bq.*.t* | awk -F . '{print $3}' | sort | uniq`
    LEGACY_FILE_API_HEX=`echo ${LEGACY_CASE_API_HEX} ${TWO_FILE_DATA_HEX} | tr " " "\n" | \
                         sort | uniq -c | grep "^ *1 " | awk '{print $2}'`
    echo ${LEGACY_CASE_API_HEX} > legacy_case_api_hex.txt
    echo ${LEGACY_FILE_API_HEX} > legacy_file_api_hex.txt

fi

#
# If file data was done correctly in the current archive, the line counts and byte counts should match! Actually,
# the listings should match too, but fields that are ";"-delimited can have a different order! Empirically, even
# "sum -s" results match (the -s specifies 512-byte blocks) despite the order difference.
#
# For legacy files, the difference between case and file API listings is:
#
# 837960-761544=76416 lines for rel15
#
# 837960-761544=76416 lines for rel19
#
# So we can QC legacy loads based on expected file count absolute values.
#

if [ "${CURR_FILE_CHECK}" == "run" ]; then
    echo "Running CURR_FILE_CHECK"
    cd ${REL_ROOT}/${RELNAME}-current
    CURRENT_CASE_API_HEX=`cat current_case_api_hex.txt`
    CURRENT_FILE_API_HEX=`cat current_file_api_hex.txt`
    SIZE_BY_CASE_API=`cat fileData.bq.${CURRENT_CASE_API_HEX}.tsv | wc -c`
    SIZE_BY_FILE_API=`cat fileData.bq.${CURRENT_FILE_API_HEX}.tsv | wc -c`
    if [ ${SIZE_BY_CASE_API} != ${SIZE_BY_FILE_API} ]; then
        echo "ERROR: Current file size mismatch: ${SIZE_BY_CASE_API} vs. ${SIZE_BY_FILE_API}"
        exit
    fi
fi

#
# We set the expected values in the setEnvVarsGDCMetadata.sh file, **not here**, in case they change:
#
#EXPECTED_LEGACY_FILE_SIZE_BY_CASE=761544
#EXPECTED_LEGACY_FILE_SIZE_BY_FILE=837960
#
# Note: the SIZE of the downloaded file for the by-case file listing can change, since in some releases (rel12, rel21)
# an additional column "file_state" appears in the by-by case listing. Thus, fileData.bq.hex-for-case.tsv = 631339195
# and fileData.bq.hex-for-files.tsv = 658598393. Compared to fileData.bq.hex-for-case.tsv = 623723754.
#
# However, since we do not use the by-case listing, we can ignore the difference.
#
#

if [ "${LEG_FILE_CHECK}" == "run" ]; then
    echo "Running LEG_FILE_CHECK"
    cd ${REL_ROOT}/${RELNAME}-legacy
    LEGACY_CASE_API_HEX=`cat legacy_case_api_hex.txt`
    LEGACY_FILE_API_HEX=`cat legacy_file_api_hex.txt`
    SIZE_BY_CASE_API=`cat fileData.bq.${LEGACY_CASE_API_HEX}.tsv | wc -l`
    SIZE_BY_FILE_API=`cat fileData.bq.${LEGACY_FILE_API_HEX}.tsv | wc -l`
    if [ ${SIZE_BY_CASE_API} -ne ${EXPECTED_LEGACY_FILE_SIZE_BY_CASE} ]; then
        echo "ERROR: Unexpected legacy SIZE_BY_CASE_API: ${SIZE_BY_CASE_API} vs. ${EXPECTED_LEGACY_FILE_SIZE_BY_CASE}"
        exit
    fi
    if [ ${SIZE_BY_FILE_API} -ne ${EXPECTED_LEGACY_FILE_SIZE_BY_FILE} ]; then
        echo "ERROR: Unexpected legacy SIZE_BY_FILE_API: ${SIZE_BY_FILE_API} vs. ${EXPECTED_LEGACY_FILE_SIZE_BY_FILE}"
        exit
    fi
fi

#
# The current case table preparation includes counting the number of legacy files per case. Rel17 and Rel18
# had API failures that made this count incorrect. Above test should catch this, but belt-and-suspenders.
# NOTE: If we had API failures on the legacy run, move those files off to the side and copy in the files
# from a previous clean legacy load, unless there really has been a change to the legacy archive
#

if [ "${QC_CHECK}" == "run" ]; then
    echo "Running QC_CHECK"
    cd ${REL_ROOT}/${RELNAME}-legacy
    missing_data.sh > ../scratch/missingCases.txt
    MISSING_CASE_SIZE=`cat ../scratch/missingCases.txt | wc -l`
    if [ ${MISSING_CASE_SIZE} -ne 0 ]; then
        echo "ERROR: Missing files for ${MISSING_CASE_SIZE} cases"
        exit
    fi
fi

#
# Only certain columns are retained. This was formerly done with manual curation. Automate this:
#

if [ "${GEN_CUT_LISTS}" == "run" ]; then
    echo "Running GEN_CUT_LISTS"

    #
    # We set the expected cut lists in the setEnvVarsGDCMetadata.sh file, **not here**, in case they change:
    #
    #KEEP_COLS='case_id project__dbgap_accession_number project__disease_type project__name '
    #KEEP_COLS=${KEEP_COLS}'project__program__dbgap_accession_number project__program__name '
    #KEEP_COLS=${KEEP_COLS}'project__project_id submitter_id'
    #
    #KEEP_COUNT="summary__file_count"
    #TOSS_COLS="error_type"
    #
    #EXPECTED_LEGACY="2,5,6,7,8,9,10,28"
    #EXPECTED_CURRENT="2,5,6,7,8,9,10,29"
    #EXPECTED_LEGACY_FILE_COUNT_COL="30"
    #EXPECTED_CURRENT_FILE_COUNT_COL="31"
    #EXPECTED_ERROR_COL="30"

    # Legacy columns to keep:
    cd ${REL_ROOT}/${RELNAME}-legacy
    LEGACY_CASE_API_HEX=`cat legacy_case_api_hex.txt`
    FILE_IN="caseData.bq.${LEGACY_CASE_API_HEX}.tsv"

    CUT_LIST=`generate_cut_list.sh ${FILE_IN} ${EXPECTED_LEGACY} "${KEEP_COLS}"`
    HAVE_ERROR=$?
    if [ ${HAVE_ERROR} -ne 0 ]; then
        echo "WARNING: Retained legacy columns have changed; perform manual curation!"
        exit
    fi
    echo ${CUT_LIST} > legacy_case_cut_list.txt

    #Legacy column holding file count:
    SUMMARY_COL=`generate_cut_list.sh ${FILE_IN} ${EXPECTED_LEGACY_FILE_COUNT_COL} ${KEEP_COUNT}`
    HAVE_ERROR=$?
    if [ ${HAVE_ERROR} -ne 0 ]; then
        echo "WARNING: file count column has changed; perform manual curation!"
        exit
    fi
    echo ${SUMMARY_COL} > legacy_count_col.txt

    # Current columns to keep:
    cd ${REL_ROOT}/${RELNAME}-current
    CURRENT_CASE_API_HEX=`cat current_case_api_hex.txt`
    FILE_IN="caseData.bq.${CURRENT_CASE_API_HEX}.tsv"

    CUT_LIST=`generate_cut_list.sh ${FILE_IN} ${EXPECTED_CURRENT} "${KEEP_COLS}"`
    HAVE_ERROR=$?
    if [ ${HAVE_ERROR} -ne 0 ]; then
        echo "WARNING: Retained current columns have changed; perform manual curation!"
        exit
    fi
    echo ${CUT_LIST} > current_case_cut_list.txt

    #current column holding file count:

    SUMMARY_COL=`generate_cut_list.sh ${FILE_IN} ${EXPECTED_CURRENT_FILE_COUNT_COL} ${KEEP_COUNT}`
    HAVE_ERROR=$?
    if [ ${HAVE_ERROR} -ne 0 ]; then
        echo "WARNING: Retained current columns have changed; perform manual curation!"
        exit
    fi
    echo ${SUMMARY_COL} > current_count_col.txt

    # current file error_type column to ditch:
    CURRENT_FILE_API_HEX=`cat current_file_api_hex.txt`
    FILE_IN="fileData.bq.${CURRENT_FILE_API_HEX}.tsv"


    TOSS_COL=`generate_cut_list.sh ${FILE_IN} ${EXPECTED_ERROR_COL} ${TOSS_COLS}`
    HAVE_ERROR=$?
    if [ ${HAVE_ERROR} -ne 0 ]; then
        echo "WARNING: Retained current file columns have changed; perform manual curation!"
        exit
    fi

    ONE_LESS="$((${TOSS_COL}-1))"
    ONE_MORE="$((${TOSS_COL}+1))"
    DITCH_LIST="1-${ONE_LESS},${ONE_MORE}-"
    echo ${DITCH_LIST} > current_skip_error_type_cols.txt

fi

#
# Prep the cases for BQ:
#

if [ "${BQ_PREP_CASES}" == "run" ]; then
    echo "Running BQ_PREP_CASES"
    cd ${REL_ROOT}/${RELNAME}-forBQ
    CURRENT_CASE_API_HEX=`cat ${REL_ROOT}/${RELNAME}-current/current_case_api_hex.txt`
    LEGACY_CASE_API_HEX=`cat ${REL_ROOT}/${RELNAME}-legacy/legacy_case_api_hex.txt`
    LEGACY_FILE_API_HEX=`cat ${REL_ROOT}/${RELNAME}-legacy/legacy_file_api_hex.txt`
    CURRENT_CUT=`cat ${REL_ROOT}/${RELNAME}-current/current_case_cut_list.txt`
    LEGACY_CUT=`cat ${REL_ROOT}/${RELNAME}-legacy/legacy_case_cut_list.txt`
    CURRENT_COUNT_COL=`cat ${REL_ROOT}/${RELNAME}-current/current_count_col.txt`
    LEGACY_COUNT_COL=`cat ${REL_ROOT}/${RELNAME}-legacy/legacy_count_col.txt`
    just_case.sh ${CURRENT_CASE_API_HEX} ${LEGACY_CASE_API_HEX} ${RELNAME} ${SCRATCH_DIR} \
                 ${CURRENT_CUT} ${LEGACY_CUT} ${CURRENT_COUNT_COL} ${LEGACY_COUNT_COL}

fi

#
# Prep the other files for BQ:
#

if [ "${BQ_PREP_OTHER}" == "run" ]; then
    echo "Running BQ_PREP_OTHER"
    source ~/pyVenvForTwo/bin/activate
    cd ${REL_ROOT}/${RELNAME}-forBQ
    CURRENT_CASE_API_HEX=`cat ${REL_ROOT}/${RELNAME}-current/current_case_api_hex.txt`
    LEGACY_CASE_API_HEX=`cat ${REL_ROOT}/${RELNAME}-legacy/legacy_case_api_hex.txt`
    LEGACY_FILE_API_HEX=`cat ${REL_ROOT}/${RELNAME}-legacy/legacy_file_api_hex.txt`
    DITCH_LIST=`cat ${REL_ROOT}/${RELNAME}-current/current_skip_error_type_cols.txt`
    proc_release_tables.sh ${CURRENT_CASE_API_HEX} ${LEGACY_CASE_API_HEX} ${LEGACY_FILE_API_HEX} \
                           ${RELNAME} ${SCRATCH_DIR} ${DITCH_LIST}
    deactivate
fi

#
# Check raw schemas. If there is a change, we need to call a halt and figure out what is going on:
#

if [ "${RAW_SCHEMA_CHECK}" == "run" ]; then
    echo "Running RAW_SCHEMA_CHECK"
    ALL_GOOD=true
    ALIQ_DIFF=`diff ${REL_ROOT}/textFiles/aliqMap.rawSchema.json ${REL_ROOT}/${RELNAME}-forBQ/aliqMap.merge.t1.json`
    if [ ! -z "${ALIQ_DIFF}" ]; then
        ALL_GOOD=false
        echo "Aliquot raw schema mismatch: ${ALIQ_DIFF}"
    fi
    CASE_DIFF=`diff ${REL_ROOT}/textFiles/caseData.rawSchema.json ${REL_ROOT}/${RELNAME}-forBQ/caseData.merge.t1.json`
    if [ ! -z "${CASE_DIFF}" ]; then
        ALL_GOOD=false
        echo "Case data schema mismatch: ${CASE_DIFF}"
    fi
    FILE_CURR_DIFF=`diff ${REL_ROOT}/textFiles/fileData.current.rawSchema.json ${REL_ROOT}/${RELNAME}-forBQ/fileData.current.t1.json`
    if [ ! -z "${FILE_CURR_DIFF}" ]; then
        ALL_GOOD=false
        echo "Current file schema mismatch: ${FILE_CURR_DIFF}"
    fi
    FILE_LEG_DIFF=`diff ${REL_ROOT}/textFiles/fileData.legacy.rawSchema.json ${REL_ROOT}/${RELNAME}-forBQ/fileData.legacy.t1.json`
    if [ ! -z "${FILE_LEG_DIFF}" ]; then
        ALL_GOOD=false
        echo "Legacy file schema mismatch: ${FILE_LEG_DIFF}"
    fi
    SLIDE_DIFF=`diff ${REL_ROOT}/textFiles/slidMap.rawSchema.json ${REL_ROOT}/${RELNAME}-forBQ/slidMap.merge.t1.json`
    if [ ! -z "${SLIDE_DIFF}" ]; then
        ALL_GOOD=false
        echo "Slide raw schema mismatch: ${SLIDE_DIFF}"
    fi
    if [ ${ALL_GOOD} != true ]; then
        echo "RAW SCHEMA MATCH FAILURE: EXITING"
        exit
    fi
fi

#
# "LOOK" files are
#

if [ "${OUTPUT_LOOK_FILES}" == "run" ]; then
    echo "Running OUTPUT_LOOK_FILES"
    cd ${REL_ROOT}/${RELNAME}-forBQ
    echo "------------------------- LOOK AT ALIQUOT"
    cat aliqMap.merge.t1.look
    echo "------------------------- LOOK AT CASES"
    cat caseData.merge.t1.look
    echo "------------------------- LOOK AT CURRENT FILES"
    cat fileData.current.t1.look
    echo "------------------------- LOOK AT LEGACY FILES"
    cat fileData.legacy.t1.look
    echo "------------------------- LOOK AT_SLIDES"
    cat slidMap.merge.t1.look
fi

#
# Normalize lists. Many GDC fields are lists delimited by ";", and the order is not consistent
# between releases. To check for real changes between releases, we want to normalize these
# lists to be lexicographic.
#
# WARNING! DANGER! These tables cannot be the ones uploaded to the BQ table, because the ordering appears to be
# consistent across columns. So normalizing the order of the columns will break that correspondence.
# DO NOT USE THEM EXCEPT FOR CHECKS!
#

if [ "${BUILD_NORM_TSVS}" == "run" ]; then
    echo "Running BUILD_NORM_TSVS"
    cd ${REL_ROOT}
    rm -rf ${RELNAME}norm-forBQ
    rm -rf ${PREV_RELNAME}norm-forBQ
    mkdir -p ${RELNAME}norm-forBQ
    mkdir -p ${PREV_RELNAME}norm-forBQ
    source ~/pyVenvForThree/bin/activate

    python scripts/multiNormalize.py ${RELNAME}-forBQ/caseData.merge.t1 ${RELNAME}norm-forBQ/caseData.merge.t1
    python scripts/multiNormalize.py ${RELNAME}-forBQ/aliqMap.merge.t1 ${RELNAME}norm-forBQ/aliqMap.merge.t1
    python scripts/multiNormalize.py ${RELNAME}-forBQ/slidMap.merge.t1 ${RELNAME}norm-forBQ/slidMap.merge.t1
    python scripts/multiNormalize.py ${RELNAME}-forBQ/fileData.current.t1 ${RELNAME}norm-forBQ/fileData.current.t1
    python scripts/multiNormalize.py ${RELNAME}-forBQ/fileData.legacy.t1 ${RELNAME}norm-forBQ/fileData.legacy.t1

    python scripts/multiNormalize.py ${PREV_RELNAME}-forBQ/caseData.merge.t1 ${PREV_RELNAME}norm-forBQ/caseData.merge.t1
    python scripts/multiNormalize.py ${PREV_RELNAME}-forBQ/aliqMap.merge.t1 ${PREV_RELNAME}norm-forBQ/aliqMap.merge.t1
    python scripts/multiNormalize.py ${PREV_RELNAME}-forBQ/slidMap.merge.t1 ${PREV_RELNAME}norm-forBQ/slidMap.merge.t1
    python scripts/multiNormalize.py ${PREV_RELNAME}-forBQ/fileData.current.t1 ${PREV_RELNAME}norm-forBQ/fileData.current.t1
    python scripts/multiNormalize.py ${PREV_RELNAME}-forBQ/fileData.legacy.t1 ${PREV_RELNAME}norm-forBQ/fileData.legacy.t1

    deactivate
fi

#
# Run the compare program to find the differences
#

if [ "${COMPARE_TO_LAST}" == "run" ]; then
    echo "Running COMPARE_TO_LAST"
    cd ${REL_ROOT}
    compare_to_last.sh ${REL_ROOT} ${RELNAME}norm ${PREV_RELNAME}norm
fi

#
# Take the above differences that were found and drill down to find out more:
#

if [ "${DETAILED_DIFFS}" == "run" ]; then
    echo "Running DETAILED_DIFFS"
    cd ${REL_ROOT}
    source ~/pyVenvForThree/bin/activate

    echo "##### DETAILED ALIQUOT CHANGES ######"
    python3 scripts/columnChanges.py changed_aliquot.txt ${REL_ROOT} scratch \
            ${PREV_RELNAME}norm-forBQ ${RELNAME}norm-forBQ aliqMap.merge.t1 \
            ${ALIQUOT_CHANGE_ID_FIELD} ${ALIQUOT_CHANGE_TSV_ID_FIELD} "silent"

    echo "##### DETAILED SLIDE CHANGES ######"
    python3 scripts/columnChanges.py changed_slide.txt ${REL_ROOT} scratch \
            ${PREV_RELNAME}norm-forBQ ${RELNAME}norm-forBQ slidMap.merge.t1 \
            ${SLIDE_CHANGE_ID_FIELD} ${SLIDE_CHANGE_TSV_ID_FIELD} "silent"

    echo "##### DETAILED CASE CHANGES ######"
    python3 scripts/columnChanges.py changed_caseData.txt ${REL_ROOT} scratch \
            ${PREV_RELNAME}norm-forBQ ${RELNAME}norm-forBQ caseData.merge.t1 \
            ${CASE_CHANGE_ID_FIELD} ${CASE_CHANGE_TSV_ID_FIELD} "silent"

    echo "##### DETAILED CURRENT FILE CHANGES ######"
    python3 scripts/columnChanges.py changed_currentFiles.txt ${REL_ROOT} scratch \
            ${PREV_RELNAME}norm-forBQ ${RELNAME}norm-forBQ fileData.current.t1 \
            ${CURR_FILE_CHANGE_ID_FIELD} ${CURR_FILE_CHANGE_TSV_ID_FIELD} "silent"

    echo "##### DETAILED LEGACY FILE CHANGES ######"
    python3 scripts/columnChanges.py changed_legacyFiles.txt ${REL_ROOT} scratch \
            ${PREV_RELNAME}norm-forBQ ${RELNAME}norm-forBQ fileData.legacy.t1 \
            ${LEG_FILE_CHANGE_ID_FIELD} ${LEG_FILE_CHANGE_TSV_ID_FIELD} "silent"
    deactivate
fi

#
# If the raw schemas are all good, we can swap in the prepared schemas, descriptions, and labels from our BQEcosystem repo:
#

if [ "${COPY_ANNOT_SCHEMA}" == "run" ]; then
    echo "Running COPY_ANNOT_SCHEMA"
    source ~/pyVenvForThree/bin/activate
    cd ~
    rm -rf ~/BQEcosystem
    git clone https://github.com/isb-cgc/BQEcosystem.git

    cd ${REL_ROOT}/${RELNAME}-forBQ
    SCH_DATE=`date +%d%b%Y | tr A-Z a-z`
    echo ${SCH_DATE} > current_schema_date.txt

    python ../scripts/generateTableDetails.py \
      ~/BQEcosystem/TableSchemas/isb-cgc-bq/GDC_case_file_metadata/aliquot2caseIDmap_${PUB_RELNAME}.json aliq_bqe
    HAVE_ERROR=$?
    if [ ${HAVE_ERROR} -ne 0 ]; then
        echo "ERROR: isb-cgc-bq.GDC_case_file_metadata.aliquot2caseIDmap_${PUB_RELNAME}.json NOT PROCESSED"
        exit
    fi
    mv aliq_bqe_schema.json aliqMap.${SCH_DATE}.json

    python ../scripts/generateTableDetails.py \
      ~/BQEcosystem/TableSchemas/isb-cgc-bq/GDC_case_file_metadata/caseData_${PUB_RELNAME}.json case_bqe
    HAVE_ERROR=$?
    if [ ${HAVE_ERROR} -ne 0 ]; then
        echo "ERROR: isb-cgc-bq.GDC_case_file_metadata.caseData_${PUB_RELNAME}.json NOT PROCESSED"
        exit
    fi

    mv case_bqe_schema.json caseData.${SCH_DATE}.json

    python ../scripts/generateTableDetails.py \
      ~/BQEcosystem/TableSchemas/isb-cgc-bq/GDC_case_file_metadata/fileData_active_${PUB_RELNAME}.json file_current_bqe
    HAVE_ERROR=$?
    if [ ${HAVE_ERROR} -ne 0 ]; then
        echo "ERROR: isb-cgc-bq.GDC_case_file_metadata.fileData_active_${PUB_RELNAME}.json NOT PROCESSED"
        exit
    fi

    mv file_current_bqe_schema.json fileData.current.${SCH_DATE}.json

    python ../scripts/generateTableDetails.py \
      ~/BQEcosystem/TableSchemas/isb-cgc-bq/GDC_case_file_metadata/fileData_legacy_${PUB_RELNAME}.json file_legacy_bqe
    HAVE_ERROR=$?
    if [ ${HAVE_ERROR} -ne 0 ]; then
        echo "ERROR: isb-cgc-bq.GDC_case_file_metadata.fileData_legacy_${PUB_RELNAME}.json NOT PROCESSED"
        exit
    fi

    mv file_legacy_bqe_schema.json fileData.legacy.${SCH_DATE}.json

    python ../scripts/generateTableDetails.py \
       ~/BQEcosystem/TableSchemas/isb-cgc-bq/GDC_case_file_metadata/slide2caseIDmap_${PUB_RELNAME}.json slide_bqe
    HAVE_ERROR=$?
    if [ ${HAVE_ERROR} -ne 0 ]; then
        echo "ERROR: isb-cgc-bq.GDC_case_file_metadata.slide2caseIDmap_${PUB_RELNAME}.json NOT PROCESSED"
        exit
    fi

    mv slide_bqe_schema.json slidMap.${SCH_DATE}.json

    deactivate
fi

#
# Load up to BQ:
#

if [ "${LOAD_BQ}" == "run" ]; then
    echo "Running LOAD_BQ"
    cd ${REL_ROOT}/${RELNAME}-forBQ
    SCHEMA_DATE=`cat current_schema_date.txt`
    just_loadBQ.sh ${SCHEMA_DATE} ${RELNAME} ${BUCK_TARGET} ${PROJ_AND_DATASET}
fi

#
# Install table descriptions and labels extracted from the BQEcosystem repo:
#

if [ "${DESC_AND_LABELS}" == "run" ]; then
    echo "Running DESC_AND_LABELS"
    source ~/pyVenvForThree/bin/activate
    cd ${REL_ROOT}/${RELNAME}-forBQ
    python3 ../scripts/install_desc_and_labels.py ${WORKING_PROJECT} ${DATASET} ${RELNAME}_slide2caseIDmap slide_bqe
    python3 ../scripts/install_desc_and_labels.py ${WORKING_PROJECT} ${DATASET} ${RELNAME}_aliquot2caseIDmap aliq_bqe
    python3 ../scripts/install_desc_and_labels.py ${WORKING_PROJECT} ${DATASET} ${RELNAME}_caseData case_bqe
    python3 ../scripts/install_desc_and_labels.py ${WORKING_PROJECT} ${DATASET} ${RELNAME}_fileData_legacy file_legacy_bqe
    python3 ../scripts/install_desc_and_labels.py ${WORKING_PROJECT} ${DATASET} ${RELNAME}_fileData_current file_current_bqe
    deactivate
fi


#
# Publish the tables:
#

if [ "${PUBLISH_TABLES}" == "run" ]; then
    echo "Running PUBLISH_TABLES"
    source ~/pyVenvForThree/bin/activate
    cd ${REL_ROOT}/${RELNAME}-forBQ
    python3 ../scripts/publish_table.py ${WORKING_PROJECT}.${DATASET}.${RELNAME}_slide2caseIDmap \
                                        ${PUB_PROJECT}.${PUB_DATASET}_versioned.slide2caseIDmap_${PUB_RELNAME}
    python3 ../scripts/publish_table.py ${WORKING_PROJECT}.${DATASET}.${RELNAME}_aliquot2caseIDmap \
                                        ${PUB_PROJECT}.${PUB_DATASET}_versioned.aliquot2caseIDmap_${PUB_RELNAME}
    python3 ../scripts/publish_table.py ${WORKING_PROJECT}.${DATASET}.${RELNAME}_caseData \
                                        ${PUB_PROJECT}.${PUB_DATASET}_versioned.caseData_${PUB_RELNAME}
    python3 ../scripts/publish_table.py ${WORKING_PROJECT}.${DATASET}.${RELNAME}_fileData_legacy \
                                        ${PUB_PROJECT}.${PUB_DATASET}_versioned.fileData_legacy_${PUB_RELNAME}
    # Note what we call "current" is now being called "active" in the published table:
    python3 ../scripts/publish_table.py ${WORKING_PROJECT}.${DATASET}.${RELNAME}_fileData_current \
                                        ${PUB_PROJECT}.${PUB_DATASET}_versioned.fileData_active_${PUB_RELNAME}
    deactivate
fi

if [ "${PUBLISH_TO_CURRENT}" == "run" ]; then
   echo "Running PUBLISH_TO_CURRENT"
   source ~/pyVenvForThree/bin/activate
   cd ${REL_ROOT}/${RELNAME}-forBQ
   python3 ../scripts/publish_table.py ${WORKING_PROJECT}.${DATASET}.${RELNAME}_slide2caseIDmap \
	   			       ${PUB_PROJECT}.${PUB_DATASET}.slide2caseIDmap_current
   python3 ../scripts/publish_table.py ${WORKING_PROJECT}.${DATASET}.${RELNAME}_aliquot2caseIDmap \
   				       ${PUB_PROJECT}.${PUB_DATASET}.aliquot2caseIDmap_current
   python3 ../scripts/publish_table.py ${WORKING_PROJECT}.${DATASET}.${RELNAME}_caseData \
	                               ${PUB_PROJECT}.${PUB_DATASET}.caseData_current
   python3 ../scripts/publish_table.py ${WORKING_PROJECT}.${DATASET}.${RELNAME}_fileData_legacy \
   				       ${PUB_PROJECT}.${PUB_DATASET}.fileData_legacy_current
   python3 ../scripts/publish_table.py ${WORKING_PROJECT}.${DATASET}.${RELNAME}_fileData_current \
	                               ${PUB_PROJECT}.${PUB_DATASET}.fileData_active_current
   deactivate
fi

#
# Get all the data from the run archived as a compressed tar file up in a google bucket:
#

if [ "${ARCHIVE_TARS}" == "run" ]; then
    echo "Running ARCHIVE_TARS"
    cd ${REL_ROOT}
    tar cvzf tar-archive-${RELNAME}.tgz ${RELNAME}-current ${RELNAME}-legacy ${RELNAME}-forBQ
    gsutil cp tar-archive-${RELNAME}.tgz ${TAR_TARGET}
fi

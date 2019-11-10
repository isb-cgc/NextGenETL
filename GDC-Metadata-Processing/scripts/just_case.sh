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

#
# hex codes for the current and legacy case data files:
#

r1=$1
r2=$2

RELNAME=$3
SCRATCHDIR=$4

#
# Formerly, cut lists were hand-curated. Now they are calculated and provided as arguments:
#

CURRENT_CUT=$5
LEGACY_CUT=$6

#
# Same with count columns:
#

CURRENT_COUNT_COL=$7
LEGACY_COUNT_COL=$8

echo " "
echo " ************************************* "
echo " looking at the CASE DATA tables ... "
echo " "

f1=../${RELNAME}-current/caseData.bq.$r1.tsv
f2=../${RELNAME}-legacy/caseData.bq.$r2.tsv

#
# Output line counts for the two caseData files:
#

echo $f1
wc -l $f1
echo $f2
wc -l $f2

# f1 cut into f3 (current)
# f2 cut into f4 (legacy)

f3=caseData.merge.t0
f4=caseData.merge.t1
f5=caseData.merge.t2

rm -fr $f3 $f4 $f5

# WJRL 11/10/19: This former legacy manual curation of fields is now being done automatically:
#
# 04/05/2019
# all we want to keep are: case_id, project_dbgap_accession, project_disease_type, project_name,
# program_dbgap_accession, program_name, project_id, case_barcode
#
# and the file_count ...
#
#  so for $f1 that is: 2, 5, 6, 7, 8, 9, 10, 29, and 31
# and for $f2 that is: 2, 5, 6, 7, 8, 9, 10, 27, and 29
#
# 04/22/2019
# the 'active' caseData.bq.*.tsv file has 31 columns
# the 'legacy' caseData.bq.*.tsv file has 30 columns -- the one that is missing is samples__preservation_method
#
# we really only want to *keep* case-level (or higher) information, so that is just:
#       case_id and case_barcode
#       project_dbgap_accession, project_disease_type, project_name, and project_id
#       program_dbgap_accession and program_name
# and   the file_count (# of data files that exist for this case)
#
#-bash-4.2$ head -n 1 caseData.bq.2a88c9c6.tsv | tr '\t' '\n' | cat -n
#     1	dbName
#     2	case_id **
#     3	files__file_id
#     4	id
#     5	project__dbgap_accession_number **
#     6	project__disease_type **
#     7	project__name **
#     8	project__program__dbgap_accession_number **
#     9	project__program__name **
#    10	project__project_id **
#    11	sample_ids
#    12	samples__is_ffpe
#    13	samples__pathology_report_uuid
#    14	samples__portions__analytes__aliquots__aliquot_id
#    15	samples__portions__analytes__aliquots__submitter_id
#    16	samples__portions__analytes__analyte_id
#    17	samples__portions__analytes__submitter_id
#    18	samples__portions__portion_id
#    19	samples__portions__slides__slide_id
#    20	samples__portions__slides__submitter_id
#    21	samples__portions__submitter_id
#    22	samples__preservation_method
#    23	samples__sample_id
#    24	samples__sample_type
#    25	samples__sample_type_id
#    26	samples__submitter_id
#    27	samples__tumor_code
#    28	state
#    29	submitter_id **
#    30	submitter_sample_ids **
#    31	summary__file_count
#
#
# which, results in these columns being 'cut' from the original caseData.bq.*.tsv files:
#   for $f1 (active): 2, 5, 6, 7, 8, 9, 10, 29, and 31
#   for $f2 (legacy): 2, 5, 6, 7, 8, 9, 10, 28, and 30
#
# Remember, from above:
#f3=caseData.merge.t0
#f4=caseData.merge.t1
#f5=caseData.merge.t2

#sort $f1 | grep -v 'dbName' | cut -f2,5,6,7,8,9,10,29 >& $f3
#sort $f2 | grep -v 'dbName' | cut -f2,5,6,7,8,9,10,28 >& $f4

sort $f1 | grep -v 'dbName' | cut -f${CURRENT_CUT} >& $f3
sort $f2 | grep -v 'dbName' | cut -f${LEGACY_CUT} >& $f4


wc -l $f3
wc -l $f4


#
# get the set of unique current and legacy case IDs. Not sure why file_id is being omitted; should not be there anyway?
#

rm -fr *.xxx
cut -f1 $f3 | sort | uniq | grep -v "file_id" >& active.ids.xxx
cut -f1 $f4 | sort | uniq | grep -v "file_id" >& legacy.ids.xxx

#
# Create the list of legacy case IDs that are in active collection. Then create the list of case IDs that are only in legacy
#
for u in `cat legacy.ids.xxx`
    do
        grep $u active.ids.xxx >> legacy_in_active.xxx
    done
diff legacy_in_active.xxx legacy.ids.xxx | grep "^>" | cut -c3- | sort > only_in_legacy.xxx

#
# get counts of active, legacy, intersection, and only in legacy case IDs:
#

wc -l *.xxx
i

# Create a table of data rows for data only in legacy (going into $f5 caseData.merge.t2) and output count
for u in `cat only_in_legacy.xxx`
    do
        grep $u $f4 >> $f5
    done
echo " after checking only_in_legacy ... "
wc -l $f5

# Then glue in all the active cases to (also going into $f5), then output count:
for u in `cat active.ids.xxx`
    do
        grep $u $f3 >> $f5
    done

echo " after adding in active ids ... "
echo " number of cases in f5 file "
wc -l $f5

#
# This loop is building the *legacy* file count. It takes the last field (summary file count) out of the original legacy table.
#

# Remember f2 is legacy:
rm -fr filecountcol.leg.tmp
touch filecountcol.leg.tmp
for a in `cut -f1 $f5`
    do
        # n=`grep $a $f2 | cut -f30`  Now an argument
        n=`grep $a $f2 | cut -f${LEGACY_COUNT_COL}`
        ## echo $n
        if [ -z "$n" ]
            then
                ## echo " is blank ... putting in a 0 "
                echo '0' >> filecountcol.leg.tmp
            else
                ## echo " is not blank "
                echo $n >> filecountcol.leg.tmp
            fi
    done
echo filecountcol.leg.tmp
wc -l filecountcol.leg.tmp

# Remember f1 is current:
rm -fr filecountcol.act.tmp
touch filecountcol.act.tmp
for a in `cut -f1 $f5`
    do
        # n=`grep $a $f1 | cut -f31` Now an argument
        n=`grep $a $f1 | cut -f${CURRENT_COUNT_COL}`
        ## echo $n
        if [ -z "$n" ]
            then
                ## echo " is blank ... putting in a 0 "
                echo '0' >> filecountcol.act.tmp
            else
                ## echo " is not blank "
                echo $n >> filecountcol.act.tmp
            fi
    done
echo filecountcol.act.tmp
wc -l filecountcol.act.tmp

rm -fr aa bb cc

paste $f5 filecountcol.leg.tmp filecountcol.act.tmp >& aa
wc -l aa
../scripts/good_look.sh aa ${SCRATCHDIR}

###### rm -fr $f3 $f4 $f5

## start the 'bb' file with the header row ...
cp ../textFiles/just_case.header.KEEP bb

cat aa >> bb
../scripts/good_look.sh bb ${SCRATCHDIR}

cp bb caseData.merge.t1


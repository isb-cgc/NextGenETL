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
# WJRL 11/10/19
#
# Originally, the just-case.sh and proc_release tables.sh scripts used hand-curated cut lists
# to throw away unused columns. This script is intended to automate this curation step.
#

# Text pulled from just_case.sh (somewhat dated):
#
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
# WJRL 11/10/19: The above is obsolete. Expected list is provided as an argument, and we will
# exit with an error code if things change again:
#

FILE_IN=$1
USE_EXPECTED=$2
KEEP_COLS=$3

#
# Step 1: transpose the header to get column numbers:
#

TRANSPOSE_FILE=${FILE_IN}.h1.t

head -n 1 ${FILE_IN} | tr '\t' '\n' | cat -n > ${TRANSPOSE_FILE}

#
# These are the columns to keep with current:
#

CUT_LIST=""
for COLNAME in ${KEEP_COLS}; do
    CUT_COL=`cat ${TRANSPOSE_FILE} | grep $'\t'"${COLNAME}"'$' | cut -f1 | sed 's/ //g'`
    if [ -z "${CUT_COL}" ]; then
        echo "ERROR: ${CUT_COL} not found"
        exit 1
    fi
    if [ -z ${CUT_LIST} ]; then
        CUT_LIST="${CUT_COL}"
    else
        CUT_LIST="${CUT_LIST},${CUT_COL}"
    fi
done

# If you encounter an error here due to the GDC adding a new column you may
# temporarily comment out "exit 1" to permit compare_to_last.sh to run
# after fully investigating the changes caused by the addition
if [ "${CUT_LIST}" != "${USE_EXPECTED}" ]; then
    echo "WARNING cut list has changed. Formerly ${USE_EXPECTED} and now ${CUT_LIST}"
    exit 1
fi

echo ${CUT_LIST}


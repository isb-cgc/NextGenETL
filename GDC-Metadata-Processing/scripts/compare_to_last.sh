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

PARENT_DIR=$1
CURR_RELNAME=$2
LAST_RELNAME=$3

CURR_PATH=${PARENT_DIR}/${CURR_RELNAME}-forBQ
LAST_PATH=${PARENT_DIR}/${LAST_RELNAME}-forBQ
SCRATCH_PATH=${PARENT_DIR}/scratch
export PATH=${PARENT_DIR}/scripts/:${PATH}

compare () {

    local ITEM=$1
    local TABLE=$2
    local CUT_COLS=$3
    local EXPECTED_CUTS=$4
    local CUT_ARG_2=$5
    local HEADER_STRING=$6
    local TITLE=`echo "${ITEM}" | tr "[a-z]" "[A-Z]"`

    echo " "
    echo "*********************************************************** "
    echo ${TITLE}
    echo " "

    CUT_ARG=`generate_cut_list.sh ${CURR_PATH}/${TABLE}.t1 "${EXPECTED_CUTS}" "${CUT_COLS}"`
    HAVE_ERROR=$?
    if [ ${HAVE_ERROR} -ne 0 ]; then
        echo "WARNING: Retained current columns have changed; perform manual curation!" ${CUT_ARG}
        exit
    fi

    local CURR_COUNT=`cat ${CURR_PATH}/${TABLE}.t1 | wc -l`
    local LAST_COUNT=`cat ${LAST_PATH}/${TABLE}.t1 | wc -l`
    local DIFF_COUNT=$((CURR_COUNT - LAST_COUNT))

    echo "Current" ${ITEM} "count:" ${CURR_COUNT}
    echo "Previous" ${ITEM} "count:" ${LAST_COUNT}
    echo "Difference:" ${DIFF_COUNT}
    echo " "

    #
    # How many items have been removed or added since the last release?
    #

    # Step 1: Create sorted reduced lists that just contain minimal identifying info:

    cat ${CURR_PATH}/${TABLE}.t1 | cut -f ${CUT_ARG} | grep -v "${HEADER_STRING}" | sort > ${SCRATCH_PATH}/new_${ITEM}.txt
    cat ${LAST_PATH}/${TABLE}.t1 | cut -f ${CUT_ARG} | grep -v "${HEADER_STRING}" | sort > ${SCRATCH_PATH}/old_${ITEM}.txt

    # Step 2: Use diff with suppression of one of the two output lines to determine who is new and old:

    diff --new-line-format="" --unchanged-line-format="" ${SCRATCH_PATH}/old_${ITEM}.txt ${SCRATCH_PATH}/new_${ITEM}.txt > ${SCRATCH_PATH}/removed_${ITEM}.txt
    diff --old-line-format="" --unchanged-line-format="" ${SCRATCH_PATH}/old_${ITEM}.txt ${SCRATCH_PATH}/new_${ITEM}.txt > ${SCRATCH_PATH}/added_${ITEM}.txt

    # And report the counts:

    local REMOVED_COUNT=`cat ${SCRATCH_PATH}/removed_${ITEM}.txt | wc -l`
    local ADDED_COUNT=`cat ${SCRATCH_PATH}/added_${ITEM}.txt | wc -l`

    echo "Removed" ${ITEM} "count:" ${REMOVED_COUNT}

    cat ${SCRATCH_PATH}/removed_${ITEM}.txt | cut -f ${CUT_ARG_2} | sort | uniq -c

    echo " "
    echo "Added" ${ITEM} "count:" ${ADDED_COUNT}

    cat ${SCRATCH_PATH}/added_${ITEM}.txt | cut -f ${CUT_ARG_2} | sort | uniq -c

    # Create the count of changed files. First command creates list of lines that appear only once
    # in old and new, i.e. drops lines that are identical across both.

    cat ${CURR_PATH}/${TABLE}.t1 ${LAST_PATH}/${TABLE}.t1 | sort | uniq -c | grep "^ *1 " > ${SCRATCH_PATH}/comp_${ITEM}.txt

    # Next step tosses away all but the identifying info, matching schema of the added and removed files.

    cat ${SCRATCH_PATH}/comp_${ITEM}.txt | sed 's/^ *1 //' | cut -f ${CUT_ARG} | sort | uniq > ${SCRATCH_PATH}/diff_${ITEM}.txt

    # Third step glues uniques with the added and removed, and finds those that appear only once, e.g. tosses
    # away all unique lines that have been in both old and new. Thus, the changed lines.

    cat ${SCRATCH_PATH}/diff_${ITEM}.txt ${SCRATCH_PATH}/removed_${ITEM}.txt ${SCRATCH_PATH}/added_${ITEM}.txt | sort | uniq -c | grep "^ *1 " > ${SCRATCH_PATH}/changed_${ITEM}.txt

    local CHANGED_COUNT=`cat ${SCRATCH_PATH}/changed_${ITEM}.txt | wc -l`

    echo " "
    echo "Changed" ${ITEM} "count:" ${CHANGED_COUNT}

    cat ${SCRATCH_PATH}/changed_${ITEM}.txt | cut -f ${CUT_ARG_2} | sort | uniq -c
}

# Note the last argument is any column header that will not appear in body (grep -v arg)
# The text headers are used to determine the cuts, and the number lists just to confirm no changes from the past
# The second list of numbers are the columns to keep (e.g. project, sample type) for counting how many times
# they occur (by deleting the unique ID field)

compare "aliquot" "aliqMap.merge" "project_id sample_type aliquot_gdc_id" "2,8,15" "1,2" "portion_barcode"
compare "slide" "slidMap.merge" "project_id sample_type slide_gdc_id" "2,8,11" "1,2" "portion_barcode"
compare "caseData" "caseData.merge" "case_id project__project_id" "1,8" "2" "project__disease_type"
compare "currentFiles" "fileData.current" "file_id cases__project__project_id data_format" "2,22,25" "2,3" "file_id"
compare "legacyFiles" "fileData.legacy" "file_id cases__project__project_id data_format" "2,19,26" "2,3" "file_id"

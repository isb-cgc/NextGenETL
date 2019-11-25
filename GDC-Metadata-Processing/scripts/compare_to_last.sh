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


echo " "
echo "*********************************************************** "
echo "ALIQUOT MAP"
echo " "

CURR_ALIQ_COUNT=`cat ${CURR_PATH}/aliqMap.merge.t1 | wc -l`
LAST_ALIQ_COUNT=`cat ${LAST_PATH}/aliqMap.merge.t1 | wc -l`
DIFF_ALIQ_COUNT=$((CURR_ALIQ_COUNT - LAST_ALIQ_COUNT))

echo "Current aliquot count:  " ${CURR_ALIQ_COUNT}
echo "Previous aliquot count: " ${LAST_ALIQ_COUNT}
echo "Difference:             " ${DIFF_ALIQ_COUNT}
echo " "

#
# How many aliquots have been removed or added since the last release?
#

cat ${CURR_PATH}/aliqMap.merge.t1 | awk -F '\t' '{print $15 " " $2 " " $8}' | grep -v "file_id" | sort > ${SCRATCH_PATH}/newAliq.txt
cat ${LAST_PATH}/aliqMap.merge.t1 | awk -F '\t' '{print  $15 " " $2 " " $8}' | grep -v "file_id" | sort > ${SCRATCH_PATH}/oldAliq.txt

diff --new-line-format="" --unchanged-line-format="" ${SCRATCH_PATH}/oldAliq.txt ${SCRATCH_PATH}/newAliq.txt > ${SCRATCH_PATH}/removedAliq.txt
diff --old-line-format="" --unchanged-line-format="" ${SCRATCH_PATH}/oldAliq.txt ${SCRATCH_PATH}/newAliq.txt > ${SCRATCH_PATH}/addedAliq.txt

REMOVED_ALIQ_COUNT=`cat ${SCRATCH_PATH}/removedAliq.txt | wc -l`
ADDED_ALIQ_COUNT=`cat ${SCRATCH_PATH}/addedAliq.txt | wc -l`

echo "Removed aliquot count:" ${REMOVED_ALIQ_COUNT}

cat ${SCRATCH_PATH}/removedAliq.txt | awk '{print $2 " " $3}' | sort | uniq -c

echo " "
echo "Added aliquot count:  " ${ADDED_ALIQ_COUNT}

cat ${SCRATCH_PATH}/addedAliq.txt | awk '{print $2 " " $3}' | sort | uniq -c

cat ${CURR_PATH}/aliqMap.merge.t1 ${LAST_PATH}/aliqMap.merge.t1 | sort | uniq -c | grep "^ *1 " > ${SCRATCH_PATH}/aliqComp.txt
cat ${SCRATCH_PATH}/aliqComp.txt | sed 's/^ *1 //' | awk -F '\t' '{print $15 " " $2 " " $8}' | sort | uniq > ${SCRATCH_PATH}/diffAliq.txt
cat ${SCRATCH_PATH}/diffAliq.txt ${SCRATCH_PATH}/removedAliq.txt ${SCRATCH_PATH}/addedAliq.txt | sort | uniq -c | grep "^ *1 " > ${SCRATCH_PATH}/changedAliq.txt

CHANGED_ALIQ_COUNT=`cat ${SCRATCH_PATH}/changedAliq.txt | wc -l`

echo " "
echo "Changed aliquot count:  " ${CHANGED_ALIQ_COUNT}

cat ${SCRATCH_PATH}/changedAliq.txt | awk '{print $3 " " $4}' | sort | uniq -c

echo " "
echo "*********************************************************** "
echo "SLIDE MAP"
echo " "

CURR_SLID_COUNT=`cat ${CURR_PATH}/slidMap.merge.t1 | wc -l`
LAST_SLID_COUNT=`cat ${LAST_PATH}/slidMap.merge.t1 | wc -l`
DIFF_SLID_COUNT=$((CURR_SLID_COUNT - LAST_SLID_COUNT))

echo "Current slide count:  " ${CURR_SLID_COUNT}
echo "Previous slide count: " ${LAST_SLID_COUNT}
echo "Difference:           " ${DIFF_SLID_COUNT}
echo " "

cat ${CURR_PATH}/slidMap.merge.t1 | awk -F '\t' '{print $11 " " $2 " " $8}' | grep -v "file_id" | sort > ${SCRATCH_PATH}/newSlide.txt
cat ${LAST_PATH}/slidMap.merge.t1 | awk -F '\t' '{print  $11 " " $2 " " $8}' | grep -v "file_id" | sort > ${SCRATCH_PATH}/oldSlide.txt

diff --new-line-format="" --unchanged-line-format="" ${SCRATCH_PATH}/oldSlide.txt ${SCRATCH_PATH}/newSlide.txt > ${SCRATCH_PATH}/removedSlide.txt
diff --old-line-format="" --unchanged-line-format="" ${SCRATCH_PATH}/oldSlide.txt ${SCRATCH_PATH}/newSlide.txt > ${SCRATCH_PATH}/addedSlide.txt

REMOVED_SLIDE_COUNT=`cat ${SCRATCH_PATH}/removedSlide.txt | wc -l`
ADDED_SLIDE_COUNT=`cat ${SCRATCH_PATH}/addedSlide.txt | wc -l`

echo "Removed slide count:" ${REMOVED_SLIDE_COUNT}

cat ${SCRATCH_PATH}/removedSlide.txt | awk '{print $2 " " $3}' | sort | uniq -c

echo " "
echo "Added slide count:  " ${ADDED_SLIDE_COUNT}

cat ${SCRATCH_PATH}/addedSlide.txt | awk '{print $2 " " $3}' | sort | uniq -c

cat ${CURR_PATH}/slidMap.merge.t1 ${LAST_PATH}/slidMap.merge.t1 | sort | uniq -c | grep "^ *1 " > ${SCRATCH_PATH}/slideComp.txt
cat ${SCRATCH_PATH}/slideComp.txt | sed 's/^ *1 //' | awk -F '\t' '{print $11 " " $2 " " $8}' | sort | uniq > ${SCRATCH_PATH}/diffSlide.txt
cat ${SCRATCH_PATH}/diffSlide.txt ${SCRATCH_PATH}/removedSlide.txt ${SCRATCH_PATH}/addedSlide.txt | sort | uniq -c | grep "^ *1 " > ${SCRATCH_PATH}/changedSlide.txt

CHANGED_SLIDE_COUNT=`cat ${SCRATCH_PATH}/changedSlide.txt | wc -l`

echo " "
echo "Changed slide count:  " ${CHANGED_SLIDE_COUNT}

cat ${SCRATCH_PATH}/changedSlide.txt | awk '{print $3 " " $4}' | sort | uniq -c

echo " "
echo "*********************************************************** "
echo "CASE DATA"

CURR_CASE_COUNT=`cat ${CURR_PATH}/caseData.merge.t1 | wc -l`
LAST_CASE_COUNT=`cat ${LAST_PATH}/caseData.merge.t1 | wc -l`
DIFF_CASE_COUNT=$((CURR_CASE_COUNT - LAST_CASE_COUNT))

echo "Current case count:  " ${CURR_CASE_COUNT}
echo "Previous case count: " ${LAST_CASE_COUNT}
echo "Difference:          " ${DIFF_CASE_COUNT}
echo " "

#
# How many aliquots have been removed or added since the last release?
#

cat ${CURR_PATH}/caseData.merge.t1 | awk -F '\t' '{print $1 " " $7}' | grep -v "file_id" | sort > ${SCRATCH_PATH}/newCase.txt
cat ${LAST_PATH}/caseData.merge.t1 | awk -F '\t' '{print  $1 " " $7}' | grep -v "file_id" | sort > ${SCRATCH_PATH}/oldCase.txt

diff --new-line-format="" --unchanged-line-format="" ${SCRATCH_PATH}/oldCase.txt ${SCRATCH_PATH}/newCase.txt > ${SCRATCH_PATH}/removedCase.txt
diff --old-line-format="" --unchanged-line-format="" ${SCRATCH_PATH}/oldCase.txt ${SCRATCH_PATH}/newCase.txt > ${SCRATCH_PATH}/addedCase.txt

REMOVED_CASE_COUNT=`cat ${SCRATCH_PATH}/removedCase.txt | wc -l`
ADDED_CASE_COUNT=`cat ${SCRATCH_PATH}/addedCase.txt | wc -l`

echo "Removed case count:" ${REMOVED_CASE_COUNT}

cat ${SCRATCH_PATH}/removedCase.txt | awk '{print $2}' | sort | uniq -c

echo " "
echo "Added case count:  " ${ADDED_CASE_COUNT}

cat ${SCRATCH_PATH}/addedCase.txt | awk '{print $2}' | sort | uniq -c

cat ${CURR_PATH}/caseData.merge.t1 ${LAST_PATH}/caseData.merge.t1 | sort | uniq -c | grep "^ *1 " > ${SCRATCH_PATH}/caseComp.txt
cat ${SCRATCH_PATH}/caseComp.txt | sed 's/^ *1 //' | awk -F '\t' '{print $1 " " $7}' | sort | uniq > ${SCRATCH_PATH}/diffCase.txt
cat ${SCRATCH_PATH}/diffCase.txt ${SCRATCH_PATH}/removedCase.txt ${SCRATCH_PATH}/addedCase.txt | sort | uniq -c | grep "^ *1 " > ${SCRATCH_PATH}/changedCase.txt

CHANGED_CASE_COUNT=`cat ${SCRATCH_PATH}/changedCase.txt | wc -l`

echo " "
echo "Changed case count:  " ${CHANGED_CASE_COUNT}

cat ${SCRATCH_PATH}/changedCase.txt | awk '{print $3}' | sort | uniq -c

echo " "
echo "*********************************************************** "
echo "CURRENT FILE DATA"
echo " "

#
# With current file data, I am seeing:
#
# Some files are removed
# Some files are added
# Some files with the same ID are "updated". They have a different "updated_datetime", and the ordering of multiple
# files in ";" delimited lists are different!
#

CURR_CURR_FILE_COUNT=`cat ${CURR_PATH}/fileData.current.t1| wc -l`
LAST_CURR_FILE_COUNT=`cat ${LAST_PATH}/fileData.current.t1 | wc -l`
DIFF_CURR_FILE_COUNT=$((CURR_CURR_FILE_COUNT - LAST_CURR_FILE_COUNT))

echo "Current active file count:  " ${CURR_CURR_FILE_COUNT}
echo "Previous active file count: " ${LAST_CURR_FILE_COUNT}
echo "Difference:                 " ${DIFF_CURR_FILE_COUNT}
echo " "

#
# How many files have been removed or added since the last release?
#

cat ${CURR_PATH}/fileData.current.t1 | awk -F '\t' '{print $2 " " $22 " " $25}' | grep -v "file_id" | sort > ${SCRATCH_PATH}/newCurrentFiles.txt
cat ${LAST_PATH}/fileData.current.t1 | awk -F '\t' '{print $2 " " $22 " " $25}' | grep -v "file_id" | sort > ${SCRATCH_PATH}/oldCurrentFiles.txt

diff --new-line-format="" --unchanged-line-format="" ${SCRATCH_PATH}/oldCurrentFiles.txt ${SCRATCH_PATH}/newCurrentFiles.txt > ${SCRATCH_PATH}/removedCurrFiles.txt
diff --old-line-format="" --unchanged-line-format="" ${SCRATCH_PATH}/oldCurrentFiles.txt ${SCRATCH_PATH}/newCurrentFiles.txt > ${SCRATCH_PATH}/addedCurrFiles.txt

REMOVED_FILE_COUNT=`cat ${SCRATCH_PATH}/removedCurrFiles.txt | wc -l`
ADDED_FILE_COUNT=`cat ${SCRATCH_PATH}/addedCurrFiles.txt | wc -l`

echo "Removed current file count:" ${REMOVED_FILE_COUNT}

cat ${SCRATCH_PATH}/removedCurrFiles.txt | awk '{print $2 " " $3}' | sort | uniq -c

echo " "
echo "Added current file count:  " ${ADDED_FILE_COUNT}

cat ${SCRATCH_PATH}/addedCurrFiles.txt | awk '{print $2 " " $3}' | sort | uniq -c

cat ${CURR_PATH}/fileData.current.t1 ${LAST_PATH}/fileData.current.t1 | sort | uniq -c | grep "^ *1 " > ${SCRATCH_PATH}/currFileComp.txt
cat ${SCRATCH_PATH}/currFileComp.txt | sed 's/^ *1 //' | awk -F '\t' '{print $2 " " $22 " " $25}' | sort | uniq > ${SCRATCH_PATH}/diffCurrFiles.txt
cat ${SCRATCH_PATH}/diffCurrFiles.txt ${SCRATCH_PATH}/removedCurrFiles.txt ${SCRATCH_PATH}/addedCurrFiles.txt | sort | uniq -c | grep "^ *1 " > ${SCRATCH_PATH}/changedCurrFiles.txt

CHANGED_FILE_COUNT=`cat ${SCRATCH_PATH}/changedCurrFiles.txt | wc -l`

echo " "
echo "Changed current file count: " ${CHANGED_FILE_COUNT}

cat ${SCRATCH_PATH}/changedCurrFiles.txt | awk '{print $3 " " $4}' | sort | uniq -c


echo " "
echo "*********************************************************** "
echo "LEGACY FILE DATA"
echo " "

CURR_LEG_FILE_COUNT=`cat ${CURR_PATH}/fileData.legacy.t1| wc -l`
LAST_LEG_FILE_COUNT=`cat ${LAST_PATH}/fileData.legacy.t1 | wc -l`
DIFF_LEG_FILE_COUNT=$((CURR_LEG_FILE_COUNT - LAST_LEG_FILE_COUNT))

echo "Current legacy file count:  " ${CURR_LEG_FILE_COUNT}
echo "Previous legacy file count: " ${LAST_LEG_FILE_COUNT}
echo "Difference:                 " ${DIFF_LEG_FILE_COUNT}
echo " "

#
# How many files have been removed or added since the last release?
#

cat ${CURR_PATH}/fileData.legacy.t1 | awk -F '\t' '{print $2 " " $22 " " $25}' | grep -v "file_id" | sort > ${SCRATCH_PATH}/newLegFiles.txt
cat ${LAST_PATH}/fileData.legacy.t1 | awk -F '\t' '{print $2 " " $22 " " $25}' | grep -v "file_id" | sort > ${SCRATCH_PATH}/oldLegFiles.txt

diff --new-line-format="" --unchanged-line-format="" ${SCRATCH_PATH}/oldLegFiles.txt ${SCRATCH_PATH}/newLegFiles.txt > ${SCRATCH_PATH}/removedLegFiles.txt
diff --old-line-format="" --unchanged-line-format="" ${SCRATCH_PATH}/oldLegFiles.txt ${SCRATCH_PATH}/newLegFiles.txt > ${SCRATCH_PATH}/addedLegFiles.txt

REMOVED_LEG_FILE_COUNT=`cat ${SCRATCH_PATH}/removedLegFiles.txt | wc -l`
ADDED_LEG_FILE_COUNT=`cat ${SCRATCH_PATH}/addedLegFiles.txt | wc -l`

echo "Removed legacy file count:" ${REMOVED_LEG_FILE_COUNT}

cat ${SCRATCH_PATH}/removedLegFiles.txt | awk '{print $2 " " $3}' | sort | uniq -c

echo " "
echo "Added legacy file count:  " ${ADDED_LEG_FILE_COUNT}

cat ${SCRATCH_PATH}/addedLegFiles.txt | awk '{print $2 " " $3}' | sort | uniq -c

cat ${CURR_PATH}/fileData.legacy.t1 ${LAST_PATH}/fileData.legacy.t1 | sort | uniq -c | grep "^ *1 " > ${SCRATCH_PATH}/legFileComp.txt
cat ${SCRATCH_PATH}/legFileComp.txt | sed 's/^ *1 //' | awk -F '\t' '{print $2 " " $22 " " $25}' | sort | uniq > ${SCRATCH_PATH}/diffLegFiles.txt
cat ${SCRATCH_PATH}/diffLegFiles.txt ${SCRATCH_PATH}/removedLegFiles.txt ${SCRATCH_PATH}/addedLegFiles.txt | sort | uniq -c | grep "^ *1 " > ${SCRATCH_PATH}/changedLegFiles.txt

CHANGED_LEG_FILE_COUNT=`cat ${SCRATCH_PATH}/changedLegFiles.txt | wc -l`

echo " "
echo "Changed legacy file count:" ${CHANGED_LEG_FILE_COUNT}

cat ${SCRATCH_PATH}/changedLegFiles.txt | awk '{print $3 " " $4}' | sort | uniq -c

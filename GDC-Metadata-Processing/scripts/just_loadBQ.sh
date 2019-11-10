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

SCHEMA_DATE=$1
RELNAME=$2
BUCK_TARGET=$3
DATASET=$4

echo " "
echo " *********************************************************** "
echo " slide2caseIDmap ... "

f=slidMap.merge.t1
j=slidMap.${SCHEMA_DATE}.json
gsutil cp $f ${BUCK_TARGET}
tName=${RELNAME}_slide2caseIDmap
bq -q load --source_format=CSV --field_delimiter='\t' --skip_leading_rows=1 \
    --replace ${DATASET}.$tName ${BUCK_TARGET}/$f $j

echo " "
echo " *********************************************************** "
echo " aliquot2caseIDmap ... "

f=aliqMap.merge.t1
j=aliqMap.${SCHEMA_DATE}.json
gsutil cp $f ${BUCK_TARGET}
tName=${RELNAME}_aliquot2caseIDmap
bq -q load --source_format=CSV --field_delimiter='\t' --skip_leading_rows=1 \
    --replace ${DATASET}.$tName ${BUCK_TARGET}/$f $j

echo " "
echo " *********************************************************** "
echo " caseData ... "

f=caseData.merge.t1
j=caseData.${SCHEMA_DATE}.json
gsutil cp $f ${BUCK_TARGET}
tName=${RELNAME}_caseData
bq -q load --source_format=CSV --field_delimiter='\t' --skip_leading_rows=1 \
    --replace ${DATASET}.$tName ${BUCK_TARGET}/$f $j

echo " "
echo " *********************************************************** "
echo " fileData legacy ... "

f=fileData.legacy.t1
j=fileData.legacy.${SCHEMA_DATE}.json
gsutil cp $f ${BUCK_TARGET}
tName=${RELNAME}_fileData_legacy
bq -q load --source_format=CSV --field_delimiter='\t' --skip_leading_rows=1 \
    --replace ${DATASET}.$tName ${BUCK_TARGET}/$f $j

echo " "
echo " *********************************************************** "
echo " fileData current ... "

f=fileData.current.t1
j=fileData.current.${SCHEMA_DATE}.json
gsutil cp $f ${BUCK_TARGET}
tName=${RELNAME}_fileData_current
bq -q load --source_format=CSV --field_delimiter='\t' --skip_leading_rows=1 \
    --replace ${DATASET}.$tName ${BUCK_TARGET}/$f $j
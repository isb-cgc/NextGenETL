#!/usr/bin/env bash

# Copyright 2020, Institute for Systems Biology
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

echo "THIS VM NEEDS STORAGE READ AND BQ SCOPES"
echo "The function description MUST have a version tag of the form: \"VERSION: *[0-9][0-9]*.[0-9][0-9]*\" e.g. \"VERSION: 1.1\""

source ~/setEnvVars.sh

mkdir -p ~/config
pushd ~/config > /dev/null
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/PublishBQUserFunction.sh .
popd > /dev/null

# The config script must set these variables, e.g.:
#FUNCTION_NAME=...
#FUNCTION_FILE=${FUNCTION_NAME}.sql
#PROJECT_ID=...
#DATASET_ID_CURR=functions
#DATASET_ID_VERS=functions_versioned
#EXPECTED_VERSION=1.1

source ~/config/PublishBQUserFunction.sh .

cd ${HOME}

mkdir -p scratch
rm -rf UDFRepo
mkdir UDFRepo
cd UDFRepo
git clone https://github.com/isb-cgc/Community-Notebooks.git
cd Community-Notebooks/BQUserFunctions
cp ${FUNCTION_FILE} ~/scratch
cd ~/scratch
VERSION=`grep "VERSION:" ${FUNCTION_FILE} | sed -e 's/.*VERSION: *\([0-9][0-9]*\.[0-9][0-9]*\).*/\1/'`

if [ ${EXPECTED_VERSION} != ${VERSION} ]; then
    echo "Version mismatch: ${VERSION} vs ${EXPECTED_VERSION}"
    exit 1
fi

U_VERSION=`echo ${VERSION} | sed -e 's/\./_/'`

MATCH_TAG=__PROJECTID__\.__DATASET__\.${FUNCTION_NAME}__VERSIONTAG__
REPLACE_TAG_CURR=${PROJECT_ID}.${DATASET_ID_CURR}.${FUNCTION_NAME}_current
REPLACE_TAG_VERS=${PROJECT_ID}.${DATASET_ID_VERS}.${FUNCTION_NAME}_v${U_VERSION}

cat ${FUNCTION_FILE} | sed -e 's/'${MATCH_TAG}'/'${REPLACE_TAG_CURR}'/' > current_${FUNCTION_FILE}
cat ${FUNCTION_FILE} | sed -e 's/'${MATCH_TAG}'/'${REPLACE_TAG_VERS}'/' > vers_${FUNCTION_FILE}

bq query --nouse_legacy_sql < current_${FUNCTION_FILE}
bq query --nouse_legacy_sql < vers_${FUNCTION_FILE}
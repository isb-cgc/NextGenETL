#!/usr/bin/env bash

# Copyright 2021, Institute for Systems Biology
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

source ~/setEnvVars.sh

SCRIPT_NAME=$1

CASE_ARG="case"
CLINICAL_ARG="clinical"
FILE_ARG="file"
STUDY_ARG="study"
QUANT_ARG="quant"

SHARED_CONFIG_FILE="PDCSharedConfig.yaml"

if [[ ${SCRIPT_NAME} = ${CASE_ARG} ]] ; then
    CONFIG_FILE="PDCCaseMetadata.yaml"
    SCRIPT_FILE="build_pdc_case_metadata.py"
elif [[ ${SCRIPT_NAME} = ${CLINICAL_ARG} ]] ; then
    CONFIG_FILE="PDCClinical.yaml"
    SCRIPT_FILE="build_pdc_clinical.py"
elif [[ ${SCRIPT_NAME} = ${FILE_ARG} ]] ; then
    CONFIG_FILE="PDCFileMetadata.yaml"
    SCRIPT_FILE="build_pdc_file_metadata.py"
elif [[ ${SCRIPT_NAME} = ${STUDY_ARG} ]] ; then
    CONFIG_FILE="PDCStudy.yaml"
    SCRIPT_FILE="build_pdc_study.py"
elif [[ ${SCRIPT_NAME} = ${QUANT_ARG} ]] ; then
    CONFIG_FILE="PDCQuant.yaml"
    SCRIPT_FILE="build_pdc_quant_data_matrix.py"
else
    echo "Error: incorrect or missing script data type argument. Accepted values: case, clinical, file, study, quant"
    exit 1
fi

export MY_VENV=~/virtualEnvETL
export PYTHONPATH=.:${MY_VENV}/lib:~/extlib

mkdir -p ~/config
pushd ~/config > /dev/null

gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/${SHARED_CONFIG_FILE} .
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/${CONFIG_FILE} .
popd > /dev/null

pushd ${MY_VENV} > /dev/null
source bin/activate
popd > /dev/null

mkdir -p ~/scratch

cd ..
python3 ./BQ_Table_Building/PDC/${SCRIPT_FILE} ~/config/${SHARED_CONFIG_FILE} ~/config/${CONFIG_FILE}
deactivate

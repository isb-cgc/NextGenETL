#!/usr/bin/env bash

# Copyright 2023, Institute for Systems Biology
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

ALIQUOT_ARG="aliquot_case_map"
CASE_ARG="case"
CLINICAL_ARG="clinical"
FILE_ARG="file"
FILE_ENTITY_ARG="file_entity"
PER_SAMPLE_FILE_ARG="per_sample_file"
STUDIES_ARG="study"

if [[ ${SCRIPT_NAME} = ${ALIQUOT_ARG} ]] ; then
    CONFIG_FILE="CDATestCreateTablesAliquotCaseMapGDC.yaml"
    SCRIPT_FILE="test_create_tables_base.py"
elif [[ ${SCRIPT_NAME} = ${CASE_ARG} ]] ; then
    CONFIG_FILE="CDATestCreateTablesCaseGDC.yaml"
    SCRIPT_FILE="test_create_tables_base.py"
elif [[ ${SCRIPT_NAME} = ${CLINICAL_ARG} ]] ; then
    CONFIG_FILE="CDATestCreateTablesClinicalGDC.yaml"
    SCRIPT_FILE="test_create_tables_program_or_project.py"
elif [[ ${SCRIPT_NAME} = ${FILE_ARG} ]] ; then
    CONFIG_FILE="CDATestCreateTablesFileGDC.yaml"
    SCRIPT_FILE="test_create_tables_base.py"
elif [[ ${SCRIPT_NAME} = ${FILE_ENTITY_ARG} ]] ; then
    CONFIG_FILE="CDATestCreateTablesFileEntityGDC.yaml"
    SCRIPT_FILE="test_create_tables_base.py"
elif [[ ${SCRIPT_NAME} = ${PER_SAMPLE_FILE_ARG} ]] ; then
    CONFIG_FILE="CDATestCreateTablesPerSampleFileGDC.yaml"
    SCRIPT_FILE="test_create_tables_program_or_project.py"
else
    echo "Error: incorrect or missing script data type argument."
    echo "Accepted values: aliquot_case_map, case, clinical, file, file_entity, per_sample_file, study"
    exit 1
fi

export MY_VENV=~/virtualEnvETL3_9
export PYTHONPATH=.:${MY_VENV}/lib:~/extlib

mkdir -p ~/config
pushd ~/config > /dev/null
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/${CONFIG_FILE} .
popd > /dev/null

pushd ${MY_VENV} > /dev/null
source bin/activate
popd > /dev/null

mkdir -p ~/scratch

cd ..
python3.9 ./BQ_Table_Building/CDA/tests/${SCRIPT_FILE} ~/config/${CONFIG_FILE}
deactivate
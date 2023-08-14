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

CASE_ARG="case"
CLINICAL_ARG="clinical"
FILE_ARG="file"
FILE_ENTITY_ARG="file_entity"
PER_SAMPLE_FILE_ARG="per_sample_file"
ALIQUOT_ARG="aliquot_case_map"
STUDIES_ARG="study"

SHARED_CONFIG_FILE="CDASharedConfigPDC.yaml"

if [[ ${SCRIPT_NAME} = ${CASE_ARG} ]] ; then
    CONFIG_FILE="CDACreateTablesCasePDC.yaml"
    SCRIPT_FILE="create_tables_case_metadata_pdc.py"
elif [[ ${SCRIPT_NAME} = ${CLINICAL_ARG} ]] ; then
    CONFIG_FILE="CDACreateTablesClinicalPDC.yaml"
    SCRIPT_FILE="create_tables_clinical_pdc.py"
elif [[ ${SCRIPT_NAME} = ${ALIQUOT_ARG} ]] ; then
    CONFIG_FILE="CDACreateTablesAliquotCaseMapPDC.yaml"
    SCRIPT_FILE="create_tables_aliquot_case_map_pdc.py"
elif [[ ${SCRIPT_NAME} = ${FILE_ARG} ]] ; then
    CONFIG_FILE="CDACreateTablesFilePDC.yaml"
    SCRIPT_FILE="create_tables_file_metadata_pdc.py"
elif [[ ${SCRIPT_NAME} = ${FILE_ENTITY_ARG} ]] ; then
    CONFIG_FILE="CDACreateTablesFileAssociatedEntityMappingPDC.yaml"
    SCRIPT_FILE="create_tables_file_associated_entity_mapping_pdc.py"
elif [[ ${SCRIPT_NAME} = ${PER_SAMPLE_FILE_ARG} ]] ; then
    CONFIG_FILE="CDACreateTablesPerSampleFilePDC.yaml"
    SCRIPT_FILE="create_tables_per_sample_file_pdc.py"
elif [[ ${SCRIPT_NAME} = ${PER_SAMPLE_FILE_ARG} ]] ; then
    CONFIG_FILE="CDACreateTablesPerSampleFilePDC.yaml"
    SCRIPT_FILE="create_tables_per_sample_file_pdc.py"
elif [[ ${SCRIPT_NAME} = ${STUDIES_ARG} ]] ; then
    CONFIG_FILE="CDACreateTablesStudiesPDC.yaml"
    SCRIPT_FILE="create_tables_studies_pdc.py"
else
    echo "Error: incorrect or missing script data type argument. Accepted values: case, clinical, file, per_sample_file, aliquot_case_map, slide_case_map"
    exit 1
fi

export MY_VENV=~/virtualEnvETL3_9
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
python3.9 ./BQ_Table_Building/CDA/PDC/${SCRIPT_FILE} ~/config/${SHARED_CONFIG_FILE} ~/config/${CONFIG_FILE}
deactivate
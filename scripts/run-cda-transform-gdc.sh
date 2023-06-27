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
PER_SAMPLE_FILE_ARG="per_sample_file"
ALIQUOT_ARG="aliquot_to_case"
SLIDE_ARG="slide_to_case"
DISEASE_ARG="project_disease_type"

if [[ ${SCRIPT_NAME} = ${CASE_ARG} ]] ; then
    CONFIG_FILE="CDATransformCaseBQBuildGDC.yaml"
    SCRIPT_FILE="transform_tables_case_metadata_gdc.py"
elif [[ ${SCRIPT_NAME} = ${CLINICAL_ARG} ]] ; then
    CONFIG_FILE="CDATransformClinicalBQBuildGDC.yaml"
    SCRIPT_FILE="transform_tables_clinical_gdc.py"
elif [[ ${SCRIPT_NAME} = ${ALIQUOT_ARG} ]] ; then
    CONFIG_FILE="CDATransformAliquotBQBuildGDC.yaml"
    SCRIPT_FILE="transform_tables_aliquot_case_map_gdc.py"
elif [[ ${SCRIPT_NAME} = ${SLIDE_ARG} ]] ; then
    CONFIG_FILE="CDATransformSlideBQBuildGDC.yaml"
    SCRIPT_FILE="transform_tables_slide_case_map_gdc.py"
elif [[ ${SCRIPT_NAME} = ${FILE_ARG} ]] ; then
    CONFIG_FILE="CDATransformFileBQBuildGDC.yaml"
    SCRIPT_FILE="transform_tables_file_metadata_gdc.py"
elif [[ ${SCRIPT_NAME} = ${PER_SAMPLE_FILE_ARG} ]] ; then
    CONFIG_FILE="CDATransformPerSampleFileBQBuildGDC.yaml"
    SCRIPT_FILE="transform_tables_per_sample_file_metadata_gdc.py"
elif [[ ${SCRIPT_NAME} = ${DISEASE_ARG} ]] ; then
    CONFIG_FILE="CDATransformProjectDiseaseTypeBuildGDC.yaml"
    SCRIPT_FILE="transform_tables_project_disease_type.py"
else
    echo "Error: incorrect or missing script data type argument. Accepted values: case, clinical, file, per_sample_file, aliquot_to_case, slide_to_case"
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
python3.9 ./BQ_Table_Building/CDA/${SCRIPT_FILE} ~/config/${CONFIG_FILE}
deactivate
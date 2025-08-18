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

SHARED_CONFIG_FILE="CDASharedConfigICDC.yaml"
EXTRACT_CONFIG_FILE="CDAExtractFromTSVICDC.yaml"
STUDY_CONFIG_FILE="CDACreateTablesStudyICDC.yaml"
# COHORT_CONFIG_FILE="CDACreateTablesCohortICDC.yaml"
CASE_CONFIG_FILE="CDACreateTablesCaseICDC.yaml"
FILE_CONFIG_FILE="CDACreateTablesFileICDC.yaml"
PER_SAMPLE_CONFIG_FILE="CDACreateTablesPerSampleFileICDC.yaml"
CLINICAL_CONFIG_FILE="CDACreateTablesClinicalICDC.yaml"

export MY_VENV=~/virtualEnvETL3_11
export PYTHONPATH=.:${MY_VENV}/lib:~/extlib

mkdir -p ~/config
pushd ~/config > /dev/null
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/${SHARED_CONFIG_FILE} .
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/${EXTRACT_CONFIG_FILE} .
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/${STUDY_CONFIG_FILE} .
# gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/${COHORT_CONFIG_FILE} .
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/${CASE_CONFIG_FILE} .
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/${FILE_CONFIG_FILE} .
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/${PER_SAMPLE_CONFIG_FILE} .
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/${CLINICAL_CONFIG_FILE} .
popd > /dev/null

pushd ${MY_VENV} > /dev/null
source bin/activate
popd > /dev/null

mkdir -p ~/scratch

cd ..
echo "*** Downloading CDA files and building raw BQ tables"
python3.11 ./BQ_Table_Building/CDA/extract_from_tsv.py ~/config/${SHARED_CONFIG_FILE} ~/config/${EXTRACT_CONFIG_FILE}
echo "*** Building study dev table"
python3.11 ./BQ_Table_Building/CDA/ICDC/create_tables_study_icdc.py ~/config/${SHARED_CONFIG_FILE} ~/config/${STUDY_CONFIG_FILE}
# echo "*** Building cohort dev table"
# python3.11 ./BQ_Table_Building/CDA/ICDC/create_tables_cohort_icdc.py ~/config/${SHARED_CONFIG_FILE} ~/config/${COHORT_CONFIG_FILE}
echo "*** Building case metadata dev table"
python3.11 ./BQ_Table_Building/CDA/ICDC/create_tables_case_metadata_icdc.py ~/config/${SHARED_CONFIG_FILE} ~/config/${CASE_CONFIG_FILE}
echo "*** Building file metadata dev table"
python3.11 ./BQ_Table_Building/CDA/ICDC/create_tables_file_metadata_icdc.py ~/config/${SHARED_CONFIG_FILE} ~/config/${FILE_CONFIG_FILE}
echo "*** Building per sample file metadata dev tables"
python3.11 ./BQ_Table_Building/CDA/ICDC/create_tables_per_sample_file_icdc.py ~/config/${SHARED_CONFIG_FILE} ~/config/${PER_SAMPLE_CONFIG_FILE}
echo "*** Building clinical dev tables"
python3.11 ./BQ_Table_Building/CDA/ICDC/create_tables_clinical_icdc.py ~/config/${SHARED_CONFIG_FILE} ~/config/${CLINICAL_CONFIG_FILE}
echo "*** Load complete. Please run run-cda-compare-publish-tables.sh to test and publish."
deactivate
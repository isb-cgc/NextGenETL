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

SHARED_CONFIG_FILE="CDASharedConfigPDC.yaml"
EXTRACT_CONFIG_FILE="CDAExtractFromTSVPDC.yaml"
STUDY_CONFIG_FILE="CDACreateTablesStudiesPDC.yaml"
CASE_CONFIG_FILE="CDACreateTablesCasePDC.yaml"
ALIQUOT_CONFIG_FILE="CDACreateTablesAliquotCaseMapPDC.yaml"
FILE_CONFIG_FILE="CDACreateTablesFilePDC.yaml"
FILE_ENTITY_CONFIG_FILE="CDACreateTablesFileAssociatedEntityMappingPDC.yaml"
PER_SAMPLE_CONFIG_FILE="CDACreateTablesPerSampleFilePDC.yaml"
CLINICAL_CONFIG_FILE="CDACreateTablesClinicalPDC.yaml"

export MY_VENV=~/virtualEnvETL3_9
export PYTHONPATH=.:${MY_VENV}/lib:~/extlib

mkdir -p ~/config
pushd ~/config > /dev/null
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/${SHARED_CONFIG_FILE} .
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/${EXTRACT_CONFIG_FILE} .
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/${STUDY_CONFIG_FILE} .
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/${CASE_CONFIG_FILE} .
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/${ALIQUOT_CONFIG_FILE} .
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/${FILE_CONFIG_FILE} .
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/${FILE_ENTITY_CONFIG_FILE} .
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/${PER_SAMPLE_CONFIG_FILE} .
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/${CLINICAL_CONFIG_FILE} .
popd > /dev/null

pushd ${MY_VENV} > /dev/null
source bin/activate
popd > /dev/null

mkdir -p ~/scratch
mkdir -p ~/scratch/cda_pdc
mkdir -p ~/scratch/cda_pdc/quant

cd ..
echo "*** Downloading CDA files and building raw BQ tables"
python3.9 ./BQ_Table_Building/CDA/extract_from_tsv.py ~/config/${SHARED_CONFIG_FILE} ~/config/${EXTRACT_CONFIG_FILE}
echo "*** Building studies dev table"
python3.9 ./BQ_Table_Building/CDA/PDC/create_tables_studies_pdc.py ~/config/${SHARED_CONFIG_FILE} ~/config/${STUDY_CONFIG_FILE}
echo "*** Building case metadata dev table"
python3.9 ./BQ_Table_Building/CDA/PDC/create_tables_case_metadata_pdc.py ~/config/${SHARED_CONFIG_FILE} ~/config/${CASE_CONFIG_FILE}
echo "*** Building aliquot to case map dev table"
python3.9 ./BQ_Table_Building/CDA/PDC/create_tables_aliquot_case_map_pdc.py ~/config/${SHARED_CONFIG_FILE} ~/config/${ALIQUOT_CONFIG_FILE}
echo "*** Building file metadata dev table"
python3.9 ./BQ_Table_Building/CDA/PDC/create_tables_file_metadata_pdc.py ~/config/${SHARED_CONFIG_FILE} ~/config/${FILE_CONFIG_FILE}
echo "*** Building file associated entity mapping dev table"
python3.9 ./BQ_Table_Building/CDA/PDC/create_tables_file_associated_entity_mapping_pdc.py ~/config/${SHARED_CONFIG_FILE} ~/config/${FILE_ENTITY_CONFIG_FILE}
echo "*** Building per_sample_file_metadata dev tables"
python3.9 ./BQ_Table_Building/CDA/PDC/create_tables_per_sample_file_pdc.py ~/config/${SHARED_CONFIG_FILE} ~/config/${PER_SAMPLE_CONFIG_FILE}
echo "*** Building clinical dev tables"
python3.9 ./BQ_Table_Building/CDA/PDC/create_tables_clinical_pdc.py ~/config/${SHARED_CONFIG_FILE} ~/config/${CLINICAL_CONFIG_FILE}
echo "*** Load complete. Please run run-cda-compare-publish-tables.sh to test and publish."
deactivate
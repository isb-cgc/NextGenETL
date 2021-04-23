#!/usr/bin/env bash
source ~/setEnvVars.sh

export MY_VENV=~/virtualEnvETL3_9
export PYTHONPATH=.:${MY_VENV}/lib:~/extlib

mkdir -p ~/config
pushd ~/config > /dev/null
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/ClinicalBQBuild.yaml .
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/ClinicalProgramBQBuild.yaml .
popd > /dev/null

pushd ${MY_VENV} > /dev/null
source bin/activate
popd > /dev/null

mkdir -p ~/scratch

cd ..
python3.9 ./BQ_Table_Building/build_gdc_api_clinical_bulk_table.py ~/config/ClinicalBQBuild.yaml
python3.9 ./BQ_Table_Building/build_gdc_api_clinical_program_tables.py ~/config/ClinicalProgramBQBuild.yaml
deactivate

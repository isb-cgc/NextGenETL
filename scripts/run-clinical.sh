#!/usr/bin/env bash
source ~/setEnvVars.sh

export MY_VENV=~/virtualEnvETL3_11
export PYTHONPATH=.:${MY_VENV}/lib:~/extlib

mkdir -p ~/config
pushd ~/config > /dev/null
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/test_ClinicalBQBuild.yaml .
popd > /dev/null

pushd ${MY_VENV} > /dev/null
source bin/activate
popd > /dev/null

mkdir -p ~/scratch

cd ..
python3.11 ./BQ_Table_Building/build_gdc_api_clinical_bulk_table.py ~/config/test_ClinicalBQBuild.yaml
deactivate

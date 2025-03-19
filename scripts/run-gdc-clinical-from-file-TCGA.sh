#!/usr/bin/env bash
source ~/setEnvVars.sh

export MY_VENV=~/virtualEnvETL3_11
export PYTHONPATH=.:${MY_VENV}/lib:~/extlib

mkdir -p ~/config
pushd ~/config > /dev/null
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/GDCFileClinicalTablesBuildTCGA.yaml .
popd > /dev/null

pushd ${MY_VENV} > /dev/null
source bin/activate
popd > /dev/null
cd ..
python3.11 ./BQ_Table_Building/Clinical_From_File/build_gdc_file_clinical_tables_TCGA.py ~/config/GDCFileClinicalTablesBuildTCGA.yaml
deactivate

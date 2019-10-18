#!/usr/bin/env bash

source ./setEnvVars.sh

export MY_VENV=virtualEnvETL
export PYTHONPATH=.:${MY_VENV}/lib

mkdir -p ~/config
pushd ~/config > /dev/null
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/ManifestBQBuild.yaml .
popd > /dev/null

pushd ${MY_VENV} > /dev/null
source bin/activate
popd > /dev/null
cd ..
python3 ./DCF-Manifest-Pulls/build_dcf_manifest_bq_tables.py ~/config/ManifestBQBuild.yaml
deactivate

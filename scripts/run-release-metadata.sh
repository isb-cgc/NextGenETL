#!/usr/bin/env bash

source ~/setEnvVars.sh

export MY_VENV=~/virtualEnvETL
export PYTHONPATH=.:${MY_VENV}/lib:~/extlib

mkdir -p ~/config
pushd ~/config > /dev/null
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/MetadataForRelease.yaml .
popd > /dev/null

pushd ${MY_VENV} > /dev/null
source bin/activate
popd > /dev/null
cd ..
python3 ./GDC-Metadata-Processing/build_release_metadata_bq_tables.py ~/config/MetadataForRelease.yaml
deactivate

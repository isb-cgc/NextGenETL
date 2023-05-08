#!/usr/bin/env bash
source ~/setEnvVars.sh

export MY_VENV=~/virtualEnvETL3_9
export PYTHONPATH=.:${MY_VENV}/lib:~/extlib

pushd ${MY_VENV} > /dev/null
source bin/activate
popd > /dev/null

mkdir -p ~/scratch

cd ..
python3.9 ./BQ_Table_Building/CDA/transform_tables_gdc.py
deactivate
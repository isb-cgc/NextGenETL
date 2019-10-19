#!/usr/bin/env bash

source ~/setEnvVars.sh

export MY_VENV=~/virtualEnvETL
export PYTHONPATH=.:${MY_VENV}/lib:~/extlib

mkdir -p ~/config
pushd ~/config > /dev/null
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/MirnaIsoformExprBQBuild.yaml .
popd > /dev/null

pushd ${MY_VENV} > /dev/null
source bin/activate
popd > /dev/null
cd ..
python3 ./BQ_Table_Building/build_mirna_isoform_expr_bq_table.py ~/config/MirnaIsoformExprBQBuild.yaml
deactivate

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

DATA_SOURCE=$1

GDC_ARG="gdc"
PDC_ARG="pdc"
IDC_ARG="idc"

if [[ ${DATA_SOURCE} = ${GDC_ARG} ]] ; then
    CONFIG_FILE="CDAIntermediateViewsBQBuildGDC.yaml"
elif [[ ${DATA_SOURCE} = ${PDC_ARG} ]] ; then
    CONFIG_FILE="CDAIntermediateViewsBQBuildPDC.yaml"
elif [[ ${DATA_SOURCE} = ${IDC_ARG} ]] ; then
    CONFIG_FILE="CDAIntermediateViewsBQBuildIDC.yaml"
else
    echo "Error: incorrect or missing script data type argument. Accepted values: gdc, idc, pdc"
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
python3.9 ./BQ_Table_Building/CDA/create_intermediate_views.py ~/config/${CONFIG_FILE}
deactivate
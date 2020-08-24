#!/usr/bin/env bash

# Copyright 2020, Institute for Systems Biology
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

#
# Working on the desktop, you need to run this to authenticate:
#

gcloud auth login ${1}

source ~/setEnvVarsForDesktop.sh

export PYTHONPATH=.:${MY_VENV}/lib

mkdir -p ${CONFIG_DIR}
pushd ${CONFIG_DIR} > /dev/null
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/BQMetadataTransfer.yaml .
popd > /dev/null

pushd ${MY_VENV} > /dev/null
source bin/activate
popd > /dev/null
cd ..
python3 ./BQ_Table_Building/transfer_project_bq_metadata.py ${CONFIG_DIR}/BQMetadataTransfer.yaml
deactivate


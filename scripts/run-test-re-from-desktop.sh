#!/usr/bin/env bash

# Copyright 2024, Institute for Systems Biology
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

source ~/setEnvVarsForDesktop.sh

export PYTHONPATH=.:${MY_VENV}/lib

mkdir -p ${CONFIG_DIR}
pushd ${CONFIG_DIR} > /dev/null
gsutil cp gs://${CONFIG_BUCKET}/${CURRENT_CONFIG_PATH}/TestRegulomeExplorerTables.yaml .
popd > /dev/null

INSTALL_LIBS=FALSE

if [ "${INSTALL_LIBS}" = "TRUE" ]; then
  pushd ${MY_VENV} > /dev/null
  source bin/activate
  popd > /dev/null
  echo "Installing Python Libraries..."
  python3 -m pip install pip
  python3 -m pip install wheel
  python3 -m pip install PyYaml
  python3 -m pip install gitpython
  python3 -m pip install google-api-python-client
  # If you specify the target lib (-t ${MY_VENV}/lib) then these two conflict:
  python3 -m pip install google-cloud-bigquery
  python3 -m pip install google-cloud-storage
  python3 -m pip install pandas
  python3 -m pip install db-dtypes
  python3 -m pip install --upgrade oauth2client
  echo ${PYTHONPATH}
  echo "Libraries Installed"
  deactivate
fi

pushd ${MY_VENV} > /dev/null
source bin/activate
popd > /dev/null
cd ..
python3 ./BQ_Table_Building/test_re_explorer_tables.py ${CONFIG_DIR}/TestRegulomeExplorerTables.yaml
deactivate

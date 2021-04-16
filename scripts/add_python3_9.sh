#!/usr/bin/env bash
#
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

# check for updates and upgrades
sudo apt-get update
sudo apt-get upgrade -y

#
# Install python version 3.9 without overriding the system version
#

# Get the python files
wget https://www.python.org/ftp/python/3.9.2/Python-3.9.2.tgz
# make sure all of the correct build requirements installed
sudo apt-get install -y make build-essential libssl-dev zlib1g-dev \
       libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
       libncurses5-dev libncursesw5-dev xz-utils tk-dev liblzma-dev lzma
# unzip the file
tar xvf Python-3.9.2.tgz
# move to the directory
cd Python-3.9.2
# run the python configure and make
./configure && make && make test
# install python using altinstall, so to avoid overwriting the system's python version
sudo make altinstall
cd ~

#
# Now set up the python3.9 virtualenv for the BQ building scripts
#

python3.9 -m venv virtualEnvETL3_9
source virtualEnvETL3_9/bin/activate
python3.9 -m pip install wheel
python3.9 -m pip install google-api-python-client
python3.9 -m pip install google-cloud-storage
python3.9 -m pip install google-cloud-bigquery
python3.9 -m pip install PyYaml
python3.9 -m pip install gitpython
python3.9 -m pip install pandas
python3.9 -m pip install xlrd
python3.9 -m pip install wget
python3.9 -m pip install alive_progress
# used by build_schema:
python3.9 -m pip install python-dateutil
deactivate
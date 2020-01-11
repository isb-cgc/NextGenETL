#!/usr/bin/env bash

# Copyright 2019, Institute for Systems Biology
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

sudo apt-get update
sudo apt-get upgrade -y

sudo apt-get install -y	git

#
# Do not use pip3 to upgrade pip. Does not play well with Debian pip
#

sudo apt-get install -y python3-pip

#
# We want venv:
#

sudo apt-get install -y python3-venv

#
# Google packages get the infamous "Failed building wheel for ..." message. SO suggestions
# for this situation:
# https://stackoverflow.com/questions/53204916/what-is-the-meaning-of-failed-building-wheel-for-x-in-pip-install
#
# pip3 install wheel
# OR:
# pip install <package> --no-cache-dir.
#
# Using the first option
#

python3 -m venv virtualEnvETL
source virtualEnvETL/bin/activate
python3 -m pip install wheel
python3 -m pip install google-api-python-client
python3 -m pip install google-cloud-storage
python3 -m pip install google-cloud-bigquery
python3 -m pip install PyYaml
python3 -m pip install gitpython
# used by build_schema:
python3 -m pip install python-dateutil
deactivate

# Install Libraries From GitHub
# We need the ISB-CGC schema builder

mkdir -p ~/extlib
cd ~/extlib
rm -f createSchemaP3.py
wget https://raw.githubusercontent.com/isb-cgc/examples-Python/master/python/createSchemaP3.py

#
# Off to github to get the code!
#

cd ~
rm -rf ~/NextGenETL
git clone https://github.com/isb-cgc/NextGenETL.git
cd NextGenETL/scripts
chmod u+x *.sh

mv setEnvVars.sh ~
echo "Be sure to now customize the ~/setEnvVars.sh file to your system!"

echo "Note: 8G machines are not big enough to run metadata download (trying 30G)"





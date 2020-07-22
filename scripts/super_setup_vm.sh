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

sudo apt-get update
sudo apt-get upgrade -y

sudo apt-get install -y	git

#
# Legacy GDC metadata scripts are all written in Python 2, so we will take the Python 2 approach to installing
# a virtual environment for this stuff:
#

sudo apt-get install -y python-pip

#
# We want venv. Used to be (pre-Feb 2020) that the second pair of lines did what
# was needed. But not anymore. Gotta use the apt-get approach.
#

sudo apt-get install -y python-virtualenv

#pip install virtualenv
#sudo /usr/bin/easy_install virtualenv

cd ~
virtualenv pyVenvForTwo
source ./pyVenvForTwo/bin/activate

pip install pandas
pip install requests
pip install python-dateutil

deactivate

#
# Newer GDC metadata scripts are written in Python 3, so we will create a virtual environment for that stuff:
#
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

cd ~
python3 -m venv pyVenvForThree
source pyVenvForThree/bin/activate
python3 -m pip install wheel
python3 -m pip install google-api-python-client
python3 -m pip install google-cloud-storage
python3 -m pip install google-cloud-bigquery
python3 -m pip install pandas
deactivate

#
# Build the directory structure:
#

cd ~
mkdir -p GDC-metadata/scratch
mkdir -p GDC-metadata/scripts
mkdir -p GDC-metadata/scripts/common_etl
mkdir -p GDC-metadata/textFiles

#
# Off to github to get the code!
#

cd ~
rm -rf ~/NextGenETL
git clone https://github.com/isb-cgc/NextGenETL.git
cd NextGenETL/GDC-Metadata-Processing/scripts
chmod u+x *.sh

#
# Put the scripts and text files in the processing directories:
#

cd ~
cp ~/NextGenETL/GDC-Metadata-Processing/scripts/*  GDC-metadata/scripts
cp ~/NextGenETL/common_etl/* GDC-metadata/scripts/common_etl
cp ~/NextGenETL/GDC-Metadata-Processing/textFiles/*  GDC-metadata/textFiles

mv NextGenETL/GDC-Metadata-Processing/scripts/setEnvVarsGDCMetadata.sh ~

#
# Now set up the python3 virtualenv for the BQ building scripts
#

python3 -m venv virtualEnvETL
source virtualEnvETL/bin/activate
python3 -m pip install wheel
python3 -m pip install google-api-python-client
python3 -m pip install google-cloud-storage
python3 -m pip install google-cloud-bigquery
python3 -m pip install PyYaml
python3 -m pip install gitpython
python3 -m pip install pandas
python3 -m pip install xlrd
# used by build_schema:
python3 -m pip install python-dateutil
deactivate

# Make a place for schemas to be placed:

mkdir -p ~/schemaRepo

# Make a place for scratch files:

mkdir -p ~/scratch

# Install Libraries From GitHub
# We need the ISB-CGC schema builder

mkdir -p ~/extlib
cd ~/extlib
rm -f createSchemaP3.py
wget https://raw.githubusercontent.com/isb-cgc/examples-Python/master/python/createSchemaP3.py

mv ~/NextGenETL/scripts/setEnvVars.sh ~

echo "Be sure to now customize the ~/setEnvVars.sh file to your system!"
echo "Be sure to now customize the ~/setEnvVarsGDCMetadata.sh file to your system and version!"

echo "A 30G machine is required to process the current/active collection"
echo "A 60G machine is required to process the legacy collection"




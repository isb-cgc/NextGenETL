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
# Legacy GDC metadata scripts are all written in Python 2, so we will take the Python 2 approach to installing
# a virtual environment this stuff:
#

sudo apt-get install -y python-pip

#
# We want venv:
#


pip install virtualenv
sudo /usr/bin/easy_install virtualenv

cd ~
virtualenv pyVenvForTwo
source ./pyvenvForTwo/bin/activate

pip install pandas
pip install requests

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
deactivate

#
# Build the directory structure:
#

cd ~
mkdir -p GDC-metadata/scratch
mkdir -p GDC-metadata/scripts
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
cp ~/NextGenETL/GDC-Metadata-Processing/textFiles/*  GDC-metadata/textFiles


mv NextGenETL/GDC-Metadata-Processing/scripts/setEnvVarsGDCMetadata.sh ~
echo "Be sure to now customize the ~/setEnvVarsGDCMetadata.sh file to your system and version!"

echo "A 30G machine is required to process the current/active collection"
echo "A 60G machine is required to process the legacy collection"





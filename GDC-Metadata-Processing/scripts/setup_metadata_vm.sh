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
# These scripts ae python 2, so we go that route:
#

sudo apt-get install -y python-pip

#
# We want venv:
#

pip install virtualenv

sudo /usr/bin/easy_install virtualenv

virtualenv pyvenv

source ./pyvenv/bin/activate

pip install pandas
pip install requests

deactivate

#
# Off to github to get the code!
#

cd ~
rm -rf ~/NextGenETL
git clone https://github.com/isb-cgc/NextGenETL.git
cd NextGenETL/GDC-Metadata-Processing/scripts
chmod u+x *.sh

cp setEnvVarsGDCMetadata.sh ~
echo "Be sure to now customize the ~/setEnvVarsGDCMetadata.sh file to your system!"
mkdir -p ~/GDC-metadata/scripts
cp * ~/GDC-metadata/scripts
mkdir -p ~/GDC-metadata/textFiles
cd ../textFiles
cp * ~/GDC-metadata/textFiles






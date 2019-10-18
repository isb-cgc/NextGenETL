#!/usr/bin/env bash

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
deactivate

# Install Libraries From GitHub
# We need the ISB-CGC schema builder

cd ~
rm -f createSchemaP3.py
wget https://raw.githubusercontent.com/isb-cgc/examples-Python/master/python/createSchemaP3.py

#
# Off to github to get the code!
#

rm -rf ~/NextGenETL
git clone https://github.com/isb-cgc/NextGenETL.git






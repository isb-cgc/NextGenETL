#!/usr/bin/env bash

#
# Simples and stupid: just ditch the whole directory and pull it in:
#

cd ~
rm -rf ~/NextGenETL
git clone https://github.com/isb-cgc/NextGenETL.git
cd NextGenETL/scripts
chmod u+x *.sh





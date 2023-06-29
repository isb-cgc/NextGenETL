# Targetome ETL

This repository contains scripts and instructions supporting the Targetome ETL pipeline. The original Targetome dataset is available as a MySQL dump, and can be found here: https://github.com/ablucher/The-Cancer-Targetome. 

These commands must be run on a standard Debian VM in Google Cloud. 

1. From the Targetome directory of the NextGenETL repository, run the make install command to install and MySQL on a Debian VM:

        make install

2. When prompted, select the following options:

  - select N to setting root password

  - select Y to remove anonymous user

  - select Y to disallow root login remotely

  - select Y to remove test database and access to it

  - select Y to reload privilege tables

3. Initialize the database with the raw Targetome data, this downloads the raw data from the Targetome GitHub repository:

        make init

4. Export the flattened Targetome data to TSV files. The exported TSV files, located in the "export" directory, can now be used in the Targetome ETL pipeline.

        make export

5. Upload the exported TSV files to cloud storage, e.g., gs://next-gen-etl-scratch/jhp/etl/targetome-2020-07, changing the date of the folder accordingly (original should have been 2021-07). 

6. Update the Targetome ETL config file to reflect the file and directory names in step 5. The latest config file is located in gs://next-gen-etl-archives/live-job-jhp/. Download the config file to the ~/config directory of your VM. 

7. Once these manual steps have been completed, the ETL pipeline can proceed as usual by running the scripts/run-targetome.sh script in the NextGenETL repo. 


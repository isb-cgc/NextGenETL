# Reactome ETL

This repository contains scripts and instructions supporting the Reactome ETL pipeline. The Reactome ETL pipeline requires some manual file downloads, which can be semi-automated with the script in this directory. 

These commands must be run on a standard Debian VM in Google Cloud. 

1. From the Reactome directory of the NextGenETL repository, run the make download command:

        make download

2. Upload these files to a bucket, e.g., gs://next-gen-etl-scratch/jhp/etl/reactome-2020-08. Be sure to change the directory name according to the date of the download (original should have been 2021-08).
    
3. Update the Reactome ETL config file to reflect the file and directory names in steps 1 and 2. The version of the Reactome dataset can be obtained from their News page: https://reactome.org/about/news, which documents the latest release number. The latest config file is located in gs://next-gen-etl-archives/live-job-jhp/. Download the config file to the ~/config directory of your VM.

4. Once these manual steps have been completed, the ETL pipeline can proceed as usual by running the scripts/run-reactome.sh script in the NextGenETL repo.


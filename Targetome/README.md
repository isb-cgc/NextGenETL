# Targetome ETL

This repository contains scripts that support the Targetome ETL pipeline. The Targetome dataset is available as a MySQL dump, and can be found here: https://github.com/ablucher/The-Cancer-Targetome. 

Steps to export the required TSV files from the Targetome MySQL dump. These commands must be run on a standard Debian VM in Google Cloud. 

1. From the Targetome directory of the NextGenETL repository, run the make install command to install and MySQL on a Debian VM:

        make install

When prompted, select the following options:

    - select N to setting root password

    - select Y to remove anonymous user

    - select Y to disallow root login remotely

    - select Y to remove test database and access to it

    - select Y to reload privilege tables

2. Initialize the database with the raw Targetome data, this downloads the raw data from the Targetome GitHub repository:

        make init

3. Export the flattened Targetome data to TSV files. 

        make export

The exported TSV files, located in the "export" directory, can now be used in the Targetome ETL pipeline.


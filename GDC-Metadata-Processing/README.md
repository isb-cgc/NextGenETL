# ETL HOWTO: Metadata BQ table creation (currently performed on ISB internal machine)


---------------------------
A) Metadata BQ tables

1) We get release info from "GDC Support Team" <fitz@UCHICAGO.EDU> going to GDC-USERS-L@LIST.NIH.GOV.
   I subscribed to GDC-USERS-L at https://LIST.NIH.GOV.

2) The scripts/master_etl.sh script handles the ETL. The process is a mixture of old shell scripts and
   python2 scripts. Configuration is set using environment variables set in scripts/setEnvVarsGDCMetadata.sh


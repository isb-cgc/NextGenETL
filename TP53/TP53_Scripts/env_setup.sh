#load csv files
cd ~/NextGenETL/TP53
gsutil cp gs://tp53-data-files/etl/*.csv ../P53_Database/P53_data_csv/.
export GOOGLE_APPLICATION_CREDENTIALS={PROJECT_KEY_FILE_PATH}
export GCLOUD_PROJECT='isb-cgc-tp53-dev'


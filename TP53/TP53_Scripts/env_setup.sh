#load csv files
cd ~/NextGenETL/TP53
gsutil cp gs://project-zero/tp53/csv_data/*.csv ./P53_Database/P53_data_csv/.
export GOOGLE_APPLICATION_CREDENTIALS=~/secret_keys/tp53-dev-etl.key.json
export GCLOUD_PROJECT=isb-cgc-tp53-dev


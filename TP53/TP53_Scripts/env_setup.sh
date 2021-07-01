#load csv files
cd ~/NextGenETL/TP53
#gsutil cp gs://tp53-data-files/etl/*.csv P53_Database/P53_data_csv/.
export GOOGLE_APPLICATION_CREDENTIALS='/home/elainelee/secret_keys/isb-cgc-tp53-test.key.json'
export GCLOUD_PROJECT='isb-cgc-tp53-test'


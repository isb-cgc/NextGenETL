#load csv files
cd ~/NextGenETL/TP53
gsutil cp gs://tp53-data-files/etl/*.csv P53_Database/P53_data_csv/.
TIER=test

## data_dump.sh
#
# script to store BQ table data, sql statement of BQ views and static files in bucket tp53-data-backup
# should be run after every data release
# gs://tp53-data_backup/[YEAR_MONTH]
#                           /data      : static data file copies
#                           /list-files: static list file copies
#                           /tables    : BQ table data compressed copies (.csv.gz)
#                           /views     : BQ view SQL statement copies (.sql.txt)
#                           /schema    : BQ table schema files (.schema.json)

## set GCP of the copy source
SOURCE_GCP_NAME=isb-cgc-tp53


## back up static files
gsutil cp gs://tp53-static-files/data/*.csv gs://tp53-data-backup/$(date +%Y%m)/data/
gsutil cp gs://tp53-static-files/list-files/*.* gs://tp53-data-backup/$(date +%Y%m)/list-files/


## get list of tables and extract data in csv, compressed (.gz) and store in gcs bucket
tables=$(bq ls $SOURCE_GCP_NAME:P53_data | awk '{ if($2 == "TABLE"){ print $1; } }')
for table in $tables
do
    bq extract \
    --compression "GZIP" \
    --destination_format "CSV" \
    --field_delimiter '\t' \
    $SOURCE_GCP_NAME:P53_data.$table \
    gs://tp53-data-backup/$(date +%Y%m)/tables/$table.csv.gz
done

## get list of tables, extract each table's schema in a json file and store in gcs bucket
tables=$(bq ls $SOURCE_GCP_NAME:P53_data | awk '{ if($2 == "TABLE"){ print $1; } }')
for table in $tables
do
    bq show --format=prettyjson --schema $SOURCE_GCP_NAME:P53_data.$table \
    gs://tp53-data-backup/$(date +%Y%m)/schemas/$table.schema.json
done

## get list of views and extract each defined SQL statement. Store each SQL statement in a gcs bucket as a text file
views=$(bq ls $SOURCE_GCP_NAME:P53_data | awk '{ if($2 == "VIEW"){ print $1; } }')
for view in $views
do
    bq show --view  $SOURCE_GCP_NAME:P53_data.$view |tail +5 > temp.txt
    gsutil cp temp.txt gs://tp53-data-backup/$(date +%Y%m)/views/$view.sql.txt

done
rm temp.txt
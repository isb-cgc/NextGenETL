#copy BQ tables or gs bucket files from tier to tier
SOURCE_TIER=-test
DEST_TIER=

SOURCE_GCP_NAME=isb-cgc-tp53-test
#SOURCE_GCP_NAME=isb-cgc-tp53-test
#DEST_GCP_NAME=isb-cgc-tp53-test
DEST_GCP_NAME=isb-cgc-tp53
#DEST_GCP_NAME=isb-cgc-tp53-dev

#copy bucket files from source to dest tier
gsutil cp gs://tp53-static-files${SOURCE_TIER}/data/GermlineDownload_r20.csv gs://tp53-static-files${DEST_TIER}/data/GermlineDownload_r20.csv
gsutil cp gs://tp53-static-files${SOURCE_TIER}/data/MutationView_r20.csv gs://tp53-static-files${DEST_TIER}/data/MutationView_r20.csv


bq query --nouse_legacy_sql \
'UPDATE
  `'$DEST_GCP_NAME'.P53_data.MUTATION`
SET CLINVARlink=1509393
WHERE MUT_ID=1443'

bq query --nouse_legacy_sql \
'UPDATE
  `'$DEST_GCP_NAME'.P53_data.MutationView`
SET CLINVARlink=1509393
WHERE MUT_ID=1443'

bq query --nouse_legacy_sql \
'UPDATE
  `'$DEST_GCP_NAME'.P53_data.GermlineView`
SET CLINVARlink=1509393
WHERE MUT_ID=1443'

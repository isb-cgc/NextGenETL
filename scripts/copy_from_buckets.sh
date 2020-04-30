#! /bin/bash

CHARGE_PROJECT=$1

echo "Authorized user, should work:"
gsutil cp gs://idc-sandbox-002-auth-bucket/20191229_115013.jpg 20191229_115013_auugs.jpg
gsutil cp gs://idc-sandbox-002-auth-bucket/20200405_195051.jpg 20200405_195051_auugs.jpg
echo "All users, should work:"
gsutil cp gs://idc-sandbox-002-public-bucket/20191229_115013.jpg 20191229_115013_alugs.jpg
gsutil cp gs://idc-sandbox-002-public-bucket/20200405_195051.jpg 20200405_195051_alugs.jpg
echo "Auth user, requester pays, should work:"
gsutil -u ${CHARGE_PROJECT} cp gs://idc-sandbox-002-req-pays/20191229_115013.jpg 20191229_115013_aurpgs.jpg
gsutil -u ${CHARGE_PROJECT} cp gs://idc-sandbox-002-req-pays/20200405_195051.jpg 20200405_195051_aurpgs.jpg
echo "All users, requester pays, actually works:"
gsutil -u ${CHARGE_PROJECT} cp gs://idc-sandbox-002-req-pays-all/20191229_115013.jpg 20191229_115013_alrpgs.jpg
gsutil -u ${CHARGE_PROJECT} cp gs://idc-sandbox-002-req-pays-all/20200405_195051.jpg 20200405_195051_alrpgs.jpg


echo "Authorized user, should fail:"
curl https://storage.googleapis.com/idc-sandbox-002-auth-bucket/20191229_115013.jpg -o 20191229_115013_auu.jpg
curl https://storage.googleapis.com/idc-sandbox-002-auth-bucket/20200405_195051.jpg -o 20200405_195051_auu.jpg
echo "All users, should work:"
curl https://storage.googleapis.com/idc-sandbox-002-public-bucket/20191229_115013.jpg -o 20191229_115013_alu.jpg
curl https://storage.googleapis.com/idc-sandbox-002-public-bucket/20200405_195051.jpg -o 20200405_195051_alu.jpg
echo "Auth user, requester pays, should fail:"
curl https://storage.googleapis.com/idc-sandbox-002-req-pays/20191229_115013.jpg -o 20191229_115013_aurp.jpg
curl https://storage.googleapis.com/idc-sandbox-002-req-pays/20200405_195051.jpg -o 20200405_195051_aurp.jpg
echo "All users, requester pays, should fail:"
curl https://storage.googleapis.com/idc-sandbox-002-req-pays-all/20191229_115013.jpg -o 20191229_115013_alrp.jpg
curl https://storage.googleapis.com/idc-sandbox-002-req-pays-all/20200405_195051.jpg -o 20200405_195051_alrp.jpg



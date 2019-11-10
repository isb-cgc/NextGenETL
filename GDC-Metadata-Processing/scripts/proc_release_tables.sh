#!/usr/bin/env bash

# Copyright 2019, Institute for Systems Biology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# WJRL-08-03-19: This is the hex code for the case-API load in current. Note (see rel15-current) that
# there are no "caseless" files in current (file-API and case-API fileData pulls are exactly the same
# size). SO FOR CURRENT FILEDATA PULLS WE CAN USE THE CASE-API HEX FOR FILEDATA:
r1=$1
# WJRL-08-03-19: This is the hex code for the case-API load in legacy:
r2=$2
# WJRL-08-03-19: This is the hex code for the file-API load in legacy. Since there are caseless files
# in legacy, we need to use the file-API hex to get all the files:
r3=$3

RELNAME=$4

SCRATCHDIR=$5

ERR_TOSS=$6

echo " "
echo " ************************************* "
echo " looking at the ALIQUOT MAPS first ... "
echo " "

f1=../${RELNAME}-current/aliqMap.bq.$r1.tsv
f2=../${RELNAME}-legacy/aliqMap.bq.$r2.tsv

wc -l $f1
wc -l $f2
cat $f1 $f2 | sort | uniq | wc -l

f3=aliqMap.merge.t1
rm -fr $f3
cat $f1 $f2 | sort | uniq | \
    sed -e '1i\program_name	project_id	case_gdc_id	case_barcode	sample_gdc_id	sample_barcode	sample_type_id	sample_type	sample_is_ffpe	sample_preservation_method	portion_gdc_id	portion_barcode	analyte_gdc_id	analyte_barcode	aliquot_gdc_id	aliquot_barcode' >& $f3
wc -l $f3

../scripts/good_look.sh $f3 ${SCRATCHDIR}


echo " "
echo " ************************************* "
echo " looking at the SLIDE MAPS next ... "
echo " "

f1=../${RELNAME}-current/slidMap.bq.$r1.tsv
f2=../${RELNAME}-legacy/slidMap.bq.$r2.tsv

wc -l $f1
wc -l $f2
cat $f1 $f2 | sort | uniq | wc -l

f3=slidMap.merge.t1
rm -fr $f3
cat $f1 $f2 | sort | uniq | \
    sed -e '1i\program_name	project_id	case_gdc_id	case_barcode	sample_gdc_id	sample_barcode	sample_type_id	sample_type	portion_gdc_id	portion_barcode	slide_gdc_id	slide_barcode' >& $f3
wc -l $f3

../scripts/good_look.sh $f3 ${SCRATCHDIR}


## echo " "
## echo " ************************************* "
## echo " looking at the CASE DATA tables ... "
## echo " "
##
##
## echo " ... ASSUME that just_case has already been run !!! "



echo " "
echo " ************************************* "
echo " looking at the FILE DATA tables ... "
echo " "

# WJRL 8-3-19 See comments at top about requiring $r3 for legacy:
f1=../${RELNAME}-current/fileData.bq.$r1.tsv
f2=../${RELNAME}-legacy/fileData.bq.$r3.tsv

cp $f1 fileData.current.t1
cp $f2 fileData.legacy.t1

## new with the rel9 there is an error_type field in the fileData.active.t1 that
## we actually want to eliminate ... it is in column 30
## now (DR13) that error_type field is in column 26
## still true with DR14
## still true with DR15
## as of DR17, it's now column 29
## WJRL 9/24/19: NO! Actually column 30
## Check using:
#find ${RELNAME}-current -name "*.ht" -exec cat -n {} \; | grep error_type
#cut -f1-29,31- fileData.current.t1 >& fileData.current.t2
#cut -f1-28,30- fileData.current.t1 >& fileData.current.t2
# WJRL 11/10/19: Skip the hardwired curation; now hand in a precalculated argument:

cut -f${ERR_TOSS} fileData.current.t1 >& fileData.current.t2
mv fileData.current.t2 fileData.current.t1

f1=fileData.current.t1
f2=fileData.legacy.t1

echo " looking at columns in " $f1
../scripts/good_look.sh $f1 ${SCRATCHDIR}

echo " looking at columns in " $f2
../scripts/good_look.sh $f2 ${SCRATCHDIR}


## now we want to create the JSON schemas for all of these tables ...

echo " now we generate the JSON schema files ... "

for f in *.t1
    do
        echo "     ... calling createSchema for " $f
        python ../scripts/createSchema.py $f 0
    done

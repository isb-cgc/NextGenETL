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

f=$1
SCRATCH_DIR=$2

echo " "
echo $f
date

rm -fr $f.look
rm -fr $f.ht
rm -fr $f.h
rm -fr $f.n

head -1 $f >& $f.h
../scripts/transpose.sh $f.h >& $f.ht

sed -e '1d' $f > $f.n
wc -l $f
wc -l $f.n

echo " "
echo " "

maxK=`wc -l $f.ht | sed -e '1,$s/ /	/g' | cut -f1`
echo $maxK

echo " "
echo " "


mkdir -p ${SCRATCH_DIR}

for k in $(eval echo "{1..$maxK}")
    do
        echo $k
        echo " " >> $f.look
        echo $k >> $f.look
        cut -f $k $f.h >> $f.look

        rm -fr $f.t
        rm -fr $f.s
        cut -f $k $f.n >& $f.t
        sort -T ${SCRATCH_DIR} $f.t | uniq -c | sort -T ${SCRATCH_DIR} -nr >& $f.s
        wc -l $f.s >> $f.look
        head -10 $f.s >> $f.look
        tail -10 $f.s >> $f.look
   done

rm -fr $f.s $f.t $f.n $f.h


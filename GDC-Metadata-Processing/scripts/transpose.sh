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


if [ $# -ne 1 ]
then

	echo "Usage: `basename $0` INPUT_FILE"

else

	in_file=$1

	awk 'BEGIN {FS=OFS="\t"}
	{
	for (i=1;i<=NF;i++)
	{
	 arr[NR,i]=$i;
	 if(big <= NF)
	  big=NF;
	 }
	}

	END {
	  for(i=1;i<=big;i++)
	   {
	    for(j=1;j<=NR;j++)
	    {
	     printf("%s%s",arr[j,i], (j==NR ? "" : OFS));
	    }
	    print "";
	   }
	}' $in_file

fi


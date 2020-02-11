"""

Copyright 2020, Institute for Systems Biology

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import sys
import csv

'''
----------------------------------------------------------------------------------------------
Rel22 imports used 'NA' for sample_types where the field should have been null. Fix this.
'''

def main(args):

    if len(args) != 5:
        print(" ")
        print(" Usage : {} <damaged_tsv> <repaired_tsv> <fix_col> <convert_to_null>".format(args[0]))
        return

    fix_col = int(args[3])
    convert_to_null = args[4]
    with open(args[1], mode='r',newline='') as tsvfile:
        reader = csv.reader(tsvfile, delimiter='\t')
        with open(args[2], mode='w+') as normfile:
            writer = csv.writer(normfile, delimiter='\t',newline='')
            for row in reader:
                if row[fix_col] == convert_to_null:
                    row[fix_col] = ''
                writer.writerow(row)


if __name__ == "__main__":
    main(sys.argv)
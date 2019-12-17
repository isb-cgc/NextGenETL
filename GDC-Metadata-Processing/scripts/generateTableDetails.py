"""

Copyright 2019, Institute for Systems Biology

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
from json import loads as json_loads, dumps as json_dumps

'''
----------------------------------------------------------------------------------------------
Take the BQ Ecosystem json file for the table and break out the pieces into chunks that will
be arguments to the bq command used to create the table.
'''

def main(args):

    if len(args) != 3:
        print(" ")
        print(" Usage : {} <bq_table_dict_file> <file_tag>".format(args[0]))
        return

    #
    # Read in the chunks and write them out into pieces the bq command can use
    #

    file_tag = args[2]
    with open(args[1], mode='r') as bqt_dict_file:
        bqt_dict = json_loads(bqt_dict_file.read())

    with open("{}_desc.txt".format(file_tag), mode='w+') as desc_file:
        desc_file.write(bqt_dict['description'])
    with open("{}_labels.json".format(file_tag), mode='w+') as label_file:
        label_file.write(json_dumps(bqt_dict['labels'], sort_keys=True, indent=4, separators=(',', ': ')))
        label_file.write('\n')
    with open("{}_schema.json".format(file_tag), mode='w+') as schema_file:
        schema_file.write(json_dumps(bqt_dict['schema']['fields'], sort_keys=True, indent=4, separators=(',', ': ')))
        schema_file.write('\n')

if __name__ == "__main__":
    main(sys.argv)

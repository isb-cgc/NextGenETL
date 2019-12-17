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
from google.cloud import bigquery
from json import loads as json_loads

'''
----------------------------------------------------------------------------------------------
Take the labels and description of a BQ table and get them installed
'''


def main(args):

    if len(args) != 4:
        print(" ")
        print(" Usage : {} <bq_dataset> <bq_table> <file_tag>".format(args[0]))
        return

    dataset = args[1]
    table = args[2]
    file_tag = args[3]

    with open("{}_desc.txt".format(file_tag), mode='r') as desc_file:
        desc = desc_file.read()

    with open("{}_labels.json".format(file_tag), mode='r') as label_file:
        labels = json_loads(label_file.read())

    client = bigquery.Client()
    table_ref = client.dataset(dataset).table(table)
    table = client.get_table(table_ref)
    table.description = desc
    table.labels = labels
    client.update_table(table, ['description', 'labels'])

if __name__ == "__main__":
    main(sys.argv)

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

'''
----------------------------------------------------------------------------------------------
Publish a table by copying it.
'''


def main(args):

    if len(args) != 3:
        print(" ")
        print(" Usage : {} <source_table_proj.dataset.table> <dest_table_proj.dataset.table>".format(args[0]))
        return

    source_table = args[1]
    target_table = args[2]

    client = bigquery.Client()
    job = client.copy_table(source_table, target_table)
    job.result()

if __name__ == "__main__":
    main(sys.argv)

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
from common_etl.support import install_labels_and_desc

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

    install_labels_and_desc(dataset, table, file_tag)

if __name__ == "__main__":
    main(sys.argv)

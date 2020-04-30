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
from common_etl.support import generate_table_detail_files

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

    success = generate_table_detail_files(args[1], args[2])
    if not success:
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main(sys.argv)

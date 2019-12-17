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
import csv

'''
----------------------------------------------------------------------------------------------
Tables coming out of the GDC API contain semicolon-delimited lists of IDs. The order is not conserved,
so "sort | uniq" shell operations duplicate lines. Normalize these lists to be lexicographically ordered.
'''


def main(args):

    if len(args) != 3:
        print(" ")
        print(" Usage : {} <input_tsv> <normalized_out_tsv>".format(args[0]))
        return

    #
    # Look for entries with semicolons, and rewrite them to be lexicographically ordered:
    #

    with open(args[2], mode='w+') as normfile:
        with open(args[1], mode='r') as tsvfile:
            reader = csv.reader(tsvfile, delimiter='\t')
            for row in reader:
                first = True
                for field in row:
                    if not first:
                        normfile.write("\t")
                    first = False
                    if ";" in field:
                        toks = field.split(";")
                        toks.sort()
                        sort_line = ";".join(toks)
                        normfile.write(sort_line)
                    else:
                        normfile.write(field)
                normfile.write("\n")

if __name__ == "__main__":
    main(sys.argv)


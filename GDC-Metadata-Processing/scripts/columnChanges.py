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

'''
----------------------------------------------------------------------------------------------
Provide detailed breakdown of column value changes
'''


def main(args):

    if len(args) != 9:
        print(" ")
        print(" Usage : {} <change_file_name> <parent_dir> <scratch_dir> <old_dir> <new_dir> <check_file_name> <id_field> <vebose | silent>".format(args[0]))
        return

    change_file = args[1]
    parent_dir = args[2]
    scratch_dir = args[3]
    old_dir = args[4]
    new_dir = args[5]
    check_file_name = args[6]
    id_field = int(args[7])
    verbose = args[8] == "verbose"

    change_file_name = "{}/{}/{}".format(parent_dir, scratch_dir, change_file)
    old_file_name = "{}/{}/{}".format(parent_dir, old_dir, check_file_name)
    new_file_name = "{}/{}/{}".format(parent_dir, new_dir, check_file_name)

    need_cases = set()
    with open(change_file_name, mode='r') as change_file:
        for line in change_file:
            toks = line.split()
            need_cases.add(toks[1])

    if len(need_cases) == 0:
        print("No changes to analyze")
        return

    new_lines = {}
    with open(new_file_name, mode='r') as case_file:
        for line in case_file:
            toks = line.split('\t')
            if toks[id_field] in need_cases:
                new_lines[toks[id_field]] = toks

    old_lines = {}
    headers = []
    with open(old_file_name, mode='r') as case_file:
        for line in case_file:
            toks = line.split('\t')
            if len(headers) == 0:
                headers = toks
            if toks[id_field] in need_cases:
                old_lines[toks[id_field]] = toks

    change_counts = {}
    examples = {}
    for key in new_lines:
        new_toks = new_lines[key]
        old_toks = old_lines[key]
        for i in range(0, len(new_toks)):
            new_tok = new_toks[i]
            old_tok = old_toks[i]
            if new_tok != old_tok:
                if headers[i] not in change_counts:
                    change_counts[headers[i]] = 1
                    examples[headers[i]] = []
                else:
                    change_counts[headers[i]] += 1

                if change_counts[headers[i]] < 6:
                    examples[headers[i]].append("{} -> {}".format(old_tok, new_tok))

                if verbose:
                    print("{}: {}: {} -> {}".format(new_toks[1], headers[i], old_tok, new_tok))

    for key in change_counts:
        print("{}: {}".format(key, change_counts[key]))
        for examp in examples[key]:
            print("  Example: {}".format(examp))
        print("\n")

if __name__ == "__main__":
    main(sys.argv)

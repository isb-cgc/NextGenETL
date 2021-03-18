"""
Copyright 2020-2021, Institute for Systems Biology

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import time
import sys

from common_etl.utils import (format_seconds, has_fatal_error, delete_bq_table, delete_bq_dataset, load_config,
                              update_table_labels)

BQ_PARAMS = dict()
YAML_HEADERS = ('bq_params', 'steps')


def main(args):
    start_time = time.time()
    steps = None

    try:
        global BQ_PARAMS
        BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    if 'delete_tables' in steps:
        for table_id in BQ_PARAMS['DELETE_TABLES']:
            delete_bq_table(table_id)
            print("Deleted table: {}".format(table_id))

    if 'delete_datasets' in steps:
        for dataset in BQ_PARAMS['DELETE_DATASETS']:
            delete_bq_dataset(dataset)
            print("Deleted dataset: {}".format(dataset))

    if "update_table_labels" in steps:
        table_labels = BQ_PARAMS['TABLE_LABEL_UPDATES']
        for table_id in table_labels.keys():
            labels_to_add, labels_to_remove = None, None

            if "remove" in table_labels[table_id]:
                labels_to_remove = table_labels[table_id]["remove"]

            if "add" in table_labels[table_id]:
                labels_to_add = table_labels[table_id]["add"]

            update_table_labels(table_id, labels_to_remove, labels_to_add)

    end = time.time() - start_time
    print("Finished program execution in {}!\n".format(format_seconds(end)))


if __name__ == '__main__':
    main(sys.argv)

"""
Copyright 2020, Institute for Systems Biology

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies
or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
# import json
# from common_etl.utils import has_fatal_error
from google.cloud import bigquery


def get_table_list_for_curr_release(api_params, bq_params):
    dataset_id = bq_params['WORKING_PROJECT'] + '.' + bq_params['TARGET_DATASET']
    client = bigquery.Client()
    table_iter = client.list_tables(dataset_id)

    for table in table_iter:
        print(table)


def generate_docs(api_params, bq_params):
    get_table_list_for_curr_release(api_params, bq_params)

"""

'''
SELECT distinct(diag__primary_diagnosis), count(diag__primary_diagnosis) as diag_count
FROM `isb - project - zero.GDC_Clinical_Data.rel23_clin_FM` 
GROUP BY diag__primary_diagnosis
ORDER BY diag_count DESC
LIMIT 10
'''

documentation:
- list of programs with record counts, most common primary diagnoses?


- list of tables, with total record counts, schemas, id keys, reference data


- list of columns with types, descriptions and possible counts (frequency distribution?)


- data source citation
"""

'''
def generate_table_documentation(table, table_id, record_count, columns, column_order):
    print()
    print("{}".format(table))
    print("{}".format(table_id))
    print("{}".format(record_count))
    print("{}".format(columns))
    print("{}".format(column_order))
    print()
def main():
    with open('files/rel23_documentation.json', 'r') as json_file:
        doc_json = json.load(json_file)

        metadata_tables = doc_json.pop("metadata")

        for program, entries in doc_json.items():
            record_counts = entries['record_counts']
            table_columns = entries['table_columns']
            table_ids = entries['table_ids']
            table_order_dict = entries['table_order_dict']

            for table in table_columns:
                columns = table_columns[table]
                table_id = table_ids[table]
                column_order = table_order_dict[table]
                record_count = record_counts[table]

                generate_table_documentation(table, table_id, record_count, columns, column_order)
if __name__ == '__main__':
    main()
'''

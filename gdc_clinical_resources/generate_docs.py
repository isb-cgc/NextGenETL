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
from common_etl.utils import get_table_id
from google.cloud import bigquery
from math import pow


def get_program_name(api_params, bq_params, table_name):
    prefix = api_params['GDC_RELEASE'] + '_' +  bq_params['TABLE_PREFIX'] + '_'
    start_idx = len(prefix)
    table_name = table_name[start_idx:]

    return table_name.split('_')[0]


def convert_bytes_to_largest_unit(obj_bytes):
    units = [
        ('bytes', 0),
        ('Kb', 1),
        ('MB', 2),
        ('GB', 3),
        ('TB', 4)
    ]

    curr_unit = 'bytes'
    curr_size = obj_bytes

    for unit, multiplier in units:
        while int(obj_bytes) / pow(1024, multiplier) > 1:
            curr_unit = unit
            curr_size = "{:.3f}".format(int(obj_bytes) / pow(1024, multiplier))

        return curr_unit, curr_size


def get_table_list_for_curr_release(api_params, bq_params):
    dataset_id = bq_params['WORKING_PROJECT'] + '.' + bq_params['TARGET_DATASET']
    client = bigquery.Client()
    table_iter = client.list_tables(dataset_id)
    program_tables_json = dict()
    table_attr_objs = dict()

    for table_item in table_iter:
        table_name = table_item.table_id

        if api_params['GDC_RELEASE'] not in table_name \
                or bq_params['MASTER_TABLE'] in table_name:
            continue

        table_id = get_table_id(bq_params, table_name)

        table_res = client.get_table(table_id)
        table_json_attr = table_res.to_api_repr()
        unwanted_attributes = {'tableReference', 'numLongTermBytes', 'lastModifiedTime',
                               'id', 'type', 'location'}

        for attr in unwanted_attributes:
            table_json_attr.pop(attr)

        prog_name = get_program_name(api_params, bq_params, table_name)
        if prog_name not in program_tables_json:
            program_tables_json[prog_name] = dict()

        program_tables_json[prog_name][table_name] = table_json_attr

    return program_tables_json


def style_table_entry(table_name, table_json_attr):
    print(table_name)
    print(convert_bytes_to_largest_unit(table_json_attr['numBytes']))


def generate_docs(api_params, bq_params):
    program_tables_json = get_table_list_for_curr_release(api_params, bq_params)

    for program, tables in program_tables_json.items():
        for table, table_attrs in tables.items():
            style_table_entry(table, table_attrs)

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

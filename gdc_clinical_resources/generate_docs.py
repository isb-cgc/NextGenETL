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
import requests
from datetime import datetime


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
        if (int(obj_bytes) / pow(1024, multiplier)) < 1.0:
            break
        curr_unit = unit
        curr_size = "{:.3f}".format(int(obj_bytes) / pow(1024, multiplier))

    return curr_unit, curr_size


def convert_milliseconds_to_date(milli_time):
    sec_time = int(milli_time) / 1000
    d_time = datetime.fromtimestamp(sec_time)
    return d_time.strftime("%Y-%d-%b %H:%M:%S")


def get_table_list_for_curr_release(api_params, bq_params):
    dataset_id = bq_params['WORKING_PROJECT'] + '.' + bq_params['TARGET_DATASET']
    client = bigquery.Client()
    table_iter = client.list_tables(dataset_id)
    program_tables_json = dict()

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
    unit, size = convert_bytes_to_largest_unit(table_json_attr['numBytes'])
    # print("\t{} - {} {}".format(table_name, size, unit))

    print("\t{}table created on: "
          .format(convert_milliseconds_to_date(table_json_attr['creationTime'])))


def generate_docs(api_params, bq_params):
    program_tables_json = get_table_list_for_curr_release(api_params, bq_params)
    # print(program_tables_json)

    for program, tables in program_tables_json.items():
        print("For program {}:".format(program))
        print("\ttable list: {}".format(tables.keys()))

        for table, table_attrs in tables.items():
            style_table_entry(table, table_attrs)


"""
def test_query(api_params, bq_params):
    output_dict = dict()
    duplicates = dict()
    case_ids = ['2c92f9e1-b7ec-41c7-b547-b32ede1c2f66',
                '65776049-6657-5d84-85c7-d27280ef4b04',
                '53914222-f871-4b18-bfc0-9c2e848044aa',
                '713bfc2b-ca29-55e0-a876-19d221d0424a',
                '3ee4a9bf-cf89-4c72-a8fe-371c6e9d1910',
                'f2f4da54-b55b-5abe-866a-6c7b42ef1ced',
                'f7ba2a46-81d2-42a8-80be-4c7106a5b9ba',
                'eb0397d4-b7db-5733-bbb5-f3ac1825235c',
                'defe33ea-3792-41d6-877e-e322ab562ff3',
                '06a7148f-9c3c-51d2-82d1-9ba30801beaa',
                '6c32a0e9-8d6d-492f-a6f9-46bbe9962016',
                '39213a0a-5a39-5728-a959-33f23c0768a5',
                '10954a0e-6aca-55ac-8505-dacc4fb5e62a',
                '24e70d99-6ccb-4f7e-996f-cc732c6c8ed4',
                'e818e557-99de-5b01-8d9c-3f913a150ac0',
                '0faf3b5d-1277-4d1e-9445-57721610077d',
                'eccd32cd-c463-44d9-9d01-a85d8a53dab6',
                '68d219ef-4d95-5269-a71c-2b22ec1495b6',
                '7a162401-ae44-5f34-ba50-870071fe2f53',
                '7899f8bf-e32a-4b8d-bac2-bc424ba2975a']

    for case_id in case_ids:
        res = requests.get(api_params['ENDPOINT'] + '/' + case_id)
        res_json = res.json()

        submitter_id = res_json['data']['submitter_id']

        if submitter_id not in duplicates:
            duplicates[submitter_id] = []

        duplicates[submitter_id].append(res_json['data'])

    for submitter_id in duplicates:
        output_dict[submitter_id] = dict()
        case_0 = duplicates[submitter_id][0]
        case_1 = duplicates[submitter_id][1]

        keys = case_0.keys() | case_1.keys()

        shared_values = dict()
        different_values = dict()
        case_0_values = dict()
        case_1_values = dict()

        for key in keys:
            if key in case_0 and key in case_1:
                if case_0[key] == case_1[key]:
                    shared_values[key] = case_0[key]
                else:
                    different_values[key] = [case_0[key], case_1[key]]
            elif key in case_0:
                case_0_values[key] = case_0[key]
            elif key in case_1:
                case_1_values[key] = case_1[key]
            else:
                pass

        output_dict[submitter_id]['shared_values'] = shared_values
        output_dict[submitter_id]['different_values'] = different_values
        output_dict[submitter_id]['case_0_values'] = case_0_values
        output_dict[submitter_id]['case_1_values'] = case_1_values

    for submitter_id in output_dict:
        print(submitter_id)
        print(output_dict[submitter_id]['different_values']['created_datetime'])
        print(output_dict[submitter_id]['different_values']['updated_datetime'])
"""

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

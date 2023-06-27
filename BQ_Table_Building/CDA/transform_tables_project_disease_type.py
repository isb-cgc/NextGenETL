"""
Copyright 2023, Institute for Systems Biology

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
import sys

from common_etl.support import bq_harness_with_result
from common_etl.utils import load_config, has_fatal_error, write_list_to_jsonl_and_upload, \
    create_and_upload_schema_for_json, retrieve_bq_schema_object, create_and_load_table_from_jsonl

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def create_dev_table_id(table_name) -> str:
    return f"`{BQ_PARAMS['WORKING_PROJECT']}.{BQ_PARAMS['WORKING_DATASET']}.{API_PARAMS['RELEASE']}_{table_name}`"


def create_merged_project_studies_disease_type_jsonl():
    def make_project_studies_disease_type_query():
        return f"""
        SELECT *
        FROM {create_dev_table_id('project_studies_disease_type')}
        """

    print(make_project_studies_disease_type_query())

    result = bq_harness_with_result(sql=make_project_studies_disease_type_query(), do_batch=False, verbose=False)

    project_disease_type_dict = dict()

    for row in result:
        project = row.get('project_id')
        disease_type = row.get('disease_type')

        if project not in project_disease_type_dict:
            project_disease_type_dict[project] = {disease_type}
        else:
            project_disease_type_dict[project].add(disease_type)

    project_disease_type_jsonl_list = list()

    for project_id, disease_type_set in project_disease_type_dict.items():
        if len(disease_type_set) > 8:
            disease_type_string = 'multi'
        elif len(disease_type_set) == 0:
            disease_type_string = None
        else:
            disease_type_string = ''
            for disease_type in disease_type_set:
                if disease_type is not None:
                    disease_type_string += f";{disease_type}"

            disease_type_string = disease_type_string[1:]

        project_disease_type_object = {
            'project_id': project_id,
            'disease_type': disease_type_string
        }

        project_disease_type_jsonl_list.append(project_disease_type_object)

    write_list_to_jsonl_and_upload(API_PARAMS,
                                   BQ_PARAMS,
                                   prefix=BQ_PARAMS['TABLE_NAME'],
                                   record_list=project_disease_type_jsonl_list)

    create_and_upload_schema_for_json(API_PARAMS,
                                      BQ_PARAMS,
                                      record_list=project_disease_type_jsonl_list,
                                      table_name=BQ_PARAMS['TABLE_NAME'],
                                      include_release=True)


def create_table():
    table_name = f"{BQ_PARAMS['TABLE_NAME']}_{API_PARAMS['RELEASE']}"
    table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['DEV_DATASET']}.{table_name}"
    jsonl_file = f"{table_name}.jsonl"

    table_schema = retrieve_bq_schema_object(API_PARAMS,
                                             BQ_PARAMS,
                                             table_name=BQ_PARAMS['TABLE_NAME'],
                                             include_release=True)

    # Load jsonl data into BigQuery table
    create_and_load_table_from_jsonl(BQ_PARAMS, jsonl_file=jsonl_file, table_id=table_id, schema=table_schema)


def main(args):
    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    if 'create_jsonl_file_and_schema' in steps:
        create_merged_project_studies_disease_type_jsonl()
    if 'create_table' in steps:
        create_table()


if __name__ == "__main__":
    main(sys.argv)

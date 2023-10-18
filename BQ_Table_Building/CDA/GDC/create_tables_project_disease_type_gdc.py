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
import logging
import sys
import time

from cda_bq_etl.utils import load_config, create_dev_table_id, format_seconds
from cda_bq_etl.bq_helpers import query_and_retrieve_result, create_and_load_table_from_jsonl, \
    retrieve_bq_schema_object, create_and_upload_schema_for_json
from cda_bq_etl.data_helpers import write_list_to_jsonl_and_upload, initialize_logging

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def create_merged_project_studies_disease_type_jsonl():
    """
    Adds disease type to projects and creates a mapping jsonl file, used for table creation, generates a schema,
    and uploads them both to GCS bucket.
    """
    def make_project_studies_disease_type_query() -> str:
        return f"""
        SELECT *
        FROM `{create_dev_table_id(PARAMS, 'project_studies_disease_type')}`
        """

    logger = logging.getLogger('base_script')

    result = query_and_retrieve_result(sql=make_project_studies_disease_type_query())

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
        logger.debug(disease_type_set)
        if len(disease_type_set) > 8:
            disease_type_string = 'multi'
        elif len(disease_type_set) == 0:
            disease_type_string = None
        else:
            disease_type_string = ''
            for disease_type in sorted(disease_type_set):
                if disease_type is not None:
                    disease_type_string += f";{disease_type}"

            disease_type_string = disease_type_string[1:]

        project_disease_type_object = {
            'project_id': project_id,
            'disease_type': disease_type_string
        }

        project_disease_type_jsonl_list.append(project_disease_type_object)

    write_list_to_jsonl_and_upload(PARAMS,
                                   prefix=PARAMS['TABLE_NAME'],
                                   record_list=project_disease_type_jsonl_list)

    create_and_upload_schema_for_json(PARAMS,
                                      record_list=project_disease_type_jsonl_list,
                                      table_name=PARAMS['TABLE_NAME'],
                                      include_release=True)


def main(args):
    try:
        start_time = time.time()

        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        sys.exit(err)

    log_file_time = time.strftime('%Y.%m.%d-%H.%M.%S', time.localtime())
    log_filepath = f"{PARAMS['LOGFILE_PATH']}.{log_file_time}"
    logger = initialize_logging(log_filepath)

    if 'create_jsonl_file_and_schema' in steps:
        logger.info("Entering create_jsonl_file_and_schema")
        create_merged_project_studies_disease_type_jsonl()
    if 'create_table' in steps:
        logger.info("Entering create_table")
        table_id = create_dev_table_id(PARAMS, PARAMS['TABLE_NAME'])
        jsonl_file = f"{PARAMS['TABLE_NAME']}_{PARAMS['RELEASE']}.jsonl"

        table_schema = retrieve_bq_schema_object(PARAMS,
                                                 table_name=PARAMS['TABLE_NAME'],
                                                 include_release=True)

        # Load jsonl data into BigQuery table
        create_and_load_table_from_jsonl(PARAMS, jsonl_file=jsonl_file, table_id=table_id, schema=table_schema)

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)

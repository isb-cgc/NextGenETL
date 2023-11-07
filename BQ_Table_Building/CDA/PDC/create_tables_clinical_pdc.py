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
import time

from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.utils import load_config, create_dev_table_id, format_seconds, create_clinical_table_id
from cda_bq_etl.bq_helpers import (create_table_from_query, get_pdc_projects_metadata, get_project_level_schema_tags,
                                   update_table_schema_from_generic)

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_clinical_table_query(project_submitter_id: str) -> str:
    pass


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

    projects_list = get_pdc_projects_metadata(PARAMS)

    if 'create_project_tables' in steps:
        logger.info("Entering create_project_tables")

        for project in projects_list:
            project_table_base_name = f"{project['project_short_name']}_{PARAMS['TABLE_NAME']}"
            project_table_id = create_clinical_table_id(PARAMS, project_table_base_name)

            create_table_from_query(params=PARAMS,
                                    table_id=project_table_id,
                                    query=make_clinical_table_query(project['project_submitter_id']))

            schema_tags = get_project_level_schema_tags(PARAMS, project['project_submitter_id'])

            if 'program-name-1-lower' in schema_tags:
                generic_table_metadata_file = PARAMS['GENERIC_TABLE_METADATA_FILE_2_PROGRAM']
            else:
                generic_table_metadata_file = PARAMS['GENERIC_TABLE_METADATA_FILE']

            update_table_schema_from_generic(params=PARAMS,
                                             table_id=project_table_id,
                                             schema_tags=schema_tags,
                                             metadata_file=generic_table_metadata_file)
    end_time = time.time()

    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)

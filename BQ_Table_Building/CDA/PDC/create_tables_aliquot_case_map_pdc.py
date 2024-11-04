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

from cda_bq_etl.utils import load_config, format_seconds, create_dev_table_id, create_metadata_table_id
from cda_bq_etl.bq_helpers import create_table_from_query, update_table_schema_from_generic
from cda_bq_etl.data_helpers import initialize_logging

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_aliquot_table_query() -> str:
    """
    Make BigQuery sql statement, used to generate the aliquot_to_case_mapping table.
    :return: sql query statement
    """
    return f"""
        SELECT 
            prog.name AS program_name,
            proj.name AS project_name,
            c.case_id,
            c.case_submitter_id,
            s.sample_id,
            s.sample_submitter_id,
            s.sample_type,
            s.preservation_method,
            s.freezing_method,
            s.time_between_clamping_and_freezing,
            s.time_between_excision_and_freezing,
            s.days_to_collection,
            s.initial_weight,
            s.current_weight,
            s.shortest_dimension,
            s.intermediate_dimension,
            s.longest_dimension, 
            a.aliquot_id, 
            a.aliquot_submitter_id
        FROM `{create_dev_table_id(PARAMS, "aliquot")}` a
        JOIN `{create_dev_table_id(PARAMS, "sample_aliquot_id")}` sa
            ON a.aliquot_id = sa.aliquot_id
        JOIN `{create_dev_table_id(PARAMS, "sample")}` s
            ON sa.sample_id = s.sample_id
        JOIN `{create_dev_table_id(PARAMS, "case_sample_id")}` cs
            ON cs.sample_id = s.sample_id
        JOIN `{create_dev_table_id(PARAMS, "case")}` c
            ON cs.case_id = c.case_id
        JOIN `{create_dev_table_id(PARAMS, "case_project_id")}` cp
            ON cp.case_id = c.case_id
        JOIN `{create_dev_table_id(PARAMS, "project")}` proj
            ON proj.project_id = cp.project_id
        JOIN `{create_dev_table_id(PARAMS, "program_project_id")}` pp
            ON pp.project_id = proj.project_id
        JOIN `{create_dev_table_id(PARAMS, "program")}` prog
            ON prog.program_id = pp.program_id
    """


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

    dev_table_id = create_metadata_table_id(PARAMS, PARAMS['TABLE_NAME'])

    if 'create_table_from_query' in steps:
        logger.info("Entering create_table_from_query")

        create_table_from_query(params=PARAMS, table_id=dev_table_id, query=make_aliquot_table_query())

        update_table_schema_from_generic(params=PARAMS, table_id=dev_table_id)

    end_time = time.time()

    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)

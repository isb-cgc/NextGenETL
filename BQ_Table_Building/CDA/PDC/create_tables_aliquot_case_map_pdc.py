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

from cda_bq_etl.utils import load_config, has_fatal_error, format_seconds
from cda_bq_etl.bq_helpers import load_table_from_query, publish_table, update_table_schema_from_generic

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
        FROM `isb-project-zero.cda_pdc_raw.2023_06_aliquot` a
        JOIN `isb-project-zero.cda_pdc_raw.2023_06_sample_aliquot_id` sa
            ON a.aliquot_id = sa.aliquot_id
        JOIN `isb-project-zero.cda_pdc_raw.2023_06_sample` s
            ON sa.sample_id = s.sample_id
        JOIN `isb-project-zero.cda_pdc_raw.2023_06_case_sample_id` cs
            ON cs.sample_id = s.sample_id
        JOIN `isb-project-zero.cda_pdc_raw.2023_06_case` c
            ON cs.case_id = c.case_id
        JOIN `isb-project-zero.cda_pdc_raw.2023_06_case_project_id` cp
            ON cp.case_id = c.case_id
        JOIN `isb-project-zero.cda_pdc_raw.2023_06_project` proj
            ON proj.project_id = cp.project_id
        JOIN `isb-project-zero.cda_pdc_raw.2023_06_program_project_id` pp
            ON pp.project_id = proj.project_id
        JOIN `isb-project-zero.cda_pdc_raw.2023_06_program` prog
            ON prog.program_id = pp.program_id
    """


def main(args):
    try:
        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    start_time = time.time()

    dev_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_METADATA_DATASET']}.{PARAMS['TABLE_NAME']}_{PARAMS['RELEASE']}"

    if 'create_table_from_query' in steps:
        load_table_from_query(params=PARAMS, table_id=dev_table_id, query=make_aliquot_table_query())

        update_table_schema_from_generic(params=PARAMS, table_id=dev_table_id)

    if 'publish_tables' in steps:
        current_table_name = f"{PARAMS['TABLE_NAME']}_current"
        current_table_id = f"{PARAMS['PROD_PROJECT']}.{PARAMS['PROD_DATASET']}.{current_table_name}"
        versioned_table_name = f"{PARAMS['TABLE_NAME']}_{PARAMS['DC_RELEASE']}"
        versioned_table_id = f"{PARAMS['PROD_PROJECT']}.{PARAMS['PROD_DATASET']}_versioned.{versioned_table_name}"

        publish_table(params=PARAMS,
                      source_table_id=dev_table_id,
                      current_table_id=current_table_id,
                      versioned_table_id=versioned_table_id)

    end_time = time.time()

    print(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)

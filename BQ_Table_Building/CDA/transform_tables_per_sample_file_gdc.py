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
from common_etl.utils import load_config, has_fatal_error, load_table_from_query

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def create_program_name_set():
    def make_program_name_set_query():
        return f"""
        SELECT DISTINCT program_name
        FROM `{BQ_PARAMS['WORKING_PROJECT']}.{BQ_PARAMS['WORKING_DATASET']}.{API_PARAMS['RELEASE']}_case_project_program`
        """

    result = bq_harness_with_result(sql=make_program_name_set_query(), do_batch=False, verbose=False)

    program_name_set = set()

    for row in result:
        program_name_set.add(row[0])

    return program_name_set


def make_per_sample_file_program_query(program: str):
    return f"""
    SELECT 
      fm.file_gdc_id,
      cpp.case_gdc_id,
      cpp.case_barcode,
      s.sample_id AS sample_gdc_id,
      s.submitter_id AS sample_barcode,
      s.sample_type AS sample_type_name,
      cpp.project_id AS project_short_name,
      REGEXP_EXTRACT(cpp.project_id, r'^[^-]*-(.*)$') AS project_short_name_suffix,
      cpp.program_name,
      fm.data_type,
      fm.data_category,
      fm.experimental_strategy,
      fm.file_type,
      fm.file_size,
      fm.data_format,
      fm.platform,
      fm.file_name AS file_name_key,
      fm.index_file_gdc_id AS index_file_id,
      fm.index_file_name AS index_file_name_key,
      fm.index_file_size,
      fm.`access`,
      fm.acl
    FROM `{BQ_PARAMS['WORKING_PROJECT']}.{BQ_PARAMS['WORKING_DATASET']}.{API_PARAMS['RELEASE']}_sample` s
    LEFT JOIN `{BQ_PARAMS['WORKING_PROJECT']}.{BQ_PARAMS['WORKING_DATASET']}.{API_PARAMS['RELEASE']}_sample_from_case` sfc
      ON sfc.sample_id = s.sample_id
    LEFT JOIN `{BQ_PARAMS['WORKING_PROJECT']}.{BQ_PARAMS['WORKING_DATASET']}.file_metadata_{API_PARAMS['RELEASE']}` fm
      ON fm.case_gdc_id = sfc.case_id
    LEFT JOIN `{BQ_PARAMS['WORKING_PROJECT']}.{BQ_PARAMS['WORKING_DATASET']}.{API_PARAMS['RELEASE']}_case_project_program` cpp
      ON cpp.case_gdc_id = sfc.case_id
    WHERE cpp.program_name = '{program}'
    """


def main(args):
    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    if 'create_program_tables' in steps:
        program_set = create_program_name_set()

        for program in program_set:
            if program == "BEATAML1.0":
                program_name = "BEATAML1_0"
            elif program == "EXCEPTIONAL_RESPONDERS":
                program_name = "EXC_RESPONDERS"
            else:
                program_name = program

            table_name = f"per_sample_file_metadata_hg38_{program_name}_{API_PARAMS['RELEASE']}"
            table_id = f"{BQ_PARAMS['WORKING_PROJECT']}.{BQ_PARAMS['WORKING_DATASET']}.{table_name}"

            load_table_from_query(bq_params=BQ_PARAMS,
                                  table_id=table_id,
                                  query=make_per_sample_file_program_query(program))


if __name__ == "__main__":
    main(sys.argv)

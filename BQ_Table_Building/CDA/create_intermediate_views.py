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

from cda_bq_etl.utils import load_config, has_fatal_error
from cda_bq_etl.bq_helpers import create_view_from_query

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_case_project_program_view_query():
    """
    todo
    :return:
    """
    release = PARAMS['RELEASE']
    working_project = PARAMS['WORKING_PROJECT']
    working_dataset = PARAMS['WORKING_DATASET']

    return f"""
        SELECT 
            case_proj.case_id AS case_gdc_id,
            c.submitter_id AS case_barcode,
            proj.dbgap_accession_number AS project_dbgap_accession_number,
            proj.project_id, 
            proj.name AS project_name,
            prog.name AS program_name,
            prog.dbgap_accession_number AS program_dbgap_accession_number
        FROM `{working_project}.{working_dataset}.{release}_project` proj
        JOIN `{working_project}.{working_dataset}.{release}_project_in_program` proj_prog
            ON proj.project_id = proj_prog.project_id
        JOIN `{working_project}.{working_dataset}.{release}_program` prog
            ON proj_prog.program_id = prog.program_id
        JOIN `{working_project}.{working_dataset}.{release}_case_in_project` case_proj
            ON case_proj.project_id = proj.project_id
        JOIN `{working_project}.{working_dataset}.{release}_case` c
            ON c.case_id = case_proj.case_id
    """


def main(args):
    try:
        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    if 'create_case_project_program_view' in steps:
        view_id = f"{PARAMS['WORKING_PROJECT']}.{PARAMS['WORKING_DATASET']}.{PARAMS['RELEASE']}_case_project_program"
        create_view_from_query(view_id=view_id, view_query=make_case_project_program_view_query())


if __name__ == "__main__":
    main(sys.argv)

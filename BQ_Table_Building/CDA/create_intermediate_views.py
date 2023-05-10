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
from common_etl.utils import create_view_from_query


def create_project_program_view(bq_params):
    def make_project_program_view_query():
        return f"""
            SELECT 
                case_proj.case_id AS case_gdc_id,
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
        """

    working_project = bq_params['WORKING_PROJECT']
    working_dataset = bq_params['WORKING_DATASET']
    release = bq_params['RELEASE']

    view_id = f"{working_project}.{working_dataset}.{release}_case_project_program"

    create_view_from_query(view_id=view_id, view_query=make_project_program_view_query())


def main(args):
    pass


if __name__ == "__main__":
    main(sys.argv)

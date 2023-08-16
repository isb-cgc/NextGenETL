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

from cda_bq_etl.data_helpers import get_pdc_projects_list
from cda_bq_etl.utils import load_config, has_fatal_error, create_dev_table_id, format_seconds
from cda_bq_etl.bq_helpers import load_table_from_query, publish_table

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_project_per_sample_file_query(project_submitter_id):
    return f"""
        WITH file_instruments AS (
            SELECT file_id, 
                STRING_AGG(DISTINCT instrument, ';') AS instruments
            FROM `{create_dev_table_id(PARAMS, 'file_instrument')}`
            GROUP BY file_id
        )

        SELECT f.file_id,
            c.case_id,
            c.case_submitter_id,
            s.sample_id,
            s.sample_submitter_id,
            s.sample_type,
            study.project_short_name,
            study.project_submitter_id,
            study.program_short_name,
            study.program_name,
            f.data_category,
            f.experiment_type,
            f.file_type,
            f.file_size,
            f.file_format,
            fi.instruments AS instrument,
            f.file_name,
            f.file_location,
            "open" AS `access`
        FROM `{create_dev_table_id(PARAMS, 'file')}` f
        JOIN `{create_dev_table_id(PARAMS, 'file_case_id')}` fc
            ON fc.file_id = f.file_id
        JOIN `{create_dev_table_id(PARAMS, 'case')}` c
            ON c.case_id = fc.case_id
        JOIN `{create_dev_table_id(PARAMS, 'sample_case_id')}` sc
            ON sc.case_id = c.case_id
        JOIN `{create_dev_table_id(PARAMS, 'sample')}` s
            ON s.sample_id = sc.sample_id
        JOIN `{create_dev_table_id(PARAMS, 'file_study_id')}` fs
            ON fs.file_id = f.file_id
        JOIN `{create_dev_table_id(PARAMS, 'study')}` study
            ON study.study_id = fs.study_id
        JOIN file_instruments fi
            ON fi.file_id = f.file_id
        WHERE study.project_submitter_id = '{project_submitter_id}'
    """


def main(args):
    try:
        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    start_time = time.time()

    projects_list = get_pdc_projects_list(PARAMS)

    if 'create_project_tables' in steps:
        for project in projects_list:
            project_table_name = f"{PARAMS['TABLE_NAME']}_{project['project_short_name']}_{PARAMS['RELEASE']}"
            project_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_SAMPLE_DATASET']}.{project_table_name}"

            load_table_from_query(params=PARAMS,
                                  table_id=project_table_id,
                                  query=make_project_per_sample_file_query(project['project_submitter_id']))

            if

    end_time = time.time()

    print(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)

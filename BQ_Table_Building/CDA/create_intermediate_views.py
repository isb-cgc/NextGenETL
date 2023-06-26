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
from common_etl.utils import create_view_from_query, load_config, has_fatal_error

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def make_project_program_view_query():
    release = API_PARAMS['RELEASE']
    working_project = BQ_PARAMS['WORKING_PROJECT']
    working_dataset = BQ_PARAMS['WORKING_DATASET']

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


def make_aliquot_case_legacy_filtered_query():
    working_project = BQ_PARAMS['WORKING_PROJECT']
    working_dataset = BQ_PARAMS['WORKING_DATASET']
    published_project = BQ_PARAMS['PUBLISHED_PROJECT']
    published_dataset = BQ_PARAMS['PUBLISHED_DATASET']
    release = API_PARAMS['RELEASE']
    gdc_archive_release = API_PARAMS['GDC_ARCHIVE_RELEASE']

    return f"""
    SELECT * 
    FROM `{published_project}.{published_dataset}.aliquot2caseIDmap_{gdc_archive_release}`
    WHERE portion_gdc_id NOT IN (
      SELECT portion_gdc_id
      FROM `{working_project}.{working_dataset}.aliquot_to_case_{release}`
    )
    """


def make_case_metadata_legacy_filtered_query():
    working_project = BQ_PARAMS['WORKING_PROJECT']
    working_dataset = BQ_PARAMS['WORKING_DATASET']
    published_project = BQ_PARAMS['PUBLISHED_PROJECT']
    published_dataset = BQ_PARAMS['PUBLISHED_DATASET']
    release = BQ_PARAMS['RELEASE']
    gdc_archive_release = API_PARAMS['GDC_ARCHIVE_RELEASE']

    # query filters out any file/case ids that have no DCF file references

    return f"""
    SELECT * 
    FROM `{published_project}.{published_dataset}.caseData_{gdc_archive_release}`
    WHERE case_gdc_id NOT IN (
      SELECT case_gdc_id 
      FROM `{working_project}.{working_dataset}.case_metadata_{release}`
    ) AND case_gdc_id IN (
      SELECT case_gdc_id
      FROM `{published_project}.{published_dataset}.fileData_legacy_{gdc_archive_release}`
      JOIN `{published_project}.{published_dataset}.GDCfileID_to_GCSurl_{gdc_archive_release}`
        USING(file_gdc_id)
    )
    """


def make_file_metadata_legacy_filtered_query():
    working_project = BQ_PARAMS['WORKING_PROJECT']
    working_dataset = BQ_PARAMS['WORKING_DATASET']
    published_project = BQ_PARAMS['PUBLISHED_PROJECT']
    published_dataset = BQ_PARAMS['PUBLISHED_DATASET']
    release = BQ_PARAMS['RELEASE']
    gdc_archive_release = API_PARAMS['GDC_ARCHIVE_RELEASE']

    # query filters out any file/case ids that have no DCF file references

    return f"""
    SELECT * 
    FROM `{published_project}.{published_dataset}.fileData_legacy_{gdc_archive_release}`
    WHERE file_gdc_id NOT IN (
      SELECT file_gdc_id 
      FROM `{working_project}.{working_dataset}.file_metadata_{release}`
    ) AND file_gdc_id IN (
      SELECT file_gdc_id
      FROM `{published_project}.{published_dataset}.GDCfileID_to_GCSurl_{gdc_archive_release}`
    )
    """


def main(args):
    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    working_project = BQ_PARAMS['WORKING_PROJECT']
    working_dataset = BQ_PARAMS['WORKING_DATASET']
    release = API_PARAMS['RELEASE']
    gdc_archive_release = API_PARAMS['GDC_ARCHIVE_RELEASE']

    if 'create_project_program_view' in steps:
        view_id = f"{working_project}.{working_dataset}.{release}_case_project_program"

        create_view_from_query(view_id=view_id, view_query=make_project_program_view_query())

    if 'create_aliquot_case_legacy_filtered_view' in steps:
        view_id = f"{working_project}.{working_dataset}.{gdc_archive_release}_aliquot_to_case_legacy_filtered"

        create_view_from_query(view_id=view_id, view_query=make_aliquot_case_legacy_filtered_query())

    if 'create_case_metadata_legacy_filtered_view' in steps:
        view_id = f"{working_project}.{working_dataset}.{gdc_archive_release}_case_metadata_legacy_filtered"

        create_view_from_query(view_id=view_id, view_query=make_case_metadata_legacy_filtered_query())

    if 'create_file_metadata_legacy_filtered_view' in steps:
        view_id = f"{working_project}.{working_dataset}.{gdc_archive_release}_file_metadata_legacy_filtered"

        create_view_from_query(view_id=view_id, view_query=make_file_metadata_legacy_filtered_query())


if __name__ == "__main__":
    main(sys.argv)

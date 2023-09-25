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
from cda_bq_etl.utils import load_config, create_dev_table_id
from cda_bq_etl.bq_helpers import create_view_from_query

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_case_project_program_view_query():
    """
    Make SQL query used to create a BigQuery view, merging case ids and barcodes with project and program metadata.
    """
    return f"""
        SELECT 
            case_proj.case_id AS case_gdc_id,
            c.submitter_id AS case_barcode,
            proj.dbgap_accession_number AS project_dbgap_accession_number,
            proj.project_id, 
            proj.name AS project_name,
            prog.name AS program_name,
            prog.dbgap_accession_number AS program_dbgap_accession_number
        FROM `{create_dev_table_id(PARAMS, 'project')}` proj
        JOIN `{create_dev_table_id(PARAMS, 'project_in_program')}` proj_prog
            ON proj.project_id = proj_prog.project_id
        JOIN `{create_dev_table_id(PARAMS, 'program')}` prog
            ON proj_prog.program_id = prog.program_id
        JOIN `{create_dev_table_id(PARAMS, 'case_in_project')}` case_proj
            ON case_proj.project_id = proj.project_id
        JOIN `{create_dev_table_id(PARAMS, 'case')}` c
            ON c.case_id = case_proj.case_id
    """


def make_treatment_diagnosis_case_query() -> str:
    return f"""
        SELECT treatment_id, diagnosis_id, case_id
        FROM `{create_dev_table_id(PARAMS, 'treatment_of_diagnosis')}`
        JOIN `{create_dev_table_id(PARAMS, 'diagnosis_of_case')}`
            USING(diagnosis_id)
    """


def make_pathology_detail_diagnosis_case_query() -> str:
    return f"""
        SELECT pathology_detail_id, diagnosis_id, case_id
        FROM `{create_dev_table_id(PARAMS, 'pathology_detail_of_diagnosis')}`
        JOIN `{create_dev_table_id(PARAMS, 'diagnosis_of_case')}`
            USING(diagnosis_id)
    """


def make_annotation_diagnosis_case_query() -> str:
    return f"""
        SELECT pathology_detail_id, diagnosis_id, case_id
        FROM `{create_dev_table_id(PARAMS, 'diagnosis_has_annotation')}`
        JOIN `{create_dev_table_id(PARAMS, 'diagnosis_of_case')}`
            USING(diagnosis_id)
    """


def make_molecular_test_follow_up_case_query() -> str:
    return f"""
        SELECT molecular_test_id, follow_up_id, case_id
        FROM `{create_dev_table_id(PARAMS, 'molecular_test_from_follow_up')}`
        JOIN `{create_dev_table_id(PARAMS, 'follow_up_of_case')}`
            USING(follow_up_id)
    """


def main(args):
    try:
        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        sys.exit(err)

    log_file_time = time.strftime('%Y.%m.%d-%H.%M.%S', time.localtime())
    log_filepath = f"{PARAMS['LOGFILE_PATH']}.{log_file_time}"
    logger = initialize_logging(log_filepath)

    if 'create_case_project_program_view' in steps:
        logger.info("Making case project program view!")

        create_view_from_query(view_id=create_dev_table_id(PARAMS, 'case_project_program'),
                               view_query=make_case_project_program_view_query())

    if 'create_clinical_views' in steps:
        logger.info("Making treatment_diagnosis_case_id_map view!")

        create_view_from_query(view_id=create_dev_table_id(PARAMS, 'treatment_diagnosis_case_id_map'),
                               view_query=make_treatment_diagnosis_case_query())

        logger.info("Making pathology_detail_diagnosis_case_id_map view!")

        create_view_from_query(view_id=create_dev_table_id(PARAMS, 'pathology_detail_diagnosis_case_id_map'),
                               view_query=make_pathology_detail_diagnosis_case_query())

        logger.info("Making annotation_diagnosis_case_id_map view!")

        create_view_from_query(view_id=create_dev_table_id(PARAMS, 'annotation_diagnosis_case_id_map'),
                               view_query=make_annotation_diagnosis_case_query())

        logger.info("Making molecular_test_follow_up_case_id_map view!")

        create_view_from_query(view_id=create_dev_table_id(PARAMS, 'molecular_test_follow_up_case_id_map'),
                               view_query=make_molecular_test_follow_up_case_query())


if __name__ == "__main__":
    main(sys.argv)

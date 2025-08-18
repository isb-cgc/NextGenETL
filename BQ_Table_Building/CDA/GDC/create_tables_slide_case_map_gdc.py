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
from cda_bq_etl.utils import load_config, create_dev_table_id, format_seconds, create_metadata_table_id
from cda_bq_etl.bq_helpers.create_modify import create_table_from_query, update_table_schema_from_generic

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_slide_case_table_sql() -> str:
    """
    Make BigQuery sql statement that is used to create the slide to case map table
    :return: slide to case map sql query
    """
    return f"""
    (
        SELECT 
            cpp.program_name, 
            cpp.project_id,
            cpp.case_gdc_id, 
            cpp.case_barcode,
            s.sample_id AS sample_gdc_id,
            s.submitter_id AS sample_barcode,
            s.sample_type_id AS sample_type,
            s.sample_type AS sample_type_name,
            p.portion_id AS portion_gdc_id,
            p.submitter_id AS portion_barcode,
            sl.slide_id AS slide_gdc_id,
            sl.submitter_id AS slide_barcode
        FROM `{create_dev_table_id(PARAMS, 'slide')}` sl
        JOIN `{create_dev_table_id(PARAMS, 'slide_from_portion')}` sfp
            ON sfp.slide_id = sl.slide_id
        JOIN `{create_dev_table_id(PARAMS, 'portion')}` p 
            ON p.portion_id = sfp.portion_id
        JOIN `{create_dev_table_id(PARAMS, 'portion_from_sample')}` pfs  
            ON pfs.portion_id = p.portion_id
        JOIN `{create_dev_table_id(PARAMS, 'sample')}` s
            ON s.sample_id = pfs.sample_id
        JOIN `{create_dev_table_id(PARAMS, 'sample_from_case')}` sic
            ON sic.sample_id = s.sample_id
        JOIN `{create_dev_table_id(PARAMS, 'case_project_program')}` cpp
            ON cpp.case_gdc_id = sic.case_id
    ) UNION DISTINCT (
        SELECT program_name, 
            project_id,
            case_gdc_id, 
            case_barcode,
            sample_gdc_id,
            sample_barcode,
            sample_type,
            sample_type_name,
            portion_gdc_id,
            portion_barcode,
            slide_gdc_id,
            slide_barcode
        FROM `{PARAMS['LEGACY_TABLE_ID']}` 
    )
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

    if 'create_table_from_query' in steps:
        logger.info("Entering create_table_from_query")
        create_table_from_query(params=PARAMS,
                                table_id=create_metadata_table_id(PARAMS, PARAMS['TABLE_NAME']),
                                query=make_slide_case_table_sql())

        update_table_schema_from_generic(params=PARAMS, table_id=create_metadata_table_id(PARAMS, PARAMS['TABLE_NAME']))

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)

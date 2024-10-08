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
from cda_bq_etl.bq_helpers import create_table_from_query, update_table_schema_from_generic

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_file_metadata_query() -> str:
    """
    Make BigQuery sql statement, used to generate the file_metadata table.
    :return: sql query statement
    """

    return f"""
        WITH file_instruments AS (
            SELECT file_id, 
                STRING_AGG(DISTINCT instrument, ';' ORDER BY instrument) AS instruments
            FROM `{create_dev_table_id(PARAMS, 'file_instrument')}`
            GROUP BY file_id
        ), study_ids AS (
            SELECT fs.file_id,
                STRING_AGG(DISTINCT s.pdc_study_id, ';' ORDER BY s.pdc_study_id) as pdc_study_ids
            FROM `{create_dev_table_id(PARAMS, 'file_study_id')}` fs
            JOIN `{create_dev_table_id(PARAMS, 'study')}` s
                ON s.study_id = fs.study_id
            GROUP BY fs.file_id
        )
        
        SELECT f.file_id,
            f.file_name,
            si.pdc_study_ids,
            srm.study_run_metadata_id,
            # these replace clauses are to circumvent formatting issues in PDC V3.8 data, 
            # hopefully will be resolved--then they can be removed
            REPLACE(srm.study_run_metadata_submitter_id, '\\\\t', '') AS study_run_metadata_submitter_id,
            f.file_format,
            f.file_type,
            f.data_category,
            f.file_size,
            REPLACE(f.fraction_number, '\\\\r', '') AS fraction_number,
            f.experiment_type,
            REPLACE(f.plex_or_dataset_name, '\\\\t', '') AS plex_or_dataset_name,
            f.analyte,
            fi.instruments AS instrument,
            f.md5sum,
            "open" AS `access`
        FROM `{create_dev_table_id(PARAMS, 'file')}` f
        LEFT JOIN study_ids si
            ON si.file_id = f.file_id
        LEFT JOIN `{create_dev_table_id(PARAMS, 'file_study_run_metadata_ids')}` srm
            ON srm.file_id = f.file_id
        LEFT JOIN file_instruments fi
            ON fi.file_id = f.file_id
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

        dev_table_id = create_metadata_table_id(PARAMS, PARAMS['TABLE_NAME'])

        create_table_from_query(params=PARAMS, table_id=dev_table_id, query=make_file_metadata_query())
        update_table_schema_from_generic(params=PARAMS, table_id=dev_table_id)

    end_time = time.time()

    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)

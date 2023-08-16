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

from cda_bq_etl.utils import load_config, has_fatal_error, create_dev_table_id, format_seconds
from cda_bq_etl.bq_helpers import load_table_from_query, publish_table, update_table_schema_from_generic

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
                STRING_AGG(DISTINCT instrument, ';') AS instruments
            FROM `{create_dev_table_id(PARAMS, 'file_instrument')}`
            GROUP BY file_id
        )
        # todo can remove this?
        , embargoes AS (
            SELECT fs.file_id,
                STRING_AGG(DISTINCT s.embargo_date, ';') AS embargo_dates
            FROM `{create_dev_table_id(PARAMS, 'file_study_id')}` fs
            JOIN `{create_dev_table_id(PARAMS, 'study')}` s
                ON s.study_id = fs.study_id
            GROUP BY fs.file_id
        ), study_ids AS (
            SELECT fs.file_id,
                STRING_AGG(DISTINCT s.pdc_study_id, ';') as pdc_study_ids
            FROM `{create_dev_table_id(PARAMS, 'file_study_id')}` fs
            JOIN `{create_dev_table_id(PARAMS, 'study')}` s
                ON s.study_id = fs.study_id
            GROUP BY fs.file_id
        )
        
        SELECT f.file_id,
            f.file_name,
            # e.embargo_dates AS embargo_date,
            si.pdc_study_ids,
            srm.study_run_metadata_id,
            srm.study_run_metadata_submitter_id,
            f.file_format,
            f.file_type,
            f.data_category,
            f.file_size,
            f.fraction_number,
            f.experiment_type,
            f.plex_or_dataset_name,
            f.analyte,
            fi.instruments AS instrument,
            f.md5sum,
            "open" AS `access`
        FROM `{create_dev_table_id(PARAMS, 'file')}` f
        LEFT JOIN embargoes e
            ON e.file_id = f.file_id
        LEFT JOIN study_ids si
            ON si.file_id = f.file_id
        LEFT JOIN `{create_dev_table_id(PARAMS, 'file_study_run_metadata_id')}` fsrm
            ON fsrm.file_id = f.file_id
        # todo this is currently broken in PDC pipeline, awaiting fix from CDA
        LEFT JOIN `{create_dev_table_id(PARAMS, 'studyrunmetadata')}` srm
            ON srm.study_run_metadata_id = fsrm.study_run_metadata_id
        LEFT JOIN file_instruments fi
            ON fi.file_id = f.file_id
    """


def main(args):
    try:
        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    start_time = time.time()

    dev_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_METADATA_DATASET']}.{PARAMS['TABLE_NAME']}_{PARAMS['RELEASE']}"

    print(make_file_metadata_query())

    if 'create_table_from_query' in steps:
        load_table_from_query(params=PARAMS, table_id=dev_table_id, query=make_file_metadata_query())

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

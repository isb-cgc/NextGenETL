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


def make_case_file_counts_types_sql() -> str:
    """
        WITH active_counts AS (
            SELECT case_id AS case_gdc_id, COUNT(DISTINCT file_id) AS active_file_count
            FROM `{create_dev_table_id(PARAMS, 'file_in_case')}`
            GROUP BY case_id
        ),
        WITH active_counts AS (
            SELECT entity_case_id AS case_gdc_id, COUNT(DISTINCT file_id) AS active_file_count
            FROM `{create_dev_table_id(PARAMS, 'file_associated_with_entity')}`
            GROUP BY entity_case_id
        ),

    """
    return f"""
        WITH active_counts AS (
            SELECT case_id AS case_gdc_id, COUNT(DISTINCT file_id) AS active_file_count 
            FROM `{create_dev_table_id(PARAMS, 'file_in_case')}`
            GROUP BY case_id
        ),
        legacy_counts AS (
            SELECT case_gdc_id, legacy_file_count 
            FROM `{PARAMS['LEGACY_TABLE_ID']}`
        ), active_types AS (
            SELECT c.case_id AS case_gdc_id, c.primary_site, pdt.disease_type as project_disease_type
            FROM `{create_dev_table_id(PARAMS, 'case_project_program')}` cpp
            JOIN `{create_dev_table_id(PARAMS, 'case')}` c
                ON c.case_id = cpp.case_gdc_id
            JOIN `{create_dev_table_id(PARAMS, 'project_disease_types_merged')}` pdt
                ON pdt.project_id = cpp.project_id
        ), legacy_types AS (
            SELECT case_gdc_id, primary_site, project_disease_type
            FROM `{PARAMS['LEGACY_TABLE_ID']}` 
        ), case_gdc_ids AS (
            SELECT case_gdc_id 
            FROM active_counts
            UNION DISTINCT 
            SELECT case_gdc_id
            FROM legacy_counts
        )

        SELECT c.case_gdc_id,
            IFNULL(ac.active_file_count, 0) AS active_file_count,
            IFNULL(lc.legacy_file_count, 0) AS legacy_file_count,
            IFNULL(atc.primary_site, ltc.primary_site) AS primary_site,
            IFNULL(atc.project_disease_type, ltc.project_disease_type) AS project_disease_type
        FROM case_gdc_ids c
        LEFT JOIN active_counts ac
            ON c.case_gdc_id = ac.case_gdc_id
        LEFT JOIN legacy_counts lc
            ON c.case_gdc_id = lc.case_gdc_id
        LEFT JOIN active_types atc
            ON c.case_gdc_id = atc.case_gdc_id
        LEFT JOIN legacy_types ltc
            ON c.case_gdc_id = ltc.case_gdc_id
    """


def make_case_metadata_table_sql() -> str:
    """
    Make BigQuery sql statement, used to generate case metadata table.
    :return: sql string
    """
    return f"""
        WITH cases AS (
            (
                SELECT cpp.case_gdc_id, 
                    cpp.project_dbgap_accession_number, 
                    cpp.project_name, 
                    cpp.program_dbgap_accession_number,
                    cpp.program_name, 
                    cpp.project_id, 
                    c.submitter_id AS case_barcode
                FROM `{create_dev_table_id(PARAMS, 'case_project_program')}` cpp
                JOIN `{create_dev_table_id(PARAMS, 'case')}` c
                    ON c.case_id = cpp.case_gdc_id
                JOIN `{create_dev_table_id(PARAMS, 'project_disease_types_merged')}` pdt
                    ON pdt.project_id = cpp.project_id
            ) UNION DISTINCT (
                SELECT case_gdc_id, 
                    project_dbgap_accession_number, 
                    project_name, 
                    program_dbgap_accession_number,
                    program_name, 
                    project_id, 
                    case_barcode 
                FROM `{PARAMS['LEGACY_TABLE_ID']}` 
            )
        )
        
        SELECT c.case_gdc_id, 
            counts.primary_site,
            c.project_dbgap_accession_number, 
            counts.project_disease_type,
            c.project_name, 
            c.program_dbgap_accession_number,
            c.program_name, 
            c.project_id, 
            c.case_barcode,
            IFNULL(counts.legacy_file_count, 0) AS legacy_file_count
            IFNULL(counts.active_file_count, 0) AS active_file_count
        FROM cases c
        LEFT JOIN `{create_dev_table_id(PARAMS, PARAMS['COUNT_TABLE_NAME'])}` counts
            ON c.case_gdc_id = counts.case_gdc_id
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

        # """
        create_table_from_query(params=PARAMS,
                                table_id=create_dev_table_id(PARAMS, PARAMS['COUNT_TABLE_NAME']),
                                query=make_case_file_counts_types_sql())
        # """

        create_table_from_query(params=PARAMS,
                                table_id=create_metadata_table_id(PARAMS, PARAMS['TABLE_NAME']),
                                query=make_case_metadata_table_sql())

        update_table_schema_from_generic(params=PARAMS, table_id=create_metadata_table_id(PARAMS, PARAMS['TABLE_NAME']))

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)

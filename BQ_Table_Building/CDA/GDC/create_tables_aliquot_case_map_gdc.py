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

from cda_bq_etl.utils import (load_config, create_dev_table_id, format_seconds, create_metadata_table_id,
                              create_excluded_records_table_id)
from cda_bq_etl.bq_helpers.create_modify import create_table_from_query, update_table_schema_from_generic
from cda_bq_etl.data_helpers import initialize_logging

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_aliquot_case_table_base_sql() -> str:
    """
    Partial SQL query used to build the aliquot case mapping table sql, as well as to output rows from legacy table
    which are excluded due to overlapping portion_gdc_id and aliquot_gdc_id keys in active dataset.
    :return: sql query string
    """
    return f"""
        WITH active_records AS (
            SELECT cpp.program_name, 
                cpp.project_id,
                cpp.case_gdc_id, 
                cpp.case_barcode,
                s.sample_id AS sample_gdc_id,
                s.submitter_id AS sample_barcode,
                s.sample_type_id AS sample_type,
                s.sample_type AS sample_type_name,
                s.is_ffpe AS sample_is_ffpe, 
                s.preservation_method AS sample_preservation_method,
                p.portion_id AS portion_gdc_id,
                p.submitter_id AS portion_barcode,
                an.analyte_id AS analyte_gdc_id,
                an.submitter_id AS analyte_barcode,
                al.aliquot_id AS aliquot_gdc_id,
                al.submitter_id AS aliquot_barcode        
            FROM `{create_dev_table_id(PARAMS, 'sample')}` s
            JOIN `{create_dev_table_id(PARAMS, 'sample_from_case')}` sfc
                ON s.sample_id = sfc.sample_id
            JOIN `{create_dev_table_id(PARAMS, 'case_project_program')}` cpp
                ON sfc.case_id = cpp.case_gdc_id
            JOIN `{create_dev_table_id(PARAMS, 'portion_from_sample')}` pfs
                ON s.sample_id = pfs.sample_id
            JOIN `{create_dev_table_id(PARAMS, 'portion')}` p 
                ON pfs.portion_id = p.portion_id
            LEFT JOIN `{create_dev_table_id(PARAMS, 'analyte_from_portion')}` afp
                ON p.portion_id = afp.portion_id
            LEFT JOIN `{create_dev_table_id(PARAMS, 'analyte')}` an
                ON afp.analyte_id = an.analyte_id
            LEFT JOIN `{create_dev_table_id(PARAMS, 'aliquot_of_analyte')}` aoa
                ON an.analyte_id = aoa.analyte_id
            LEFT JOIN `{create_dev_table_id(PARAMS, 'aliquot')}` al
                ON aoa.aliquot_id = al.aliquot_id
        ), legacy_records AS (
            SELECT program_name, 
                project_id,
                case_gdc_id, 
                case_barcode,
                sample_gdc_id,
                sample_barcode,
                sample_type,
                sample_type_name,
                CAST(sample_is_ffpe AS BOOL) AS sample_is_ffpe, 
                sample_preservation_method,
                portion_gdc_id,
                portion_barcode,
                analyte_gdc_id,
                analyte_barcode,
                aliquot_gdc_id,
                aliquot_barcode  
            FROM `{PARAMS['LEGACY_TABLE_ID']}` legacy
            WHERE NOT EXISTS (
                SELECT 1 
                FROM active_records active
                WHERE (legacy.portion_gdc_id = active.portion_gdc_id AND
                        legacy.aliquot_gdc_id = active.aliquot_gdc_id)
                    OR (legacy.portion_gdc_id = active.portion_gdc_id AND
                        legacy.aliquot_gdc_id IS NULL AND 
                        active.aliquot_gdc_id IS NULL)
            )
        ), aliquot_records AS (
            SELECT * FROM legacy_records
            UNION DISTINCT 
            SELECT * FROM active_records
        ), excluded_portions AS (
            SELECT portion_gdc_id
            FROM aliquot_records 
            WHERE portion_gdc_id IN (
                SELECT portion_id 
                FROM `{create_dev_table_id(PARAMS, 'slide_from_portion')}`
            ) AND portion_gdc_id NOT IN (
                SELECT portion_id
                FROM `{create_dev_table_id(PARAMS, 'analyte_from_portion')}`
            )
        ), filtered_records AS (
            SELECT * 
            FROM aliquot_records
            WHERE portion_gdc_id NOT IN (
                SELECT portion_gdc_id
                FROM excluded_portions
            )
        )
    """


def make_aliquot_case_table_sql() -> str:
    """
    SQL query used to create the aliquot2caseIDmap table.
    :return: SQL query string
    """
    sql_str = make_aliquot_case_table_base_sql()

    sql_str += f"""
        SELECT * 
        FROM filtered_records
    """

    return sql_str


def make_excluded_legacy_records_sql() -> str:
    """
    SQL query used to create a table containing legacy records which were excluded from the aliquot2caseIDmap table due
    to being superseded by a record in the active dataset.
    :return: SQL query string
    """
    sql_str = make_aliquot_case_table_base_sql()

    sql_str += f"""
        SELECT program_name, 
            project_id,
            case_gdc_id, 
            case_barcode,
            sample_gdc_id,
            sample_barcode,
            sample_type,
            sample_type_name,
            CAST(sample_is_ffpe AS BOOL) AS sample_is_ffpe, 
            sample_preservation_method,
            portion_gdc_id,
            portion_barcode,
            analyte_gdc_id,
            analyte_barcode,
            aliquot_gdc_id,
            aliquot_barcode  
        FROM `{PARAMS['LEGACY_TABLE_ID']}`
        EXCEPT DISTINCT 
        SELECT *
        FROM filtered_records
    """

    return sql_str


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
                                query=make_aliquot_case_table_sql())

        update_table_schema_from_generic(params=PARAMS, table_id=create_metadata_table_id(PARAMS, PARAMS['TABLE_NAME']))

        create_table_from_query(params=PARAMS,
                                table_id=create_excluded_records_table_id(PARAMS, PARAMS['TABLE_NAME']),
                                query=make_excluded_legacy_records_sql())

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)

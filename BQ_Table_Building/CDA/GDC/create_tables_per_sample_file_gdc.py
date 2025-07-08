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
from cda_bq_etl.utils import load_config, format_seconds, create_metadata_table_id, create_per_sample_table_id, \
    create_dev_table_id
from cda_bq_etl.bq_helpers.lookup import get_gdc_program_list
from cda_bq_etl.bq_helpers.schema import get_program_schema_tags_gdc
from cda_bq_etl.bq_helpers.create_modify import create_table_from_query, delete_bq_table, update_table_schema_from_generic

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_slide_entity_query(program_name: str) -> str:
    """
    Make query to retrieve slide entities for per sample file table.
    :param program_name: program used to filter query
    :return: sql string
    """
    return f"""
        SELECT DISTINCT 
            fm.file_gdc_id,
            fm.case_gdc_id,
            stc.case_barcode,
            stc.sample_gdc_id,
            stc.sample_barcode,
            stc.sample_type_name,
            fm.project_short_name,
            REGEXP_EXTRACT(fm.project_short_name, r'^[^-]*-(.*)$') AS project_short_name_suffix,
            fm.program_name,
            fm.data_type,
            fm.data_category,
            fm.experimental_strategy,
            fm.file_type,
            fm.file_size,
            fm.data_format,
            fm.platform,
            CAST(null AS STRING) AS file_name_key,
            fm.index_file_gdc_id AS index_file_id,
            CAST(null AS STRING) as index_file_name_key,
            fm.index_file_size,
            fm.`access`,
            fm.acl
        FROM `{create_metadata_table_id(PARAMS, PARAMS['FILE_TABLE_NAME'])}` fm
        JOIN `{create_dev_table_id(PARAMS, 'file_associated_with_entity')}` fawe
          ON fawe.file_id = fm.file_gdc_id AND 
             fawe.entity_id = fm.associated_entities__entity_gdc_id
        JOIN `{create_metadata_table_id(PARAMS, PARAMS['SLIDE_TABLE_NAME'])}` stc
          ON stc.slide_gdc_id = fm.associated_entities__entity_gdc_id AND 
             stc.slide_barcode = fawe.entity_submitter_id
        WHERE fm.case_gdc_id NOT LIKE '%;%' AND
              fm.case_gdc_id != 'multi' AND
              fm.associated_entities__entity_type = 'slide' AND
              fm.program_name = '{program_name}'
    """


def make_aliquot_entity_query(program_name: str) -> str:
    """
    Make query to retrieve aliquot entities for per sample file table.
    :param program_name: program used to filter query
    :return: sql string
    """
    return f"""
        WITH fm1 AS ( 
            # Files with < 8 associated aliquots (otherwise they're marked as multi) 
            # Also, files are only associated with a single case.
            SELECT DISTINCT 
                fm.file_gdc_id,
                fm.case_gdc_id,
                fm.associated_entities__entity_gdc_id AS aliquot_gdc_id,
                fm.project_short_name,
                REGEXP_EXTRACT(fm.project_short_name, r'^[^-]*-(.*)$') AS project_short_name_suffix,
                fm.program_name,
                fm.data_type,
                fm.data_category,
                fm.experimental_strategy,
                fm.file_type,
                fm.file_size,
                fm.data_format,
                fm.platform,
                CAST(null AS STRING) AS file_name_key,
                fm.index_file_gdc_id AS index_file_id,
                CAST(null AS STRING) as index_file_name_key,
                fm.index_file_size,
                fm.`access`,
                fm.acl
            FROM `{create_metadata_table_id(PARAMS, PARAMS['FILE_TABLE_NAME'])}` fm
            WHERE 
                fm.associated_entities__entity_type = 'aliquot' AND
                fm.case_gdc_id NOT LIKE '%;%' AND
                fm.case_gdc_id != 'multi' AND 
                fm.associated_entities__entity_gdc_id != 'multi' AND
                fm.program_name = '{program_name}'
        ), fm2 AS (
            # Files with >= 8 associated aliquots, where 'multi' is substituted for concatenated aliquot string
            # Also, files are only associated with a single case.
            SELECT DISTINCT fm.file_gdc_id,
                fm.case_gdc_id,
                fm.associated_entities__entity_gdc_id AS aliquot_gdc_id,
                fm.project_short_name,
                REGEXP_EXTRACT(fm.project_short_name, r'^[^-]*-(.*)$') AS project_short_name_suffix,
                fm.program_name,
                fm.data_type,
                fm.data_category,
                fm.experimental_strategy,
                fm.file_type,
                fm.file_size,
                fm.data_format,
                fm.platform,
                CAST(null AS STRING) AS file_name_key,
                fm.index_file_gdc_id AS index_file_id,
                CAST(null AS STRING) as index_file_name_key,
                fm.index_file_size,
                fm.`access`,
                fm.acl
            FROM `{create_metadata_table_id(PARAMS, PARAMS['FILE_TABLE_NAME'])}` fm
            WHERE 
                fm.associated_entities__entity_type = 'aliquot' AND
                fm.case_gdc_id NOT LIKE '%;%' AND
                fm.case_gdc_id != 'multi' AND 
                fm.associated_entities__entity_gdc_id = 'multi' AND
                fm.program_name = '{program_name}'
        )
    
        SELECT DISTINCT fm1.file_gdc_id,
            fm1.case_gdc_id,
            atc.case_barcode,
            atc.sample_gdc_id,
            atc.sample_barcode,
            atc.sample_type_name,
            fm1.project_short_name,
            fm1.project_short_name_suffix,
            fm1.program_name,
            fm1.data_type,
            fm1.data_category,
            fm1.experimental_strategy,
            fm1.file_type,        
            fm1.file_size,
            fm1.data_format,
            fm1.platform,
            fm1.file_name_key,
            fm1.index_file_id,
            fm1.index_file_name_key,
            fm1.index_file_size,
            fm1.`access`,
            fm1.acl
        FROM fm1
        JOIN `{create_dev_table_id(PARAMS, 'file_associated_with_entity')}` fawe
            ON fm1.file_gdc_id = fawe.file_id
        JOIN `{create_metadata_table_id(PARAMS, PARAMS['ALIQUOT_TABLE_NAME'])}` atc
            ON  atc.case_gdc_id = fm1.case_gdc_id AND
                atc.aliquot_gdc_id = fawe.entity_id AND
                atc.aliquot_barcode = fawe.entity_submitter_id
    
        UNION ALL
        # merging the multi and individual aliquot rows
        
        SELECT DISTINCT fm2.file_gdc_id,
            fm2.case_gdc_id,
            cm.case_barcode,
            CAST(null AS STRING) AS sample_gdc_id,
            CAST(null AS STRING) AS sample_barcode,
            CAST(null AS STRING) AS sample_type_name,
            fm2.project_short_name,
            fm2.project_short_name_suffix,
            fm2.program_name,
            fm2.data_type,
            fm2.data_category,
            fm2.experimental_strategy,
            fm2.file_type,        
            fm2.file_size,
            fm2.data_format,
            fm2.platform,
            fm2.file_name_key,
            fm2.index_file_id,
            fm2.index_file_name_key,
            fm2.index_file_size,
            fm2.`access`,
            fm2.acl
        FROM fm2
        JOIN `{create_metadata_table_id(PARAMS, PARAMS['CASE_TABLE_NAME'])}` cm
            ON cm.case_gdc_id = fm2.case_gdc_id
    """


def make_case_entity_query(program_name: str) -> str:
    """
    Make query to retrieve case entities for per sample file table.
    :param program_name: program used to filter query
    :return: sql string
    """
    return f"""
        SELECT DISTINCT 
            fm.file_id as file_gdc_id,
            fm.case_gdc_id,
            c.case_barcode,
            CAST(null AS STRING) AS sample_gdc_id,
            CAST(null AS STRING) AS sample_barcode,
            CAST(null AS STRING) AS sample_type_name,
            fm.project_short_name, # TCGA-OV
            REGEXP_EXTRACT(fm.project_short_name, r'^[^-]*-(.*)$') AS project_short_name_suffix,
            fm.program_name, # TCGA
            fm.data_type,
            fm.data_category,
            fm.experimental_strategy,
            fm.file_type,
            fm.file_size,
            fm.data_format,
            fm.platform,
            CAST(null AS STRING) as file_name_key,
            fm.index_file_gdc_id as index_file_id,
            CAST(null AS STRING) as index_file_name_key,
            fm.index_file_size,
            fm.`access`,
            fm.acl
        FROM `{create_metadata_table_id(PARAMS, PARAMS['FILE_TABLE_NAME'])}` AS fm
        JOIN `{create_metadata_table_id(PARAMS, PARAMS['CASE_TABLE_NAME'])}` AS c
            ON fm.case_gdc_id = c.case_gdc_id
        WHERE fm.case_gdc_id NOT LIKE '%;%' AND
              fm.case_gdc_id != 'multi' AND
              fm.associated_entities__entity_type = 'case' AND
              fm.program_name = '{program_name}'
    """


def make_merged_sql_query(program_name: str) -> str:
    """
    Merge sql statements to create one statement which creates the per sample file table for a given project.
    :return: sql string
    """
    slide_entity_sql = make_slide_entity_query(program_name)
    aliquot_entity_sql = make_aliquot_entity_query(program_name)
    case_entity_sql = make_case_entity_query(program_name)

    return f"""
        ({slide_entity_sql})
        UNION ALL
        ({aliquot_entity_sql})
        UNION ALL
        ({case_entity_sql})
    """


def make_add_uris_and_index_file_sql_query(no_uri_table_id: str, drs_uri_table_id: str) -> str:
    """
    Make sql query that takes the almost completed per sample file table and adds drs uris for file and index file.
    :param no_uri_table_id: Intermediate per sample file table id
    :param drs_uri_table_id: DRS uri table id
    :return: sql string
    """
    return f"""
        SELECT psf.file_gdc_id,
            psf.case_gdc_id,
            psf.case_barcode,
            psf.sample_gdc_id,
            psf.sample_barcode,
            psf.sample_type_name,
            psf.project_short_name,					
            psf.project_short_name_suffix,
            psf.program_name,
            psf.data_type,
            psf.data_category,
            psf.experimental_strategy,
            psf.file_type,
            psf.file_size,
            psf.data_format,
            psf.platform,
            f_uri.gcs_path AS file_name_key,
            psf.index_file_id,
            i_uri.gcs_path AS index_file_name_key,
            psf.index_file_size,
            psf.`access`,
            psf.acl
            FROM `{no_uri_table_id}` psf
            JOIN `{drs_uri_table_id}` f_uri
                ON f_uri.file_uuid = psf.file_gdc_id
            LEFT JOIN `{drs_uri_table_id}` i_uri
                ON i_uri.file_uuid = psf.index_file_id
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

    program_list = get_gdc_program_list(PARAMS)

    for program_name in program_list:
        if program_name == "BEATAML1_0":
            program_name_original = "BEATAML1.0"
        elif program_name == "EXC_RESPONDERS":
            program_name_original = "EXCEPTIONAL_RESPONDERS"
        else:
            program_name_original = program_name

        no_url_table_id = create_per_sample_table_id(PARAMS, f"{program_name}_{PARAMS['TABLE_NAME']}_no_url")
        table_id = create_per_sample_table_id(PARAMS, f"{program_name}_{PARAMS['TABLE_NAME']}")

        if 'create_program_tables_no_url' in steps:
            logger.info(f"Creating base table for {program_name_original}!\n")

            # create table with everything but file uris from manifest
            create_table_from_query(params=PARAMS,
                                    table_id=no_url_table_id,
                                    query=make_merged_sql_query(program_name_original))

        if 'add_url_to_program_tables' in steps:
            logger.info(f"Creating table with added uris for {program_name_original}!\n")

            drs_uri_table_id = PARAMS['DRS_URI_TABLE_ID']

            # add index file size and file/index file keys to finish populating the table
            create_table_from_query(params=PARAMS,
                                    table_id=table_id,
                                    query=make_add_uris_and_index_file_sql_query(no_url_table_id, drs_uri_table_id))

            schema_tags = get_program_schema_tags_gdc(params=PARAMS, program_name=program_name_original)

            if 'program-label' in schema_tags:
                metadata_file = PARAMS['METADATA_FILE_SINGLE_PROGRAM']
            else:
                metadata_file = PARAMS['METADATA_FILE_MULTI_PROGRAM']

            update_table_schema_from_generic(params=PARAMS,
                                             table_id=table_id,
                                             schema_tags=schema_tags,
                                             metadata_file=metadata_file)

            delete_bq_table(no_url_table_id)

    end_time = time.time()

    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)

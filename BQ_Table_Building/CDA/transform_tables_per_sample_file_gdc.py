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
from cda_bq_etl.bq_helpers import delete_bq_table, load_table_from_query, query_and_retrieve_result, publish_table, \
    find_most_recent_published_table_id

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def create_program_name_set() -> set[str]:
    """
    Create a list of programs with case associations using the case_project_program view.
    :return: set of program names
    """
    def make_program_name_set_query():
        return f"""
        SELECT DISTINCT program_name
        FROM `{create_dev_table_id(PARAMS, 'case_project_program')}`
        """

    result = query_and_retrieve_result(sql=make_program_name_set_query())

    program_name_set = set()

    for row in result:
        program_name_set.add(row[0])

    return program_name_set


def make_slide_entity_query(program_name: str) -> str:
    """
    Make query to retrieve slide entities for per sample file table.
    :param program_name: program used to filter query
    :return: sql string
    """
    file_metadata_table_id = create_dev_table_id(PARAMS, 'file_metadata', True)
    file_entity_table_id = create_dev_table_id(PARAMS, 'file_associated_with_entity')
    slide_case_table_id = create_dev_table_id(PARAMS, 'slide_to_case', True)

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
        FROM `{file_metadata_table_id}` fm
        JOIN `{file_entity_table_id}` fawe
          ON fawe.file_id = fm.file_gdc_id AND 
             fawe.entity_id = fm.associated_entities__entity_gdc_id
        JOIN `{slide_case_table_id}` stc
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
    case_metadata_table_id = create_dev_table_id(PARAMS, 'case_metadata', True)
    file_metadata_table_id = create_dev_table_id(PARAMS, 'file_metadata', True)
    file_entity_table_id = create_dev_table_id(PARAMS, 'file_associated_with_entity')
    aliquot_case_table_id = create_dev_table_id(PARAMS, 'aliquot_to_case', True)

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
            FROM `{file_metadata_table_id}` fm
            WHERE 
                fm.associated_entities__entity_type = 'aliquot' AND
                fm.case_gdc_id NOT LIKE '%;%' AND
                fm.case_gdc_id != 'multi' AND 
                fm.associated_entities__entity_gdc_id != 'multi' AND
                fm.program_name = '{program_name}'
        ), 
    
        fm2 AS (
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
            FROM `{file_metadata_table_id}` fm
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
        JOIN `{file_entity_table_id}` fawe
            ON fm1.file_gdc_id = fawe.file_id
        JOIN `{aliquot_case_table_id}` atc
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
        # should this be 
        JOIN `{case_metadata_table_id}` cm
            ON cm.case_gdc_id = fm2.case_gdc_id
    """


def make_case_entity_query(program_name: str) -> str:
    """
    Make query to retrieve case entities for per sample file table.
    :param program_name: program used to filter query
    :return: sql string
    """
    working_project = PARAMS['WORKING_PROJECT']
    working_dataset = PARAMS['WORKING_DATASET']
    file_metadata_table_id = f"{working_project}.{working_dataset}.file_metadata_{PARAMS['RELEASE']}"
    case_metadata_table_id = f"{working_project}.{working_dataset}.case_metadata_{PARAMS['RELEASE']}"

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
        FROM `{file_metadata_table_id}` AS fm
        JOIN `{case_metadata_table_id}` AS c
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
        (
        {slide_entity_sql}
        )
        UNION ALL
        (
        {aliquot_entity_sql}
        )
        UNION ALL
        (
        {case_entity_sql}
        )
    """


def make_add_uris_and_index_file_sql_query(no_uri_table_id: str, drs_uri_table_id: str) -> str:
    """
    Make sql query that takes the almost completed per sample file table and adds drs uris for file and index file.
    :param no_uri_table_id: Intermediate per sample file table id
    :param drs_uri_table_id: DRS uri table id
    :return: sql string
    """
    working_project = PARAMS['WORKING_PROJECT']
    working_dataset = PARAMS['WORKING_DATASET']
    file_metadata_table_id = f"{working_project}.{working_dataset}.file_metadata_{PARAMS['RELEASE']}"

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
            fm.file_size AS index_file_size,
            psf.`access`,
            psf.acl
            FROM `{no_uri_table_id}` psf
            JOIN `{drs_uri_table_id}` f_uri
                ON f_uri.file_uuid = psf.file_gdc_id
            LEFT JOIN `{file_metadata_table_id}` fm
                ON fm.file_gdc_id = psf.index_file_id
            LEFT JOIN `{drs_uri_table_id}` i_uri
                ON i_uri.file_uuid = psf.index_file_id
    """


def main(args):
    try:
        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    start_time = time.time()

    program_set = create_program_name_set()

    if 'create_program_tables' in steps:
        for program in sorted(program_set):
            if program == "BEATAML1.0":
                program_name = "BEATAML1_0"
            elif program == "EXCEPTIONAL_RESPONDERS":
                program_name = "EXC_RESPONDERS"
            else:
                program_name = program

            no_url_table_name = f"per_sample_file_metadata_hg38_{program_name}_{PARAMS['RELEASE']}_no_url"
            no_url_table_id = f"{PARAMS['WORKING_PROJECT']}.{PARAMS['TARGET_DATASET']}.{no_url_table_name}"

            table_name = f"per_sample_file_metadata_hg38_{program_name}_{PARAMS['RELEASE']}"
            table_id = f"{PARAMS['WORKING_PROJECT']}.{PARAMS['TARGET_DATASET']}.{table_name}"

            drs_uri_table_id = f"isb-project-zero.GDC_manifests.dr37_paths_active"

            print(f"\nCreating base table for {program}!\n")

            # create table with everything but file uris from manifest
            load_table_from_query(params=PARAMS,
                                  table_id=no_url_table_id,
                                  query=make_merged_sql_query(program))

            print(f"\nCreating table with added uris for {program}!\n")

            # add index file size and file/index file keys to finish populating the table
            load_table_from_query(params=PARAMS,
                                  table_id=table_id,
                                  query=make_add_uris_and_index_file_sql_query(no_url_table_id, drs_uri_table_id))

            delete_bq_table(no_url_table_id)

    if 'publish_tables' in steps:
        for program in sorted(program_set):
            if program == "BEATAML1.0":
                program_name = "BEATAML1_0"
            elif program == "EXCEPTIONAL_RESPONDERS":
                program_name = "EXC_RESPONDERS"
            else:
                program_name = program

            dev_table_name = f"per_sample_file_metadata_hg38_{program_name}_{PARAMS['RELEASE']}"
            dev_table_id = f"{PARAMS['WORKING_PROJECT']}.{PARAMS['TARGET_DATASET']}.{dev_table_name}"

            current_table_name = f"{PARAMS['PROD_TABLE_NAME']}_current"
            current_table_id = f"{PARAMS['PROD_PROJECT']}.{program_name}.{current_table_name}"

            versioned_table_name = f"{PARAMS['PROD_TABLE_NAME']}_{PARAMS['DC_RELEASE']}"
            versioned_table_id = f"{PARAMS['PROD_PROJECT']}.{program_name}_versioned.{versioned_table_name}"

            publish_table(params=PARAMS, source_table_id=dev_table_id, current_table_id=current_table_id,
                          versioned_table_id=versioned_table_id)

    end_time = time.time()

    print(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)

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

from cda_bq_etl.utils import load_config, has_fatal_error
from cda_bq_etl.bq_helpers import delete_bq_table, load_table_from_query, bq_harness_with_result

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def create_program_name_set():
    """
    todo
    :return:
    """
    def make_program_name_set_query():
        return f"""
        SELECT DISTINCT program_name
        FROM `{PARAMS['WORKING_PROJECT']}.{PARAMS['WORKING_DATASET']}.{PARAMS['RELEASE']}_case_project_program`
        """

    result = bq_harness_with_result(sql=make_program_name_set_query(), do_batch=False, verbose=False)

    program_name_set = set()

    for row in result:
        program_name_set.add(row[0])

    return program_name_set


def make_aliquot_count_query() -> str:
    # todo could this be used in test suite?
    return f"""
    WITH aliquot_counts AS (
        SELECT distinct file_gdc_id,
        # Count the number of ';'' in the field, if any; if not, count is one, 
        # which covers rows for both single aliquots and multi
            CASE WHEN ARRAY_LENGTH(REGEXP_EXTRACT_ALL(associated_entities__entity_gdc_id, r'(;)')) >= 1
                 THEN ARRAY_LENGTH(REGEXP_EXTRACT_ALL(associated_entities__entity_gdc_id, r'(;)')) + 1
                 ELSE 1
            END AS entity_count
        FROM `isb-project-zero.cda_gdc_test.file_metadata_2023_03`
        WHERE case_gdc_id NOT LIKE "%;%" AND
              case_gdc_id != "multi" AND
              associated_entities__entity_type = "aliquot"            
    )

    SELECT sum(entity_count) AS aliquot_count            
    FROM aliquot_counts
    """


def make_slide_entity_query(program_name: str) -> str:
    """
    todo
    :param program_name:
    :return:
    """
    working_project = PARAMS['WORKING_PROJECT']
    working_dataset = PARAMS['WORKING_DATASET']
    file_metadata_table_id = f"{working_project}.{working_dataset}.file_metadata_{PARAMS['RELEASE']}"
    file_entity_table_id = f"{working_project}.{working_dataset}.{PARAMS['RELEASE']}_file_associated_with_entity"
    slide_case_table_id = f"{working_project}.{working_dataset}.slide_to_case_{PARAMS['RELEASE']}"

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
    todo
    :param program_name:
    :return:
    """
    working_project = PARAMS['WORKING_PROJECT']
    working_dataset = PARAMS['WORKING_DATASET']
    file_metadata_table_id = f"{working_project}.{working_dataset}.file_metadata_{PARAMS['RELEASE']}"
    case_metadata_table_id = f"{working_project}.{working_dataset}.case_metadata_{PARAMS['RELEASE']}"
    file_entity_table_id = f"{working_project}.{working_dataset}.{PARAMS['RELEASE']}_file_associated_with_entity"
    aliquot_case_table_id = f"{working_project}.{working_dataset}.aliquot_to_case_{PARAMS['RELEASE']}"

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
    todo
    :param program_name:
    :return:
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
    todo
    :param program_name:
    :return:
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
    todo
    :param no_uri_table_id:
    :param drs_uri_table_id:
    :return:
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

    # Steps, perhaps--some may be mergable. For each progrma:
    # 1) find all slide associated entities in file metadata. There won't be multiples here, one slide = a file.
    #    Using rows from slide associated entities, and slide_to_case_map, expand slide rows, and drop duplicates.
    #    Then, get slide barcodes and sample id, barcode and sample_type_name, perhaps other columns?
    # ** Able to do all this in a single query: make_slide_view_query()

    # 2) find all aliquot associated entities in file metadata;
    #    expand aliquots that are concatenated list into multiple rows.
    #    If multi, then include the file, but leave sample id blank.
    #    Then, get aliquot barcodes and sample id, barcode and sample_type_name, perhaps other columns?

    # 4) add in files with "multi" aliquots. These get NULL gdc_sample_id, sample_barcode, sample_type_name.
    # 5) find all case associated entities in file metadata where file doesn't have multiple case_gdc_ids.
    #    These get NULL sample id, barcode, sample_type_name
    # 6) finally, all these tables get merged together into a single table.

    if 'create_program_tables' in steps:
        program_set = create_program_name_set()

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


if __name__ == "__main__":
    main(sys.argv)

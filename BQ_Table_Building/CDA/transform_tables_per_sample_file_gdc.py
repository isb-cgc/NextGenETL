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

from common_etl.cda_utils import create_program_name_set
from common_etl.utils import load_config, has_fatal_error, load_table_from_query

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


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

def make_drs_paths_query() -> str:
    # todo abstract the table id
    return f"""
    WITH drs_uris AS (
        SELECT *
        FROM isb-project-zero.GDC_manifests.dr37_paths_active
    )
    """

def make_slide_entity_query(program_name: str) -> str:
    working_project = BQ_PARAMS['WORKING_PROJECT']
    working_dataset = BQ_PARAMS['WORKING_DATASET']
    file_metadata_table_id = f"{working_project}.{working_dataset}.file_metadata_{API_PARAMS['RELEASE']}"
    file_entity_table_id = f"{working_project}.{working_dataset}.{API_PARAMS['RELEASE']}_file_associated_with_entity"
    slide_case_table_id = f"{working_project}.{working_dataset}.slide_to_case_{API_PARAMS['RELEASE']}"

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
        file_drs_uris.gcs_path AS file_name_key,
        fm.index_file_gdc_id AS index_file_id,
        index_drs_uris.gcs_path AS index_file_name_key,
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
    # (INNER)
    JOIN drs_uris file_drs_uris
        ON file_drs_uris.file_uuid = fm.file_gdc_id
    LEFT JOIN drs_uris index_drs_uris
        ON file_drs_uris.file_uuid = fm.index_file_gdc_id 
    WHERE fm.case_gdc_id NOT LIKE '%;%' AND
          fm.case_gdc_id != 'multi' AND
          fm.associated_entities__entity_type = 'slide' AND
          fm.program_name = '{program_name}'
    """


def make_aliquot_entity_query(program_name: str) -> str:
    working_project = BQ_PARAMS['WORKING_PROJECT']
    working_dataset = BQ_PARAMS['WORKING_DATASET']
    file_metadata_table_id = f"{working_project}.{working_dataset}.file_metadata_{API_PARAMS['RELEASE']}"
    case_metadata_table_id = f"{working_project}.{working_dataset}.case_metadata_{API_PARAMS['RELEASE']}"
    file_entity_table_id = f"{working_project}.{working_dataset}.{API_PARAMS['RELEASE']}_file_associated_with_entity"
    aliquot_case_table_id = f"{working_project}.{working_dataset}.aliquot_to_case_{API_PARAMS['RELEASE']}"

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
            file_drs_uris.gcs_path AS file_name_key,
            fm.index_file_gdc_id AS index_file_id,
            index_drs_uris.gcs_path AS index_file_name_key,
            fm.index_file_size,
            fm.`access`,
            fm.acl
        FROM `{file_metadata_table_id}` fm
        # if LEFT, keep file records with no file in dcs
        JOIN drs_uris file_drs_uris
            ON file_drs_uris.file_uuid = fm.file_gdc_id
        LEFT JOIN drs_uris index_drs_uris
            ON file_drs_uris.file_uuid = fm.index_file_gdc_id 
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
            file_drs_uris.gcs_path AS file_name_key,
            fm.index_file_gdc_id AS index_file_id,
            index_drs_uris.gcs_path AS index_file_name_key,
            fm.index_file_size,
            fm.`access`,
            fm.acl
        FROM `{file_metadata_table_id}` fm
        # if LEFT, keep file records with no file in dcs
        JOIN drs_uris file_drs_uris
            ON file_drs_uris.file_uuid = fm.file_gdc_id
        LEFT JOIN drs_uris index_drs_uris
            ON file_drs_uris.file_uuid = fm.index_file_gdc_id 
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
    working_project = BQ_PARAMS['WORKING_PROJECT']
    working_dataset = BQ_PARAMS['WORKING_DATASET']
    file_metadata_table_id = f"{working_project}.{working_dataset}.file_metadata_{API_PARAMS['RELEASE']}"
    case_metadata_table_id = f"{working_project}.{working_dataset}.case_metadata_{API_PARAMS['RELEASE']}"

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
        file_drs_uris.gcs_path AS file_name_key,
        fm.index_file_gdc_id AS index_file_id,
        index_drs_uris.gcs_path AS index_file_name_key,
            fm.index_file_size,
            fm.`access`,
            fm.acl
        FROM `{file_metadata_table_id}` AS fm
        JOIN `{case_metadata_table_id}` AS c
            ON fm.case_gdc_id = c.case_gdc_id
        JOIN drs_uris file_drs_uris
            ON file_drs_uris.file_uuid = fm.file_gdc_id
        LEFT JOIN drs_uris index_drs_uris
            ON file_drs_uris.file_uuid = fm.index_file_gdc_id 
        WHERE fm.case_gdc_id NOT LIKE '%;%' AND
              fm.case_gdc_id != 'multi' AND
              fm.associated_entities__entity_type = 'case' AND
              fm.program_name = '{program_name}'
    """


def make_merged_sql_query(program_name: str) -> str:
    slide_entity_sql = make_slide_entity_query(program_name)
    aliquot_entity_sql = make_aliquot_entity_query(program_name)
    case_entity_sql = make_case_entity_query(program_name)
    drs_paths_sql = make_drs_paths_query()

    return f"""
    {drs_paths_sql}
    ({slide_entity_sql})
    UNION ALL
    ({aliquot_entity_sql})
    UNION ALL
    ({case_entity_sql})
    """


def main(args):
    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
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
        program_set = create_program_name_set(API_PARAMS, BQ_PARAMS)

        for program in sorted(program_set):
            if program == "BEATAML1.0":
                program_name = "BEATAML1_0"
            elif program == "EXCEPTIONAL_RESPONDERS":
                program_name = "EXC_RESPONDERS"
            else:
                program_name = program

            table_name = f"per_sample_file_metadata_hg38_{program_name}_{API_PARAMS['RELEASE']}"
            table_id = f"{BQ_PARAMS['WORKING_PROJECT']}.{BQ_PARAMS['TARGET_DATASET']}.{table_name}"

            print(f"\nLoading table for {program}!\n")

            load_table_from_query(bq_params=BQ_PARAMS,
                                  table_id=table_id,
                                  query=make_merged_sql_query(program))


if __name__ == "__main__":
    main(sys.argv)

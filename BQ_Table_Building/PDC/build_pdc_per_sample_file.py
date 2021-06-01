import time
import sys

from common_etl.utils import (has_fatal_error, load_config, format_seconds, construct_table_name,
                              create_view_from_query)

from BQ_Table_Building.PDC.pdc_utils import (get_prefix)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def make_webapp_per_sample_view_query():
    meta_dataset = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}"

    file_metadata_table_name = construct_table_name(API_PARAMS, prefix=BQ_PARAMS['FILE_METADATA_TABLE'])
    file_metadata_table_id = f"{meta_dataset}.{file_metadata_table_name}"

    file_assoc_table_name = construct_table_name(API_PARAMS, prefix=BQ_PARAMS['FILE_ASSOC_MAPPING_TABLE'])
    file_assoc_table_id = f"{meta_dataset}.{file_assoc_table_name}"

    aliquot_table_name = construct_table_name(API_PARAMS, prefix=BQ_PARAMS['ALIQUOT_TO_CASE_TABLE'])
    aliquot_table_id = f"{meta_dataset}.{aliquot_table_name}"

    study_table_name = construct_table_name(API_PARAMS, prefix=get_prefix(API_PARAMS, 'allPrograms'))
    study_table_id = f"{meta_dataset}.{study_table_name}"

    """
    SELECT fm.file_id AS file_pdc_id, # changed
    fa.case_id AS case_pdc_id, # changed
    ac.sample_id AS sample_pdc_id, # changed
    ac.sample_submitter_id AS sample_barcode, # changed 
    ac.project_name AS project_short_name, # changed
    fm.file_type AS file_type, # added
    fm.file_size, # added
    """

    return f"""
        SELECT fm.file_id AS file_pdc_id,  
            fa.case_id AS case_pdc_id, 
            ac.case_submitter_id AS case_barcode, 
            ac.sample_id AS sample_pdc_id, 
            ac.sample_submitter_id AS sample_barcode, 
            ac.sample_type, 
            ac.project_name AS project_short_name, 
            CAST(NULL AS STRING) AS project_name_suffix, 
            s.program_short_name AS program_name,
            fm.file_type AS data_type,
            fm.data_category, 
            fm.experiment_type AS experimental_strategy,
            fm.file_type AS file_type,
            fm.file_size,
            fm.file_format AS data_format, 
            fm.instrument AS platform, 
            fm.file_name as file_name_key, 
            CAST(NULL AS STRING) AS index_file_id, 
            CAST(NULL AS STRING) AS index_file_name_key, 
            CAST(NULL AS STRING) AS index_file_size,
            CAST(NULL AS STRING) AS cloud_path, 
            fm.`access`,
            CAST(NULL AS STRING) AS acl
        FROM `{file_metadata_table_id}` fm
        JOIN `{file_assoc_table_id}` fa
            ON fm.file_id = fa.file_id
        JOIN `{aliquot_table_id}` ac
            ON fa.case_id = ac.case_id
        JOIN `{study_table_id}` s
            ON s.project_name = ac.project_name
        """


def main(args):
    start_time = time.time()
    print(f"PDC script started at {time.strftime('%x %X', time.localtime())}")
    steps = None

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    if 'build_per_sample_webapp_view' in steps:
        per_sample_view_name = construct_table_name(API_PARAMS, prefix=BQ_PARAMS['WEBAPP_PER_SAMPLE_VIEW'])
        webapp_per_sample_view_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['WEBAPP_DATASET']}.{per_sample_view_name}"

        print("Creating webapp view!")
        create_view_from_query(view_id=webapp_per_sample_view_id, view_query=make_webapp_per_sample_view_query())

    end = time.time() - start_time
    print(f"Finished program execution in {format_seconds(end)}!\n")


if __name__ == '__main__':
    main(sys.argv)

import time

from common_etl.utils import (has_fatal_error, load_config, format_seconds, construct_table_name,
                              create_view_from_query)

from BQ_Table_Building.PDC.pdc_utils import (get_prefix)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def make_webapp_per_sample_view_query():
    meta_dataset = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}"

    file_metadata_table_name = construct_table_name(API_PARAMS, prefix=BQ_PARAMS['FILE_METADATA'])
    file_metadata_table_id = f"{meta_dataset}.{file_metadata_table_name}"

    file_assoc_table_name = construct_table_name(API_PARAMS, prefix=BQ_PARAMS['FILE_ASSOC_MAPPING_TABLE'])
    file_assoc_table_id = f"{meta_dataset}.{file_assoc_table_name}"

    aliquot_table_name = construct_table_name(API_PARAMS, prefix=BQ_PARAMS['ALIQUOT_TO_CASE_TABLE'])
    aliquot_table_id = f"{meta_dataset}.{aliquot_table_name}"

    study_table_name = construct_table_name(API_PARAMS, prefix=get_prefix(API_PARAMS, 'allPrograms'))
    study_table_id = f"{meta_dataset}.{study_table_name}"

    return f"""
        SELECT fm.file_id, fa.case_id as case_node_id, 
            'PDC' as source_node, ac.case_submitter_id, ac.sample_id, ac.sample_submitter_id, ac.sample_type, 
            ac.project_name, null as project_name_suffix, s.program_short_name as program_name,
            fm.data_category, fm.experiment_type as experimental_strategy, fm.file_type as data_type, 
            fm.file_format as data_format, fm.instrument as platform, fm.file_name, null as cloud_path, fm.`access`
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
        app_per_sample_view_name = construct_table_name(API_PARAMS, prefix=BQ_PARAMS['WEBAPP_PER_SAMPLE_VIEW'])
        webapp_per_sample_view_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{app_per_sample_view_name}"
        create_view_from_query(view_id=webapp_per_sample_view_id, view_query=make_webapp_per_sample_view_query())

    end = time.time() - start_time
    print(f"Finished program execution in {format_seconds(end)}!\n")

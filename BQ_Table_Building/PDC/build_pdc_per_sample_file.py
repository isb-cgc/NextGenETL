import time
import sys

from common_etl.utils import (has_fatal_error, load_config, format_seconds, construct_table_name,
                              create_view_from_query, load_table_from_query)

from BQ_Table_Building.PDC.pdc_utils import (get_prefix, get_pdc_projects_list, get_project_program_names,
                                             get_project_level_schema_tags, update_table_schema_from_generic_pdc)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def get_mapping_table_ids():
    dev_meta_dataset = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}"
    prod_meta_dataset = f"{BQ_PARAMS['PROD_PROJECT']}.{BQ_PARAMS['PUBLIC_META_DATASET']}"

    file_metadata_table_name = f"{BQ_PARAMS['FILE_METADATA_TABLE']}_current"
    file_metadata_table_id = f"{prod_meta_dataset}.{file_metadata_table_name}"

    case_metadata_table_name = f"{BQ_PARAMS['CASE_METADATA_TABLE']}_current"
    case_metadata_table_id = f"{prod_meta_dataset}.{case_metadata_table_name}"

    file_assoc_table_name = f"{BQ_PARAMS['FILE_ASSOC_MAPPING_TABLE']}_current"
    file_assoc_table_id = f"{prod_meta_dataset}.{file_assoc_table_name}"

    aliquot_table_name = f"{BQ_PARAMS['ALIQUOT_TO_CASE_TABLE']}_current"
    aliquot_table_id = f"{prod_meta_dataset}.{aliquot_table_name}"

    # todo switch to published table
    study_table_name = construct_table_name(API_PARAMS, prefix=get_prefix(API_PARAMS, 'allPrograms'))
    study_table_id = f"{dev_meta_dataset}.{study_table_name}"

    return file_metadata_table_id, case_metadata_table_id, file_assoc_table_id, aliquot_table_id, study_table_id


def make_webapp_per_sample_view_query():
    file_table_id, case_table_id, file_assoc_table_id, aliquot_table_id, study_table_id = get_mapping_table_ids()

    # todo change query after next file metadata pull
    return f"""
        SELECT DISTINCT fm.file_id, fa.case_id, ac.case_submitter_id, ac.sample_id, ac.sample_submitter_id, 
            ac.sample_type, REPLACE(s.project_short_name, '_', '-') AS project_short_name, s.program_short_name, 
            fm.file_type, fm.data_category, fm.experiment_type, 
            fm.file_size, fm.file_format, fm.instrument, fm.file_name,
            REPLACE(fm.url, 'https://d3iwtkuvwz4jtf.cloudfront.net/', 's3://pdcdatastore/') AS file_name_key, 
            fm.`access`
        FROM `{file_table_id}` fm
        JOIN `{file_assoc_table_id}` fa
            ON fm.file_id = fa.file_id
        JOIN `{aliquot_table_id}` ac
            ON fa.case_id = ac.case_id
        JOIN `{case_table_id}` cm
            ON ac.case_id = cm.case_id
        JOIN `{study_table_id}` s
            ON s.project_id = cm.project_id
        """


def make_project_level_per_sample_query(project_submitter_id):
    file_table_id, case_table_id, file_assoc_table_id, aliquot_table_id, study_table_id = get_mapping_table_ids()

    # todo change query after next file metadata pull
    return f"""
        SELECT DISTINCT 
            fm.file_id, 
            fa.case_id, 
            ac.case_submitter_id, 
            ac.sample_id, 
            ac.sample_submitter_id,     
            ac.sample_type, 
            REPLACE(s.project_short_name, '_', '-') AS project_short_name, 
            s.project_submitter_id, 
            s.program_short_name, 
            s.program_name, 
            fm.data_category, 
            fm.experiment_type, 
            fm.file_type, 
            fm.file_size, 
            fm.file_format, 
            fm.instrument, 
            fm.file_name,
            REPLACE(fm.url, 'https://d3iwtkuvwz4jtf.cloudfront.net/', 's3://pdcdatastore/') AS file_location, 
            fm.`access`
        FROM `{file_table_id}` fm
        JOIN `{file_assoc_table_id}` fa
            ON fm.file_id = fa.file_id
        JOIN `{aliquot_table_id}` ac
            ON fa.case_id = ac.case_id
        JOIN `{case_table_id}` cm
            ON ac.case_id = cm.case_id
        JOIN `{study_table_id}` s
            ON s.project_id = cm.project_id
        WHERE s.project_submitter_id = '{project_submitter_id}'
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

    projects_list = get_pdc_projects_list(API_PARAMS, BQ_PARAMS, include_embargoed=True)

    if 'build_per_sample_webapp_view' in steps:
        per_sample_view_name = f"{BQ_PARAMS['WEBAPP_PER_SAMPLE_VIEW']}"
        webapp_per_sample_view_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['WEBAPP_DATASET']}.{per_sample_view_name}"

        print("Creating webapp view!")
        create_view_from_query(view_id=webapp_per_sample_view_id, view_query=make_webapp_per_sample_view_query())

    if 'build_project_level_per_sample_tables' in steps:
        print("Building project-level per-sample metadata tables!")

        for project in projects_list:
            dev_meta_dataset = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}"
            table_prefix = f"{BQ_PARAMS['PROJECT_PER_SAMPLE_FILE_TABLE']}"
            table_suffix = f"{API_PARAMS['DATA_SOURCE']}_{API_PARAMS['RELEASE']}"

            project_table_name = f"{table_prefix}_{project['project_short_name']}_{table_suffix}"
            project_table_id = f"{dev_meta_dataset}.{project_table_name}"

            project_query = make_project_level_per_sample_query(project['project_submitter_id'])

            load_table_from_query(BQ_PARAMS, table_id=project_table_id, query=project_query)

            schema_tags = get_project_level_schema_tags(API_PARAMS, BQ_PARAMS,
                                                        project_submitter_id=project['project_submitter_id'])

            print(schema_tags)

            if 'program-name-1-lower' in schema_tags:
                update_table_schema_from_generic_pdc(API_PARAMS, BQ_PARAMS,
                                                     table_id=project_table_id,
                                                     schema_tags=schema_tags,
                                                     metadata_file=BQ_PARAMS['GENERIC_TABLE_METADATA_FILE_2_PROGRAM'])
            else:
                update_table_schema_from_generic_pdc(API_PARAMS, BQ_PARAMS,
                                                     table_id=project_table_id,
                                                     schema_tags=schema_tags)

    end = time.time() - start_time
    print(f"Finished program execution in {format_seconds(end)}!\n")


if __name__ == '__main__':
    main(sys.argv)

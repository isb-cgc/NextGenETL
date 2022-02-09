"""
Copyright 2020-2021, Institute for Systems Biology

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

import time
import sys

from common_etl.utils import (format_seconds, has_fatal_error, load_config, construct_table_name, load_table_from_query,
                              retrieve_bq_schema_object, publish_table, create_and_upload_schema_for_json,
                              write_list_to_jsonl_and_upload, test_table_for_version_changes)

from BQ_Table_Building.PDC.pdc_utils import (build_obj_from_pdc_api, build_table_from_jsonl, get_prefix,
                                             update_table_schema_from_generic_pdc, get_publish_table_ids,
                                             find_most_recent_published_table_id)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def get_mapping_table_ids():
    """
    Create mapping table ids used in multiple queries (in order to avoid code duplication).
    :return: tuple of three table id strings: cases aliquots, studies, case external mapping
    """
    #
    aliquot_prefix = get_prefix(API_PARAMS, API_PARAMS['ALIQUOT_ENDPOINT'])
    case_aliquot_table_name = construct_table_name(API_PARAMS, prefix=aliquot_prefix)
    case_aliquot_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{case_aliquot_table_name}"

    study_table_name = construct_table_name(API_PARAMS, prefix=get_prefix(API_PARAMS, API_PARAMS['STUDY_ENDPOINT']))
    study_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{study_table_name}"

    case_external_mapping_prefix = get_prefix(API_PARAMS, API_PARAMS['CASE_EXTERNAL_MAP_ENDPOINT'])
    case_ext_map_table_name = construct_table_name(API_PARAMS, prefix=case_external_mapping_prefix)
    case_external_mapping_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{case_ext_map_table_name}"

    return case_aliquot_table_id, study_table_id, case_external_mapping_table_id


def make_cases_aliquots_query(offset, limit):
    """
    Create a graphQL string for querying the PDC API's paginatedCasesSamplesAliquots endpoint.
    :param offset: starting index for which to return records
    :param limit: maximum number of records to return
    :return: GraphQL query string
    """
    return f"""{{ 
        paginatedCasesSamplesAliquots(offset:{offset} limit:{limit} acceptDUA: true) {{ 
            total 
            casesSamplesAliquots {{
                case_id 
                case_submitter_id
                days_to_lost_to_followup
                disease_type
                index_date
                lost_to_followup
                primary_site
                samples {{
                    sample_id
                    sample_submitter_id
                    sample_type
                    #is_ffpe # field removed from API
                    preservation_method
                    biospecimen_anatomic_site
                    current_weight
                    days_to_collection
                    freezing_method
                    initial_weight
                    intermediate_dimension
                    longest_dimension
                    shortest_dimension
                    time_between_clamping_and_freezing
                    time_between_excision_and_freezing
                    aliquots {{ 
                        aliquot_id 
                        aliquot_submitter_id
                        analyte_type
                        aliquot_run_metadata {{
                            aliquot_run_metadata_id
                        }}
                    }}
                }}
            }}
            pagination {{ 
                count 
                from 
                page 
                total 
                pages 
                size 
            }}
        }}
    }}"""


def alter_cases_aliquots_objects(json_obj_list):
    """
    This function is passed as a parameter to build_jsonl_from_pdc_api(). It allows for the json object to be mutated
    prior to writing it to a file.
    :param json_obj_list: list of json objects to mutate
    """
    for case in json_obj_list:
        if 'is_ffpe' in case:
            if case['is_ffpe'] == "0" or case['is_ffpe'] == 0:
                case['is_ffpe'] = "False"
            if case['is_ffpe'] == "1" or case['is_ffpe'] == 1:
                case['is_ffpe'] = "True"


def make_case_metadata_table_query():
    """
    Make query used to create case metadata table.
    :return: Case metadata query string
    """
    case_aliquot_table_id, study_table_id, case_external_mapping_table_id = get_mapping_table_ids()

    file_count_table_name = construct_table_name(API_PARAMS, prefix=BQ_PARAMS['FILE_COUNT_TABLE'])
    file_count_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{file_count_table_name}"

    return f"""
        WITH case_project_file_count AS (
            SELECT DISTINCT c.case_id, c.case_submitter_id, c.project_submitter_id, 
                s.project_name, s.program_name, s.project_id,
                fc.file_id_count as file_count
            FROM `{case_external_mapping_table_id}` c
            JOIN `{study_table_id}` s
                ON c.project_submitter_id = s.project_submitter_id
            JOIN `{file_count_table_id}` fc
                ON c.case_id = fc.case_id
        )

        SELECT ca.case_id, ca.case_submitter_id, ca.primary_site, ca.disease_type,
            cp.project_name, cp.program_name, cp.project_id, cp.file_count
        FROM `{case_aliquot_table_id}` AS ca
        JOIN case_project_file_count AS cp
            ON cp.case_id = ca.case_id
    """


def make_aliquot_to_case_id_query():
    """
    Make query to construct aliquot to case id mapping table.
    :return: Aliquot to case id query string
    """
    case_aliquot_table_id, study_table_id, case_external_mapping_table_id = get_mapping_table_ids()

    return f"""
        WITH cases_samples AS (
            SELECT c.case_id, c.case_submitter_id, 
                s.sample_id, s.sample_submitter_id, s.sample_type, s.preservation_method, 
                s.freezing_method, s.time_between_clamping_and_freezing, s.time_between_excision_and_freezing,
                s.days_to_collection, s.initial_weight, s.current_weight, s.shortest_dimension, 
                s.intermediate_dimension, s.longest_dimension,
                s.aliquots
            FROM `{case_aliquot_table_id}` AS c
            CROSS JOIN UNNEST(samples) AS s
        ),
        samples_aliquots AS (
            SELECT case_id, case_submitter_id, sample_id, sample_submitter_id, sample_type, preservation_method, 
                is_ffpe, freezing_method, time_between_clamping_and_freezing, time_between_excision_and_freezing,
                days_to_collection, initial_weight, current_weight, shortest_dimension, 
                intermediate_dimension, longest_dimension,
                a.aliquot_id, a.aliquot_submitter_id 
            FROM cases_samples 
            CROSS JOIN UNNEST (aliquots) AS a
        ), 
        cases_projects AS (
            SELECT DISTINCT ext_map.case_id, studies.program_name, studies.project_name
            FROM `{case_external_mapping_table_id}` ext_map
            JOIN `{study_table_id}` studies
                ON studies.project_submitter_id = ext_map.project_submitter_id
        )
        
        SELECT p.program_name, p.project_name, 
            sa.case_id, sa.case_submitter_id, sa.sample_id, sa.sample_submitter_id, 
            sa.sample_type, sa.is_ffpe, sa.preservation_method, sa.freezing_method, 
            sa.time_between_clamping_and_freezing, sa.time_between_excision_and_freezing,
            sa.days_to_collection, sa.initial_weight, sa.current_weight, 
            sa.shortest_dimension, sa.intermediate_dimension, sa.longest_dimension,
            sa.aliquot_id, sa.aliquot_submitter_id 
        FROM samples_aliquots AS sa
        JOIN cases_projects AS p
            ON sa.case_id = p.case_id
        """


def make_aliquot_run_metadata_query():
    """
    Make aliquot run metadata table query.
    :return: Aliquot run metadata table query string
    """

    case_aliquot_table_id, study_table_id, case_external_mapping_table_id = get_mapping_table_ids()

    return f"""
        WITH cases_samples AS (
            SELECT c.case_id, s.sample_id, s.aliquots
            FROM `{case_aliquot_table_id}` AS c
            CROSS JOIN UNNEST(samples) AS s
        ),
        samples_aliquots AS (
            SELECT case_id, sample_id, a.aliquot_id, a.aliquot_run_metadata
            FROM cases_samples
            CROSS JOIN UNNEST (aliquots) AS a
        )
    
        SELECT case_id, sample_id, aliquot_id, r.aliquot_run_metadata_id
        FROM samples_aliquots
        CROSS JOIN UNNEST (aliquot_run_metadata) as r
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

    aliquot_prefix = get_prefix(API_PARAMS, API_PARAMS['ALIQUOT_ENDPOINT'])

    if 'build_case_aliquot_jsonl' in steps:
        per_study_case_aliquot_list = build_obj_from_pdc_api(API_PARAMS,
                                                             endpoint=API_PARAMS['ALIQUOT_ENDPOINT'],
                                                             request_function=make_cases_aliquots_query,
                                                             alter_json_function=alter_cases_aliquots_objects,
                                                             insert_id=True)
        create_and_upload_schema_for_json(API_PARAMS, BQ_PARAMS, record_list=per_study_case_aliquot_list,
                                          table_name=aliquot_prefix, include_release=True)
        write_list_to_jsonl_and_upload(API_PARAMS, BQ_PARAMS, aliquot_prefix, per_study_case_aliquot_list)

    if 'build_case_aliquot_table' in steps:
        aliquot_schema = retrieve_bq_schema_object(API_PARAMS, BQ_PARAMS,
                                                   table_name=aliquot_prefix)
        build_table_from_jsonl(API_PARAMS, BQ_PARAMS,
                               endpoint=API_PARAMS['ALIQUOT_ENDPOINT'],
                               infer_schema=True,
                               schema=aliquot_schema)

    if 'build_aliquot_run_metadata_map_table' in steps:
        aliquot_run_metadata_query = make_aliquot_run_metadata_query()
        table_name = construct_table_name(API_PARAMS, BQ_PARAMS['ALIQUOT_RUN_METADATA_TABLE'])
        table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{table_name}"

        load_table_from_query(BQ_PARAMS, table_id, aliquot_run_metadata_query)

    if 'build_case_metadata_table' in steps:
        case_metadata_table_query = make_case_metadata_table_query()
        table_name = construct_table_name(API_PARAMS, BQ_PARAMS['CASE_METADATA_TABLE'])
        table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{table_name}"

        load_table_from_query(BQ_PARAMS, table_id, case_metadata_table_query)
        update_table_schema_from_generic_pdc(API_PARAMS, BQ_PARAMS, table_id)

    if 'build_aliquot_to_case_id_map_table' in steps:
        aliquot_to_case_id_query = make_aliquot_to_case_id_query()
        table_name = construct_table_name(API_PARAMS, BQ_PARAMS['ALIQUOT_TO_CASE_TABLE'])
        table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{table_name}"

        load_table_from_query(BQ_PARAMS, table_id, aliquot_to_case_id_query)
        update_table_schema_from_generic_pdc(API_PARAMS, BQ_PARAMS,
                                             table_id=table_id,
                                             metadata_file=BQ_PARAMS['GENERIC_ALIQ_MAP_METADATA_FILE'])

    if 'test_new_version_case_metadata_table' in steps:
        case_metadata_table_name = construct_table_name(API_PARAMS, prefix=BQ_PARAMS['CASE_METADATA_TABLE'])
        case_metadata_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{case_metadata_table_name}"

        test_table_for_version_changes(API_PARAMS, BQ_PARAMS,
                                       public_dataset=BQ_PARAMS['PUBLIC_META_DATASET'],
                                       source_table_id=case_metadata_table_id,
                                       get_publish_table_ids=get_publish_table_ids,
                                       find_most_recent_published_table_id=find_most_recent_published_table_id,
                                       id_keys="case_id")

    if 'test_new_version_aliquot_to_case_mapping_table' in steps:
        # Publish aliquot to case mapping table
        mapping_table_name = construct_table_name(API_PARAMS, prefix=BQ_PARAMS['ALIQUOT_TO_CASE_TABLE'])
        mapping_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{mapping_table_name}"

        test_table_for_version_changes(API_PARAMS, BQ_PARAMS,
                                       public_dataset=BQ_PARAMS['PUBLIC_META_DATASET'],
                                       source_table_id=mapping_table_id,
                                       get_publish_table_ids=get_publish_table_ids,
                                       find_most_recent_published_table_id=find_most_recent_published_table_id,
                                       id_keys="aliquot_id")

    if "publish_case_metadata_tables" in steps:
        # Publish master case metadata table
        case_metadata_table_name = construct_table_name(API_PARAMS, prefix=BQ_PARAMS['CASE_METADATA_TABLE'])
        case_metadata_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{case_metadata_table_name}"

        publish_table(API_PARAMS, BQ_PARAMS,
                      public_dataset=BQ_PARAMS['PUBLIC_META_DATASET'],
                      source_table_id=case_metadata_table_id,
                      get_publish_table_ids=get_publish_table_ids,
                      find_most_recent_published_table_id=find_most_recent_published_table_id,
                      overwrite=True)

        # Publish aliquot to case mapping table
        mapping_table_name = construct_table_name(API_PARAMS, prefix=BQ_PARAMS['ALIQUOT_TO_CASE_TABLE'])
        mapping_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{mapping_table_name}"

        publish_table(API_PARAMS, BQ_PARAMS,
                      public_dataset=BQ_PARAMS['PUBLIC_META_DATASET'],
                      source_table_id=mapping_table_id,
                      get_publish_table_ids=get_publish_table_ids,
                      find_most_recent_published_table_id=find_most_recent_published_table_id,
                      overwrite=True)

    end = time.time() - start_time
    print(f"Finished program execution in {format_seconds(end)}!\n")


if __name__ == '__main__':
    main(sys.argv)

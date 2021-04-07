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

from common_etl.utils import (format_seconds, has_fatal_error, load_config, construct_table_name, get_query_results,
                              return_schema_object_for_bq, normalize_value, load_table_from_query, get_filepath,
                              update_schema)

from BQ_Table_Building.PDC.pdc_utils import (build_obj_from_pdc_api, build_table_from_jsonl, get_pdc_study_ids,
                                             get_dev_table_id, normalize_data_and_create_schema, get_prefix,
                                             write_jsonl_and_upload)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')

"""
WITH cases_samples AS (
    SELECT * except(samples)
    FROM `isb-project-zero.PDC_metadata.case_sample_aliquot_mapping_V1_11`
    CROSS JOIN UNNEST(samples) as s
),
samples_aliquots AS (
    SELECT * except(aliquots)
    FROM cases_samples 
    CROSS JOIN UNNEST (aliquots) as a
)

SELECT * FROM samples_aliquots 
"""


def make_cases_aliquots_query(offset, limit):
    """
    
    Creates a graphQL string for querying the PDC API's paginatedCasesSamplesAliquots endpoint.
    :param offset: starting index for which to return records
    :param limit: maximum number of records to return
    :return: GraphQL query string
    """
    return """{{ 
        paginatedCasesSamplesAliquots(offset:{0} limit:{1} acceptDUA: true) {{ 
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
                    is_ffpe
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
    }}""".format(offset, limit)


def alter_cases_aliquots_objects(json_obj_list):
    """
    This function is passed as a parameter to build_jsonl_from_pdc_api(). It allows for the json object to be mutated
    prior to writing it to a file.
    :param json_obj_list: list of json objects to mutate
    :param pdc_study_id: pdc study id for this set of json objects
    """
    for case in json_obj_list:
        if 'is_ffpe' in case:
            if case['is_ffpe'] == "0" or case['is_ffpe'] == 0:
                case['is_ffpe'] == "False"
            if case['is_ffpe'] == "1" or case['is_ffpe'] == 1:
                case['is_ffpe'] == "True"


def make_biospecimen_per_study_query(pdc_study_id):
    """

    Creates a graphQL string for querying the PDC API's biospecimenPerStudy endpoint.
    :return: GraphQL query string
    """
    return '''{{ 
        biospecimenPerStudy( pdc_study_id: \"{}\" acceptDUA: true) {{
            aliquot_id 
            sample_id 
            case_id 
            aliquot_submitter_id 
            sample_submitter_id 
            case_submitter_id 
            project_name 
            sample_type 
            disease_type 
            primary_site 
            pool 
        }}
    }}'''.format(pdc_study_id)


def alter_biospecimen_per_study_objects(json_obj_list, pdc_study_id):
    """

    Passed as a parameter to build_jsonl_from_pdc_api(). Allows for the dataset's json object to be mutated prior
    to writing it to a file.
    :param json_obj_list: list of json objects to mutate
    :param pdc_study_id: pdc study id for this set of json objects
    """
    # todo change how this works -- not dependent on biospecimen

    def get_case_file_count_mapping():
        """

        Gets a dictionary of form {case_id: file_count} using table created during file metadata ingestion;
        derived from associated entity mapping table
        :return: { '<case_id>': '<file_count>'}
        """
        table_name = construct_table_name(API_PARAMS, prefix=BQ_PARAMS['FILE_COUNT_TABLE'])
        table_id = get_dev_table_id(BQ_PARAMS,
                                    dataset=BQ_PARAMS['META_DATASET'],
                                    table_name=table_name)
        case_file_count_query = """
        SELECT case_id, file_id_count
        FROM {}
        """.format(table_id)

        res = get_query_results(case_file_count_query)
        case_file_count_map = dict()

        for case_file_count_row in res:
            case_id = case_file_count_row[0]
            file_count = case_file_count_row[1]
            case_file_count_map[case_id] = file_count

        return case_file_count_map

    case_file_count_map = get_case_file_count_mapping()

    for case in json_obj_list:
        case['pdc_study_id'] = pdc_study_id
        case_id = case['case_id']
        case['file_count'] = case_file_count_map[case_id] if case_id in case_file_count_map else 0


def main(args):
    start_time = time.time()
    print("PDC script started at {}".format(time.strftime("%x %X", time.localtime())))

    steps = None

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    all_pdc_study_ids = get_pdc_study_ids(API_PARAMS, BQ_PARAMS, include_embargoed_studies=True)

    biospecimen_endpoint = 'biospecimenPerStudy'
    biospecimen_prefix = get_prefix(API_PARAMS, biospecimen_endpoint)

    aliquot_endpoint = 'paginatedCasesSamplesAliquots'
    aliquot_prefix = get_prefix(API_PARAMS, aliquot_endpoint)

    if 'build_biospecimen_jsonl' in steps:
        per_study_biospecimen_list = build_obj_from_pdc_api(API_PARAMS,
                                                            endpoint=biospecimen_endpoint,
                                                            request_function=make_biospecimen_per_study_query,
                                                            alter_json_function=alter_biospecimen_per_study_objects,
                                                            ids=all_pdc_study_ids,
                                                            insert_id=True)

        normalize_data_and_create_schema(API_PARAMS, BQ_PARAMS,
                                         joined_record_list=per_study_biospecimen_list,
                                         table_type=biospecimen_prefix)

        write_jsonl_and_upload(API_PARAMS, BQ_PARAMS, biospecimen_prefix, per_study_biospecimen_list)

    if 'build_biospecimen_table' in steps:
        biospecimen_schema = return_schema_object_for_bq(API_PARAMS, BQ_PARAMS,
                                                         table_type=biospecimen_prefix)
        build_table_from_jsonl(API_PARAMS, BQ_PARAMS,
                               endpoint=biospecimen_endpoint,
                               infer_schema=True,
                               schema=biospecimen_schema)

    if 'build_case_aliquot_jsonl' in steps:
        per_study_case_aliquot_list = build_obj_from_pdc_api(API_PARAMS,
                                                             endpoint=aliquot_endpoint,
                                                             request_function=make_cases_aliquots_query,
                                                             alter_json_function=alter_cases_aliquots_objects,
                                                             insert_id=True)
        normalize_data_and_create_schema(API_PARAMS, BQ_PARAMS,
                                         joined_record_list=per_study_case_aliquot_list,
                                         table_type=aliquot_prefix)

        write_jsonl_and_upload(API_PARAMS, BQ_PARAMS, aliquot_prefix, per_study_case_aliquot_list)

    if 'build_case_aliquot_table' in steps:
        aliquot_schema = return_schema_object_for_bq(API_PARAMS, BQ_PARAMS,
                                                     table_type=aliquot_prefix)
        build_table_from_jsonl(API_PARAMS, BQ_PARAMS,
                               endpoint=aliquot_endpoint,
                               infer_schema=True,
                               schema=aliquot_schema)

    case_aliquot_table_name = construct_table_name(API_PARAMS, prefix=aliquot_prefix)
    case_aliquot_table_id = get_dev_table_id(BQ_PARAMS,
                                             dataset=BQ_PARAMS['META_DATASET'],
                                             table_name=case_aliquot_table_name)

    study_endpoint = 'allPrograms'
    study_prefix = get_prefix(API_PARAMS, study_endpoint)
    study_table_name = construct_table_name(API_PARAMS, prefix=study_prefix)
    study_table_id = get_dev_table_id(BQ_PARAMS,
                                      dataset=BQ_PARAMS['META_DATASET'],
                                      table_name=study_table_name)

    case_external_mapping_endpoint = 'allCases'
    case_external_mapping_prefix = get_prefix(API_PARAMS, case_external_mapping_endpoint)
    case_external_mapping_table_name = construct_table_name(API_PARAMS, prefix=case_external_mapping_prefix)
    case_external_mapping_table_id = get_dev_table_id(BQ_PARAMS,
                                                      dataset=BQ_PARAMS['META_DATASET'],
                                                      table_name=case_external_mapping_table_name)


    file_count_table_name = construct_table_name(API_PARAMS, prefix=BQ_PARAMS['FILE_COUNT_TABLE'])
    file_count_table_id = get_dev_table_id(BQ_PARAMS,
                                          dataset=BQ_PARAMS['META_DATASET'],
                                          table_name=file_count_table_name)

    fields_file = "{}.json".format(BQ_PARAMS['FIELD_DESC_FILE_SUFFIX'])
    fields_path = '/'.join([BQ_PARAMS['BQ_REPO'], BQ_PARAMS['FIELD_DESC_DIR']])
    field_desc_fp = get_filepath(fields_path, fields_file)

    with open(field_desc_fp) as field_output:
        descriptions = json.load(field_output)

    if 'build_aliquot_run_metadata_map_table' in steps:
        aliquot_run_metadata_query = """
        WITH cases_samples AS (
            SELECT c.case_id, s.sample_id, s.aliquots
            FROM `{}` AS c
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
        """.format(case_aliquot_table_id)

        table_name = construct_table_name(API_PARAMS, BQ_PARAMS['ALIQUOT_RUN_METADATA_TABLE'])
        table_id = get_dev_table_id(BQ_PARAMS, dataset=BQ_PARAMS['META_DATASET'], table_name=table_name)
        load_table_from_query(BQ_PARAMS, table_id, aliquot_run_metadata_query)

        update_schema(table_id, descriptions)

    if 'build_case_metadata_table' in steps:
        # case_id, case_submitter_id, primary_site,
        # project_disease_type, project_name, program_name, project_id,  file_count
        case_metadata_table_query = """
        WITH case_project_file_count AS (
            SELECT DISTINCT c.case_id, c.case_submitter_id, c.project_submitter_id, 
                s.project_name, s.program_name, s.project_id,
                fc.file_id_count as file_count
            FROM `{0}` c
            JOIN `{1}` s
                ON c.project_submitter_id = s.project_submitter_id
            JOIN `{2}` fc
                ON c.case_id = fc.case_id
        )

        SELECT ca.case_id, ca.case_submitter_id, ca.primary_site, ca.disease_type,
            cp.project_name, cp.program_name, cp.project_id, cp.file_count
        FROM `{3}` AS ca
        JOIN case_project_file_count AS cp
            ON cp.case_id = ca.case_id
        """.format(case_external_mapping_table_id, study_table_id, file_count_table_id, case_aliquot_table_id)

        table_name = construct_table_name(API_PARAMS, BQ_PARAMS['CASE_METADATA_TABLE'])
        table_id = get_dev_table_id(BQ_PARAMS, dataset=BQ_PARAMS['META_DATASET'], table_name=table_name)
        load_table_from_query(BQ_PARAMS, table_id, case_metadata_table_query)

        update_schema(table_id, descriptions)

    if 'build_aliquot_to_case_id_map_table' in steps:
        aliquot_to_case_id_query = """
        WITH cases_samples AS (
            SELECT c.case_id, c.case_submitter_id, 
                s.sample_id, s.sample_submitter_id, s.sample_type, s.is_ffpe, s.preservation_method, s.aliquots
            FROM `{0}` AS c
            CROSS JOIN UNNEST(samples) AS s
        ),
        samples_aliquots AS (
            SELECT case_id, case_submitter_id, sample_id, sample_submitter_id, sample_type, is_ffpe, 
            preservation_method, a.aliquot_id, a.aliquot_submitter_id 
            FROM cases_samples 
            CROSS JOIN UNNEST (aliquots) AS a
        ), 
        cases_projects AS (
            SELECT DISTINCT ext_map.case_id, studies.program_name, studies.project_name
            FROM `{1}` ext_map
            JOIN `{2}` studies
                ON studies.project_submitter_id = ext_map.project_submitter_id
        )
        
        SELECT p.program_name, p.project_name, 
            sa.case_id, sa.case_submitter_id, sa.sample_id, sa.sample_submitter_id, 
            sa.sample_type, sa.is_ffpe, sa.preservation_method, sa.aliquot_id, sa.aliquot_submitter_id 
        FROM samples_aliquots AS sa
        JOIN cases_projects AS p
            ON sa.case_id = p.case_id
        """.format(case_aliquot_table_id, case_external_mapping_table_id, study_table_id)

        table_name = construct_table_name(API_PARAMS, BQ_PARAMS['ALIQUOT_TO_CASE_TABLE'])
        table_id = get_dev_table_id(BQ_PARAMS, dataset=BQ_PARAMS['META_DATASET'], table_name=table_name)
        load_table_from_query(BQ_PARAMS, table_id, aliquot_to_case_id_query)

        update_schema(table_id, descriptions)

    end = time.time() - start_time
    print("Finished program execution in {}!\n".format(format_seconds(end)))


if __name__ == '__main__':
    main(sys.argv)

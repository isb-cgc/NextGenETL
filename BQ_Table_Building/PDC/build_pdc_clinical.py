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

from google.cloud import bigquery

from common_etl.utils import (format_seconds, write_list_to_jsonl, get_scratch_fp, upload_to_bucket,
                              has_fatal_error, load_bq_schema_from_json, create_and_load_table_from_jsonl,
                              load_table_from_query, delete_bq_table, load_config, list_bq_tables, publish_table,
                              construct_table_name, construct_table_id, create_and_upload_schema_for_json,
                              retrieve_bq_schema_object, create_view_from_query, test_table_for_version_changes,
                              recursively_normalize_field_values)

from BQ_Table_Building.PDC.pdc_utils import (infer_schema_file_location_by_table_id, get_pdc_study_ids,
                                             get_pdc_studies_list, build_obj_from_pdc_api, build_table_from_jsonl,
                                             get_filename, get_records, write_jsonl_and_upload, get_prefix,
                                             update_table_schema_from_generic_pdc, get_project_program_names,
                                             find_most_recent_published_table_id, get_project_level_schema_tags,
                                             get_publish_table_ids, create_project_dataset_map)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def make_cases_query():
    """
    Create a graphQL string for querying the PDC API's allCases endpoint.
    :return: GraphQL query string
    """
    return """{
        allCases (acceptDUA: true) {
            case_id 
            case_submitter_id
            project_id
            project_submitter_id 
            primary_site 
            disease_type
            externalReferences { 
                external_reference_id 
                reference_resource_shortname 
                reference_resource_name 
                reference_entity_location 
            }
        }
    }"""


def alter_case_objects(cases_obj_list):
    """
    Function used to alter case objects prior to insertion into jsonl file.
    :param cases_obj_list: List of case dict objects
    """

    for case_obj in cases_obj_list:
        if case_obj['project_submitter_id'] == 'CPTAC2 Retrospective':
            case_obj['project_submitter_id'] = 'CPTAC-2'


def get_cases():
    """
    Get records from allCases API endpoint.
    :return: allCases response records
    """
    endpoint = 'allCases'
    dataset = API_PARAMS['ENDPOINT_SETTINGS'][endpoint]['dataset']

    select_statement = "SELECT case_id, case_submitter_id, project_submitter_id, primary_site, disease_type"

    return get_records(API_PARAMS, BQ_PARAMS, endpoint, select_statement, dataset)


def get_case_demographics():
    """
    Get records from paginatedCaseDemographicsPerStudy API endpoint.
    :return: paginatedCaseDemographicsPerStudy response records
    """
    endpoint = 'paginatedCaseDemographicsPerStudy'
    dataset = API_PARAMS['ENDPOINT_SETTINGS'][endpoint]['dataset']

    select_statement = """
        SELECT demographic_id, demographic_submitter_id, case_id, case_submitter_id, gender, ethnicity, race, 
        days_to_birth, days_to_death, year_of_birth, year_of_death, vital_status, cause_of_death
        """

    return get_records(API_PARAMS, BQ_PARAMS, endpoint, select_statement, dataset)


def get_case_diagnoses():
    """
    Get records from paginatedCaseDiagnosesPerStudy API endpoint.
    :return: paginatedCaseDiagnosesPerStudy response records
    """
    endpoint = 'paginatedCaseDiagnosesPerStudy'
    dataset = API_PARAMS['ENDPOINT_SETTINGS'][endpoint]['dataset']
    select_statement = "SELECT case_id, case_submitter_id, diagnoses"

    return get_records(API_PARAMS, BQ_PARAMS, endpoint, select_statement, dataset)


def make_cases_diagnoses_query(pdc_study_id, offset, limit):
    """
    Creates a graphQL string for querying the PDC API's paginatedCaseDiagnosesPerStudy endpoint.
    :param pdc_study_id: PDC study id for which to return case records
    :param offset: starting response record index
    :param limit: maximum number of records to return
    :return: GraphQL query string
    """
    return f''' 
    {{ paginatedCaseDiagnosesPerStudy
        (pdc_study_id: "{pdc_study_id}" offset: {offset} limit: {limit} acceptDUA: true) {{
            total caseDiagnosesPerStudy {{
                case_id
                case_submitter_id
                diagnoses {{
                    diagnosis_id
                    tissue_or_organ_of_origin
                    age_at_diagnosis
                    primary_diagnosis
                    tumor_grade
                    tumor_stage
                    diagnosis_submitter_id
                    classification_of_tumor
                    days_to_last_follow_up
                    days_to_last_known_disease_status
                    days_to_recurrence
                    last_known_disease_status
                    morphology
                    progression_or_recurrence
                    site_of_resection_or_biopsy
                    prior_malignancy
                    ajcc_clinical_m
                    ajcc_clinical_n
                    ajcc_clinical_stage
                    ajcc_clinical_t
                    ajcc_pathologic_m
                    ajcc_pathologic_n
                    ajcc_pathologic_stage
                    ajcc_pathologic_t
                    ann_arbor_b_symptoms
                    ann_arbor_clinical_stage
                    ann_arbor_extranodal_involvement
                    ann_arbor_pathologic_stage
                    best_overall_response
                    burkitt_lymphoma_clinical_variant
                    circumferential_resection_margin
                    colon_polyps_history
                    days_to_best_overall_response
                    days_to_diagnosis
                    days_to_hiv_diagnosis
                    days_to_new_event
                    figo_stage
                    hiv_positive
                    hpv_positive_type
                    hpv_status
                    iss_stage
                    laterality
                    ldh_level_at_diagnosis
                    ldh_normal_range_upper
                    lymph_nodes_positive
                    lymphatic_invasion_present
                    method_of_diagnosis
                    new_event_anatomic_site
                    new_event_type
                    overall_survival
                    perineural_invasion_present
                    prior_treatment
                    progression_free_survival
                    progression_free_survival_event
                    residual_disease
                    vascular_invasion_present
                    year_of_diagnosis
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
    }}'''


def make_cases_demographics_query(pdc_study_id, offset, limit):
    """
    Creates a graphQL string for querying the PDC API's paginatedCaseDemographicsPerStudy endpoint.
    :param pdc_study_id: PDC study id for which to return case records
    :param offset: starting response record index
    :param limit: maximum number of records to return
    :return: GraphQL query string
    """
    return f"""
    {{ paginatedCaseDemographicsPerStudy 
        (pdc_study_id: "{pdc_study_id}" offset: {offset} limit: {limit} acceptDUA: true) {{ 
            total caseDemographicsPerStudy {{ 
                case_id 
                case_submitter_id
                demographics {{ 
                    demographic_id
                    ethnicity
                    gender
                    demographic_submitter_id
                    race
                    cause_of_death
                    days_to_birth
                    days_to_death
                    vital_status
                    year_of_birth
                    year_of_death 
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


def alter_case_demographics_json(json_obj_list, pdc_study_id):
    """
    This function is passed as a parameter to build_jsonl_from_pdc_api(). It allows for the json object to be mutated
    prior to writing it to a file.
    :param json_obj_list: list of json objects to mutate
    :param pdc_study_id: pdc study id for this set of json objects
    """
    for case in json_obj_list:

        demographics = case.pop("demographics")

        if len(demographics) > 1:
            ref_dict = None
            has_fatal_error("Cannot unnest case demographics because multiple records exist.")
        elif len(demographics) == 1:
            ref_dict = demographics[0]
        else:
            demographics_key_list = ["demographic_id", "ethnicity", "gender", "demographic_submitter_id",
                                     "race", "cause_of_death", "days_to_birth", "days_to_death",
                                     "vital_status", "year_of_birth", "year_of_death"]

            ref_dict = dict.fromkeys(demographics_key_list, None)

        case['pdc_study_id'] = pdc_study_id
        case.update(ref_dict)


def alter_case_diagnoses_json(json_obj_list, pdc_study_id):
    """
    This function is passed as a parameter to build_jsonl_from_pdc_api(). It allows for the json object to be mutated
    prior to writing it to a file.
    :param json_obj_list: list of json objects to mutate
    :param pdc_study_id: pdc study id for this set of json objects
    """
    for case in json_obj_list:
        case['pdc_study_id'] = pdc_study_id


def remove_nulls_and_create_temp_table(records, project_submitter_id, is_diagnoses=False, infer_schema=False,
                                       schema=None):
    """
    Remove columns where only null values would exist for entire table, then construct temporary project-level
    clinical table.
    :param records: clinical case record dictionary
    :param project_submitter_id: name of project to which the case records belong
    :param is_diagnoses: if True, the temp table is a supplement to the project's clinical table, due to some cases
    having multiple diagnosis records; defaults to False
    :param infer_schema: if True, script will use BigQuery's native schema inference; defaults to False
    :param schema: list of SchemaFields representing desired BQ table schema; defaults to None
    :return: newly created BQ table id
    """

    def remove_null_values(json_obj_list):
        """
        Recursively remove any fields with only null values for the entire project's set of cases.
        """
        for obj in json_obj_list:
            obj_keys = list(obj.keys())
            for key in obj_keys:
                if not obj[key]:
                    obj.pop(key)
                elif isinstance(obj[key], list):
                    remove_null_values(obj[key])

    remove_null_values(records)

    clinical_type = "clinical" if not is_diagnoses else "clinical_diagnoses"

    clinical_jsonl_filename = get_filename(API_PARAMS,
                                           file_extension='jsonl',
                                           prefix=project_submitter_id,
                                           suffix=clinical_type)

    clinical_scratch_fp = get_scratch_fp(BQ_PARAMS, clinical_jsonl_filename)

    write_list_to_jsonl(jsonl_fp=clinical_scratch_fp, json_obj_list=records)

    upload_to_bucket(BQ_PARAMS,
                     scratch_fp=clinical_scratch_fp,
                     delete_local=True)

    prefix = "_".join(["temp", project_submitter_id, clinical_type])

    clinical_or_child_table_name = construct_table_name(API_PARAMS, prefix=prefix)
    clinical_or_child_table_id = construct_table_id(BQ_PARAMS['DEV_PROJECT'],
                                                    dataset=BQ_PARAMS['CLINICAL_DATASET'],
                                                    table_name=clinical_or_child_table_name)

    if not infer_schema and not schema:
        schema_filename = infer_schema_file_location_by_table_id(clinical_or_child_table_id)
        schema = load_bq_schema_from_json(BQ_PARAMS, schema_filename)

    print(f"Creating {clinical_or_child_table_id}:")

    create_and_load_table_from_jsonl(BQ_PARAMS,
                                     jsonl_file=clinical_jsonl_filename,
                                     table_id=clinical_or_child_table_id,
                                     schema=schema)

    return clinical_or_child_table_id


def create_ordered_clinical_table(temp_table_id, project_submitter_id, clinical_type):
    """
    Using column ordering provided in YAML config file, builds a BQ table from the previously created,
    temporary clinical table. Deletes temporary table upon completion.
    :param temp_table_id: full BQ table id of temporary table
    :param project_submitter_id: Name of PDC project associated with the clinical records
    :param clinical_type: Type of clinical table, e.g. 'clinical,' 'clinical_diagnoses'
    """

    def make_subquery_string(nested_field_list):
        """
        Build subquery representing new column ordering for demographic and/or diagnoses columns.
        :param nested_field_list: List of fields nested by field group
        :return: subquery string portion of table-building sql query
        """
        subqueries = ""

        if nested_field_list:
            for field in nested_field_list:
                # sort list by index, output list of column names for use in sql query
                select_list = [tup[0] for tup in sorted(fields[field], key=lambda t: t[1])]
                select_str = ", ".join(select_list)

                subquery = f"""
                    , ARRAY(
                        SELECT AS STRUCT
                            {select_str}
                        FROM clinical.{field}
                    ) AS {field}
                """

                subqueries += subquery

        return subqueries

    def make_ordered_clinical_table_query(_fields):
        """
        Sort list by index, output list of column names for use in sql query
        """
        select_parent_list = [tup[0] for tup in sorted(_fields['parent_level'], key=lambda t: t[1])]
        select_parent_query_str = ", ".join(select_parent_list)

        _fields.pop("parent_level")

        subquery_str = make_subquery_string(_fields.keys())

        return f"""
        SELECT {select_parent_query_str}
        {subquery_str}
        FROM {temp_table_id} clinical
        """

    client = bigquery.Client()
    temp_table = client.get_table(temp_table_id)
    table_schema = temp_table.schema

    project_name_dict = get_project_program_names(API_PARAMS, BQ_PARAMS, project_submitter_id)

    clinical_project_prefix = f"{clinical_type}_{project_name_dict['project_short_name']}_{API_PARAMS['DATA_SOURCE']}"

    table_name = construct_table_name(API_PARAMS, prefix=clinical_project_prefix)
    clinical_project_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['CLINICAL_DATASET']}.{table_name}"

    fields = {"parent_level": list()}

    # create dictionary containing an ordered field list for each field group,
    # using column ordering provided in YAML file.
    for schema_field in table_schema:
        if schema_field.field_type == "RECORD":
            fields[schema_field.name] = list()
            for child_schema_field in schema_field.fields:
                column_position = BQ_PARAMS['COLUMN_ORDER'].index(child_schema_field.name)
                fields[schema_field.name].append((child_schema_field.name, column_position))
        else:
            column_position = BQ_PARAMS['COLUMN_ORDER'].index(schema_field.name)
            fields["parent_level"].append((schema_field.name, column_position))

    load_table_from_query(BQ_PARAMS,
                          table_id=clinical_project_table_id,
                          query=make_ordered_clinical_table_query(fields))

    delete_bq_table(temp_table_id)

    return clinical_project_table_id


def get_cases_by_project_submitter(studies_list):
    """
    Retrieve list of cases based on project submitter id.
    :param studies_list: list of PDC studies
    :return: Dictionary in form {"project_submitter_id" : [case list]}
    """
    # get unique project_submitter_ids from studies_list
    cases_by_project_submitter = dict()

    for study in studies_list:
        cases_by_project_submitter[study['project_submitter_id']] = {
            'cases': list(),
            'max_diagnosis_count': 0
        }

    # get all case records, append to list for its project submitter id
    for case in get_cases():
        if not case or 'project_submitter_id' not in case:
            continue

        project_submitter_id = case['project_submitter_id']
        try:
            cases_by_project_submitter[project_submitter_id]['cases'].append(case)
        except KeyError:
            print('There are no cases in ' + project_submitter_id + 'project')
            continue

    return cases_by_project_submitter


def get_diagnosis_demographic_records_by_case():
    """
    Retrieve diagnoses and demographic records for each case.
    :return: Two dicts: demographic_records_by_case_id, diagnosis_records_by_case_id
    """
    # get all demographic records
    demographic_records = get_case_demographics()
    demographic_records_by_case_id = dict()

    # create dict where key = (case_id, case_submitter_id) and value = dict of remaining query results
    for record in demographic_records:
        case_id_key_tuple = (record.pop("case_id"), record.pop("case_submitter_id"))
        demographic_records_by_case_id[case_id_key_tuple] = record

    # get all diagnoses records, create dict where
    # key = (case_id, case_submitter_id) and value = dict of remaining query results
    diagnosis_records = get_case_diagnoses()
    diagnosis_records_by_case_id = dict()

    for record in diagnosis_records:
        case_id_key_tuple = (record.pop("case_id"), record.pop("case_submitter_id"))
        diagnosis_records_by_case_id[case_id_key_tuple] = record

    return demographic_records_by_case_id, diagnosis_records_by_case_id


def append_diagnosis_demographic_to_case(cases_by_project, diagnosis_by_case, demographic_by_case):
    """
    Merge diagnosis and demographic records with cases_by_project dict.
    :param cases_by_project: Dict of project submitter ids : cases list
    :param diagnosis_by_case: Dict of case_ids : diagnosis list
    :param demographic_by_case: Dict of case_ids : demographic list
    """
    cases_with_no_clinical_data = list()

    modified_cases_by_project = dict()

    for project_name, project_dict in cases_by_project.items():
        for case in project_dict['cases']:
            case_id_key_tuple = (case['case_id'], case['case_submitter_id'])

            if case_id_key_tuple not in diagnosis_by_case:
                if case_id_key_tuple not in demographic_by_case:
                    cases_with_no_clinical_data.append(case_id_key_tuple)
                    continue

            if case_id_key_tuple in diagnosis_by_case:
                diagnosis_record = diagnosis_by_case[case_id_key_tuple]

                if len(diagnosis_record['diagnoses']) > project_dict['max_diagnosis_count']:
                    project_dict['max_diagnosis_count'] = len(diagnosis_record['diagnoses'])

                case.update(diagnosis_record)

            if case_id_key_tuple in demographic_by_case:
                demographic_record = demographic_by_case[case_id_key_tuple]

                case.update(demographic_record)

    exclude_case_id_set = set()

    for case in cases_with_no_clinical_data:
        exclude_case_id_set.add(case[0])

    for project_name, project_dict in cases_by_project.items():

        if project_name not in modified_cases_by_project:
            modified_cases_by_project[project_name] = dict()
            modified_cases_by_project[project_name]['cases'] = list()
            modified_cases_by_project[project_name]['max_diagnosis_count'] = project_dict['max_diagnosis_count']

        for case in project_dict['cases']:
            if case['case_id'] not in exclude_case_id_set:
                modified_cases_by_project[project_name]['cases'].append(case)

    return modified_cases_by_project


def build_per_project_clinical_tables(cases_by_project_submitter):
    """
    Construction of null filtering, clinical data retrieval, and per-project clinical table creation.
    :param cases_by_project_submitter: dict of form project_submitter_id : cases list
    """
    for project_submitter_id, project_dict in cases_by_project_submitter.items():
        record_count = len(project_dict['cases'])
        max_diagnosis_count = project_dict['max_diagnosis_count']

        print(f"\n{project_submitter_id}: {record_count} records, {max_diagnosis_count} max diagnoses")

        clinical_records = []
        clinical_diagnoses_records = []

        # iterate over now-populated project dicts
        # - if max diagnosis record length is 1, create single PROJECT_clinical_pdc_current table
        # - else create a PROJECT_clinical_pdc_current table and a PROJECT_clinical_diagnoses_pdc_current table
        cases = project_dict['cases']
        for case in cases:
            if 'case_id' not in case:
                continue
            clinical_case_record = case
            clinical_diagnoses_record = dict()
            diagnoses = case.pop('diagnoses') if 'diagnoses' in case else None

            if not clinical_case_record or max_diagnosis_count == 0:
                continue
            if max_diagnosis_count == 1 and diagnoses:
                clinical_case_record.update(diagnoses[0])
            elif max_diagnosis_count > 1 and diagnoses:
                for diagnosis in diagnoses:
                    clinical_diagnoses_record['case_id'] = clinical_case_record['case_id']
                    clinical_diagnoses_record['case_submitter_id'] = clinical_case_record['case_submitter_id']
                    clinical_diagnoses_record['project_submitter_id'] = clinical_case_record['project_submitter_id']
                    clinical_diagnoses_record.update(diagnosis)
                    clinical_diagnoses_records.append(clinical_diagnoses_record)

            clinical_records.append(clinical_case_record)

        schema_tags = get_project_level_schema_tags(API_PARAMS, BQ_PARAMS, project_submitter_id)

        if clinical_records:
            temp_clinical_table_id = remove_nulls_and_create_temp_table(records=clinical_records,
                                                                        project_submitter_id=project_submitter_id,
                                                                        infer_schema=True)

            final_table_id = create_ordered_clinical_table(temp_table_id=temp_clinical_table_id,
                                                           project_submitter_id=project_submitter_id,
                                                           clinical_type=BQ_PARAMS['CLINICAL_TABLE'])

            if 'program-name-1-lower' in schema_tags:
                update_table_schema_from_generic_pdc(API_PARAMS, BQ_PARAMS,
                                                     table_id=final_table_id,
                                                     schema_tags=schema_tags,
                                                     metadata_file=BQ_PARAMS['GENERIC_TABLE_METADATA_FILE_2_PROGRAM'])
            else:
                update_table_schema_from_generic_pdc(API_PARAMS, BQ_PARAMS,
                                                     table_id=final_table_id,
                                                     schema_tags=schema_tags)

        if clinical_diagnoses_records:
            schema_tags['mapping-name'] = 'DIAGNOSES '

            temp_diagnoses_table_id = remove_nulls_and_create_temp_table(records=clinical_diagnoses_records,
                                                                         project_submitter_id=project_submitter_id,
                                                                         is_diagnoses=True,
                                                                         infer_schema=True)

            final_table_id = create_ordered_clinical_table(temp_table_id=temp_diagnoses_table_id,
                                                           project_submitter_id=project_submitter_id,
                                                           clinical_type=BQ_PARAMS['CLINICAL_DIAGNOSES_TABLE'])

            if 'program-name-1-lower' in schema_tags:
                update_table_schema_from_generic_pdc(API_PARAMS, BQ_PARAMS,
                                                     table_id=final_table_id,
                                                     schema_tags=schema_tags,
                                                     metadata_file=BQ_PARAMS['GENERIC_TABLE_METADATA_FILE_2_PROGRAM'])
            else:
                update_table_schema_from_generic_pdc(API_PARAMS, BQ_PARAMS,
                                                     table_id=final_table_id,
                                                     schema_tags=schema_tags)


def create_filtered_clinical_table_list():
    """
    todo
    :return:
    """
    # iterate over existing dev project clinical tables for current API version
    current_clinical_table_list = list_bq_tables(dataset_id=BQ_PARAMS['CLINICAL_DATASET'],
                                                 release=API_PARAMS['RELEASE'])

    filtered_clinical_table_list = list()

    for table in current_clinical_table_list:
        table_name = table.split('.')[-1]
        if table_name[0:4] != 'case':
            filtered_clinical_table_list.append(table)

    return filtered_clinical_table_list


def main(args):
    start_time = time.time()
    print(f"PDC script started at {time.strftime('%x %X', time.localtime())}")

    steps = None

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    pdc_study_ids = get_pdc_study_ids(API_PARAMS, BQ_PARAMS, include_embargoed_studies=False)

    if 'build_cases_jsonl' in steps:
        endpoint = API_PARAMS['CASE_EXTERNAL_MAP_ENDPOINT']
        table_name = API_PARAMS['ENDPOINT_SETTINGS'][endpoint]['output_name']

        raw_joined_cases_list = build_obj_from_pdc_api(API_PARAMS,
                                                       endpoint=endpoint,
                                                       request_function=make_cases_query,
                                                       alter_json_function=alter_case_objects)

        norm_joined_cases_list = recursively_normalize_field_values(raw_joined_cases_list)

        create_and_upload_schema_for_json(API_PARAMS, BQ_PARAMS,
                                          record_list=norm_joined_cases_list,
                                          table_name=table_name,
                                          include_release=True)

        write_jsonl_and_upload(API_PARAMS, BQ_PARAMS,
                               prefix=get_prefix(API_PARAMS, 'allCases'),
                               joined_record_list=norm_joined_cases_list)

        write_jsonl_and_upload(API_PARAMS, BQ_PARAMS,
                               prefix=get_prefix(API_PARAMS, 'allCases') + '_raw',
                               joined_record_list=raw_joined_cases_list)

    if 'build_cases_table' in steps:
        endpoint = API_PARAMS['CASE_EXTERNAL_MAP_ENDPOINT']
        table_name = API_PARAMS['ENDPOINT_SETTINGS'][endpoint]['output_name']

        schema = retrieve_bq_schema_object(API_PARAMS, BQ_PARAMS, table_name=table_name)

        build_table_from_jsonl(API_PARAMS, BQ_PARAMS, endpoint=endpoint, schema=schema)

    if 'build_case_diagnoses_jsonl' in steps:
        endpoint = API_PARAMS['PER_STUDY_DIAGNOSES_ENDPOINT']
        table_name = API_PARAMS['ENDPOINT_SETTINGS'][endpoint]['output_name']

        raw_joined_cases_list = build_obj_from_pdc_api(API_PARAMS,
                                                       endpoint=endpoint,
                                                       request_function=make_cases_diagnoses_query,
                                                       alter_json_function=alter_case_diagnoses_json,
                                                       ids=pdc_study_ids,
                                                       insert_id=True)

        norm_joined_cases_list = recursively_normalize_field_values(raw_joined_cases_list)

        create_and_upload_schema_for_json(API_PARAMS, BQ_PARAMS,
                                          record_list=norm_joined_cases_list,
                                          table_name=table_name,
                                          include_release=True)

        write_jsonl_and_upload(API_PARAMS, BQ_PARAMS,
                               prefix=get_prefix(API_PARAMS, endpoint),
                               joined_record_list=norm_joined_cases_list)
        write_jsonl_and_upload(API_PARAMS, BQ_PARAMS,
                               prefix=get_prefix(API_PARAMS, endpoint) + '_raw',
                               joined_record_list=raw_joined_cases_list)

    if 'build_case_diagnoses_table' in steps:
        endpoint = API_PARAMS['PER_STUDY_DIAGNOSES_ENDPOINT']
        table_name = API_PARAMS['ENDPOINT_SETTINGS'][endpoint]['output_name']

        schema = retrieve_bq_schema_object(API_PARAMS, BQ_PARAMS, table_name=table_name)

        build_table_from_jsonl(API_PARAMS, BQ_PARAMS,
                               endpoint=endpoint,
                               infer_schema=False,
                               schema=schema)

    if 'build_case_demographics_jsonl' in steps:
        endpoint = API_PARAMS['PER_STUDY_DEMOGRAPHIC_ENDPOINT']
        table_name = API_PARAMS['ENDPOINT_SETTINGS'][endpoint]['output_name']

        raw_joined_cases_list = build_obj_from_pdc_api(API_PARAMS,
                                                       endpoint=endpoint,
                                                       request_function=make_cases_demographics_query,
                                                       alter_json_function=alter_case_demographics_json,
                                                       ids=pdc_study_ids,
                                                       insert_id=True)

        norm_joined_cases_list = recursively_normalize_field_values(raw_joined_cases_list)

        create_and_upload_schema_for_json(API_PARAMS, BQ_PARAMS,
                                          record_list=norm_joined_cases_list,
                                          table_name=table_name,
                                          include_release=True)

        write_jsonl_and_upload(API_PARAMS, BQ_PARAMS,
                               prefix=get_prefix(API_PARAMS, endpoint),
                               joined_record_list=norm_joined_cases_list)

        write_jsonl_and_upload(API_PARAMS, BQ_PARAMS,
                               prefix=get_prefix(API_PARAMS, endpoint) + "_raw",
                               joined_record_list=raw_joined_cases_list)

    if 'build_case_demographics_table' in steps:
        endpoint = API_PARAMS['PER_STUDY_DEMOGRAPHIC_ENDPOINT']
        table_name = API_PARAMS['ENDPOINT_SETTINGS'][endpoint]['output_name']

        schema = retrieve_bq_schema_object(API_PARAMS, BQ_PARAMS, table_name=table_name)

        build_table_from_jsonl(API_PARAMS, BQ_PARAMS, endpoint=endpoint, schema=schema)

    if 'build_case_clinical_jsonl_and_tables_per_project' in steps:
        studies_list = get_pdc_studies_list(API_PARAMS, BQ_PARAMS, include_embargoed=False)
        # get unique project_submitter_ids from studies_list
        cases_by_project_submitter = get_cases_by_project_submitter(studies_list)

        # get demographic and diagnosis records for each case, and append to cases_by_project dictionary
        demographics_by_case, diagnosis_by_case = get_diagnosis_demographic_records_by_case()

        # retrieve case demographic and diagnoses for case, pop, add to case record
        cases_by_project = append_diagnosis_demographic_to_case(cases_by_project=cases_by_project_submitter,
                                                                diagnosis_by_case=diagnosis_by_case,
                                                                demographic_by_case=demographics_by_case)

        # build clinical tables--flattens or creates supplemental diagnoses tables as needed
        build_per_project_clinical_tables(cases_by_project)

    if 'test_new_version_clinical_tables' in steps:
        project_dataset_map = create_project_dataset_map(API_PARAMS, BQ_PARAMS)
        filtered_clinical_table_list = create_filtered_clinical_table_list()

        for table_name in filtered_clinical_table_list:
            project_short_name = table_name

            # strip table name down to project short name; use as key to look up program dataset name
            for rem_str in ['clinical_diagnoses_', 'clinical_', f"_pdc_{API_PARAMS['RELEASE']}"]:
                if rem_str in project_short_name:
                    project_short_name = project_short_name.replace(rem_str, '')

            clinical_table_id = construct_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['CLINICAL_DATASET'], table_name)

            public_dataset = project_dataset_map[project_short_name]

            test_table_for_version_changes(API_PARAMS, BQ_PARAMS,
                                           public_dataset=public_dataset,
                                           source_table_id=clinical_table_id,
                                           get_publish_table_ids=get_publish_table_ids,
                                           find_most_recent_published_table_id=find_most_recent_published_table_id,
                                           id_keys="case_id")

    if "publish_clinical_tables" in steps:
        # create dict of project short names and the dataset they belong to
        project_dataset_map = create_project_dataset_map(API_PARAMS, BQ_PARAMS)
        filtered_clinical_table_list = create_filtered_clinical_table_list()

        for table_name in filtered_clinical_table_list:
            project_short_name = table_name

            # strip table name down to project short name; use as key to look up program dataset name
            for rem_str in ['clinical_diagnoses_', 'clinical_', f"_pdc_{API_PARAMS['RELEASE']}"]:
                if rem_str in project_short_name:
                    project_short_name = project_short_name.replace(rem_str, '')

            clinical_table_id = construct_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['CLINICAL_DATASET'], table_name)

            public_dataset = project_dataset_map[project_short_name]

            publish_table(API_PARAMS, BQ_PARAMS,
                          public_dataset=public_dataset,
                          source_table_id=clinical_table_id,
                          get_publish_table_ids=get_publish_table_ids,
                          find_most_recent_published_table_id=find_most_recent_published_table_id,
                          overwrite=True)

    if 'create_solr_views' in steps:
        # todo abstract this
        prod_meta_dataset = f"{BQ_PARAMS['PROD_PROJECT']}.{BQ_PARAMS['PUBLIC_META_DATASET']}"
        webapp_dataset = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['WEBAPP_DATASET']}"

        clinical_query = f"""
        SELECT cl.case_id AS case_pdc_id, 
            cl.case_submitter_id AS case_barcode,
            ARRAY_TO_STRING(SPLIT(st.project_short_name, '_'), '-') AS project_short_name, 
            cl.primary_site, 
            cl.disease_type, 
            cl.gender, 
            cl.primary_diagnosis, 
            cl.last_known_disease_status,
            cl.tissue_or_organ_of_origin,
            CAST(null AS STRING) AS disease_code
        FROM {BQ_PARAMS['PROD_PROJECT']}.GPRP.clinical_georgetown_lung_cancer_pdc_current cl
        JOIN {BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.studies_{API_PARAMS['RELEASE']} st
            ON st.project_submitter_id = cl.project_submitter_id
        """

        clinical_view_id = f"{webapp_dataset}.clinical_georgetown_lung_cancer_pdc"
        create_view_from_query(view_id=clinical_view_id, view_query=clinical_query)

        biospec_query = f"""
        SELECT st.program_short_name, 
        ARRAY_TO_STRING(SPLIT(st.project_short_name, '_'), '-') AS project_short_name, 
        cl.case_id AS case_pdc_id, 
        cl.case_submitter_id AS case_barcode, 
        atc.sample_id AS sample_pdc_id, 
        atc.sample_submitter_id AS sample_barcode
        FROM {BQ_PARAMS['PROD_PROJECT']}.GPRP.clinical_georgetown_lung_cancer_pdc_current cl
        JOIN {prod_meta_dataset}.{BQ_PARAMS['ALIQUOT_TO_CASE_TABLE']}_current atc
            ON cl.case_id = atc.case_id
        JOIN {BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.studies_{API_PARAMS['RELEASE']} st
            ON st.project_submitter_id = cl.project_submitter_id
        """

        biospec_view_id = f"{webapp_dataset}.biospecimen_stub_georgetown_lung_cancer_pdc"
        create_view_from_query(view_id=biospec_view_id, view_query=biospec_query)

    end = time.time() - start_time
    print(f"Finished program execution in {format_seconds(end)}!\n")


if __name__ == '__main__':
    main(sys.argv)

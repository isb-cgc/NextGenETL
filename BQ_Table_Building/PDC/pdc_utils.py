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
import json
import os
import time

from datetime import date

from common_etl.utils import (get_filename, get_filepath, get_query_results, write_list_to_jsonl,
                              get_scratch_fp, upload_to_bucket, get_graphql_api_response, has_fatal_error,
                              load_bq_schema_from_json, create_and_load_table_from_jsonl,
                              load_table_from_query, delete_bq_table, copy_bq_table, exists_bq_table,
                              update_table_metadata, construct_table_name, construct_table_id,
                              add_generic_table_metadata, add_column_descriptions, construct_table_name_from_list)

from common_etl.support import (bq_harness_with_result)


def request_data_from_pdc_api(api_params, endpoint, request_body_function, request_parameters=None):
    """

    Used internally by build_jsonl_from_pdc_api().
    :param api_params: API params from YAML config
    :param endpoint: PDC API endpoint
    :param request_body_function: function outputting GraphQL request body (including query)
    :param request_parameters: API request parameters
    :return: Response results list
    """
    is_paginated = api_params['ENDPOINT_SETTINGS'][endpoint]['is_paginated']
    payload_key = api_params['ENDPOINT_SETTINGS'][endpoint]['payload_key']

    def append_api_response_data(_graphql_request_body):
        # Adds api response data to record list

        api_response = get_graphql_api_response(api_params, _graphql_request_body)

        try:
            response_body = api_response['data'] if not is_paginated else api_response['data'][endpoint]

            for record in response_body[payload_key]:
                record_list.append(record)

            return response_body['pagination']['pages'] if 'pagination' in response_body else None
        except TypeError:
            has_fatal_error(f"Unexpected GraphQL response format: {api_response}")

    record_list = list()

    if not is_paginated:
        # * operator unpacks tuple for use as positional function args
        graphql_request_body = request_body_function(*request_parameters)
        total_pages = append_api_response_data(graphql_request_body)

        # should be None, if value is returned then endpoint is actually paginated
        if total_pages:
            has_fatal_error(f"Paginated API response ({total_pages} pages), but is_paginated set to False.")
    else:
        limit = api_params['ENDPOINT_SETTINGS'][endpoint]['batch_size']
        offset = 0
        page = 1

        paginated_request_params = request_parameters + (offset, limit)

        # * operator unpacks tuple for use as positional function args
        graphql_request_body = request_body_function(*paginated_request_params)
        total_pages = append_api_response_data(graphql_request_body)

        # Useful for endpoints which don't access per-study data, otherwise too verbose
        if 'Study' not in endpoint:
            print(f" - Appended page {page} of {total_pages}")

        if not total_pages:
            has_fatal_error("API did not return a value for total pages, but is_paginated set to True.")

        while page < total_pages:
            offset += limit
            page += 1

            paginated_request_params = request_parameters + (offset, limit)
            graphql_request_body = request_body_function(*paginated_request_params)
            new_total_pages = append_api_response_data(graphql_request_body)
            if 'Study' not in endpoint:
                print(f" - Appended page {page} of {total_pages}")

            if new_total_pages != total_pages:
                has_fatal_error(f"Page count change mid-ingestion (from {total_pages} to {new_total_pages})")

    return record_list


def build_obj_from_pdc_api(api_params, endpoint, request_function, request_params=tuple(), alter_json_function=None,
                           ids=None, insert_id=False, pause=0):
    """

    Create jsonl file based on results from PDC API request
    :param api_params: API params from YAML config
    :param endpoint: PDC API endpoint
    :param request_function: PDC API request function
    :param request_params: API request parameters
    :param alter_json_function: Used to mutate json object prior to writing to file
    :param ids: generically will loop over a set of ids, such as study ids or project ids, in order to merge results
    from endpoints which require id specification
    :param insert_id: if true, add id to json obj before writing to file; defaults to False
    :param pause: number of seconds to wait between calls; used when iterating over ids
    """

    print(f"Sending {endpoint} API request: ")

    if ids:
        joined_record_list = list()
        for idx, id_entry in enumerate(ids):
            combined_request_parameters = request_params + (id_entry,)
            record_list = request_data_from_pdc_api(api_params, endpoint, request_function, combined_request_parameters)

            if alter_json_function and insert_id:
                alter_json_function(record_list, id_entry)
            elif alter_json_function:
                alter_json_function(record_list)

            joined_record_list += record_list

            if len(ids) < 100:
                print(f" - {len(joined_record_list):6d} current records (added {id_entry})")
            elif len(joined_record_list) % 1000 == 0 and len(joined_record_list) != 0:
                print(f" - {len(joined_record_list)} records appended.")

            time.sleep(pause)
    else:
        joined_record_list = request_data_from_pdc_api(api_params, endpoint, request_function, request_params)
        print(f" - collected {len(joined_record_list)} records")

        if alter_json_function:
            alter_json_function(joined_record_list)

    return joined_record_list


def write_jsonl_and_upload(api_params, bq_params, prefix, joined_record_list):
    jsonl_filename = get_filename(api_params,
                                  file_extension='jsonl',
                                  prefix=prefix)
    local_filepath = get_scratch_fp(bq_params, jsonl_filename)

    write_list_to_jsonl(local_filepath, joined_record_list)
    upload_to_bucket(bq_params, local_filepath, delete_local=True)


def build_table_from_jsonl(api_params, bq_params, endpoint, infer_schema=False, schema=None):
    """

    Build BQ table from jsonl file.
    :param api_params: API params from YAML config
    :param bq_params: BQ params from YAML config
    :param endpoint: PDC API endpoint
    :param infer_schema: if True, use native BQ schema inference. Defaults to False.
    :param schema: TableSchema object (inferred by script)
    """
    prefix = api_params['ENDPOINT_SETTINGS'][endpoint]['output_name']
    dataset = api_params['ENDPOINT_SETTINGS'][endpoint]['dataset']

    table_name = construct_table_name(api_params, prefix)
    filename = get_filename(api_params,
                            file_extension='jsonl',
                            prefix=prefix)
    table_id = construct_table_id(bq_params['DEV_PROJECT'], dataset=dataset, table_name=table_name)

    print(f"Creating {table_id}:")

    if not schema and not infer_schema:
        schema_filename = infer_schema_file_location_by_table_id(table_id)
        schema = load_bq_schema_from_json(bq_params, schema_filename)

        if not schema:
            has_fatal_error("No schema found and infer_schema set to False, exiting")

    create_and_load_table_from_jsonl(bq_params, filename, table_id, schema)

    return table_id


def get_prefix(api_params, endpoint):
    """

    Data type (a.k.a. prefix) readability syntactic sugar.
    :param api_params: API params from YAML config
    :param endpoint: PDC API endpoint name
    :return: data type prefix string
    """
    return api_params['ENDPOINT_SETTINGS'][endpoint]['output_name']


def get_records(api_params, bq_params, endpoint, select_statement, dataset):
    """

    Get records for a given built query (custom subqueries).
    :param api_params: API params from YAML config
    :param bq_params: BQ params from YAML config
    :param endpoint: PDC API endpoint
    :param select_statement: SELECT statement used to query and include nested columns
    :param dataset: dataset for query's table_id
    :return: result records for query
    """
    table_name = construct_table_name(api_params, api_params['ENDPOINT_SETTINGS'][endpoint]['output_name'])
    table_id = construct_table_id(bq_params['DEV_PROJECT'], dataset=dataset, table_name=table_name)

    query = select_statement
    query += f" FROM `{table_id}`"

    records = list()

    for row in get_query_results(query):
        records.append(dict(row.items()))

    return records


def infer_schema_file_location_by_table_id(table_id):
    """
    Use table id to infer json file location (for BQ ecosystem schema location)
    :param table_id: BQ table id
    :return: file path
    """
    split_table_id = table_id.split('.')
    filepath = ".".join(split_table_id) + ".json"
    return filepath


def create_modified_temp_table(bq_params, table_id, query):
    """
    Create temp table based on existing table's filtered rows
    :param bq_params: BQ params from YAML config
    :param table_id: BQ table id
    :param query: temp table creation sql query
    """
    temp_table_id = table_id + '_temp'
    delete_bq_table(temp_table_id)
    copy_bq_table(bq_params, table_id, temp_table_id)
    load_table_from_query(bq_params, table_id, query)


def update_pdc_table_metadata(api_params, bq_params, table_type=None):
    """
    Create a list of newly created tables based on bucket file names for a given table type, then access its schema
    BQEcosystem schema file and update BQ table metadata (labels, friendly name, description)
    :param api_params: API params from YAML config
    :param bq_params: BQ params from YAML config
    :param table_type: data type for table (based on Table prefixes section of YAML config; e.g. file_metadata, studies)
    """

    fp = "/".join([bq_params['BQ_REPO'], bq_params['TABLE_METADATA_DIR'], api_params["RELEASE"]])
    metadata_fp = get_filepath(fp)

    # list filepath contents in current release directory, and filters to ensure it returns only files)
    metadata_files = [f for f in os.listdir(metadata_fp) if os.path.isfile(os.path.join(metadata_fp, f))]

    filtered_metadata_files = list()

    if not table_type:
        filtered_metadata_files = metadata_files
    else:
        for metadata_file in metadata_files:
            if table_type in str(metadata_file):
                filtered_metadata_files.append(metadata_file)

    print("Updating table metadata:")

    for table_metadata_json_file in filtered_metadata_files:
        table_id = construct_table_id(bq_params['DEV_PROJECT'],
                                      dataset=bq_params['META_DATASET'],
                                      table_name=table_metadata_json_file.split('.')[-2])

        if not exists_bq_table(table_id):
            print(f"skipping {table_id} (no bq table found)")
            continue

        print(f"- {table_id}")
        json_fp = "/".join([metadata_fp, table_metadata_json_file])

        with open(json_fp) as json_file_output:
            metadata = json.load(json_file_output)
            update_table_metadata(table_id, metadata)


def make_retrieve_all_studies_query(api_params, bq_params, output_name):
    """
    Retrieve select study columns (pdc_study_id, study_name, embargo_date, project_submitter_id, analytical_fraction)
    from study metadata BQ table.

    :param api_params: API params from YAML config
    :param bq_params: BQ params from YAML config
    :param output_name: prefix from YAML file, used for table/file output naming
    :return: sql query string
    """
    table_name = construct_table_name(api_params, output_name)
    table_id = construct_table_id(bq_params['DEV_PROJECT'], dataset=bq_params['META_DATASET'], table_name=table_name)

    return f"""
    SELECT distinct pdc_study_id, submitter_id_name AS study_name, embargo_date, project_submitter_id, 
    analytical_fraction, program_short_name, project_short_name, project_friendly_name, study_friendly_name,
    program_labels
    FROM  `{table_id}`
    """


def print_embargoed_studies(excluded_studies_list):
    """
    Print list of embargoed studies (used when script only ingests data from non-embargoed studies)
    :param excluded_studies_list: list of embargoed study objects
    """
    print("\nStudies excluded due to data embargo:")

    for study in sorted(excluded_studies_list, key=lambda item: item['study_name']):
        print(f" - {study['study_name']} ({study['pdc_study_id']}, expires {study['embargo_date']})")

    print()


def is_under_embargo(embargo_date):
    """
    Checks study embargo date for expiration.
    :param embargo_date: embargo date
    :return: returns True if study data is still under embargo, False otherwise
    """
    if not embargo_date or embargo_date < date.today():
        return False
    return True


def get_pdc_split_studies_lists(api_params, bq_params):
    """
    Get two lists, one for embargoed studies, one for non-embargoed studies.
    Used by get_pdc_study_ids() and get_pdc_studies_list().

    :param api_params: API params from YAML config
    :param bq_params: BQ params from YAML config 
    :return: studies list, embargoed studies list
    """
    studies_output_name = get_prefix(api_params, api_params['STUDY_ENDPOINT'])

    studies_table_name = construct_table_name(api_params, studies_output_name)
    studies_table_id = construct_table_id(bq_params['DEV_PROJECT'],
                                          dataset=bq_params['META_DATASET'],
                                          table_name=studies_table_name)

    if not exists_bq_table(studies_table_id):
        has_fatal_error("Studies table for release {} does not exist. "
                        "Run studies build script prior to running this script.")

    studies_list = list()
    embargoed_studies_list = list()

    for study in get_query_results(make_retrieve_all_studies_query(api_params, bq_params, studies_output_name)):
        if is_under_embargo(study['embargo_date']):
            embargoed_studies_list.append(dict(study.items()))
        else:
            studies_list.append(dict(study.items()))

    return studies_list, embargoed_studies_list


def get_pdc_study_ids(api_params, bq_params, include_embargoed_studies=False):
    """
    Returns current list of PDC study ids (pulled from study metadata table).
    :param api_params: API params from YAML config 
    :param bq_params: BQ params from YAML config 
    :param include_embargoed_studies: If True, returns list of every PDC study id regardless of embargo status,
    defaults to False, which will return only non-embargoed study ids
    :return: PDC study id list
    """
    studies_list, embargoed_studies_list = get_pdc_split_studies_lists(api_params, bq_params)

    pdc_study_ids = list()
    embargoed_pdc_study_ids = list()

    for study in sorted(studies_list, key=lambda item: item['pdc_study_id']):
        pdc_study_ids.append(study['pdc_study_id'])

    if include_embargoed_studies:
        for study in embargoed_studies_list:
            embargoed_pdc_study_ids.append(study['pdc_study_id'])
        return embargoed_pdc_study_ids + pdc_study_ids

    print_embargoed_studies(embargoed_studies_list)

    return pdc_study_ids


def get_pdc_studies_list(api_params, bq_params, include_embargoed=False):
    """
    Returns current list of PDC studies (pulled from study metadata table).
    :param api_params: API params from YAML config 
    :param bq_params: BQ params from YAML config 
    :param include_embargoed: If True, returns every PDC study regardless of embargo status; defaults to False, which
        will return only non-embargoed study ids
    :return: a list of study dict objects containing the following keys: program_short_name, project_friendly_name,
             study_friendly_name, project_submitter_id, pdc_study_id, study_name (AKA submitter_id_name), embargo_date,
             analytical_fraction
    """

    studies_list, embargoed_studies_list = get_pdc_split_studies_lists(api_params, bq_params)

    if include_embargoed:
        return studies_list + embargoed_studies_list

    return studies_list


def get_pdc_projects_list(api_params, bq_params, include_embargoed=False):
    """
    Returns current list of PDC projects (pulled from study metadata table).
    :param api_params: API params from YAML config
    :param bq_params: BQ params from YAML config
    :param include_embargoed: If True, returns every PDC project regardless of embargo status; defaults to False, which
        will only exclude projects in which *all* of the project's studies are currently embargoed.
    :return: a list of project dict objects with project_submitter_id as the key, and containing the following keys: program_short_name, project_friendly_name,
             study_friendly_name, project_submitter_id, pdc_study_id, study_name (AKA submitter_id_name), embargo_date,
             analytical_fraction
    """
    projects_list = list()

    projects_set = set()

    studies_list = get_pdc_studies_list(api_params, bq_params, include_embargoed)

    for study in studies_list:
        if study['project_short_name'] not in projects_set:

            project_dict = {
                'project_friendly_name': study['project_friendly_name'],
                'project_short_name': study['project_short_name'],
                'project_submitter_id': study['project_submitter_id'],
                'program_short_name': study['program_short_name']
            }

            projects_list.append(project_dict)
            projects_set.add(study['project_short_name'])

    return projects_list


def update_table_schema_from_generic_pdc(api_params, bq_params, table_id, schema_tags=dict(), metadata_file=None):
    """
    todo
    :param api_params:
    :param bq_params:
    :param table_id:
    :param schema_tags:
    :param metadata_file:
    """
    # remove underscore, add decimal to version number
    schema_tags['version'] = ".".join(api_params['RELEASE'].split('_'))
    schema_tags['extracted-month-year'] = api_params['EXTRACTED_MONTH_YEAR']

    add_generic_table_metadata(bq_params=bq_params,
                               table_id=table_id,
                               schema_tags=schema_tags,
                               metadata_file=metadata_file)
    add_column_descriptions(bq_params=bq_params, table_id=table_id)


def get_project_program_names(api_params, bq_params, project_submitter_id):
    """
    todo
    :param api_params:
    :param bq_params:
    :param project_submitter_id:
    :return: tuple containing (project_short_name, program_short_name, project_name) strings
    """
    endpoint = 'allPrograms'
    prefix = get_prefix(api_params, endpoint)
    study_table_name = construct_table_name(api_params=api_params, prefix=prefix)
    study_table_id = f"{bq_params['DEV_PROJECT']}.{bq_params['META_DATASET']}.{study_table_name}"

    query = f"""
        SELECT project_short_name, program_short_name, project_name, project_friendly_name, program_labels
        FROM {study_table_id}
        WHERE project_submitter_id = '{project_submitter_id}'
        LIMIT 1
    """

    res = bq_harness_with_result(sql=query, do_batch=False, verbose=False)
    for row in res:
        if not row:
            has_fatal_error(f"No result for query: {query}")
        project_short_name = row[0]
        program_short_name = row[1]
        project_name = row[2]
        project_friendly_name = row[3]
        program_labels = row[4]

        project_name_dict = {
            "project_short_name": project_short_name,
            "program_short_name": program_short_name,
            "project_name": project_name,
            "project_friendly_name": project_friendly_name,
            "program_labels": program_labels
        }

        return project_name_dict


def get_project_level_schema_tags(api_params, bq_params, project_submitter_id):
    project_name_dict = get_project_program_names(api_params, bq_params, project_submitter_id)

    program_labels_list = project_name_dict['program_labels'].split("; ")

    if len(program_labels_list) > 2:
        has_fatal_error("PDC clinical isn't set up to handle >2 program labels yet; support needs to be added.")
    elif len(program_labels_list) == 0:
        has_fatal_error(f"No program label included for {project_submitter_id}, please add to PDCStudy.yaml")
    elif len(program_labels_list) == 2:
        schema_tags = {
            "project-name": project_name_dict['project_name'],
            "mapping-name": "",  # only used by clinical, but including it elsewhere is harmless
            "friendly-project-name-upper": project_name_dict['project_friendly_name'],
            "program-name-0-lower": program_labels_list[0].lower(),
            "program-name-1-lower": program_labels_list[1].lower()
        }
    else:
        schema_tags = {
            "project-name": project_name_dict['project_name'],
            "mapping-name": "",  # only used by clinical, but including it elsewhere is harmless
            "friendly-project-name-upper": project_name_dict['project_friendly_name'],
            "program-name-lower": project_name_dict['program_labels'].lower()
        }

    return schema_tags


def find_most_recent_published_table_id(api_params, versioned_table_id):
    """
    Function for locating published table id for dataset's previous release, if it exists
    :param api_params: api_params supplied in yaml config
    :param versioned_table_id: public versioned table id for current release
    :return: last published table id, if any; otherwise None
    """
    # todo assuming PDC will use 2-digit minor releases -- check
    max_minor_release_num = 99
    split_current_etl_release = api_params['RELEASE'][1:].split("_")
    # set to current release initially, decremented in loop
    last_major_rel_num = int(split_current_etl_release[0])
    last_minor_rel_num = int(split_current_etl_release[1])

    while True:
        if last_minor_rel_num > 0 and last_major_rel_num >= 1:
            last_minor_rel_num -= 1
        elif last_major_rel_num > 1:
            last_major_rel_num -= 1
            last_minor_rel_num = max_minor_release_num
        else:
            return None

        table_id_no_release = versioned_table_id.replace(f"_{api_params['RELEASE']}", '')
        prev_release_table_id = f"{table_id_no_release}_V{last_major_rel_num}_{last_minor_rel_num}"

        if exists_bq_table(prev_release_table_id):
            # found last release table, stop iterating
            return prev_release_table_id


def get_publish_table_ids_metadata(api_params, bq_params, source_table_id, public_dataset):
    """
    Create current and versioned table ids.
    :param api_params: api_params supplied in yaml config
    :param bq_params: bq_params supplied in yaml config
    :param source_table_id: id of source table (located in dev project)
    :param public_dataset: base name of dataset in public project where table should be published
    :return: public current table id, public versioned table id
    """
    rel_prefix = api_params['RELEASE']
    split_table_id = source_table_id.split('.')

    # derive data type from table id
    data_type = split_table_id[-1]
    data_type = data_type.replace(rel_prefix, '').strip('_')
    data_type = data_type.replace(public_dataset + '_', '')
    data_type = data_type.replace(api_params['DATA_SOURCE'], '').strip('_')

    curr_table_name = construct_table_name_from_list([data_type, 'current'])
    curr_table_id = f"{bq_params['PROD_PROJECT']}.{public_dataset}.{curr_table_name}"
    vers_table_name = construct_table_name_from_list([data_type, rel_prefix])
    vers_table_id = f"{bq_params['PROD_PROJECT']}.{public_dataset}_versioned.{vers_table_name}"

    return curr_table_id, vers_table_id


def find_most_recent_published_table_id_uniprot(api_params, versioned_table_id):
    # oldest uniprot release used in published dataset
    oldest_year = 2021
    max_month = 12

    split_release = api_params['UNIPROT_RELEASE'].split('_')
    last_year = int(split_release[0])
    last_month = int(split_release[1])

    while True:
        if last_month > 1 and last_year >= oldest_year:
            last_month -= 1
        elif last_year > oldest_year:
            last_year -= 1
            last_month = max_month
        else:
            return None

        table_id_no_release = versioned_table_id.replace(f"_{api_params['UNIPROT_RELEASE']}", '')
        prev_release_table_id = f"{table_id_no_release}_{last_year}_{last_month}"

        if exists_bq_table(prev_release_table_id):
            return prev_release_table_id

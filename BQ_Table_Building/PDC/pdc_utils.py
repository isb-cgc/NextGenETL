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

from common_etl.utils import (get_filename, get_filepath, get_query_results, format_seconds, write_list_to_jsonl,
                              get_scratch_fp, upload_to_bucket, get_graphql_api_response, has_fatal_error,
                              load_bq_schema_from_json, create_and_load_table_from_tsv, create_and_load_table,
                              load_table_from_query, delete_bq_table, copy_bq_table, exists_bq_table,
                              update_schema, update_table_metadata, construct_table_id,
                              construct_table_name, get_rel_prefix, construct_table_name_from_list,
                              recursively_detect_object_structures, convert_object_structure_dict_to_schema_dict)


def request_data_from_pdc_api(api_params, endpoint, request_body_function, request_parameters=None):
    """
    Used internally by build_jsonl_from_pdc_api()
    :param api_params: API params from YAML config
    :param endpoint: PDC API endpoint
    :param request_body_function: function outputting GraphQL request body (including query)
    :param request_parameters: API request parameters
    :return: Response results list
    """
    is_paginated = api_params['ENDPOINT_SETTINGS'][endpoint]['is_paginated']
    payload_key = api_params['ENDPOINT_SETTINGS'][endpoint]['payload_key']

    def append_api_response_data():
        """
        Add api response data to record list
        """
        api_response = get_graphql_api_response(api_params, graphql_request_body)

        response_body = api_response['data'] if not is_paginated else api_response['data'][endpoint]

        for record in response_body[payload_key]:
            record_list.append(record)

        return response_body['pagination']['pages'] if 'pagination' in response_body else None

    record_list = list()

    if not is_paginated:
        # * operator unpacks tuple for use as positional function args
        graphql_request_body = request_body_function(*request_parameters)
        total_pages = append_api_response_data()

        # should be None, if value is returned then endpoint is actually paginated
        if total_pages:
            has_fatal_error("Paginated API response ({} pages), but is_paginated set to False.".format(total_pages))
    else:
        limit = api_params['PAGINATED_LIMIT']
        offset = 0
        page = 1

        paginated_request_params = request_parameters + (offset, limit)

        # * operator unpacks tuple for use as positional function args
        graphql_request_body = request_body_function(*paginated_request_params)
        total_pages = append_api_response_data()

        # Useful for endpoints which don't access per-study data, otherwise too verbose
        if 'Study' not in endpoint:
            print(" - Appended page {} of {}".format(page, total_pages))

        if not total_pages:
            has_fatal_error("API did not return a value for total pages, but is_paginated set to True.")

        while page < total_pages:
            offset += limit
            page += 1

            paginated_request_params = request_parameters + (offset, limit)
            graphql_request_body = request_body_function(*paginated_request_params)
            new_total_pages = append_api_response_data()
            if 'Study' not in endpoint:
                print(" - Appended page {} of {}".format(page, total_pages))

            if new_total_pages != total_pages:
                has_fatal_error("Page count change mid-ingestion (from {} to {})".format(total_pages, new_total_pages))

    return record_list


def build_jsonl_from_pdc_api(api_params, bq_params, endpoint, request_function, request_params=tuple(),
                             alter_json_function=None, ids=None, insert_id=False):
    """
    Create jsonl file based on results from PDC API request
    :param api_params: API params from YAML config
    :param bq_params: BQ params from YAML config
    :param endpoint: PDC API endpoint
    :param request_function: PDC API request function
    :param request_params: API request parameters
    :param alter_json_function: Used to mutate json object prior to writing to file
    :param ids: generically will loop over a set of ids, such as study ids or project ids, in order to merge results
    from endpoints which require id specification
    :param insert_id: if true, add id to json obj before writing to file; defaults to False
    """

    print("Sending {} API request: ".format(endpoint))

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
                print(" - {:6d} current records (added {})".format(len(joined_record_list), id_entry))
            elif len(joined_record_list) % 1000 == 0 and len(joined_record_list) != 0:
                print(" - {} records appended.".format(len(joined_record_list)))
    else:
        joined_record_list = request_data_from_pdc_api(api_params, endpoint, request_function, request_params)
        print(" - collected {} records".format(len(joined_record_list)))

        if alter_json_function:
            alter_json_function(joined_record_list)

    jsonl_filename = get_filename(api_params,
                                  file_extension='jsonl',
                                  prefix=api_params['ENDPOINT_SETTINGS'][endpoint]['output_name'])
    local_filepath = get_scratch_fp(bq_params, jsonl_filename)

    write_list_to_jsonl(local_filepath, joined_record_list)
    upload_to_bucket(bq_params, local_filepath, delete_local=True)

    return joined_record_list


def create_schema_from_pdc_api(api_params, bq_params, joined_record_list, table_type):
    """
    todo
    :param api_params:
    :param bq_params:
    :param joined_record_list:
    :param table_type:
    :return:
    """

    data_types_dict = recursively_detect_object_structures(joined_record_list)

    schema_list = convert_object_structure_dict_to_schema_dict(data_types_dict, list())

    schema_obj = {
        "fields": schema_list
    }

    schema_filename = get_filename(api_params,
                                   file_extension='json',
                                   prefix="schema",
                                   suffix=table_type)
    schema_fp = get_scratch_fp(bq_params, schema_filename)

    with open(schema_fp, 'w') as schema_json_file:
        json.dump(schema_obj, schema_json_file, indent=4)

    upload_to_bucket(bq_params, schema_fp, delete_local=True)


def build_table_from_jsonl(api_params, bq_params, endpoint, infer_schema=False, schema=None):
    """
    Build BQ table from jsonl file.
    :param api_params: API params from YAML config
    :param bq_params: BQ params from YAML config
    :param endpoint: PDC API endpoint
    :param infer_schema: if True, use native BQ schema inference. Defaults to False.
    """
    prefix = api_params['ENDPOINT_SETTINGS'][endpoint]['output_name']
    dataset = api_params['ENDPOINT_SETTINGS'][endpoint]['dataset']

    table_name = construct_table_name(api_params, prefix)
    filename = get_filename(api_params,
                            file_extension='jsonl',
                            prefix=prefix)
    table_id = get_dev_table_id(bq_params, dataset=dataset, table_name=table_name)
    print("Creating {}:".format(table_id))

    if infer_schema and not schema:
        schema = None
    elif not infer_schema:
        schema_filename = infer_schema_file_location_by_table_id(table_id)
        schema = load_bq_schema_from_json(bq_params, schema_filename)

        if not schema:
            has_fatal_error("No schema found and infer_schema set to False, exiting")

    create_and_load_table(bq_params, filename, table_id, schema)


def build_table_from_tsv(api_params, bq_params, table_prefix, table_suffix=None, backup_table_suffix=None):
    """
    Build BQ table from tsv file.
    :param api_params: API params from YAML config
    :param bq_params: BQ params from YAML config 
    :param table_prefix: table name prefix
    :param table_suffix: table name suffix (defaults to None
    :param backup_table_suffix: (NOTE: not currently using this anywhere--necessary for future dev?)
    """
    build_start = time.time()

    project = bq_params['DEV_PROJECT']
    dataset = bq_params['DEV_DATASET']

    table_name = construct_table_name(api_params, table_prefix)
    table_id = construct_table_id(project, dataset, table_name)

    schema_filename = '{}/{}/{}.json'.format(project, dataset, table_name)
    schema = load_bq_schema_from_json(bq_params, schema_filename)

    if not schema and backup_table_suffix:
        print("No schema file found for {}, trying backup ({})".format(table_suffix, backup_table_suffix))
        table_name = construct_table_name(api_params, table_prefix, backup_table_suffix)
        table_id = construct_table_id(project, dataset, table_name)
        schema_filename = '{}/{}/{}.json'.format(project, dataset, table_name)
        schema = load_bq_schema_from_json(bq_params, schema_filename)

    # still no schema? return
    if not schema:
        print("No schema file found for {}, skipping table.".format(table_id))
        return

    print("\nBuilding {0}... ".format(table_id))
    tsv_name = get_filename(api_params,
                            file_extension='tsv',
                            prefix=table_prefix,
                            suffix=table_suffix)
    create_and_load_table_from_tsv(bq_params=bq_params,
                                   tsv_file=tsv_name,
                                   schema=schema,
                                   table_id=table_id)

    build_end = time.time() - build_start
    print("Table built in {0}!\n".format(format_seconds(build_end)))


def get_dev_table_id(bq_params, dataset=None, table_name=None):
    """
    Get dev table id.
    :param bq_params: BQ params from YAML config
    :param dataset: dataset for table id (e.g. PDC_clinical, PDC_metadata, PDC)
    :param table_name: name of table
    :return: BQ table id
    """
    project = bq_params['DEV_PROJECT']
    if not dataset:
        dataset = bq_params['DEV_DATASET']

    return "{}.{}.{}".format(project, dataset, table_name)


def get_prefix(api_params, endpoint):
    """
    todo
    :param api_params:
    :param endpoint:
    :return:
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
    table_id = get_dev_table_id(bq_params, dataset=dataset, table_name=table_name)

    query = select_statement
    query += " FROM `{}`".format(table_id)

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


def update_column_metadata(api_params, bq_params, table_id, include_release=True):
    """
    Update column descriptions for existing BQ table
    :param api_params: API params from YAML config
    :param bq_params: BQ params from YAML config
    :param table_id: BQ table id
    """

    def get_schema_filename(data_type, suffix=None):
        """
        Get file name for BQ schema.
        """
        source = api_params["DATA_SOURCE"]
        file_list = [source, data_type]

        if suffix:
            file_list.append(suffix)

        if include_release:
            file_list.append(get_rel_prefix(api_params))

        return construct_table_name_from_list(file_list)

    file_path = "/".join([bq_params['BQ_REPO'], bq_params['FIELD_DESC_DIR']])
    field_desc_file_name = get_schema_filename(bq_params['FIELD_DESC_FILE_SUFFIX']) + '.json'
    field_desc_fp = get_filepath(file_path, field_desc_file_name)

    if not os.path.exists(field_desc_fp):
        has_fatal_error("BQEcosystem schema path not found", FileNotFoundError)
    with open(field_desc_fp) as field_output:
        descriptions = json.load(field_output)
        print("Updating metadata for {}\n".format(table_id))
        update_schema(table_id, descriptions)


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
        table_id = get_dev_table_id(bq_params, dataset=bq_params['META_DATASET'],
                                    table_name=table_metadata_json_file.split('.')[-2])

        if not exists_bq_table(table_id):
            print("skipping {} (no bq table found)".format(table_id))
            continue

        print("- {}".format(table_id))
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
    table_id = get_dev_table_id(bq_params, dataset=bq_params['META_DATASET'], table_name=table_name)

    return """
    SELECT pdc_study_id, study_name, embargo_date, project_submitter_id, analytical_fraction
    FROM  `{}`
    """.format(table_id)


def print_embargoed_studies(excluded_studies_list):
    """
    Print list of embargoed studies (used when script only ingests data from non-embargoed studies)
    :param excluded_studies_list: list of embargoed study objects
    """
    print("\nStudies excluded due to data embargo:")

    for study in sorted(excluded_studies_list, key=lambda item: item['study_name']):
        print(" - {} ({}, expires {})".format(study['study_name'], study['pdc_study_id'], study['embargo_date']))

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
    studies_output_name = api_params['ENDPOINT_SETTINGS']['allPrograms']['output_name']

    studies_table_name = construct_table_name(api_params, studies_output_name)
    studies_table_id = get_dev_table_id(bq_params,
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
    :return: a list of study dict objects containing the following keys: pdc_study_id, study_name, embargo_date,
        project_submitter_id, analytical_fraction
    """

    studies_list, embargoed_studies_list = get_pdc_split_studies_lists(api_params, bq_params)

    if include_embargoed:
        return studies_list + embargoed_studies_list

    return studies_list

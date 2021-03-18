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
import time
import sys

from common_etl.utils import (get_query_results, format_seconds, write_list_to_jsonl, get_scratch_fp, upload_to_bucket,
                              get_graphql_api_response, has_fatal_error, load_table_from_query, load_config,
                              publish_table, construct_table_name, download_from_bucket)

from common_etl.pdc_utils import (get_pdc_study_ids, build_jsonl_from_pdc_api, build_table_from_jsonl, get_filename,
                                  get_dev_table_id, create_modified_temp_table, update_column_metadata,
                                  update_pdc_table_metadata)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


# ***** FILE METADATA FUNCTIONS

def make_files_per_study_query(study_id):
    """
    Creates a graphQL string for querying the PDC API's filesPerStudy endpoint.
    :return: GraphQL query string
    """

    return """
    {{ filesPerStudy (pdc_study_id: \"{}\" acceptDUA: true) {{
            pdc_study_id 
            study_submitter_id
            study_name 
            file_id 
            file_name 
            file_submitter_id 
            file_type 
            md5sum 
            file_location 
            file_size 
            data_category 
            file_format
            signedUrl {{
                url
            }}
        }} 
    }}""".format(study_id)


def make_file_id_query(table_id):
    """
    Create sql query to retrieve all file ids from files_per_study metadata table.
    :param table_id: files_per_study metadata table id
    :return: sql query string
    """
    return """
    SELECT file_id
    FROM `{}`
    ORDER BY file_id
    """.format(table_id)


def make_file_metadata_query(file_id):
    """
    Creates a graphQL string for querying the PDC API's filesMetadata endpoint.
    :return: GraphQL query string
    """

    return """
    {{ filesMetadata(file_id: \"{}\" acceptDUA: true) {{
        file_id 
        file_name
        fraction_number 
        experiment_type 
        plex_or_dataset_name 
        analyte 
        instrument 
        study_run_metadata_submitter_id 
        study_run_metadata_id 
        aliquots {{
            aliquot_id
            aliquot_submitter_id
            sample_id
            sample_submitter_id
            case_id
            case_submitter_id
            }}
        }} 
    }}    
    """.format(file_id)


def make_associated_entities_query():
    """
    Create sql query to retrieve all associated entity records from file metadata table.
    :return: sql query string
    """
    table_name = construct_table_name(API_PARAMS,
                                      prefix=API_PARAMS['ENDPOINT_SETTINGS']['filesMetadata']['output_name'])

    table_id = get_dev_table_id(BQ_PARAMS,
                                dataset=BQ_PARAMS['META_DATASET'],
                                table_name=table_name)

    return """
    SELECT  file_id, 
            aliquots.case_id AS case_id, 
            aliquots.aliquot_id AS entity_id, 
            aliquots.aliquot_submitter_id AS entity_submitter_id, 
            "aliquot" AS entity_type
    FROM `{}`
    CROSS JOIN UNNEST(aliquots) AS aliquots
    GROUP BY file_id, case_id, entity_id, entity_submitter_id, entity_type
    """.format(table_id)


def make_combined_file_metadata_query():
    """
    Create sql query to retrieve columns from file metadata and files per study API endpoints, in order to create the
    publishable file metadata table.
    :return: sql query string
    """
    file_metadata_output_name = API_PARAMS['ENDPOINT_SETTINGS']['filesMetadata']['output_name']
    file_metadata_table_name = construct_table_name(API_PARAMS,
                                                    prefix=file_metadata_output_name)

    file_metadata_table_id = get_dev_table_id(BQ_PARAMS,
                                              dataset=BQ_PARAMS['META_DATASET'],
                                              table_name=file_metadata_table_name)

    file_per_study_output_name = API_PARAMS['ENDPOINT_SETTINGS']['filesPerStudy']['output_name']
    file_per_study_table_name = construct_table_name(API_PARAMS,
                                                     prefix=file_per_study_output_name)

    file_per_study_table_id = get_dev_table_id(BQ_PARAMS,
                                               dataset=BQ_PARAMS['META_DATASET'],
                                               table_name=file_per_study_table_name)

    return """
    SELECT distinct fps.file_id, fps.file_name, fps.embargo_date, fps.pdc_study_ids,
        fm.study_run_metadata_id, fm.study_run_metadata_submitter_id,
        fps.file_format, fps.file_type, fps.data_category, fps.file_size, 
        fm.fraction_number, fm.experiment_type, fm.plex_or_dataset_name, fm.analyte, fm.instrument, 
        fps.md5sum, fps.url, "open" AS `access`
    FROM `{}` AS fps
    INNER JOIN `{}` AS fm
        ON fm.file_id = fps.file_id
    """.format(file_per_study_table_id, file_metadata_table_id)


def modify_api_file_metadata_table_query(fm_table_id):
    """
    Modify api file metadata table in order to merge duplicate file_id rows,
    by combining those with two instrument values into one entry. The two instrument values become a comma-delimited
    string stored in "instruments" column.
    :param fm_table_id: file metadata BQ table id
    :return: sql query string
    """
    temp_table_id = fm_table_id + "_temp"

    return """
        WITH grouped_instruments AS (
            SELECT file_id, 
                ARRAY_TO_STRING(ARRAY_AGG(instrument), ';') as instruments
            FROM `{0}`
        GROUP BY file_id
        )

        SELECT g.file_id, f.analyte, f.experiment_type, g.instruments as instrument, 
            f.study_run_metadata_submitter_id, f.study_run_metadata_id, f.plex_or_dataset_name,
            f.fraction_number, f.aliquots
        FROM grouped_instruments g
        LEFT JOIN `{0}` f
            ON g.file_id = f.file_id
        """.format(temp_table_id)


def modify_per_study_file_table_query(fps_table_id):
    """
    Modify api files per study table in order to merge duplicate file_id rows,
    by combining those with multiple PDC study ids into one row. The PDC study ids become a comma-delimited
    string.
    :param fps_table_id: files per study BQ table id
    :return: sql query string
    """
    temp_table_id = fps_table_id + "_temp"

    study_table_name = construct_table_name(API_PARAMS,
                                            prefix=API_PARAMS["ENDPOINT_SETTINGS"]["allPrograms"]["output_name"])

    study_table_id = get_dev_table_id(BQ_PARAMS,
                                      dataset="PDC_metadata",
                                      table_name=study_table_name)

    return """
    WITH grouped_study_ids AS (
        SELECT fps.file_id, stud.embargo_date, 
            ARRAY_TO_STRING(ARRAY_AGG(stud.pdc_study_id), ';') AS pdc_study_ids
        FROM `{0}` fps
        JOIN `{1}` stud
            ON fps.pdc_study_id = stud.pdc_study_id
    GROUP BY fps.file_id, stud.embargo_date
    )

    SELECT distinct g.file_id, f.file_name, g.embargo_date, g.pdc_study_ids,
        f.data_category, f.file_format, f.file_type, f.file_size, f.md5sum, 
        SPLIT(f.url, '?')[OFFSET(0)] AS url
    FROM grouped_study_ids g
    INNER JOIN `{0}` f
        ON g.file_id = f.file_id
    """.format(temp_table_id, study_table_id)


def alter_files_per_study_json(files_per_study_obj_list):
    """
    This function is passed as a parameter to build_jsonl_from_pdc_api(). It allows for the json object to be mutated
    prior to writing it to a file.
    :param files_per_study_obj_list: list of json objects to mutate
    """
    for files_per_study_obj in files_per_study_obj_list:
        signed_url = files_per_study_obj.pop('signedUrl', None)
        url = signed_url.pop('url', None)

        if not url:
            print("url not found in filesPerStudy response:\n{}\n".format(files_per_study_obj))

        files_per_study_obj['url'] = url


def get_file_ids():
    """
    Generates a list of file ids from table created using filesPerStudy endpoint data.
    :return: file ids list
    """
    fps_table_name = construct_table_name(API_PARAMS,
                                          prefix=API_PARAMS['ENDPOINT_SETTINGS']['filesPerStudy']['output_name'])

    fps_table_id = get_dev_table_id(BQ_PARAMS,
                                    dataset=API_PARAMS['ENDPOINT_SETTINGS']['filesPerStudy']['dataset'],
                                    table_name=fps_table_name)

    curr_file_ids = get_query_results(make_file_id_query(fps_table_id))

    fm_table_name = construct_table_name(API_PARAMS,
                                         prefix=API_PARAMS['ENDPOINT_SETTINGS']['filesMetadata']['output_name'],
                                         release=API_PARAMS['PREV_RELEASE'])

    fm_table_id = get_dev_table_id(BQ_PARAMS,
                                   dataset=API_PARAMS['ENDPOINT_SETTINGS']['filesMetadata']['dataset'],
                                   table_name=fm_table_name)

    old_file_ids = get_query_results(make_file_id_query(fm_table_id))

    curr_file_id_set = set()
    old_file_id_set = set()

    for old_file in old_file_ids:
        file_id = old_file['file_id']
        old_file_id_set.add(file_id)

    for curr_file in curr_file_ids:
        file_id = curr_file['file_id']
        curr_file_id_set.add(file_id)

    new_file_ids = curr_file_id_set - old_file_id_set

    return new_file_ids


def get_previous_version_file_metadata():
    prefix = API_PARAMS['ENDPOINT_SETTINGS']['filesMetadata']['output_name']
    dataset = API_PARAMS['ENDPOINT_SETTINGS']['filesMetadata']['dataset']

    table_name = construct_table_name(API_PARAMS, prefix, release=API_PARAMS['PREV_RELEASE'])
    table_id = get_dev_table_id(BQ_PARAMS, dataset=dataset, table_name=table_name)

    query = """
    SELECT * 
    FROM {}
    """.format(table_id)

    return get_query_results(query)


def build_file_pdc_metadata_jsonl(file_ids):
    """
    Build jsonl file for creating the api file metadata BQ table.
    :param file_ids: list of file ids
    """
    jsonl_start = time.time()
    file_metadata_list = []

    """
    print("Getting {} new files".format(len(file_ids)))

    for count, file_id in enumerate(file_ids):

        file_metadata_res = get_graphql_api_response(API_PARAMS, make_file_metadata_query(file_id))

        if 'data' not in file_metadata_res:
            print("No data returned by file metadata query for {}".format(file_id))
            continue

        for metadata_row in file_metadata_res['data']['filesMetadata']:
            if 'fraction_number' in metadata_row and metadata_row['fraction_number']:
                fraction_number = metadata_row['fraction_number'].strip()

                if fraction_number == 'N/A' or fraction_number == 'NOFRACTION' or not fraction_number.isalnum():
                    fraction_number = None
                elif fraction_number == 'Pool' or fraction_number == 'pool':
                    fraction_number = 'POOL'

                metadata_row['fraction_number'] = fraction_number

            file_metadata_list.append(metadata_row)
            count += 1

            if count % 100 == 0:
                print("{} of {} file records retrieved".format(count, len(file_ids)))
    """

    old_file_metadata = get_previous_version_file_metadata()

    for file in old_file_metadata:
        file_dict = dict()
        for key in file.keys():
            file_dict[key] = file[key]

        file_metadata_list.append(file_dict)

    record_with_aliquots_idx = None

    for idx, file_dict in enumerate(file_metadata_list):
        if len(file_dict['aliquots']) > 1:
            record_with_aliquots_idx = idx
            break

    record_with_aliquots = file_metadata_list.pop(record_with_aliquots_idx)
    file_metadata_list.insert(0, record_with_aliquots)

    file_metadata_jsonl_file = get_filename(API_PARAMS,
                                            file_extension='jsonl',
                                            prefix=API_PARAMS['ENDPOINT_SETTINGS']['filesMetadata']['output_name'])
    file_metadata_jsonl_path = get_scratch_fp(BQ_PARAMS, file_metadata_jsonl_file)

    write_list_to_jsonl(file_metadata_jsonl_path, file_metadata_list)
    upload_to_bucket(BQ_PARAMS, file_metadata_jsonl_path)

    jsonl_end = time.time() - jsonl_start
    print("File PDC metadata jsonl file created in {0}!\n".format(format_seconds(jsonl_end)))


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

    if 'build_per_study_file_jsonl' in steps:
        build_jsonl_from_pdc_api(API_PARAMS,
                                             BQ_PARAMS,
                                             endpoint="filesPerStudy",
                                             request_function=make_files_per_study_query,
                                             alter_json_function=alter_files_per_study_json,
                                             ids=all_pdc_study_ids)

        schema_filename = get_filename(API_PARAMS,
                                       file_extension='jsonl',
                                       prefix="schema",
                                       suffix=API_PARAMS['ENDPOINT_SETTINGS']['filesPerStudy']['output_name'])

        download_from_bucket(BQ_PARAMS, schema_filename)
        scratch_fp = get_scratch_fp(BQ_PARAMS, schema_filename)

        with open(scratch_fp, "r") as schema_json:
            schema_obj = json.load(schema_json)

        print(schema_obj)

    if 'build_per_study_file_table' in steps:
        build_table_from_jsonl(API_PARAMS,
                               BQ_PARAMS,
                               endpoint="filesPerStudy",
                               infer_schema=True)

    if 'alter_per_study_file_table' in steps:
        fps_table_name = construct_table_name(API_PARAMS,
                                              prefix=API_PARAMS["ENDPOINT_SETTINGS"]["filesPerStudy"]["output_name"])

        fps_table_id = get_dev_table_id(BQ_PARAMS,
                                        dataset=BQ_PARAMS['META_DATASET'],
                                        table_name=fps_table_name)

        create_modified_temp_table(BQ_PARAMS,
                                   table_id=fps_table_id,
                                   query=modify_per_study_file_table_query(fps_table_id))

    if 'build_api_file_metadata_jsonl' in steps:
        file_ids = get_file_ids()
        build_file_pdc_metadata_jsonl(file_ids)

    if 'build_api_file_metadata_table' in steps:
        build_table_from_jsonl(API_PARAMS,
                               BQ_PARAMS,
                               endpoint="filesMetadata",
                               infer_schema=True)

    if 'alter_api_file_metadata_table' in steps:
        fm_table_name = construct_table_name(API_PARAMS,
                                             prefix=API_PARAMS["ENDPOINT_SETTINGS"]["filesMetadata"]["output_name"])

        fm_table_id = get_dev_table_id(BQ_PARAMS,
                                       dataset=BQ_PARAMS['META_DATASET'],
                                       table_name=fm_table_name)

        create_modified_temp_table(BQ_PARAMS,
                                   table_id=fm_table_id,
                                   query=modify_api_file_metadata_table_query(fm_table_id))

    if 'build_file_associated_entries_table' in steps:
        # Note, this assumes aliquot id will exist, because that's true. This will either be null,
        # or it'll have an aliquot id. If this ever changes, we'll need to adjust, but not expected that it will.
        table_name = construct_table_name(API_PARAMS,
                                          prefix=BQ_PARAMS['FILE_ASSOC_MAPPING_TABLE'])

        full_table_id = get_dev_table_id(BQ_PARAMS,
                                         dataset=BQ_PARAMS['META_DATASET'],
                                         table_name=table_name)

        load_table_from_query(BQ_PARAMS,
                              table_id=full_table_id,
                              query=make_associated_entities_query())

        update_column_metadata(API_PARAMS, BQ_PARAMS, full_table_id)

    if 'build_file_metadata_table' in steps:
        table_name = construct_table_name(API_PARAMS,
                                          prefix=BQ_PARAMS['FILE_METADATA'])

        full_table_id = get_dev_table_id(BQ_PARAMS,
                                         dataset=BQ_PARAMS['META_DATASET'],
                                         table_name=table_name)

        load_table_from_query(BQ_PARAMS,
                              table_id=full_table_id,
                              query=make_combined_file_metadata_query())

        update_column_metadata(API_PARAMS, BQ_PARAMS, full_table_id)

    if 'update_file_metadata_tables_metadata' in steps:
        update_pdc_table_metadata(API_PARAMS, BQ_PARAMS, table_type=BQ_PARAMS['FILE_METADATA'])
        update_pdc_table_metadata(API_PARAMS, BQ_PARAMS, table_type=BQ_PARAMS['FILE_ASSOC_MAPPING_TABLE'])

    if "publish_file_metadata_tables" in steps:
        # Publish master file metadata table
        file_metadata_table_name = construct_table_name(API_PARAMS,
                                                        prefix=BQ_PARAMS['FILE_METADATA'])

        file_metadata_table_id = get_dev_table_id(BQ_PARAMS,
                                                  dataset=BQ_PARAMS['META_DATASET'],
                                                  table_name=file_metadata_table_name)

        publish_table(API_PARAMS, BQ_PARAMS,
                      public_dataset=BQ_PARAMS['FILE_METADATA'],
                      source_table_id=file_metadata_table_id,
                      overwrite=True)

        # Publish master associated entities table
        mapping_table_name = construct_table_name(API_PARAMS,
                                                  prefix=BQ_PARAMS['FILE_ASSOC_MAPPING_TABLE'])

        mapping_table_id = get_dev_table_id(BQ_PARAMS,
                                            dataset=BQ_PARAMS['META_DATASET'],
                                            table_name=mapping_table_name)

        publish_table(API_PARAMS, BQ_PARAMS,
                      public_dataset=BQ_PARAMS['FILE_ASSOC_MAPPING_TABLE'],
                      source_table_id=mapping_table_id,
                      overwrite=True)

    end = time.time() - start_time
    print("Finished program execution in {}!\n".format(format_seconds(end)))


if __name__ == '__main__':
    main(sys.argv)
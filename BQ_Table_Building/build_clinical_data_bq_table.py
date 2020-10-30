"""
Copyright 2020, Institute for Systems Biology

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
from common_etl.utils import *

API_PARAMS = dict()
BQ_PARAMS = dict()
# used to capture returned yaml config sections
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


##################################################################################
#
#   API calls and data normalization (for BQ table insert)
#
##################################################################################


def request_data_from_gdc_api(curr_index):
    """ Make a POST API request and return response (if valid).

    :param curr_index: current API poll start position
    :return: response object
    """
    err_list = []

    try:
        request_params = {
            'from': curr_index,
            'size': API_PARAMS['BATCH_SIZE'],
            'expand': get_field_groups(API_PARAMS)
        }

        # retrieve and parse a "page" (batch) of case objects
        res = requests.post(url=API_PARAMS['ENDPOINT'], data=request_params)

        # return response body if request was successful
        if res.status_code == requests.codes.ok:
            return res

        err_list.append("{}".format(res.raise_for_status()))

        restart_idx = curr_index
        err_list.append('API request returned status code {}.'.format(res.status_code))

        if BQ_PARAMS['IO_MODE'] == 'a':
            err_list.append('Script is being run in "append" mode. To resume, set '
                            'START_INDEX = {} in yaml config.'.format(restart_idx))
    except requests.exceptions.MissingSchema as err:
        err_list.append(err)

    has_fatal_error("{}\n(HINT: incorrect ENDPOINT url in yaml config?".format(err_list))
    return None


def retrieve_and_save_case_records(scratch_fp):
    """Retrieves case records from API and outputs them to a JSONL file, which is later
        used to populate the clinical data BQ table.

    :param scratch_fp: absolute path to data output file
    """
    start_time = time.time()  # for benchmarking
    cases_count = 0
    is_last_page = False
    io_mode = BQ_PARAMS['IO_MODE']

    with open(scratch_fp, io_mode) as jsonl_file:
        console_out("Outputting json objects to {0} in {1} mode",
                    (scratch_fp, io_mode))
        have_printed_totals = False

        curr_index = API_PARAMS['START_INDEX']
        while not is_last_page:
            res = request_data_from_gdc_api(curr_index)

            res_json = res.json()['data']
            cases_json = res_json['hits']

            # If response doesn't contain pagination, indicates an invalid request.
            if 'pagination' not in res_json:
                has_fatal_error("'pagination' key not found in response json, exiting.",
                                KeyError)

            batch_record_count = res_json['pagination']['count']
            cases_count = res_json['pagination']['total']

            if not have_printed_totals:
                have_printed_totals = True
                console_out("Total cases for r{0}: {1}",
                            (BQ_PARAMS['RELEASE'], cases_count))
                console_out("Batch size: {0}", (batch_record_count,))

            for case in cases_json:
                case_copy = case.copy()

                for field in API_PARAMS['EXCLUDE_FIELDS']:
                    if field in case_copy:
                        case.pop(field)

                no_list_value_case = convert_dict_to_string(case)
                json.dump(obj=no_list_value_case, fp=jsonl_file)
                jsonl_file.write('\n')

            curr_page = res_json['pagination']['page']
            last_page = res_json['pagination']['pages']

            if curr_page == last_page:
                is_last_page = True
            elif API_PARAMS['MAX_PAGES'] and curr_page == API_PARAMS['MAX_PAGES']:
                is_last_page = True

            console_out("Inserted page {0} of {1} into jsonl file",
                        (curr_page, last_page))
            curr_index += batch_record_count

    # calculate processing time and file size
    total_time = time.time() - start_time
    file_size = os.stat(scratch_fp).st_size / 1048576.0

    console_out("\nClinical data retrieval complete!"
                      "\n\t{0} of {1} cases retrieved"
                      "\n\t{2:.2f} mb jsonl file size"
                      "\n\t{3:.1f} sec to retrieve from GDC API output to jsonl file\n",
                (curr_index, cases_count, file_size, total_time))


##################################################################################
#
#   BQ table creation and data insertion
#
##################################################################################


def create_field_records_dict(field_mappings, field_data_types):
    """ Generate schema dict composed of schema field dicts.

    :param field_mappings: dict of {fields: schema entry dicts}
    :param field_data_types: dict of {fields: data types}
    :return: SchemaField object dict
    """
    # this could use BQ Python API built-in method
    schema_dict = {}

    for field in field_data_types:
        try:
            column_name = field_mappings[field]['name'].split('.')[-1]
            description = field_mappings[field]['description']
        except KeyError:
            # cases.id not returned by mapping endpoint. In such cases,
            # substitute an empty description string.
            column_name = field.split(".")[-1]
            description = ""

        if field_data_types[field]:
            # if script was able to infer a data type using field's values,
            # default to using that type
            field_type = field_data_types[field]
        elif field in field_mappings:
            # otherwise, include type from _mapping endpoint
            field_type = field_mappings[field]['type']
        else:
            # this could happen in the case where a field was added to the
            # cases endpoint with only null values,
            # and no entry for the field exists in mapping
            console_out(
                "[INFO] Not adding field {0} because no type found", (field,))
            continue

        # this is the format for bq schema json object entries
        schema_dict[field] = {
            "name": column_name,
            "type": field_type,
            "description": description
        }

    return schema_dict


def create_bq_schema(data_fp):
    """Generates two dicts (one using data type inference, one using _mapping API
    endpoint.) Compares values and builds a SchemaField object, used to
    initialize the bq table.

    :param data_fp: path to API data output file (jsonl format)
    """
    # generate dict containing field mapping results
    field_mapping_dict = create_mapping_dict(API_PARAMS['ENDPOINT'])

    with open(data_fp, 'r') as data_file:
        field_dict = dict()

        for line in data_file:
            json_case = json.loads(line)
            for key in json_case:
                field_dict = collect_values(field_dict, key, json_case, 'cases.')

    field_data_type_dict = infer_data_types(field_dict)

    # create a flattened dict of schema fields
    schema_dict = create_field_records_dict(field_mapping_dict, field_data_type_dict)

    endpoint_name = API_PARAMS['ENDPOINT'].split('/')[-1]

    return generate_bq_schema(schema_dict,
                              record_type=endpoint_name,
                              expand_fields_list=get_field_groups(API_PARAMS))


def main(args):
    """Script execution function.

    :param args: command-line arguments
    """
    start = time.time()
    steps = []

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error("{}".format(err), ValueError)

    jsonl_output_file = build_jsonl_output_filename(BQ_PARAMS)
    scratch_fp = get_scratch_fp(BQ_PARAMS, jsonl_output_file)

    if 'retrieve_cases_and_write_to_jsonl' in steps:
        # Hits the GDC api endpoint, outputs data to jsonl file (format required by bq)
        console_out('Starting GDC API calls!')
        retrieve_and_save_case_records(scratch_fp)

    if 'upload_jsonl_to_cloud_storage' in steps:
        # Insert the generated jsonl file into google storage bucket, for later
        # ingestion by BQ
        console_out('Uploading jsonl file to cloud storage!')
        upload_to_bucket(BQ_PARAMS, scratch_fp)

    if 'build_bq_table' in steps:
        # Creates a BQ schema python object consisting of nested SchemaField objects
        console_out('Creating BQ schema object!')
        schema = create_bq_schema(scratch_fp)

        # Creates and populates BQ table
        if not schema:
            has_fatal_error('Empty SchemaField object', UnboundLocalError)
        console_out('Building BQ Table!')

        table_name = "_".join([get_rel_prefix(BQ_PARAMS), BQ_PARAMS['MASTER_TABLE']])
        table_id = get_working_table_id(BQ_PARAMS, table_name)

        print(schema)

        create_and_load_table(BQ_PARAMS, jsonl_output_file, schema, table_id)

    end = time.time() - start
    console_out("Script executed in {0:.0f} seconds\n", (end,))


if __name__ == '__main__':
    main(sys.argv)

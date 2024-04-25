"""
Copyright 2024, Institute for Systems Biology

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
import logging
import sys
import time
import os
import requests
from typing import Union, Optional, Any, Callable

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud.bigquery import SchemaField, Client, LoadJobConfig, QueryJob
from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator

from cda_bq_etl.utils import get_filename, get_scratch_fp, get_filepath, create_dev_table_id, create_metadata_table_id
from cda_bq_etl.gcs_helpers import download_from_bucket, upload_to_bucket
from cda_bq_etl.data_helpers import (recursively_detect_object_structures, get_column_list_tsv,
                                     aggregate_column_data_types_tsv, resolve_type_conflicts, resolve_type_conflict)

Params = dict[str, Union[str, dict, int, bool]]
ColumnTypes = Union[None, str, float, int, bool]
RowDict = dict[str, Union[None, str, float, int, bool]]
JSONList = list[RowDict]
BQQueryResult = Union[None, RowIterator, _EmptyRowIterator]
SchemaFieldFormat = dict[str, list[dict[str, str]]]


def get_graphql_api_response(params: Params, query: str, fail_on_error: bool = True):
    """
    Create and submit graphQL API request, returning API response serialized as json object.
    :param params: params supplied in yaml config
    :param query: GraphQL-formatted query string
    :param fail_on_error: if True, will fail fast--otherwise, tries up to 3 times before failing. False is good for
    longer paginated queries, which often throw random server errors
    :return: json response object
    """
    logger = logging.getLogger('base_script.cda_bq_etl.pdc_helpers')

    max_retries = 10

    headers = {'Content-Type': 'application/json'}
    endpoint = params['ENDPOINT_URL']

    if not query:
        logger.critical("Must specify query for get_graphql_api_response.", SyntaxError)
        exit(-1)

    req_body = {'query': query}
    api_res = requests.post(endpoint, headers=headers, json=req_body)

    tries = 0

    while not api_res.ok and tries < max_retries:
        if api_res.status_code == 400:
            # don't try again!
            logger.critical(f"Request failed; exiting. Response status code {api_res.status_code}: \n{api_res.reason}.")
            logger.critical(f"Request body: \n{req_body}")
            exit(-1)

        logger.warning(f"Response code {api_res.status_code}: {api_res.reason}")
        logger.warning(query)

        sleep_time = 3 * tries
        logger.warning(f"Retry {tries} of {max_retries}... sleeping for {sleep_time}")
        time.sleep(sleep_time)

        api_res = requests.post(endpoint, headers=headers, json=req_body)

        tries += 1

        # Failed up to max retry value, stop making requests
        if tries > max_retries:
            api_res.raise_for_status()

    json_res = api_res.json()

    if 'errors' in json_res and json_res['errors']:
        if fail_on_error:
            logger.critical(f"Failed, exiting. Errors returned by {endpoint}. \nError json: \n{json_res['errors']}")
            exit(-1)

    return json_res


def request_data_from_pdc_api(params: Params,
                              endpoint: str,
                              request_body_function: Callable) -> list:
    """
    Used internally by build_obj_from_pdc_api().
    :param params: params from YAML config
    :param endpoint: PDC API endpoint
    :param request_body_function: function outputting GraphQL request body (including query)
    :return: Response results list
    """
    logger = logging.getLogger("base_script.cda_bq_etl.pdc_helpers")

    payload_key = params['ENDPOINT_SETTINGS'][endpoint]['payload_key']

    def append_api_response_data(_graphql_request_body):
        # Adds api response data to record list
        api_response = get_graphql_api_response(params, _graphql_request_body)

        try:
            response_body = api_response['data'][endpoint]

            for record in response_body[payload_key]:
                record_list.append(record)

            return response_body['pagination']['pages'] if 'pagination' in response_body else None
        except TypeError:
            logger.critical(f"Unexpected GraphQL response format: {api_response}")
            exit(-1)

    record_list = list()

    limit = params['ENDPOINT_SETTINGS'][endpoint]['batch_size']
    offset = 0
    page = 1

    paginated_request_params = (offset, limit)

    # * operator unpacks tuple for use as positional function args
    graphql_request_body = request_body_function(*paginated_request_params)
    total_pages = append_api_response_data(graphql_request_body)

    logger.info(f" - Appended page {page} of {total_pages}")

    if not total_pages:
        logger.critical("API did not return a value for total pages, but is_paginated set to True.")
        exit(-1)

    while page < total_pages:
        offset += limit
        page += 1

        paginated_request_params = (offset, limit)
        graphql_request_body = request_body_function(*paginated_request_params)
        new_total_pages = append_api_response_data(graphql_request_body)

        logger.info(f" - Appended page {page} of {total_pages}")

        if new_total_pages != total_pages:
            logger.critical(f"Page count change mid-ingestion (from {total_pages} to {new_total_pages})")
            exit(-1)

    return record_list





def build_obj_from_pdc_api(params: Params,
                           endpoint: str,
                           request_function: Callable,
                           alter_json_function: Callable = None) -> list:
    """
    Create jsonl file based on results from PDC API request.
    :param params: params from YAML config
    :param endpoint: PDC API endpoint
    :param request_function: PDC API request function
    :param alter_json_function: Used to mutate json object prior to writing to file
    :return
    """
    logger = logging.getLogger('base_script.cda_bq_etl.pdc_helpers')

    logger.info(f"Sending {endpoint} API request: ")

    joined_record_list = request_data_from_pdc_api(params, endpoint, request_function)
    logger.info(f" - collected {len(joined_record_list)} records")

    if alter_json_function:
        alter_json_function(joined_record_list)

    return joined_record_list
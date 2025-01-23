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
import ast

from google.cloud import bigquery

from cda_bq_etl.gcs_helpers import transfer_between_buckets
from cda_bq_etl.utils import (load_config, format_seconds, input_with_timeout)
from cda_bq_etl.bq_helpers import (create_and_load_table_from_tsv, query_and_retrieve_result,
                                   create_and_load_table_from_jsonl, create_table_from_query,
                                   update_table_schema_from_generic, create_view_from_query, delete_bq_table,
                                   copy_bq_table, update_friendly_name, change_status_to_archived,
                                   find_most_recent_published_table_id)
from cda_bq_etl.data_helpers import initialize_logging, write_list_to_jsonl_and_upload

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_manifest_url_query(table_id) -> str:
    return f"""
    SELECT id, acl, gs_url
    FROM `{table_id}`
    """


def parse_manifest_url_records(manifest_table_name) -> list[dict[str, str]]:
    logger = logging.getLogger('base_script')
    file_record_list = list()

    table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_DATASET']}.{manifest_table_name}"
    result = query_and_retrieve_result(make_manifest_url_query(table_id))

    for row in result:
        file_uuid = row.get('id')
        acl = row.get('acl')
        gs_url = row.get('gs_url')

        file_record_dict = {
            'file_gdc_id': file_uuid,
            'gdc_file_url_web': None,
            'gdc_file_url': None,
            'gdc_file_url_aws': None
        }

        if gs_url and '[' in gs_url:
            url_list = list(map(str.strip, ast.literal_eval(gs_url)))
        else:
            url_list = [gs_url]

        for url in url_list:
            if not url:
                continue
            if 'https://' in url:
                file_record_dict['gdc_file_url_web'] = url
            else:
                if 'open' in acl and 'phs' not in acl:
                    if 'gs://' in url:
                        file_record_dict['gdc_file_url'] = url
                    elif 's3://' in url:
                        file_record_dict['gdc_file_url_aws'] = url
                    else:
                        logger.critical(f"Invalid URL scheme: {url}")
                        sys.exit(-1)
        file_record_list.append(file_record_dict)

    return file_record_list


def make_combined_table_query(table_ids: list[str]) -> str:
    table_id_0 = table_ids[0]
    table_id_1 = table_ids[1]

    return f"""
        SELECT * FROM {table_id_0}
        UNION ALL
        SELECT * FROM {table_id_1}
    """


def make_reordered_table_query(combined_table_id) -> str:
    return f"""
    SELECT file_gdc_id,
            gdc_file_url,
            gdc_file_url_aws,
            gdc_file_url_web
    FROM `{combined_table_id}`  
    """


def publish_table(table_ids: dict[str, str]):
    """
    Publish production BigQuery tables using source_table_id:
        - create current/versioned table ids
        - publish tables
        - update friendly name for versioned table
        - change last version tables' status labels to archived
    :param table_ids: dict of table ids: 'source' (dev table), 'versioned' and 'current' (future published ids)

    """
    logger = logging.getLogger('base_script')

    delay = 5
    logger.info(f"Publishing the following tables:")
    logger.info(f"\t- {table_ids['versioned']}")
    logger.info(f"\t- {table_ids['current']}")
    logger.info(f"Proceed? Y/n (continues automatically in {delay} seconds)")

    response = str(input_with_timeout(seconds=delay)).lower()

    if response == 'n' or response == 'N':
        exit("Publish aborted; exiting.")

    logger.info(f"Publishing {table_ids['versioned']}")
    copy_bq_table(params=PARAMS,
                  src_table=table_ids['source'],
                  dest_table=table_ids['versioned'],
                  replace_table=PARAMS['OVERWRITE_PROD_TABLE'])

    logger.info(f"Publishing {table_ids['current']}")
    copy_bq_table(params=PARAMS,
                  src_table=table_ids['source'],
                  dest_table=table_ids['current'],
                  replace_table=PARAMS['OVERWRITE_PROD_TABLE'])

    logger.info(f"Updating friendly name for {table_ids['versioned']}")
    update_friendly_name(PARAMS, table_id=table_ids['versioned'])

    if table_ids['previous_versioned']:
        logger.info(f"Archiving {table_ids['previous_versioned']}")
        change_status_to_archived(table_ids['previous_versioned'])


def main(args):
    try:
        start_time = time.time()

        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        sys.exit(err)

    log_file_time = time.strftime('%Y.%m.%d-%H.%M.%S', time.localtime())
    log_filepath = f"{PARAMS['LOGFILE_PATH']}.{log_file_time}"
    logger = initialize_logging(log_filepath)

    # steps in existing script:
    # create dicts of tsv and bq table filenames using variables in yaml file
    # transfer manifest files from source bucket to our bucket
    # clear directory and import table schemas from BQEcosystem
    # create manifest BQ tables using tsv
    # create modified manifest BQ table to split out file_gdc_url into multiple columns
    # create file map BQ tables via sql query
    # update file map BQ table schemas using imported schema
    # create combined legacy and active file map table
    # update combined table using imported schema
    # add labels and description to combined table
    # publish table

    # create modified manifest BQ table:
    # - split gs_url into columns (web, aws, gcs) based on url scheme (gs://, https://, s3://)
    # - null urls for aws and gcs when acl is not ['open']

    manifest_dict = {
        # table name: tsv file name
        f"gdc_{PARAMS['RELEASE']}_hg19": PARAMS['LEGACY_MANIFEST_TSV'],
        f"gdc_{PARAMS['RELEASE']}_hg38": PARAMS['ACTIVE_MANIFEST_TSV']
    }

    if "pull_manifest_from_data_node" in steps:
        logger.info("Entering pull_manifest_from_data_node")
        for manifest_file_name in manifest_dict.values():
            transfer_between_buckets(PARAMS, PARAMS['SOURCE_BUCKET'], manifest_file_name, PARAMS['WORKING_BUCKET'])

    if "create_bq_manifest_table" in steps:
        logger.info("Entering create_bq_manifest_table")
        with open(PARAMS['MANIFEST_SCHEMA_LIST'], mode='r') as schema_hold_dict:
            schema_list = []
            typed_schema = json.loads(schema_hold_dict.read())

            for schema_obj in typed_schema:
                use_mode = schema_obj['mode'] if "mode" in schema_obj else 'NULLABLE'
                schema_list.append(bigquery.SchemaField(schema_obj['name'],
                                                        schema_obj['type'].upper(),
                                                        mode=use_mode,
                                                        description=schema_obj['description']))

            manifest_table_schema = schema_list
        for manifest_table_name, manifest_file in manifest_dict.items():
            table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_DATASET']}.{manifest_table_name}"

            create_and_load_table_from_tsv(params=PARAMS,
                                           tsv_file=manifest_file,
                                           table_id=table_id,
                                           num_header_rows=1,
                                           schema=manifest_table_schema)
    if "create_file_mapping_table" in steps:
        logger.info("Entering create_file_mapping_table")
        # query to retrieve id, acl, gs_url
        # iterate over results and build json object dict
        # - parse gs_url into list--either by converting string list representation or putting single value into a list
        # create list of dicts containing id, gdc_file_url_web, gdc_file_url_aws, gdc_file_url
        # - if acl isn't open, don't include gs or aws uris
        for manifest_table_name in manifest_dict.keys():
            parsed_table_name = f"{manifest_table_name}_{PARAMS['SPLIT_URL_TABLE_SUFFIX']}"
            parsed_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_DATASET']}.{parsed_table_name}"

            manifest_url_record_list = parse_manifest_url_records(manifest_table_name)

            write_list_to_jsonl_and_upload(params=PARAMS,
                                           prefix=parsed_table_name,
                                           record_list=manifest_url_record_list)

            create_and_load_table_from_jsonl(params=PARAMS,
                                             jsonl_file=f"{parsed_table_name}_{PARAMS['RELEASE']}.jsonl",
                                             table_id=parsed_table_id)

    if "create_combined_table" in steps:
        logger.info("Entering create_combined_table")
        table_ids = list()

        for manifest_table_name in manifest_dict.keys():
            parsed_table_name = f"{manifest_table_name}_{PARAMS['SPLIT_URL_TABLE_SUFFIX']}"
            parsed_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_DATASET']}.{parsed_table_name}"
            table_ids.append(parsed_table_id)

        gdc_release = PARAMS['RELEASE'][2:]

        combined_table_name = f"temp_rel{gdc_release}_{PARAMS['COMBINED_TABLE']}"
        combined_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_DATASET']}.{combined_table_name}"

        create_table_from_query(params=PARAMS,
                                table_id=combined_table_id,
                                query=make_combined_table_query(table_ids))

        update_table_schema_from_generic(params=PARAMS, table_id=combined_table_id)

    if "reorder_combined_table" in steps:
        logger.info("Entering reorder_combined_table")

        gdc_release = PARAMS['RELEASE'][2:]
        combined_table_name = f"rel{gdc_release}_{PARAMS['COMBINED_TABLE']}"
        combined_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_DATASET']}.temp_{combined_table_name}"
        reordered_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_DATASET']}.{combined_table_name}"

        create_table_from_query(PARAMS,
                                table_id=reordered_table_id,
                                query=make_reordered_table_query(combined_table_id))

        update_table_schema_from_generic(params=PARAMS, table_id=reordered_table_id)

        delete_bq_table(combined_table_id)

    if "create_paths_views" in steps:
        for manifest_table_name in manifest_dict.keys():
            parsed_table_name = f"{manifest_table_name}_{PARAMS['SPLIT_URL_TABLE_SUFFIX']}"
            parsed_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_DATASET']}.{parsed_table_name}"

            sql = f"""SELECT file_gdc_id AS file_uuid, 
                   gdc_file_url AS gcs_path
                   FROM `{parsed_table_id}`
                   """

            if "hg19" in manifest_table_name:
                data_type = 'legacy'
            elif "hg38" in manifest_table_name:
                data_type = 'active'
            else:
                logger.critical("Can't parse the manifest table name.")
                exit()

            view_table_name = f"{PARAMS['RELEASE']}_paths_{data_type}"
            view_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_DATASET']}.{view_table_name}"

            create_view_from_query(view_id=view_table_id, view_query=sql)

    if "publish_combined_table" in steps:
        gdc_release = PARAMS['RELEASE'][2:]
        combined_table_name = f"rel{gdc_release}_{PARAMS['COMBINED_TABLE']}"
        combined_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_DATASET']}.{combined_table_name}"

        current_table_id = f"{PARAMS['PROD_PROJECT']}.{PARAMS['PROD_DATASET']}.{PARAMS['COMBINED_TABLE']}_current"

        versioned_table_name = f"{PARAMS['COMBINED_TABLE']}_{PARAMS['RELEASE'][1:]}"
        versioned_table_id = f"{PARAMS['PROD_PROJECT']}.{PARAMS['PROD_DATASET']}_versioned.{versioned_table_name}"

        previous_versioned_table_id = find_most_recent_published_table_id(PARAMS,
                                                                          versioned_table_id=versioned_table_id,
                                                                          table_base_name=PARAMS['COMBINED_TABLE'])

        table_ids = {
            "source": combined_table_id,
            "current": current_table_id,
            "versioned": versioned_table_id,
            "previous_versioned": previous_versioned_table_id
        }

        publish_table(table_ids)

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)

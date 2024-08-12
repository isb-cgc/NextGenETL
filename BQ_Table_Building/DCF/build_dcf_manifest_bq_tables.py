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
import sys
import time

from google.cloud import bigquery

from cda_bq_etl.gcs_helpers import transfer_between_buckets
from cda_bq_etl.utils import (load_config, format_seconds)
from cda_bq_etl.bq_helpers import create_and_load_table_from_tsv
from cda_bq_etl.data_helpers import initialize_logging

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


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
    # todo add step -- create modified manifest BQ table to split out file_gdc_url into multiple columns
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
        for manifest_file_name in manifest_dict.values():
            transfer_between_buckets(PARAMS['SOURCE_BUCKET'], manifest_file_name, PARAMS['WORKING_BUCKET'])

    # todo do we want to reload the BQEcosystem repo here, as in existing pipeline?
    #  Probably not necessary with generic schema
    if "create_bq_manifest_table" in steps:
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

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)

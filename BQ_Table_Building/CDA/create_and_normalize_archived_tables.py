"""
Copyright 2023, Institute for Systems Biology

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
import sys

from common_etl.support import bq_harness_with_result
from common_etl.utils import load_config, has_fatal_error, normalize_flat_json_values, write_list_to_jsonl_and_upload, \
    create_and_upload_schema_for_json, retrieve_bq_schema_object, create_and_load_table_from_jsonl
from common_etl.cda_utils import convert_bq_result_to_object_list

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def make_aliquot_to_case_legacy_filtered_query():
    working_project = BQ_PARAMS['WORKING_PROJECT']
    working_dataset = BQ_PARAMS['WORKING_DATASET']
    published_project = BQ_PARAMS['PUBLISHED_PROJECT']
    published_dataset = BQ_PARAMS['PUBLISHED_DATASET']
    release = API_PARAMS['RELEASE']
    gdc_archive_release = API_PARAMS['GDC_ARCHIVE_RELEASE']

    return f"""
    SELECT program_name, 
        project_id, 
        case_gdc_id, 
        case_barcode, 
        sample_gdc_id, 
        sample_barcode,
        sample_type, 
        sample_type_name, 
        sample_is_ffpe, 
        sample_preservation_method, 
        portion_gdc_id, 
        portion_barcode, 
        analyte_gdc_id, 
        analyte_barcode, 
        aliquot_gdc_id, 
        aliquot_barcode 
    FROM `{published_project}.{published_dataset}.aliquot2caseIDmap_{gdc_archive_release}`
    WHERE portion_gdc_id NOT IN (
      SELECT portion_gdc_id
      FROM `{working_project}.{working_dataset}.aliquot_to_case_{release}`
    )
    """


def make_case_metadata_legacy_filtered_query():
    working_project = BQ_PARAMS['WORKING_PROJECT']
    working_dataset = BQ_PARAMS['WORKING_DATASET']
    published_project = BQ_PARAMS['PUBLISHED_PROJECT']
    published_dataset = BQ_PARAMS['PUBLISHED_DATASET']
    release = API_PARAMS['RELEASE']
    gdc_archive_release = API_PARAMS['GDC_ARCHIVE_RELEASE']

    # query filters out any file/case ids that have no DCF file references

    return f"""
    SELECT case_gdc_id, 
        primary_site, 
        project_dbgap_accession_number, 
        project_disease_type, 
        project_name, 
        program_dbgap_accession_number,
        program_name,
        project_id, 
        case_barcode, 
        legacy_file_count, 
        active_file_count 
    FROM `{published_project}.{published_dataset}.caseData_{gdc_archive_release}`
    WHERE case_gdc_id NOT IN (
      SELECT case_gdc_id 
      FROM `{working_project}.{working_dataset}.case_metadata_{release}`
    ) AND case_gdc_id IN (
      SELECT case_gdc_id
      FROM `{published_project}.{published_dataset}.fileData_legacy_{gdc_archive_release}`
      JOIN `{published_project}.{published_dataset}.GDCfileID_to_GCSurl_r36`
        USING(file_gdc_id)
    )
    """


def make_file_metadata_legacy_filtered_query():
    working_project = BQ_PARAMS['WORKING_PROJECT']
    working_dataset = BQ_PARAMS['WORKING_DATASET']
    published_project = BQ_PARAMS['PUBLISHED_PROJECT']
    published_dataset = BQ_PARAMS['PUBLISHED_DATASET']
    release = API_PARAMS['RELEASE']
    gdc_archive_release = API_PARAMS['GDC_ARCHIVE_RELEASE']

    # query filters out any file/case ids that have no DCF file references

    return f"""
    SELECT dbName, 
        file_gdc_id, 
        `access`, 
        acl, 
        archive_gdc_id, 
        archive_revision, 
        archive_state, 
        archive_submitter_id, 
        associated_entities__case_gdc_id, 
        associated_entities__entity_gdc_id, 
        associated_entities__entity_submitter_id, 
        associated_entities__entity_type, 
        case_gdc_id, 
        project_dbgap_accession_number, 
        project_disease_type, 
        project_name, 
        program_dbgap_accession_number, 
        program_name, 
        project_short_name, 
        center_type, 
        center_code, 
        center_name, 
        center_short_name, 
        created_datetime, 
        data_category, 
        data_format, 
        data_type, 
        experimental_strategy, 
        file_name, 
        file_size, 
        file_id, 
        index_file_gdc_id, 
        index_file_name, 
        index_file_size, 
        md5sum, 
        metadata_file_id, 
        metadata_file_name, 
        metadata_file_size, 
        metadata_file_type, 
        platform, 
        file_state, 
        file_submitter_id, 
        file_tags, 
        file_type, 
        updated_datetime 
    FROM `{published_project}.{published_dataset}.fileData_legacy_{gdc_archive_release}`
    WHERE file_gdc_id NOT IN (
      SELECT file_gdc_id 
      FROM `{working_project}.{working_dataset}.file_metadata_{release}`
    ) AND file_gdc_id IN (
      SELECT file_gdc_id
      FROM `{published_project}.{published_dataset}.GDCfileID_to_GCSurl_r36`
    )
    """


def create_jsonl_and_schema(sql: str, column_list: list[str], table_name: str):
    result = bq_harness_with_result(sql=sql, do_batch=False, verbose=False)

    obj_list = convert_bq_result_to_object_list(result=result, column_list=column_list)

    normalized_obj_list = normalize_flat_json_values(obj_list)

    write_list_to_jsonl_and_upload(API_PARAMS,
                                   BQ_PARAMS,
                                   prefix=table_name,
                                   record_list=normalized_obj_list)

    create_and_upload_schema_for_json(API_PARAMS,
                                      BQ_PARAMS,
                                      record_list=normalized_obj_list,
                                      table_name=table_name,
                                      include_release=True)

    print("JSONL file and schema uploaded to cloud!")


def create_table(table_name: str):
    gdc_archive_release = API_PARAMS['GDC_ARCHIVE_RELEASE']
    working_project = BQ_PARAMS['WORKING_PROJECT']
    working_dataset = BQ_PARAMS['WORKING_DATASET']

    table_id = f"{working_project}.{working_dataset}.{gdc_archive_release}_{table_name}"
    jsonl_file = f"{table_name}_{API_PARAMS['RELEASE']}.jsonl"

    table_schema = retrieve_bq_schema_object(API_PARAMS, BQ_PARAMS, table_name=table_name, include_release=True)

    # Load jsonl data into BigQuery table
    create_and_load_table_from_jsonl(BQ_PARAMS, jsonl_file=jsonl_file, table_id=table_id, schema=table_schema)


def main(args):
    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    if 'create_aliquot_to_case_legacy_jsonl' in steps:
        create_jsonl_and_schema(sql=make_aliquot_to_case_legacy_filtered_query(),
                                column_list=BQ_PARAMS['ALIQUOT_COLUMN_LIST'],
                                table_name=BQ_PARAMS['ALIQUOT_TABLE_NAME'])

    if 'create_aliquot_to_case_legacy_table' in steps:
        create_table(table_name=BQ_PARAMS['ALIQUOT_TABLE_NAME'])

    if 'create_case_metadata_legacy_jsonl' in steps:
        create_jsonl_and_schema(sql=make_case_metadata_legacy_filtered_query(),
                                column_list=BQ_PARAMS['CASE_COLUMN_LIST'],
                                table_name=BQ_PARAMS['CASE_TABLE_NAME'])

    if 'create_case_metadata_legacy_table' in steps:
        create_table(table_name=BQ_PARAMS['CASE_TABLE_NAME'])

    if 'create_file_metadata_legacy_jsonl' in steps:
        create_jsonl_and_schema(sql=make_file_metadata_legacy_filtered_query(),
                                column_list=BQ_PARAMS['FILE_COLUMN_LIST'],
                                table_name=BQ_PARAMS['FILE_TABLE_NAME'])

    if 'create_file_metadata_legacy_table' in steps:
        create_table(table_name=BQ_PARAMS['FILE_TABLE_NAME'])


if __name__ == "__main__":
    main(sys.argv)

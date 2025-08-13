# Copyright 2023-2025, Institute for Systems Biology

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
# Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
# WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""Generate schema objects for BigQuery."""

import json
import logging
import sys
from typing import Optional

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

from cda_bq_etl.bq_helpers.lookup import get_pdc_project_metadata
from cda_bq_etl.custom_typing import Params, JSONList, SchemaFieldFormat, RowDict, ColumnTypes
from cda_bq_etl.utils import (get_filename, get_scratch_fp, get_filepath)
from cda_bq_etl.gcs_helpers import download_from_bucket, upload_to_bucket
from cda_bq_etl.data_helpers import (recursively_detect_object_structures, get_column_list_tsv,
                                     aggregate_column_data_types_tsv, resolve_type_conflicts, resolve_type_conflict)


def create_and_upload_schema_for_tsv(params: Params,
                                     tsv_fp: str,
                                     header_row: Optional[int] = None,
                                     skip_rows: int = 0,
                                     schema_fp: Optional[str] = None,
                                     delete_local: bool = True,
                                     sample_interval: int = 1):
    """
    Create and upload schema for a file in tsv format.

    :param params: params supplied in yaml config
    :type params: Params
    :param tsv_fp: path to tsv data file, parsed to create schema
    :type tsv_fp: str
    :param header_row: integer index of header row within the file; defaults to None
    :type header_row: Optional[int]
    :param skip_rows: integer representing number of non-data rows at the start of the file; defaults to 0
    :type skip_rows: int
    :param schema_fp: path to schema location on local vm; defaults to None
    :type schema_fp: Optional[str]
    :param delete_local: delete local file after uploading to cloud bucket; defaults to True
    :type delete_local: bool
    :param sample_interval: how many rows to skip between column type checks; defaults to 1
    :type sample_interval: int
    """
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.schema')

    logger.info(f"Creating schema for {tsv_fp}")

    column_headers = get_column_list_tsv(tsv_fp=tsv_fp, header_row_index=header_row)

    if isinstance(header_row, int) and header_row >= skip_rows:
        logger.critical("Header row not excluded by skip_rows.")
        sys.exit(-1)

    data_types_dict = aggregate_column_data_types_tsv(tsv_fp, column_headers, skip_rows, sample_interval)

    data_type_dict = resolve_type_conflicts(data_types_dict)

    schema_obj = create_schema_object(column_headers, data_type_dict)

    if not schema_fp:
        schema_filename = get_filename(params, file_extension='json', prefix="schema")
        schema_fp = get_scratch_fp(params, schema_filename)

    with open(schema_fp, 'w') as schema_json_file:
        json.dump(schema_obj, schema_json_file, indent=4)

    upload_to_bucket(params, schema_fp, delete_local=delete_local)


def create_and_upload_schema_for_json(params: Params,
                                      record_list: JSONList,
                                      table_name: str,
                                      include_release: bool = False,
                                      release: Optional[str] = None,
                                      schema_fp: Optional[str] = None,
                                      delete_local: bool = True,
                                      reorder_nesting: bool = False):
    """
    Create a schema object by recursively detecting the object structure and data types, storing result,
    and converting that to a Schema dict for BQ ingestion.

    :param params: params supplied in yaml config
    :type params: Params
    :param record_list: list of records to analyze (used to determine schema)
    :type record_list: JSONList
    :param table_name: table for which the schema is being generated
    :type table_name: str
    :param include_release: if true, includes release in schema file name; defaults to False
    :type include_release: bool
    :param release: custom release value; defaults to None, in which case the value is derived from the yaml config
    :type release: Optional[str]
    :param schema_fp: path to schema location on local vm, defaults to None, in which case the value is derived from
                      the yaml config
    :type schema_fp: Optional[str]
    :param delete_local: delete local file after uploading to cloud bucket
    :type delete_local: bool
    :param reorder_nesting: whether the data_types_dict should be reordered, defaults to False
    :type reorder_nesting: bool
    """

    data_types_dict = recursively_detect_object_structures(record_list)

    if reorder_nesting:
        data_types_dict = reorder_data_types_dict(params, data_types_dict)

    schema_list = convert_object_structure_dict_to_schema_dict(data_types_dict, list())

    schema_obj = {"fields": schema_list}

    if not schema_fp:
        schema_filename = get_filename(params,
                                       file_extension='json',
                                       prefix="schema",
                                       suffix=table_name,
                                       include_release=include_release,
                                       release=release)

        schema_fp = get_scratch_fp(params, schema_filename)

    with open(schema_fp, 'w') as schema_json_file:
        json.dump(schema_obj, schema_json_file, indent=4)

    upload_to_bucket(params, schema_fp, delete_local=delete_local)


def retrieve_bq_schema_object(params: Params,
                              table_name: Optional[str] = None,
                              release: Optional[str] = None,
                              include_release: bool = True,
                              schema_filename: Optional[str] = None,
                              schema_dir: Optional[str] = None) -> list[SchemaField]:
    """
    Retrieve schema file from GDC bucket and convert into list of SchemaField objects.

    :param params: params supplied in yaml config
    :type params: Params
    :param table_name: name of table for which schema was created; defaults to None
    :type table_name: Optional[str]
    :param release: data release number; defaults to None, in which case it's derived from yaml config
    :type release: Optional[str]
    :param include_release: Whether to include release in filename
    :type include_release: bool
    :param schema_filename: schema file name
    :type schema_filename: Optional[str]
    :param schema_dir: schema file directory location
    :type schema_dir: Optional[str]
    :return: list of SchemaField objects for BigQuery ingestion
    :rtype: list[SchemaField]
    """
    if not schema_filename:
        schema_filename = get_filename(params=params,
                                       file_extension='json',
                                       prefix="schema",
                                       suffix=table_name,
                                       release=release,
                                       include_release=include_release)

    download_from_bucket(params, filename=schema_filename, dir_path=schema_dir)

    if not schema_dir:
        schema_fp = get_scratch_fp(params, schema_filename)
    else:
        schema_fp = f"{schema_dir}/{schema_filename}"

    with open(schema_fp, "r") as schema_json:
        schema_obj = json.load(schema_json)
        json_schema_obj_list = [field for field in schema_obj["fields"]]
        schema = generate_bq_schema_fields(json_schema_obj_list)

    return schema


def generate_bq_schema_fields(schema_obj_list: JSONList) -> list[SchemaField]:
    """
    Convert list of schema fields into TableSchema object.

    :param schema_obj_list: list of dicts representing BigQuery SchemaField objects
    :type schema_obj_list: JSONList
    :return: list of BigQuery SchemaField objects (represents TableSchema object)
    :rtype: list[SchemaField]
    """

    def create_schema_field_obj(_schema_obj: dict[str, str],
                                schema_fields: Optional[list[SchemaField]] = None):
        """Output BigQuery SchemaField object."""
        if schema_fields:
            return bigquery.schema.SchemaField(name=_schema_obj['name'],
                                               description=_schema_obj['description'],
                                               field_type=_schema_obj['type'],
                                               mode=_schema_obj['mode'],
                                               fields=schema_fields)
        else:
            return bigquery.schema.SchemaField(name=_schema_obj['name'],
                                               description=_schema_obj['description'],
                                               field_type=_schema_obj['type'],
                                               mode=_schema_obj['mode'])

    def generate_bq_schema_field(_schema_obj: dict[str, dict] | dict[str, str],
                                 schema_fields: list[SchemaField]):
        """Convert schema field json dict object into SchemaField object."""
        if not _schema_obj:
            return
        elif _schema_obj['type'] == 'RECORD':
            child_schema_fields = list()

            if not _schema_obj['fields']:
                logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.schema')
                logger.critical("Schema object has 'type': 'RECORD' but no 'fields' key.")
                sys.exit(-1)

            for child_obj in _schema_obj['fields']:
                generate_bq_schema_field(child_obj, child_schema_fields)

            schema_field = create_schema_field_obj(_schema_obj, child_schema_fields)
        else:
            schema_field = create_schema_field_obj(_schema_obj)

        schema_fields.append(schema_field)

    schema_fields_obj = list()

    for _schema_obj in schema_obj_list:
        generate_bq_schema_field(_schema_obj, schema_fields_obj)

    return schema_fields_obj


def create_schema_object(column_headers: list[str], data_types_dict: dict[str, str]) -> SchemaFieldFormat:
    """
    Create BigQuery SchemaField object.

    :param column_headers: list of column names
    :type column_headers: list[str]
    :param data_types_dict: dictionary of column names and their types (should have been run through
                            resolve_type_conflicts() prior to use here)
    :type data_types_dict: dict[str, str]
    :return: BQ schema field object list
    :rtype: SchemaFieldFormat
    """
    schema_field_object_list = list()

    for column_name in column_headers:
        # override typing for ids, even those which are actually in

        schema_field = {
            "name": column_name,
            "type": data_types_dict[column_name],
            "mode": "NULLABLE",
            "description": ''
        }

        schema_field_object_list.append(schema_field)

    return {
        "fields": schema_field_object_list
    }


def convert_object_structure_dict_to_schema_dict(data_schema_dict: RowDict | JSONList | ColumnTypes,
                                                 dataset_format_obj: list = list,
                                                 descriptions: Optional[dict[str, str]] = None) -> list[dict]:
    """
    Recursively parse dict of {<field>: {<data_types>}} representing data object's structure;
    convert into dict representing a TableSchema object.

    :param data_schema_dict: dictionary representing dataset's structure, fields and data types
    :type data_schema_dict: RowDict | JSONList | ColumnTypes
    :param dataset_format_obj: holds schema dict as it is recursively created
    :type dataset_format_obj: list
    :param descriptions: (optional) dictionary of field: description string pairs for inclusion in schema definition
    :type descriptions: Optional[dict[str, str]]
    :return: list of schema field dicts
    :rtype: list[dict]
    """

    for k, v in data_schema_dict.items():
        if descriptions and k in descriptions:
            description = descriptions[k]
        else:
            description = ""

        if isinstance(v, dict):
            # parent node
            schema_field = {
                "name": k,
                "type": "RECORD",
                "mode": "REPEATED",
                "description": description,
                "fields": list()
            }
            dataset_format_obj.append(schema_field)

            convert_object_structure_dict_to_schema_dict(data_schema_dict[k], schema_field['fields'])
        else:
            # v is a set
            final_type = resolve_type_conflict(k, v)

            if final_type == "ARRAY":
                schema_field = {
                    "name": k,
                    "type": "STRING",
                    "mode": "REPEATED",
                    "description": description
                }
            else:
                # child (leaf) node
                schema_field = {
                    "name": k,
                    "type": final_type,
                    "mode": "NULLABLE",
                    "description": description
                }

            dataset_format_obj.append(schema_field)

    return dataset_format_obj


def reorder_data_types_dict(params: Params, data_types_dict: dict[str, str | dict]) -> dict[str, str]:
    """
    Manually repair mis-ordered data type dict. (If a field in the first entity is null, nested values in
    data_types_dict get reordered.)
    There's probably a better way to go about this, like reordering the data_types_dict before it's returned,
    but currently only one table in ICDC uses nesting and making changes to recursively_detect_object_structure
    would impact every pipeline.

    :param params: params supplied in yaml config
    :type params: Params
    :param data_types_dict: a dictionary representing the data structure and field types.
    :type data_types_dict: dict[str, str | dict]
    :return: reordered data_types_dict
    :rtype: dict[str, str]
    """
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.schema')

    if 'NESTED_DATA_STRUCTURE' not in params:
        logger.critical(r"NESTED_DATA_STRUCTURE is not configured in the yaml file, which is required for this "
                        r"reorder_data_types_dict. Exiting.")
        exit(-1)

    for parent, children in params['NESTED_DATA_STRUCTURE'].items():
        reordered_dict = dict()
        existing_dict = data_types_dict[parent]

        for field in children:
            if not isinstance(field, str):
                logger.critical("reorder_data_types_dict does not support this level of nesting yet. Exiting.")
                exit(-1)

            reordered_dict[field] = existing_dict[field]

        data_types_dict[parent] = reordered_dict

    return data_types_dict


def get_project_level_schema_tags(params: Params, project_submitter_id: str) -> dict[str, str]:
    """
    Get project-level schema tags for populating generic table metadata schema.

    :param params: params from YAML config
    :type params: Params
    :param project_submitter_id: project submitter id for which to retrieve schema tags
    :type project_submitter_id: str
    :return: dict of schema tags
    :rtype: dict[str, str]
    """
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.schema')
    project_name_dict = get_pdc_project_metadata(params, project_submitter_id)[0]
    program_labels_list = project_name_dict['program_labels'].split("; ")

    if len(program_labels_list) > 2:
        logger.critical("PDC clinical isn't set up to handle >2 program labels yet; support needs to be added.")
        sys.exit(-1)
    elif len(program_labels_list) == 0:
        logger.critical(f"No program label included for {project_submitter_id}, please add to PDCStudy.yaml")
        sys.exit(-1)
    elif len(program_labels_list) == 2:
        return {
            "project-name": project_name_dict['project_short_name'].strip(),
            "mapping-name": "",  # only used by clinical, but including it elsewhere is harmless
            "friendly-project-name-upper": project_name_dict['project_friendly_name'].upper().strip(),
            "program-name-0-lower": program_labels_list[0].lower().strip(),
            "program-name-1-lower": program_labels_list[1].lower().strip()
        }
    else:
        return {
            "project-name": project_name_dict['project_short_name'].strip(),
            "mapping-name": "",  # only used by clinical, but including it elsewhere is harmless
            "friendly-project-name-upper": project_name_dict['project_friendly_name'].upper().strip(),
            "program-name-lower": project_name_dict['program_labels'].lower().strip()
        }


def get_program_schema_tags_gdc(params: Params, program_name: str) -> dict[str, str]:
    """
    Get GDC program schema tags from BQEcosystem.

    :param params: params from YAML config
    :type params: Params
    :param program_name: Program for which to retrieve schema tags
    :type program_name: str
    :return: Schema tag dict
    :rtype: dict[str, str]
    """
    metadata_mappings_path = f"{params['BQ_REPO']}/{params['PROGRAM_METADATA_DIR']}"
    program_metadata_fp = get_filepath(f"{metadata_mappings_path}/{params['PROGRAM_METADATA_FILE']}")
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.schema')

    with open(program_metadata_fp, 'r') as fh:
        program_metadata_dict = json.load(fh)
        program_metadata = program_metadata_dict[program_name]

        schema_tags = dict()

        schema_tags['program-name'] = program_metadata['friendly_name']
        schema_tags['friendly-name'] = program_metadata['friendly_name']

        if 'program_label' in program_metadata:
            schema_tags['program-label'] = program_metadata['program_label']
        elif 'program_label_0' in program_metadata and 'program_label_1' in program_metadata:
            schema_tags['program-label-0'] = program_metadata['program_label_0']
            schema_tags['program-label-1'] = program_metadata['program_label_1']
        else:
            logger.critical("Did not find program_label OR program_label_0 and program_label_1 in schema json file.")
            sys.exit(-1)

        return schema_tags


def get_program_schema_tags_icdc(program_name: str) -> dict[str, str]:
    """
    Get ICDC program schema tags.

    :param program_name: ICDC program name
    :type program_name: str
    :return: Schema tag dict
    :rtype: dict[str, str]
    """
    schema_tags = dict()

    schema_tags['program-name'] = program_name
    schema_tags['friendly-name'] = program_name
    schema_tags['program-label'] = program_name.lower()

    return schema_tags


def get_uniprot_schema_tags(params: Params) -> dict[str, str]:
    """
    Get UniProt schema tags.

    :param params: params from YAML config
    :type params: Params
    :return: Schema tag dict
    :rtype: dict[str, str]
    """
    return {
        "uniprot-version": params['UNIPROT_RELEASE'],
        "uniprot-extracted-month-year": params['UNIPROT_EXTRACTED_MONTH_YEAR']
    }


def get_gene_info_schema_tags(params: Params) -> dict[str, str]:
    """
    Get PDC gene info table schema tags.

    :param params: params from YAML config
    :type params: Params
    :return: Schema tag dict
    :rtype: dict[str, str]
    """
    return {
        "version": params['RELEASE'],
        "extracted-month-year": params['EXTRACTED_MONTH_YEAR']
    }

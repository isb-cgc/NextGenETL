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
import logging
import sys
import time
from typing import Any, Union, Optional

from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator

from cda_bq_etl.bq_helpers import query_and_retrieve_result, create_and_upload_schema_for_json, \
    create_and_load_table_from_jsonl, retrieve_bq_schema_object, update_table_schema_from_generic
from cda_bq_etl.utils import format_seconds, load_config, create_dev_table_id, create_metadata_table_id
from cda_bq_etl.data_helpers import normalize_flat_json_values, write_list_to_jsonl_and_upload, initialize_logging

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')

BQQueryResult = Union[None, RowIterator, _EmptyRowIterator]
ColumnTypes = Union[None, str, float, int, bool]
RowDict = dict[str, Union[None, str, float, int, bool]]
JSONList = list[RowDict]


def convert_concat_to_multi(value_string: str, max_length: int = 8, filter_duplicates: bool = False) -> str:
    """
    Evaluate number of values in concatenated string. If > max_length, set value to "multi" instead
    :param value_string: string containing 0 or more id values, concatenated by ';' if multiple values
    :param max_length: maximum number of values allowed in string before we substitute "multi"; Default is 8
    :param filter_duplicates: If true, remove any duplicate ids before conversion check
    :return: value string, either in original form, stripped of duplicates, or reset to "multi"
    """
    if filter_duplicates:
        filtered_set_length = len(set(value_string.split(';')))

        # only concatenate if set length is under the threshold to be converted to multi
        if filtered_set_length <= max_length:
            filtered_value_list = list()
            filtered_value_set = set()

            for value in value_string.split(';'):
                filtered_value_set.add(value)

                # if you add value to set and the length doesn't change, it's already contained in the set,
                # and therefore a duplicate, so don't add to list
                if len(filtered_value_set) > len(filtered_value_list):
                    filtered_value_list.append(value)

            value_string = ';'.join(filtered_value_list)

    string_length = len(value_string.split(';'))

    if string_length > max_length:
        return 'multi'
    else:
        return value_string


def create_file_metadata_dict() -> JSONList:
    """
    Create list of dicts containing file metadata, using successive queries.
    :return: list of dicts representing file metadata table rows
    """

    analysis_table_id = create_dev_table_id(PARAMS, 'analysis')
    analysis_consumed_input_file_table_id = create_dev_table_id(PARAMS, 'analysis_consumed_input_file')
    analysis_downstream_from_file_table_id = create_dev_table_id(PARAMS, 'analysis_downstream_from_file')
    analysis_produced_file_table_id = create_dev_table_id(PARAMS, 'analysis_produced_file')
    archive_table_id = create_dev_table_id(PARAMS, 'archive')
    case_project_program_table_id = create_dev_table_id(PARAMS, "case_project_program")
    case_table_id = create_dev_table_id(PARAMS, "case")
    downstream_analysis_table_id = create_dev_table_id(PARAMS, "downstream_analysis_produced_output_file")
    file_table_id = create_dev_table_id(PARAMS, 'file')
    file_associated_with_entity_table_id = create_dev_table_id(PARAMS, 'file_associated_with_entity')
    file_has_acl_table_id = create_dev_table_id(PARAMS, 'file_has_acl')
    file_has_index_file_table_id = create_dev_table_id(PARAMS, 'file_has_index_file')
    file_in_archive_table_id = create_dev_table_id(PARAMS, 'file_in_archive')
    file_in_case_table_id = create_dev_table_id(PARAMS, 'file_in_case')
    project_disease_type_table_id = create_dev_table_id(PARAMS, 'project_disease_types_merged')

    def make_base_file_metadata_sql() -> str:
        return f"""
            SELECT 'active' AS dbName,
            f.file_id AS file_gdc_id,
            f.access,
            an.workflow_link AS analysis_workflow_link,
            an.workflow_type AS analysis_workflow_type,
            ar.archive_id AS archive_gdc_id,
            ar.revision AS archive_revision,
            ar.state AS archive_state,
            ar.submitter_id AS archive_submitter_id,
            f.created_datetime,
            f.data_category,
            f.data_format,
            f.data_type,
            f.experimental_strategy,
            f.file_name,
            f.file_size,
            f.file_id,
            fhif.index_file_id AS index_file_gdc_id,
            f.md5sum,
            f.platform,
            f.state AS file_state,
            f.submitter_id AS file_submitter_id,
            f.type AS file_type,
            f.updated_datetime
            FROM `{file_table_id}` f
            LEFT OUTER JOIN `{analysis_produced_file_table_id}` apf
                ON apf.file_id = f.file_id
            LEFT OUTER JOIN `{analysis_table_id}` an
                ON apf.analysis_id = an.analysis_id
            LEFT OUTER JOIN `{file_in_archive_table_id}` fia
                ON fia.file_id = f.file_id
            LEFT OUTER JOIN `{archive_table_id}` ar
                ON ar.archive_id = fia.archive_id
            LEFT OUTER JOIN `{file_has_index_file_table_id}` fhif
                ON fhif.file_id = f.file_id
            WHERE f.file_id NOT IN (
                SELECT index_file_id 
                FROM `{file_has_index_file_table_id}`
            )
        """

    def make_acl_sql() -> str:
        return f"""
            SELECT file_id AS file_gdc_id, 
                STRING_AGG(acl_id, ';' ORDER BY acl_id) AS acl
            FROM `{file_has_acl_table_id}`
            GROUP BY file_gdc_id
        """

    def make_analysis_input_file_gdc_ids_sql() -> str:
        return f"""
            SELECT apf.file_id AS file_gdc_id, 
                STRING_AGG(acif.input_file_id, ';' ORDER BY acif.input_file_id) AS analysis_input_file_gdc_ids
            FROM `{analysis_produced_file_table_id}` apf
            JOIN `{analysis_table_id}` a
                ON apf.analysis_id = a.analysis_id
            JOIN `{analysis_consumed_input_file_table_id}` acif
                ON acif.analysis_id = a.analysis_id
            GROUP BY file_gdc_id
        """

    def make_downstream_analyses_output_file_gdc_ids_sql() -> str:
        return f"""
            SELECT adff.file_id AS file_gdc_id, 
                STRING_AGG(da.output_file_id, ';' ORDER BY da.output_file_id) 
                    AS downstream_analyses__output_file_gdc_ids
            FROM `{analysis_downstream_from_file_table_id}` adff
            JOIN `{analysis_table_id}` a
                ON a.analysis_id = adff.analysis_id
            JOIN `{downstream_analysis_table_id}` da
                ON da.analysis_id = a.analysis_id
            GROUP BY file_gdc_id
            ORDER BY downstream_analyses__output_file_gdc_ids
        """

    def make_downstream_analyses_sql() -> str:
        return f"""
            WITH downstream AS (
                SELECT adff.file_id AS file_gdc_id, 
                    a.workflow_link,
                    a.workflow_type,
                    a.analysis_id
                FROM `{analysis_downstream_from_file_table_id}` adff
                JOIN `{analysis_table_id}` a
                    ON a.analysis_id = adff.analysis_id
                ORDER BY a.workflow_type
            )
            
            SELECT file_gdc_id, 
                STRING_AGG(workflow_link, ';') AS downstream_analyses__workflow_link, 
                STRING_AGG(workflow_type, ';') AS downstream_analyses__workflow_type
            FROM downstream
            GROUP BY file_gdc_id
        """

    def make_associated_entities_sql() -> str:
        return f"""
            WITH assoc_entities AS (
                SELECT file_id AS file_gdc_id,
                    entity_id,
                    entity_case_id,
                    entity_submitter_id,
                    entity_type
            FROM `{file_associated_with_entity_table_id}`
            )
            
            SELECT file_gdc_id,
                STRING_AGG(entity_id, ';') AS associated_entities__entity_gdc_id,
                STRING_AGG(entity_case_id, ';') AS associated_entities__case_gdc_id,
                STRING_AGG(entity_submitter_id, ';') AS associated_entities__entity_submitter_id,
                entity_type AS associated_entities__entity_type
            FROM assoc_entities
            GROUP BY file_gdc_id, entity_type
        """

    def make_case_project_program_sql() -> str:
        return f"""
            WITH case_proj_prog AS (
                SELECT f.file_id AS file_gdc_id, 
                    cpp.case_gdc_id,
                    cpp.project_dbgap_accession_number, 
                    cpp.project_id AS project_short_name, 
                    cpp.project_name, 
                    cpp.program_name, 
                    cpp.program_dbgap_accession_number,
                    pdt.disease_type AS project_disease_type
                FROM `{file_table_id}` f
                JOIN `{file_in_case_table_id}` fc
                    ON f.file_id = fc.file_id
                JOIN `{case_project_program_table_id}` cpp
                    ON cpp.case_gdc_id = fc.case_id
                JOIN `{case_table_id}` c
                    ON cpp.case_gdc_id = c.case_id
                LEFT OUTER JOIN `{project_disease_type_table_id}` pdt
                    ON pdt.project_id = cpp.project_id
                WHERE f.file_id NOT IN (
                    SELECT index_file_id 
                    FROM `{file_has_index_file_table_id}`
                )
            )
            
            SELECT file_gdc_id, 
                STRING_AGG(case_gdc_id, ';') AS case_gdc_id,
                project_dbgap_accession_number, 
                project_short_name, 
                project_name, 
                program_name, 
                program_dbgap_accession_number,
                project_disease_type
            FROM case_proj_prog
            GROUP BY file_gdc_id, project_dbgap_accession_number, project_short_name, project_name, program_name, 
                program_dbgap_accession_number, project_disease_type
        """

    def make_index_file_sql() -> str:
        return f"""
            SELECT fhif.file_id AS file_gdc_id, 
                fhif.index_file_id AS index_file_gdc_id, 
                f.file_name AS index_file_name, 
                f.file_size AS index_file_size
            FROM `{file_has_index_file_table_id}` fhif
            JOIN `{file_table_id}` f
                ON fhif.index_file_id = f.file_id
        """

    def add_concat_fields_to_file_records(sql: str,
                                          concat_field_list: Union[None, list[str]],
                                          filter_duplicates: bool = False):
        # bq harness with result
        # manipulate and insert all fields in concat list
        # insert all fields in insert list
        _query_result: BQQueryResult = query_and_retrieve_result(sql=sql)

        for _record in _query_result:
            _file_id = _record.get('file_gdc_id')

            if concat_field_list:
                for _field in concat_field_list:
                    file_records[_file_id][_field] = convert_concat_to_multi(value_string=_record.get(_field),
                                                                             max_length=PARAMS['MAX_CONCAT_COUNT'],
                                                                             filter_duplicates=filter_duplicates)

    logger = logging.getLogger('base_script')
    logger.info("Creating base file metadata record objects")

    file_record_result: BQQueryResult = query_and_retrieve_result(sql=make_base_file_metadata_sql())

    file_records: dict[str, dict[str, Optional[Any]]] = dict()

    for row in file_record_result:

        file_gdc_id = row.get('file_gdc_id')

        if file_gdc_id in file_records:
            logger.info(f"Duplicate record for file_gdc_id: {file_gdc_id}")

        file_records[file_gdc_id]: dict[str, Optional[Any]] = {
            'dbName': row.get('dbName'),
            'file_gdc_id': row.get('file_gdc_id'),
            'access': row.get('access'),
            'acl': None,
            'analysis_input_file_gdc_ids': None,
            'analysis_workflow_link': row.get('analysis_workflow_link'),
            'analysis_workflow_type': row.get('analysis_workflow_type'),
            'archive_gdc_id': row.get('archive_gdc_id'),
            'archive_revision': row.get('archive_revision'),
            'archive_state': row.get('archive_state'),
            'archive_submitter_id': row.get('archive_submitter_id'),
            'associated_entities__case_gdc_id': None,
            'associated_entities__entity_gdc_id': None,
            'associated_entities__entity_submitter_id': None,
            'associated_entities__entity_type': None,
            'case_gdc_id': None,
            'project_dbgap_accession_number': None,
            'project_disease_type': None,
            'project_name': None,
            'program_dbgap_accession_number': None,
            'program_name': None,
            'project_short_name': None,
            'created_datetime': row.get('created_datetime'),
            'data_category': row.get('data_category'),
            'data_format': row.get('data_format'),
            'data_type': row.get('data_type'),
            'downstream_analyses__output_file_gdc_ids': None,
            'downstream_analyses__workflow_link': None,
            'downstream_analyses__workflow_type': None,
            'experimental_strategy': row.get('experimental_strategy'),
            'file_name': row.get('file_name'),
            'file_size': row.get('file_size'),
            'file_id': row.get('file_id'),
            'index_file_gdc_id': row.get('index_file_gdc_id'),
            'index_file_name': None,
            'index_file_size': None,
            'md5sum': row.get('md5sum'),
            'platform': row.get('platform'),
            'file_state': row.get('file_state'),
            'file_submitter_id': row.get('file_submitter_id'),
            'file_type': row.get('file_type'),
            'updated_datetime': row.get('updated_datetime')
        }

    # Add acl ids to file records
    logger.info("Adding acl ids to file records")

    acl_concat_field_list: list[str] = ['acl']

    add_concat_fields_to_file_records(sql=make_acl_sql(),
                                      concat_field_list=acl_concat_field_list,
                                      filter_duplicates=True)

    # Add analysis input file ids to file records
    logger.info("Adding analysis input file ids to file records")

    analysis_input_concat_field_list: list[str] = ['analysis_input_file_gdc_ids']

    add_concat_fields_to_file_records(sql=make_analysis_input_file_gdc_ids_sql(),
                                      concat_field_list=analysis_input_concat_field_list,
                                      filter_duplicates=True)

    # Add downstream analyses output file ids to file records
    logger.info("Adding downstream analyses output file ids to file records")

    downstream_output_concat_field_list: list[str] = ['downstream_analyses__output_file_gdc_ids']

    add_concat_fields_to_file_records(sql=make_downstream_analyses_output_file_gdc_ids_sql(),
                                      concat_field_list=downstream_output_concat_field_list,
                                      filter_duplicates=True)

    # Add downstream analyses fields to file records
    logger.info("Adding downstream analyses fields to file records")

    downstream_analyses_concat_field_list: list[str] = ['downstream_analyses__workflow_link',
                                                        'downstream_analyses__workflow_type']

    add_concat_fields_to_file_records(sql=make_downstream_analyses_sql(),
                                      concat_field_list=downstream_analyses_concat_field_list,
                                      filter_duplicates=True)

    # Add associated entity fields to file records
    logger.info("Adding associated entity fields to file records")

    associated_entities_concat_field_list: list[str] = ['associated_entities__case_gdc_id',
                                                        'associated_entities__entity_gdc_id',
                                                        'associated_entities__entity_submitter_id']

    query_result: BQQueryResult = query_and_retrieve_result(sql=make_associated_entities_sql())

    for record in query_result:
        file_id = record.get('file_gdc_id')

        for field in associated_entities_concat_field_list:
            file_records[file_id][field] = convert_concat_to_multi(record.get(field),
                                                                   max_length=PARAMS['MAX_CONCAT_COUNT'],
                                                                   filter_duplicates=True)

        associated_entities__entity_type = record.get("associated_entities__entity_type")
        # old table doesn't concatenate duplicate ids, so this eliminates any
        file_records[file_id]['associated_entities__entity_type'] \
            = ";".join(set(associated_entities__entity_type.split(';')))

    # Add case, project, program fields to file records
    logger.info("Adding case, project, program fields to file records")

    case_project_program_result: BQQueryResult = query_and_retrieve_result(sql=make_case_project_program_sql())

    for row in case_project_program_result:
        file_gdc_id = row.get('file_gdc_id')

        file_records[file_gdc_id]['project_dbgap_accession_number'] = row.get('project_dbgap_accession_number')
        file_records[file_gdc_id]['program_dbgap_accession_number'] = row.get('program_dbgap_accession_number')
        file_records[file_gdc_id]['project_short_name'] = row.get('project_short_name')
        file_records[file_gdc_id]['project_name'] = row.get('project_name')
        file_records[file_gdc_id]['program_name'] = row.get('program_name')
        file_records[file_gdc_id]['project_disease_type'] = row.get('project_disease_type')

        if row.get('case_gdc_id'):
            file_records[file_gdc_id]['case_gdc_id'] = convert_concat_to_multi(row.get('case_gdc_id'),
                                                                               max_length=PARAMS['MAX_CONCAT_COUNT'])

    del case_project_program_result

    # Add index files to file records
    logger.info("Adding index files to file records")

    index_file_result: BQQueryResult = query_and_retrieve_result(sql=make_index_file_sql())

    for row in index_file_result:
        file_gdc_id = row.get('file_gdc_id')
        file_records[file_gdc_id]["index_file_gdc_id"] = row.get('index_file_gdc_id')
        file_records[file_gdc_id]["index_file_name"] = row.get('index_file_name')
        file_records[file_gdc_id]["index_file_size"] = row.get('index_file_size')

    del index_file_result

    # convert into a list of dict objects--this is the form needed to create the jsonl file
    logger.info("Done! File records merged.")
    return list(file_records.values())


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

    if 'create_and_upload_file_metadata_jsonl' in steps:
        logger.info("Entering create_and_upload_file_metadata_jsonl")

        file_record_list = create_file_metadata_dict()

        normalized_file_record_list = normalize_flat_json_values(file_record_list)

        write_list_to_jsonl_and_upload(PARAMS, 'file', normalized_file_record_list)
        write_list_to_jsonl_and_upload(PARAMS, 'file_raw', file_record_list)

        create_and_upload_schema_for_json(PARAMS,
                                          record_list=normalized_file_record_list,
                                          table_name='file',
                                          include_release=True)

    if 'create_table' in steps:
        logger.info("Entering create_table")

        # Download schema file from Google Cloud bucket
        table_schema = retrieve_bq_schema_object(PARAMS, table_name='file', include_release=True)

        # Load jsonl data into BigQuery table
        create_and_load_table_from_jsonl(PARAMS,
                                         jsonl_file=f"file_{PARAMS['RELEASE']}.jsonl",
                                         table_id=create_metadata_table_id(PARAMS, PARAMS['TABLE_NAME']),
                                         schema=table_schema)

        update_table_schema_from_generic(params=PARAMS, table_id=create_metadata_table_id(PARAMS, PARAMS['TABLE_NAME']))

    end_time = time.time()

    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)

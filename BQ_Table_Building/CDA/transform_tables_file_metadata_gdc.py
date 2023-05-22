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
import time
from typing import Any, Union, Optional

from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator

from BQ_Table_Building.PDC.pdc_utils import build_table_from_jsonl
from common_etl.support import bq_harness_with_result
from common_etl.utils import format_seconds, normalize_flat_json_values, write_list_to_jsonl_and_upload, \
    create_and_upload_schema_for_json, create_and_load_table_from_jsonl, retrieve_bq_schema_object

BQHarnessResult = Union[None, RowIterator, _EmptyRowIterator]


def convert_concat_to_multi(value_string: str, max_length: int = 8) -> str:
    string_length: int = len(value_string.split(';'))

    if string_length > max_length:
        return 'multi'
    else:
        return value_string


def create_dev_table_id(bq_params, release, table_name) -> str:
    working_project: str = bq_params['WORKING_PROJECT']
    working_dataset: str = bq_params['WORKING_DATASET']

    return f"`{working_project}.{working_dataset}.{release}_{table_name}`"


def create_file_metadata_dict(bq_params, release) -> list[dict[str, Optional[Any]]]:
    """

    :param bq_params:
    :param release:
    :return:
    """

    analysis_table_id: str = create_dev_table_id(bq_params, release, 'analysis')
    analysis_consumed_input_file_table_id: str = create_dev_table_id(bq_params, release, 'analysis_consumed_input_file')
    analysis_downstream_from_file_table_id: str = create_dev_table_id(bq_params,
                                                                      release,
                                                                      'analysis_downstream_from_file')
    analysis_produced_file_table_id: str = create_dev_table_id(bq_params, release, 'analysis_produced_file')
    archive_table_id: str = create_dev_table_id(bq_params, release, 'archive')
    case_project_program_table_id: str = create_dev_table_id(bq_params, release, "case_project_program")
    case_table_id: str = create_dev_table_id(bq_params, release, "case")
    downstream_analysis_table_id: str = create_dev_table_id(bq_params,
                                                            release,
                                                            "downstream_analysis_produced_output_file")
    file_table_id: str = create_dev_table_id(bq_params, release, 'file')
    file_associated_with_entity_table_id: str = create_dev_table_id(bq_params, release, 'file_associated_with_entity')
    file_has_acl_table_id: str = create_dev_table_id(bq_params, release, 'file_has_acl')
    file_has_index_file_table_id: str = create_dev_table_id(bq_params, release, 'file_has_index_file')
    file_in_archive_table_id: str = create_dev_table_id(bq_params, release, 'file_in_archive')
    file_in_case_table_id: str = create_dev_table_id(bq_params, release, 'file_in_case')

    def make_base_file_metadata_sql() -> str:
        return f"""
        SELECT 'active' AS dbName,
        f.file_id AS file_gdc_id,
        f.access,
        # acl, # separate join
        # analysis_input_file_gdc_ids, # separate join
        an.workflow_link AS analysis_workflow_link,
        an.workflow_type AS analysis_workflow_type,
        ar.archive_id AS archive_gdc_id,
        ar.revision AS archive_revision,
        ar.state AS archive_state,
        ar.submitter_id AS archive_submitter_id,
        # associated_entities__case_gdc_id,
        # associated_entities__entity_gdc_id,
        # associated_entities__entity_submitter_id,
        # associated_entities__entity_type,
        # cpp.case_gdc_id,
        # cpp.project_dbgap_accession_number,
        # project_disease_type, # separate join
        # cpp.project_name,
        # cpp.program_dbgap_accession_number,
        # cpp.program_name, 
        # cpp.project_id AS project_short_name,
        f.created_datetime,
        f.data_category,
        f.data_format,
        f.data_type,
        # downstream_analyses__output_file_gdc_ids,
        # downstream_analyses__workflow_link,
        # downstream_analyses__workflow_type,
        f.experimental_strategy,
        f.file_name,
        f.file_size,
        f.file_id, # do we actually need this? There aren't two values for file_id and file_gdc_id
        fhif.index_file_id AS index_file_gdc_id,
        # index_file_name, # separate join
        # index_file_size, # separate join
        f.md5sum,
        f.platform,
        f.state AS file_state,
        f.submitter_id AS file_submitter_id,
        f.type AS file_type,
        f.updated_datetime
        
        FROM {file_table_id} f
        LEFT OUTER JOIN {analysis_produced_file_table_id} apf
            ON apf.file_id = f.file_id
        LEFT OUTER JOIN {analysis_table_id} an
            ON apf.analysis_id = an.analysis_id
        LEFT OUTER JOIN {file_in_archive_table_id} fia
            ON fia.file_id = f.file_id
        LEFT OUTER JOIN {archive_table_id} ar
            ON ar.archive_id = fia.archive_id
        LEFT OUTER JOIN {file_has_index_file_table_id} fhif
            ON fhif.file_id = f.file_id
        """

    def make_acl_sql() -> str:
        return f"""
        SELECT file_id AS file_gdc_id, 
            STRING_AGG(acl_id, ';') AS acl
        FROM {file_has_acl_table_id}
        GROUP BY file_gdc_id
        """

    def make_analysis_input_file_gdc_ids_sql() -> str:
        return f"""
        SELECT apf.file_id AS file_gdc_id, 
            STRING_AGG(acif.input_file_id, ';') AS analysis_input_file_gdc_ids
        FROM {analysis_produced_file_table_id} apf
        JOIN {analysis_table_id} a
            ON apf.analysis_id = a.analysis_id
        JOIN {analysis_consumed_input_file_table_id} acif
            ON acif.analysis_id = a.analysis_id
        GROUP BY file_gdc_id
        """

    def make_downstream_analyses_output_file_gdc_ids_sql() -> str:
        return f"""
        SELECT adff.file_id AS file_gdc_id, 
            STRING_AGG(da.output_file_id, ';') AS downstream_analyses__output_file_gdc_ids
        FROM {analysis_downstream_from_file_table_id} adff
        JOIN {analysis_table_id} a
            ON a.analysis_id = adff.analysis_id
        JOIN {downstream_analysis_table_id} da
            ON da.analysis_id = a.analysis_id
        GROUP BY file_gdc_id
        """

    def make_downstream_analyses_sql() -> str:
        return f"""
        SELECT adff.file_id AS file_gdc_id, 
            STRING_AGG(a.workflow_link) AS downstream_analyses__workflow_link, 
            STRING_AGG(a.workflow_type) AS downstream_analyses__workflow_type
        FROM {analysis_downstream_from_file_table_id} adff
        JOIN {analysis_table_id} a
            ON a.analysis_id = adff.analysis_id
        GROUP BY file_gdc_id
        """

    def make_associated_entities_sql() -> str:
        return f"""
        SELECT file_id AS file_gdc_id,
            STRING_AGG(entity_id, ';') AS associated_entities__entity_gdc_id,
            STRING_AGG(entity_case_id, ';') AS associated_entities__case_gdc_id,
            STRING_AGG(entity_submitter_id, ';') AS associated_entities__entity_submitter_id,
            entity_type AS associated_entities__entity_type
        FROM {file_associated_with_entity_table_id}
        GROUP BY file_gdc_id, entity_type
        """

    def make_case_project_program_sql() -> str:
        return f"""
        SELECT f.file_id AS file_gdc_id, 
            STRING_AGG(cpp.case_gdc_id, ';'),
            cpp.project_dbgap_accession_number, 
            cpp.project_id AS project_short_name, 
            cpp.project_name, 
            cpp.program_name, 
            cpp.program_dbgap_accession_number,
            c.disease_type AS project_disease_type
        FROM {file_table_id} f
        JOIN {file_in_case_table_id} fc
            ON f.file_id = fc.file_id
        JOIN {case_project_program_table_id} cpp
            ON cpp.case_gdc_id = fc.case_id
        JOIN {case_table_id} c
            ON cpp.case_gdc_id = c.case_id
        GROUP BY file_gdc_id, cpp.project_dbgap_accession_number, project_short_name, cpp.project_name, 
            cpp.program_name, cpp.program_dbgap_accession_number, project_disease_type
        """

    def make_index_file_sql() -> str:
        return f"""
        SELECT fhif.file_id AS file_gdc_id, 
        fhif.index_file_id AS index_file_gdc_id, 
        f.file_name AS index_file_name, 
        f.file_size AS index_file_size
        FROM {file_has_index_file_table_id} fhif
        JOIN {file_table_id} f
            ON fhif.index_file_id = f.file_id
        """

    def add_concat_fields_to_file_records(sql: str, concat_field_list: Union[None, list[str]]):
        # bq harness with result
        # manipulate and insert all fields in concat list
        # insert all fields in insert list
        query_result: BQHarnessResult = bq_harness_with_result(sql=sql, do_batch=False, verbose=False)

        for record in query_result:
            file_id: str = record.get('file_gdc_id')

            if concat_field_list:
                for field in concat_field_list:
                    file_records[file_id][field] = convert_concat_to_multi((record.get(field)))

    print("\nCreating base file metadata record objects")

    file_record_result: BQHarnessResult = bq_harness_with_result(sql=make_base_file_metadata_sql(),
                                                                 do_batch=False,
                                                                 verbose=False)

    file_records: dict[str, dict[str, Optional[Any]]] = dict()

    for row in file_record_result:

        file_gdc_id: str = row.get('file_gdc_id')

        if file_gdc_id in file_records:
            print(f"Duplicate record for file_gdc_id: {file_gdc_id}")

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
    print("Adding acl ids to file records")

    acl_concat_field_list: list[str] = ['acl']

    add_concat_fields_to_file_records(sql=make_acl_sql(), concat_field_list=acl_concat_field_list)

    # Add analysis input file ids to file records
    print("Adding analysis input file ids to file records")

    analysis_input_concat_field_list: list[str] = ['analysis_input_file_gdc_ids']

    add_concat_fields_to_file_records(sql=make_analysis_input_file_gdc_ids_sql(),
                                      concat_field_list=analysis_input_concat_field_list)

    # Add downstream analyses output file ids to file records
    print("Adding downstream analyses output file ids to file records")

    downstream_output_concat_field_list: list[str] = ['downstream_analyses__output_file_gdc_ids']

    add_concat_fields_to_file_records(sql=make_downstream_analyses_output_file_gdc_ids_sql(),
                                      concat_field_list=downstream_output_concat_field_list)

    # Add downstream analyses fields to file records
    print("Adding downstream analyses fields to file records")

    downstream_analyses_concat_field_list: list[str] = ['downstream_analyses__workflow_link',
                                                        'downstream_analyses__workflow_type']

    add_concat_fields_to_file_records(sql=make_downstream_analyses_sql(),
                                      concat_field_list=downstream_analyses_concat_field_list)

    # Add associated entity fields to file records
    print("Adding associated entity fields to file records")

    associated_entities_concat_field_list: list[str] = ['associated_entities__entity_gdc_id',
                                                        'associated_entities__entity_submitter_id']

    query_result: BQHarnessResult = bq_harness_with_result(sql=make_associated_entities_sql(),
                                                           do_batch=False,
                                                           verbose=False)

    for record in query_result:
        file_id: str = record.get('file_gdc_id')

        for field in associated_entities_concat_field_list:
            file_records[file_id][field] = convert_concat_to_multi(record.get(field))

        associated_entities__case_gdc_ids = record.get("associated_entities__case_gdc_id")
        # old table doesn't concatenate duplicate ids, so this eliminates any
        file_records[file_id]['associated_entities__case_gdc_id'] \
            = ";".join(set(associated_entities__case_gdc_ids.split(';')))

        associated_entities__entity_type = record.get("associated_entities__entity_type")
        # old table doesn't concatenate duplicate ids, so this eliminates any
        file_records[file_id]['associated_entities__entity_type'] \
            = ";".join(set(associated_entities__entity_type.split(';')))

    # Add case, project, program fields to file records
    print("Adding case, project, program fields to file records")

    case_project_program_result: BQHarnessResult = bq_harness_with_result(sql=make_case_project_program_sql(),
                                                                          do_batch=False,
                                                                          verbose=False)

    for row in case_project_program_result:
        file_gdc_id = row.get('file_gdc_id')

        file_records[file_gdc_id]['project_dbgap_accession_number'] = row.get('project_dbgap_accession_number')
        file_records[file_gdc_id]['program_dbgap_accession_number'] = row.get('program_dbgap_accession_number')
        file_records[file_gdc_id]['project_short_name'] = row.get('project_short_name')
        file_records[file_gdc_id]['project_name'] = row.get('project_name')
        file_records[file_gdc_id]['program_name'] = row.get('program_name')
        file_records[file_gdc_id]['project_disease_type'] = row.get('project_disease_type')

        if row.get('case_gdc_id'):
            file_records[file_gdc_id]['case_gdc_id'] = convert_concat_to_multi(row.get('case_gdc_id'))

    del case_project_program_result

    # Add index files to file records
    print("Adding index files to file records")

    index_file_result: BQHarnessResult = bq_harness_with_result(sql=make_index_file_sql(),
                                                                do_batch=False,
                                                                verbose=False)

    for row in index_file_result:
        file_gdc_id = row.get('file_gdc_id')
        file_records[file_gdc_id]["index_file_gdc_id"] = row.get('index_file_gdc_id')
        file_records[file_gdc_id]["index_file_name"] = row.get('index_file_name')
        file_records[file_gdc_id]["index_file_size"] = row.get('index_file_size')

    del index_file_result

    # convert into a list of dict objects--this is the form needed to create the jsonl file
    print("Done! File records merged.\n")
    return list(file_records.values())


def main(args):
    bq_params = {
        "WORKING_PROJECT": "isb-project-zero",
        "WORKING_DATASET": "cda_gdc_test",
        "WORKING_BUCKET": "next-gen-etl-scratch",
        "WORKING_BUCKET_DIR": "law/etl/cda_gdc_test",
        'SCRATCH_DIR': 'scratch',
        'LOCATION': 'us'
    }
    api_params = {
        'RELEASE': '2023_03',
    }
    steps = {
        'create_and_upload_file_metadata_json',
        'create_table'
    }

    start_time: float = time.time()

    if 'create_and_upload_file_metadata_json' in steps:
        file_record_list = create_file_metadata_dict(bq_params, api_params['RELEASE'])

        count_dict = dict()

        for record in file_record_list:
            for key in record.keys():
                if key not in count_dict:
                    count_dict[key] = 0

                if record[key] is not None:
                    count_dict[key] += 1

        for key in count_dict.keys():
            print(f"{key}: {count_dict[key]} non-null values")

        normalized_file_record_list = normalize_flat_json_values(file_record_list)
        write_list_to_jsonl_and_upload(api_params, bq_params, 'file', normalized_file_record_list)
        write_list_to_jsonl_and_upload(api_params, bq_params, 'file_raw', file_record_list)

        create_and_upload_schema_for_json(api_params,
                                          bq_params,
                                          record_list=normalized_file_record_list,
                                          table_name='file',
                                          include_release=True)

    if 'create_table' in steps:
        # Download schema file from Google Cloud bucket
        table_schema = retrieve_bq_schema_object(api_params, bq_params,
                                                 table_name='file',
                                                 include_release=True)

        # Load jsonl data into BigQuery table
        create_and_load_table_from_jsonl(bq_params,
                                         jsonl_file='file_2023_03.jsonl',
                                         table_id='isb-project-zero.cda_gdc_test.file_metadata_2023_03',
                                         schema=table_schema)

    end_time: float = time.time()

    print(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)

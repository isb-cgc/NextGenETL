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
from typing import Any, Union, Optional

from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator

BQHarnessResult = Union[None, RowIterator, _EmptyRowIterator]


def make_old_gdc_file_metadata_query() -> str:
    return f"""
    SELECT * 
    FROM `isb-cgc-bq.GDC_case_file_metadata.fileData_active_current`
    """


def make_new_gdc_file_metadata_query() -> str:
    return f"""
    SELECT * 
    FROM `isb-project-zero.cda_gdc_test.file_metadata_2023_03`
    """


def make_compare_table_column_query(table_id_1: str, table_id_2: str, column_name: str) -> str:
    return f"""
    (
        SELECT file_gdc_id, {column_name}
        FROM `{table_id_1}`
        EXCEPT DISTINCT 
        SELECT file_gdc_id, {column_name}
        FROM `{table_id_2}`
    )
    
    UNION ALL
    
    (
        SELECT file_gdc_id, {column_name}
        FROM `{table_id_2}`
        EXCEPT DISTINCT 
        SELECT file_gdc_id, {column_name}
        FROM `{table_id_1}`
    )
    """


def compare_non_concat_table_columns(old_table_id: str, new_table_id: str, columns: list[str]):
    for column in columns:
        column_comparison_query = make_compare_table_column_query(old_table_id, new_table_id, column)

        result = bq_harness_with_result(sql=column_comparison_query, do_batch=False, verbose=False)

        if result.total_rows > 0:
            print(f"Found mismatched data for {column}.")
            print(f"{result.total_rows} records do not match between old and new tables.")
        else:
            print(f"Old and new table records match for {column} column. Excellent.")


def make_compare_two_tables_query() -> str:
    return f"""
    (
        SELECT dbName,
            file_gdc_id, 
            access,
            analysis_workflow_link,
            analysis_workflow_type,
            archive_gdc_id,
            archive_revision,
            archive_state,
            archive_submitter_id,
            associated_entities__entity_type,
            case_gdc_id,
            project_dbgap_accession_number,
            project_disease_type,
            project_name,
            program_dbgap_accession_number,
            program_name, 
            project_short_name,
            created_datetime,
            data_category,
            data_format,
            data_type,
            downstream_analyses__workflow_link,
            downstream_analyses__workflow_type,
            experimental_strategy,
            file_name,
            file_size,
            file_id,
            index_file_gdc_id,
            index_file_name,
            index_file_size,
            md5sum,
            platform,
            file_state,
            file_submitter_id,
            file_type,
            updated_datetime
        FROM `isb-cgc-bq.GDC_case_file_metadata.fileData_active_current`
        EXCEPT DISTINCT 
        SELECT dbName,
            file_gdc_id, 
            access,
            analysis_workflow_link,
            analysis_workflow_type,
            archive_gdc_id,
            archive_revision,
            archive_state,
            archive_submitter_id,
            associated_entities__entity_type,
            case_gdc_id,
            project_dbgap_accession_number,
            project_disease_type,
            project_name,
            program_dbgap_accession_number,
            program_name, 
            project_short_name,
            created_datetime,
            data_category,
            data_format,
            data_type,
            downstream_analyses__workflow_link,
            downstream_analyses__workflow_type,
            experimental_strategy,
            file_name,
            file_size,
            file_id,
            index_file_gdc_id,
            index_file_name,
            index_file_size,
            md5sum,
            platform,
            file_state,
            file_submitter_id,
            file_type,
            updated_datetime
        FROM `isb-project-zero.cda_gdc_test.file_metadata_2023_03`
    )
    
    UNION ALL
    
    (
        SELECT dbName,
            file_gdc_id, 
            access,
            analysis_workflow_link,
            analysis_workflow_type,
            archive_gdc_id,
            archive_revision,
            archive_state,
            archive_submitter_id,
            associated_entities__entity_type,
            case_gdc_id,
            project_dbgap_accession_number,
            project_disease_type,
            project_name,
            program_dbgap_accession_number,
            program_name, 
            project_short_name,
            created_datetime,
            data_category,
            data_format,
            data_type,
            downstream_analyses__workflow_link,
            downstream_analyses__workflow_type,
            experimental_strategy,
            file_name,
            file_size,
            file_id,
            index_file_gdc_id,
            index_file_name,
            index_file_size,
            md5sum,
            platform,
            file_state,
            file_submitter_id,
            file_type,
            updated_datetime
        FROM `isb-project-zero.cda_gdc_test.file_metadata_2023_03`
        EXCEPT DISTINCT 
        SELECT dbName,
            file_gdc_id, 
            access,
            analysis_workflow_link,
            analysis_workflow_type,
            archive_gdc_id,
            archive_revision,
            archive_state,
            archive_submitter_id,
            associated_entities__entity_type,
            case_gdc_id,
            project_dbgap_accession_number,
            project_disease_type,
            project_name,
            program_dbgap_accession_number,
            program_name, 
            project_short_name,
            created_datetime,
            data_category,
            data_format,
            data_type,
            downstream_analyses__workflow_link,
            downstream_analyses__workflow_type,
            experimental_strategy,
            file_name,
            file_size,
            file_id,
            index_file_gdc_id,
            index_file_name,
            index_file_size,
            md5sum,
            platform,
            file_state,
            file_submitter_id,
            file_type,
            updated_datetime
        FROM `isb-cgc-bq.GDC_case_file_metadata.fileData_active_current`
    )
    """


def create_file_metadata_dict(sql: str) -> dict[str, dict[str, str]]:
    file_result: BQHarnessResult = bq_harness_with_result(sql=sql, do_batch=False, verbose=False)

    file_metadata_dict: dict = dict()

    concat_fields: set = {'acl',
                          'analysis_input_file_gdc_ids',
                          'downstream_analyses__output_file_gdc_ids',
                          'associated_entities__entity_gdc_id',
                          'associated_entities__entity_submitter_id'}

    count = 0

    for row in file_result:
        file_id: str = row.get('file_gdc_id')

        record_dict: dict = dict()

        keys: list[str] = row.keys()

        for key in keys:
            if key not in concat_fields:
                record_dict[key] = row[key]

        acl: str = row.get('acl')
        if acl:
            acl_set: set[str] = set(acl.split(';'))
        record_dict['acl'] = acl_set

        analysis_input_file_gdc_ids: str = row.get('analysis_input_file_gdc_ids')
        if analysis_input_file_gdc_ids:
            analysis_input_file_gdc_ids_set = set(analysis_input_file_gdc_ids.split(';'))
            record_dict['analysis_input_file_gdc_ids'] = analysis_input_file_gdc_ids_set

        downstream_analyses__output_file_gdc_ids: set = row.get('downstream_analyses__output_file_gdc_ids')
        if downstream_analyses__output_file_gdc_ids:
            downstream_analyses__output_file_gdc_ids_set = set(downstream_analyses__output_file_gdc_ids.split(';'))
            record_dict['downstream_analyses__output_file_gdc_ids'] = downstream_analyses__output_file_gdc_ids_set

        associated_entities__entity_gdc_id = row.get('associated_entities__entity_gdc_id')
        if associated_entities__entity_gdc_id:
            associated_entities__entity_gdc_id_list = associated_entities__entity_gdc_id.split(';')
        else:
            associated_entities__entity_gdc_id_list = list()

        associated_entities__entity_submitter_id = row.get('associated_entities__entity_submitter_id')
        if associated_entities__entity_submitter_id:
            associated_entities__entity_submitter_id_list = associated_entities__entity_submitter_id.split(';')
        else:
            associated_entities__entity_submitter_id_list = list()

        if len(associated_entities__entity_gdc_id_list) == len(associated_entities__entity_submitter_id_list):
            associated_entities_list = list()

            entity_count = len(associated_entities__entity_submitter_id_list)

            for i in range(entity_count):
                associated_entity_dict = {
                    "associated_entities__entity_gdc_id": associated_entities__entity_gdc_id_list[i],
                    "associated_entities__entity_submitter_id": associated_entities__entity_submitter_id_list[i],
                }

                associated_entities_list.append(associated_entity_dict)

            record_dict['associated_entities'] = associated_entities_list

        else:
            print(f"ERROR: associated_entities lengths are not the same for {file_id}.")
            print(f"{row}")

        file_metadata_dict[file_id] = record_dict

        count += 1

        if count % 100000 == 0:
            print(f"{count} rows processed.")

    return file_metadata_dict


def main(args):
    non_concat_columns = [
        "dbName",
        "access",
        "analysis_workflow_link",
        "analysis_workflow_type",
        "archive_gdc_id",
        "archive_revision",
        "archive_state",
        "archive_submitter_id",
        "associated_entities__entity_type",
        "project_dbgap_accession_number",
        # "project_disease_type", not yet correctly in data
        "project_name",
        "program_dbgap_accession_number",
        "program_name",
        "project_short_name",
        "created_datetime",
        "data_category",
        "data_format",
        "data_type",
        "downstream_analyses__workflow_link",
        "downstream_analyses__workflow_type",
        "experimental_strategy",
        "file_name",
        "file_size",
        "file_id",
        "index_file_gdc_id",
        # "index_file_name", not yet in data
        # "index_file_size", not yet in data
        "md5sum",
        "platform",
        "file_state",
        "file_submitter_id",
        "file_type",
        "updated_datetime"
    ]

    concat_columns = [
        'acl',
        'analysis_input_file_gdc_ids',
        'downstream_analyses__output_file_gdc_ids',
        'associated_entities__entity_gdc_id',
        'associated_entities__entity_submitter_id',
        'gdc_case_id'
    ]

    old_table_id = 'isb-cgc-bq.GDC_case_file_metadata.fileData_active_current'
    new_table_id = 'isb-project-zero.cda_gdc_test.file_metadata_2023_03'

    compare_non_concat_table_columns(old_table_id, new_table_id, non_concat_columns)


if __name__ == "__main__":
    main(sys.argv)

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


def make_concat_column_query(table_id) -> str:
    return f"""
    SELECT file_gdc_id, 
        acl, 
        analysis_input_file_gdc_ids, 
        downstream_analyses__output_file_gdc_ids, 
        associated_entities__entity_gdc_id, 
        associated_entities__entity_submitter_id, 
        gdc_case_id
    FROM `{table_id}`
    """


def compare_concat_columns(old_table_id: str, new_table_id: str, concat_columns):
    def make_records_dict(query: str) -> dict[str, dict[str, str]]:
        result = bq_harness_with_result(sql=query, do_batch=False, verbose=False)

        records_dict = dict()

        for record in result:
            file_gdc_id = record.get('file_gdc_id')

            record_dict = dict()

            for column in concat_columns:
                record_dict[column] = record.get(column)

            records_dict[file_gdc_id] = record_dict

        return records_dict

    old_records_dict = make_records_dict(query=make_concat_column_query(old_table_id))
    new_records_dict = make_records_dict(query=make_concat_column_query(new_table_id))

    count = 0

    for file_id in old_records_dict.keys():
        for column in concat_columns:
            old_column_value = old_records_dict[file_id][column]
            new_column_value = new_records_dict[file_id][column]
            old_column_value_set = set(old_column_value.split(';'))
            new_column_value_set = set(new_column_value.split(';'))

            missing_values = old_column_value_set ^ new_column_value_set

            if len(missing_values) > 0:
                print(f'file id {file_id} value mismatch for {column}.')
                print(f'old column values: {old_column_value} new column values: {new_column_value}')

        count += 1

        if count % 50000 == 0:
            print(f"{count} records evaluated!")


def make_compare_table_column_query(old_table_id: str, new_table_id: str, column_name: str) -> str:
    return f"""
    (
        SELECT file_gdc_id, {column_name}
        FROM `{old_table_id}`
        EXCEPT DISTINCT 
        SELECT file_gdc_id, {column_name}
        FROM `{new_table_id}`
    )
    
    UNION ALL
    
    (
        SELECT file_gdc_id, {column_name}
        FROM `{new_table_id}`
        EXCEPT DISTINCT 
        SELECT file_gdc_id, {column_name}
        FROM `{old_table_id}`
    )
    """


def compare_non_concat_table_columns(old_table_id: str, new_table_id: str, columns: list[str]):
    for column in columns:
        column_comparison_query = make_compare_table_column_query(old_table_id, new_table_id, column)

        result = bq_harness_with_result(sql=column_comparison_query, do_batch=False, verbose=False)

        if result.total_rows > 0:
            print(f"Found mismatched data for {column}.")
            print(f"{result.total_rows} total records do not match in old and new tables.")
        else:
            print(f"{column} column matches in published and new tables!")


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

    print("\nComparing non-concatenated columns!\n")
    compare_non_concat_table_columns(old_table_id, new_table_id, non_concat_columns)

    print("\nComparing concatenated columns!\n")
    compare_concat_columns(old_table_id, new_table_id, concat_columns)


if __name__ == "__main__":
    main(sys.argv)

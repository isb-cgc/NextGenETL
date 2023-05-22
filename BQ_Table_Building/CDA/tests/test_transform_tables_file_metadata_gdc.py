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

        if count % 10000 == 0:
            print(f"{count} rows processed.")

    return file_metadata_dict


def main(args):
    print("Creating old file metadata dict!")
    old_file_metadata_dict = create_file_metadata_dict(sql=make_old_gdc_file_metadata_query())
    print("Creating new file metadata dict!")
    new_file_metadata_dict = create_file_metadata_dict(sql=make_new_gdc_file_metadata_query())

    old_file_key_set = set(old_file_metadata_dict.keys())
    new_file_key_set = set(new_file_metadata_dict.keys())

    old_new_keys_symmetric_difference = old_file_key_set ^ new_file_key_set

    print("\nComparing old and new table file ids...")

    if len(old_new_keys_symmetric_difference) > 0:
        old_missing_keys = new_file_key_set - old_file_key_set
        new_missing_keys = old_file_key_set - new_file_key_set
        print(f"File id differences in old and new table.")
        print(f"File ids missing from new table: {new_missing_keys}")
        print(f"File ids missing from old table: {old_missing_keys}")
        print(f"Ending tests.")
        exit(-1)
    else:
        print("File ids are identical in both tables!\n")

    # to test:
    # same length, same file ids, same contents within each file id field.


if __name__ == "__main__":
    main(sys.argv)

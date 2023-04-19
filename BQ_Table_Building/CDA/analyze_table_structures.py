import csv
import sys

from common_etl.support import bq_harness_with_result
from common_etl.utils import download_from_bucket, get_scratch_fp


# tables, column names, data type
# columns appearing in multiple tables

def retrieve_dataset_columns(bq_params, version=None):
    table_column_query = f"""
        SELECT table_name, column_name
        FROM `{bq_params['WORKING_PROJECT']}`.{bq_params['WORKING_DATASET']}.INFORMATION_SCHEMA.COLUMNS
    """

    table_columns = bq_harness_with_result(sql=table_column_query, do_batch=False, verbose=False)

    if not version:
        return table_columns

    filtered_table_columns = list()

    for column_data in table_columns:
        table_version = column_data[0][:7]

        if table_version == version:
            filtered_table_columns.append(column_data)

    return filtered_table_columns


def print_tables_columns_data_types(table_columns):
    for column_data in table_columns:
        table_name = column_data[0][8:]
        column_name = column_data[1]

        print(f"{table_name}\t{column_name}")


def get_columns_in_tables(table_columns, multiple_only=False):
    column_dict = dict()

    for column_data in table_columns:
        table_name = column_data[0][8:]
        column_name = column_data[1]

        if column_name not in column_dict:
            column_dict[column_name] = list()

        column_dict[column_name].append(table_name)

    for column in sorted(column_dict.keys()):
        if multiple_only:
            multiple_column_dict = dict()
            if len(column_dict[column]) > 1:
                print(f"{column}\t{column_dict[column]}")
                multiple_column_dict[column] = column_dict[column]
        else:
            print(f"{column}\t{column_dict[column]}")

    if multiple_only:
        return multiple_column_dict
    else:
        return column_dict


def import_current_fields(bq_params, filename, bucket_path):
    download_from_bucket(bq_params, filename, bucket_path)

    with open(get_scratch_fp(bq_params, filename), mode="r") as fields_file:
        tsv_reader = csv.reader(fields_file, delimiter="\t")

        field_dict = dict()

        for row in tsv_reader:
            field_name = row[0]
            field_group = row[1]
            workflow = row[2]

            if field_name not in field_dict:
                field_dict[field_name] = {
                    "endpoint": list(),
                    "workflows": list()
                }

            field_dict[field_name]["endpoint"].append(field_group)
            field_dict[field_name]["workflows"].append(workflow)

        return field_dict


def print_field_column_diff(bq_params, table_columns, bucket_path, field_file_name):
    columns_dict = get_columns_in_tables(table_columns)

    field_dict = import_current_fields(bq_params, filename=field_file_name, bucket_path=bucket_path)

    columns = set(columns_dict.keys())
    fields = set(field_dict.keys())

    columns_not_found = columns - fields
    fields_not_found = fields - columns

    print(f"\nColumns not found:")
    for column in sorted(columns_not_found):
        print(f"{column}\t{columns_dict[column]}")

    print(f"\nFields not found:")
    for field in sorted(fields_not_found):
        print(f"{field}\t{field_dict[field]}")


def count_non_null_column_values(bq_params, table_columns):

    for table_name, column_name in table_columns:
        sql_query = f"""
            SELECT COUNTIF({column_name} IS NOT NULL) * 1.0 / COUNT(*) AS occurence_ratio
            FROM `{bq_params['WORKING_PROJECT']}`.{bq_params['WORKING_DATASET']}.{table_name}
        """

        ratio_result = bq_harness_with_result(sql_query, do_batch=False, verbose=False)

        for ratio_row in ratio_result:
            ratio = ratio_row[0]
            break

        percentage = round((ratio * 100), 4)

        print(f"{table_name}\t{column_name}\t{percentage}%")


def main(args):
    bq_params = {
        "SCRATCH_DIR": "scratch",
        "WORKING_BUCKET": "next-gen-etl-scratch",
        "WORKING_PROJECT": "isb-project-zero",
        "WORKING_DATASET": "cda_gdc_test"
    }
    version = '2023_03'
    bucket_path = 'law/etl/analysis_files'

    table_columns = retrieve_dataset_columns(bq_params, version)

    count_non_null_column_values(bq_params, table_columns)

    # column_dict = get_columns_in_tables(table_columns, multiple_only=True)

    # print_field_column_diff(bq_params, table_columns, bucket_path, field_file_name='pdc_current_fields.tsv')


if __name__ == "__main__":
    main(sys.argv)

import sys

from common_etl.support import bq_harness_with_result


# tables, column names, data type
# columns appearing in multiple tables

def retrieve_dataset_columns(project_name, dataset_name):
    table_column_query = f"""
        SELECT table_name, column_name, data_type
        FROM `{project_name}`.{dataset_name}.INFORMATION_SCHEMA.COLUMNS
    """

    return bq_harness_with_result(sql=table_column_query, do_batch=False, verbose=False)


def print_tables_columns_data_types(table_columns):
    for column_data in table_columns:
        table_name = column_data[0][8:]
        column_name = column_data[1]
        data_type = column_data[2]

        print(f"{table_name}\t{column_name}\t{data_type}")


def print_columns_in_multiple_tables(table_columns):
    column_dict = dict()

    for column_data in table_columns:
        table_name = column_data[0][8:]
        column_name = column_data[1]

        if column_name not in column_dict:
            column_dict[column_name] = list()

        column_dict[column_name].append(table_name)

    for column in sorted(column_dict.keys()):
        if len(column_dict[column]) > 1:
            print(f"{column}\t{column_dict[column]}")


def main(args):
    project_name = 'isb-project-zero'
    dataset_name = 'cda_gdc_test'

    table_columns = retrieve_dataset_columns(project_name, dataset_name)

    retrieve_dataset_columns(table_columns)


if __name__ == "__main__":
    main(sys.argv)

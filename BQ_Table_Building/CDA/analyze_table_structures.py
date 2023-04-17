import sys

from common_etl.support import bq_harness_with_result


def main(args):
    """

    :param args:
    :return:
    """

    project_name = 'isb-project-zero'
    dataset_name = 'cda_gdc_test'

    table_column_query = f"""
        SELECT table_name, column_name, data_type
        FROM `{project_name}`.{dataset_name}.INFORMATION_SCHEMA.COLUMNS
    """

    table_columns = bq_harness_with_result(sql=table_column_query, do_batch=False, verbose=False)

    column_dict = dict()

    for column_data in table_columns:
        table_name = column_data[0][7:]
        column_name = column_data[1]

        if column_name not in column_dict:
            column_dict[column_name] = [table_name]

        column_dict[column_name].append(table_name)

    print(column_dict)


if __name__ == "__main__":
    main(sys.argv)

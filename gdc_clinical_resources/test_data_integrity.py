from common_etl.utils import *
from BQ_Table_Building.build_clinical_data_program_tables import (
    find_program_structure, get_table_id_key, get_programs_list,
    get_full_table_name
)

API_PARAMS = None
BQ_PARAMS = None
YAML_HEADERS = ('api_params', 'bq_params', 'steps')

# todo include in YAML
TABLE_NAME_PREFIX = 'clin'
TABLE_NAME_FULL = 'clinical_data'


##
# Functions used for validating inserted data
##
def get_record_count_list(table, table_fg_key, parent_table_id_key):
    """
    Get record counts from newly created BQ tables.
    :param table: table for which to derive counts
    :param table_fg_key: key representing table's field group.
    :param parent_table_id_key: key used to uniquely identify the records of
    this table's ancestor.
    :return: list of max record counts.
    """
    table_path = get_table_id(BQ_PARAMS, table)
    table_id_key = get_table_id_key(table_fg_key)
    table_id_column = get_bq_name(API_PARAMS, table_id_key, table_fg_key)

    results = get_query_results(
        """
        SELECT distinct({}), count({}) as record_count 
        FROM `{}` 
        GROUP BY {}
        """.format(parent_table_id_key, table_id_column, table_path,
                   parent_table_id_key)
    )

    record_count_list = []

    for result in results:
        result_tuple = result.values()

        record_count = result_tuple[1]
        count_label = 'record_count'

        record_count_list.append({
            parent_table_id_key: parent_table_id_key,
            'table': table,
            count_label: record_count
        })

    return record_count_list


def get_main_table_count(program_name, table_id_key, field_name,
                         parent_table_id_key=None, parent_field_name=None):
    """
    Query the origin BQ table's record for a specific case_id. Used to verify
    completion of data insertion.
    :param program_name: program for which to get counts
    :param table_id_key: table for which to get counts
    :param field_name: field name for counts
    :param parent_table_id_key: parent table's unique id key name
    :param parent_field_name: parent's bq table name
    :return: case_id, parent_id (or None if not doubly nested record, integer
    count of records for these IDs.
    """
    table_path = get_table_id(BQ_PARAMS,
                              BQ_PARAMS['GDC_RELEASE'] + '_' + TABLE_NAME_FULL)
    program_table_path = BQ_PARAMS['WORKING_PROJECT'] + '.' + BQ_PARAMS[
        'PROGRAM_ID_TABLE']

    if not parent_table_id_key or not parent_field_name or \
            parent_table_id_key == 'case_id':
        query = """
            SELECT case_id, count(p.{}) as cnt
            FROM `{}`,
            UNNEST({}) as p
            WHERE case_id in (
            SELECT case_gdc_id 
            FROM `{}` 
            WHERE program_name = '{}'
            )
            GROUP BY case_id
            ORDER BY cnt DESC
            LIMIT 1
        """.format(table_id_key,
                   table_path,
                   field_name,
                   program_table_path,
                   program_name)

        results = get_query_results(query)

        for result in results:
            res = result.values()
            return res[0], None, res[1]

    else:
        query = """
            SELECT case_id, p.{}, count(pc.{}) as cnt
            FROM `{}`,
            UNNEST({}) as p,
            UNNEST(p.{}) as pc
            WHERE case_id in (
            SELECT case_gdc_id 
            FROM `{}` 
            WHERE program_name = '{}'
            )
            GROUP BY {}, case_id
            ORDER BY cnt DESC
            LIMIT 1
        """.format(parent_table_id_key,
                   table_id_key,
                   table_path,
                   parent_field_name,
                   field_name,
                   program_table_path,
                   program_name,
                   parent_table_id_key)

        results = get_query_results(query)

        for result in results:
            res = result.values()
            return res[0], res[1], res[2]


def test_table_output():
    """
    Function which compares counts from three sources: BQ queries of the
    original master table, counts achieved in
    Python using the json record output, and counts queried from the
    newly-created tables.
    """
    table_ids = get_dataset_table_list(BQ_PARAMS, None)

    program_names = get_programs_list()
    program_names.remove('CCLE')

    program_table_lists = dict()

    for program_name in program_names:
        print("\nFor program {}:".format(program_name))

        main_table_id = get_full_table_name(program_name, 'cases')
        program_table_lists[main_table_id] = []

        for table in table_ids:
            if main_table_id in table and main_table_id != table:
                program_table_lists[main_table_id].append(table)

        if not program_table_lists[main_table_id]:
            print("... no one-to-many tables")
            continue

        table_fg_list = ['cases']

        for table in program_table_lists[main_table_id]:
            table_fg_list.append(convert_bq_table_id_to_fg(table))

        program_table_query_max_counts = dict()

        for table in program_table_lists[main_table_id]:
            table_fg = convert_bq_table_id_to_fg(table)
            table_id_key = get_table_id_key(table_fg)
            table_field = get_field_name(table_fg)

            parent_table_fg = get_parent_table(table_fg_list, table_fg)
            parent_id_key = get_table_id_key(parent_table_fg)

            full_parent_id_key = get_bq_name(API_PARAMS, parent_id_key, parent_table_fg)

            record_count_list = get_record_count_list(table, table_fg,
                                                      full_parent_id_key)

            max_count, max_count_id = get_max_count(record_count_list)

            parent_fg = get_parent_field_group(table_fg)
            parent_fg_id_key = get_table_id_key(parent_fg)
            parent_fg_field = get_field_name(parent_fg)

            mt_case_id, mt_child_id, mt_max_count = get_main_table_count(
                program_name, table_id_key, table_field, parent_fg_id_key,
                parent_fg_field)

            if max_count != mt_max_count:
                has_fatal_error(
                    "NOT A MATCH for {}. {} != {}".format(table_fg, max_count,
                                                          mt_max_count))

            program_table_query_max_counts[table_fg] = max_count

        cases = get_cases_by_program(BQ_PARAMS, TABLE_NAME_FULL, program_name)

        table_columns, record_counts = find_program_structure(cases)

        cases_tally_max_counts = dict()

        for key in record_counts:
            count = record_counts[key]

            if count > 1:
                cases_tally_max_counts[key] = count

        for key in cases_tally_max_counts:
            if key not in program_table_query_max_counts:
                has_fatal_error(
                    "No match found for {} in program_table_query_max_counts: "
                    "{}".format(
                        key, program_table_query_max_counts))
            elif cases_tally_max_counts[key] != program_table_query_max_counts[key]:
                has_fatal_error("NOT A MATCH for {}. {} != {}".format(
                    key, cases_tally_max_counts[key],
                    program_table_query_max_counts[key]))
        print("Counts all match! Moving on.")

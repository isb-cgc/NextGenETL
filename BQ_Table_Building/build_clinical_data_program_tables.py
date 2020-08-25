"""
Copyright 2020, Institute for Systems Biology

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
import math
import sys
import json
import os
import time
# from gdc_clinical_resources.test_data_integrity import *
import copy
from common_etl.utils import *
from gdc_clinical_resources.generate_docs import (generate_docs)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


####
#
# Getter functions, employed for readability/consistency
#
##
def generate_long_name(program_name, table, has_rel=True):
    """
    Generate string representing a unique name, constructed from elements of
    the table name, program name and GDC release number. Used for storage
    bucket file and BQ table naming.
    :param program_name: Program to which this table is associated.
    :param table: Table name.
    :return: String representing a unique string identifier.
    """
    table_name = []

    if has_rel:
        table_name.append(get_gdc_rel(BQ_PARAMS))

    table_name += [program_name, BQ_PARAMS['MASTER_TABLE']]

    # if one-to-many table, append suffix
    suffix = get_table_suffixes(API_PARAMS)[table]

    if suffix:
        table_name.append(suffix)

    return build_table_name(table_name)


def get_jsonl_filename(program_name, table, is_webapp=False):
    """
    Gets unique (per release) jsonl filename, used for intermediately storing
    the table rows after they're flattened, but before BQ insertion. Allows for
    faster script thanks to minimal BigQuery transactions.
    :param program_name: name of the program to with the data belongs
    :param table: future insertion table for flattened data
    :return: String .jsonl filename, of the form
        relXX_TABLE_NAME_FULL_PROGRAM_supplemental-table-name
        (_supplemental-table-name optional)
    """
    file_name = 'webapp_' if is_webapp else ''

    file_name += generate_long_name(program_name, table) + '.jsonl'

    return file_name


def get_temp_filepath(program_name, table, is_webapp=False):
    """
    Get filepath for the temp storage folder.
    :param program_name: Program
    :param table: Program to which this table is associated.
    :return: String representing the temp file path.
    """

    path = get_scratch_dir(API_PARAMS) + '/'

    return path + get_jsonl_filename(program_name, table, is_webapp)


def get_full_table_name(program_name, table):
    """
    Get the full name used in table_id for a given table.
    :param program_name: name of the program to with the data belongs
    :param table: Name of desired table
    :return: String representing table name used by BQ.
    """
    return generate_long_name(program_name, table)


'''
def get_required_columns(table):
    """
    Get list of required columns. Currently generated, but intended to also
    work if supplied in YAML config file.
    :param table: name of table for which to retrieve required columns.
    :return: list of required columns (currently, only includes the table's id column)
    """
    table_id_field = get_fg_id_name(API_PARAMS, table)
    table_id_name = get_full_field_name(table, table_id_field)
    return [table_id_name]
'''


def get_id_index(table_key, column_order_dict):
    """
    Get the relative order index of the table's id column.
    :param table_key: Table for which to get index
    :param column_order_dict: Dictionary containing column names : indexes
    :return: Int representing relative column position in schema.
    """
    table_id_key = get_fg_id_name(API_PARAMS, table_key)
    return column_order_dict[table_key + '.' + table_id_key]


def get_count_column_name(table_key):
    """
    Returns name of count column for given one-to-many table.
    :param table_key: one-to-many table
    :return: count column name
    """
    return get_bq_name(API_PARAMS, 'count', table_key)


def build_column_order_dict():
    """
    Using table order provided in YAML, with add't ordering for reference
    columns added during one-to-many table creation.
    :return: dict of str column names : int representing position.
    """
    column_order_dict = dict()
    field_groups = API_PARAMS['TABLE_ORDER']
    id_index_gap = len(field_groups) - 1

    idx = 0

    for group in field_groups:
        try:
            param_column_order = API_PARAMS['TABLE_METADATA'][group]['column_order']
            id_column = API_PARAMS['TABLE_METADATA'][group]['table_id_key']

            for column in param_column_order:
                column_order_dict[group + '.' + column] = idx
                idx = idx + (id_index_gap * 2) if id_column == column else idx + 1
        except KeyError:
            has_fatal_error("{} found in API_PARAMS['TABLE_ORDER'] but not in "
                            "API_PARAMS['TABLE_METADATA']".format(group))

    column_order_dict['cases.state'] = idx
    column_order_dict['cases.created_datetime'] = idx + 1
    column_order_dict['cases.updated_datetime'] = idx + 2

    return column_order_dict


def get_column_order(table):
    """
    Returns table's column order list (from yaml config file)
    :param table: table for which to retrieve column order
    :return: table's column order list
    """
    if table not in API_PARAMS['TABLE_METADATA']:
        has_fatal_error("'{}' not found in API_PARAMS['TABLE_METADATA']".format(table))
    elif 'column_order' not in API_PARAMS['TABLE_METADATA'][table]:
        has_fatal_error("no column order provided for {} in yaml config.".format(table))

    ordered_table_fields = API_PARAMS['TABLE_METADATA'][table]['column_order']

    master_index_dict = build_column_order_dict()

    table_column_order = [table + '.' + field for field in ordered_table_fields]

    return {col: master_index_dict[col] for col in table_column_order}


####
#
# Functions used to determine a program's table structure(s)
#
##
def get_excluded_fields(table):
    """
    Get list of fields to exclude from final BQ tables (from yaml config file)
    :param table: table key for which to return excluded fields
    :return: list of excluded fields
    """
    if not API_PARAMS['TABLE_METADATA']:
        has_fatal_error("params['TABLE_METADATA'] not found")

    elif table not in API_PARAMS['TABLE_METADATA']:
        return None
    elif 'excluded_fields' not in API_PARAMS['TABLE_METADATA'][table]:
        return None

    return API_PARAMS['TABLE_METADATA'][table]['excluded_fields']


def get_all_excluded_columns():
    """
    Get excluded fields for all field groups (from yaml config file)
    :return: list of excluded fields
    """
    excluded_columns = set()

    if not API_PARAMS['TABLE_METADATA']:
        has_fatal_error("params['TABLE_METADATA'] not found")

    if not API_PARAMS['TABLE_ORDER']:
        has_fatal_error("params['TABLE_ORDER'] not found")

    for table in API_PARAMS['TABLE_ORDER']:
        if 'excluded_fields' not in API_PARAMS['TABLE_METADATA'][table]:
            has_fatal_error("{}'s excluded_fields not found in API_PARAMS".format(table))

        excluded_fields = API_PARAMS['TABLE_METADATA'][table]['excluded_fields']

        for field in excluded_fields:
            excluded_columns.add(get_bq_name(API_PARAMS, field, table))

    return excluded_columns


def flatten_tables(field_groups, record_counts):
    """
    From dict containing table_name keys and sets of column names, remove
    excluded columns and merge into parent table if the field group can be
    flattened for this program.
    :param field_groups: dict of tables and columns
    :param record_counts: set of table names
    :return: flattened table column dict.
    """
    record_counts = get_tables(record_counts)
    table_columns = dict()

    fg_depths = {fg: get_field_depth(fg) for fg in field_groups}

    for field_group, depth in sorted(fg_depths.items(), key=lambda i: i[1]):
        if depth > 3:
            has_fatal_error("This script isn't confirmed to work with field groups "
                            "nested more than two levels.")

        field_groups[field_group] = remove_excluded_fields(field_groups[field_group],
                                                           field_group)

        full_field_names = {get_full_field_name(field_group, field)
                            for field in field_groups[field_group]}

        if field_group in record_counts:
            table_columns[field_group] = full_field_names
        else:
            # field group can be flattened
            parent_table = get_parent_table(record_counts, field_group)

            table_columns[parent_table] |= full_field_names

    return table_columns


def examine_case(non_null_fields, record_counts, field_group, fg_name):
    """
    Recursively examines case and updates dicts of non-null fields and max record counts.
    :param non_null_fields: current dict of non-null fields for each field group
    :param field_group: whole or partial case record json object
    :param record_counts: dict of max field group record counts observed in program so far
    :param fg_name: name of currently-traversed field group
    :return: dicts of non-null field lists and max record counts (keys = field groups)
    """
    if is_valid_fg(API_PARAMS, fg_name):
        for field, record in field_group.items():

            if isinstance(record, list):
                child_fg = fg_name + '.' + field

                if not is_valid_fg(API_PARAMS, child_fg):
                    continue
                elif child_fg not in record_counts:
                    non_null_fields[child_fg] = set()
                    record_counts[child_fg] = len(record)
                else:
                    record_counts[child_fg] = max(record_counts[child_fg], len(record))

                for entry in record:
                    examine_case(non_null_fields, record_counts,
                                 field_group=entry, fg_name=child_fg)
            else:
                if fg_name not in non_null_fields:
                    non_null_fields[fg_name] = set()
                    record_counts[fg_name] = 1

                if isinstance(record, dict):
                    for child_field in record:
                        non_null_fields[fg_name].add(child_field)
                else:
                    if record:
                        non_null_fields[fg_name].add(field)

        return non_null_fields, record_counts


def find_program_structure(cases):
    """
    Determine table structure required for the given program.
    :param cases: dict of program's case records
    :return: dict of tables and columns, dict with maximum record count for
    this program's field groups.
    """
    field_groups = {}
    record_counts = {}

    for case in cases:
        if not case:
            continue
        examine_case(field_groups, record_counts, field_group=case, fg_name='cases')

    for fg in field_groups:
        if fg not in API_PARAMS['TABLE_METADATA']:
            print("{} not in metadata".format(fg))
            field_groups.pop(fg)
            cases.pop(fg)

    columns = flatten_tables(field_groups, record_counts)

    record_counts = {k: v for k, v in record_counts.items() if record_counts[k] > 0}

    return columns, record_counts


####
#
# Functions used for schema creation
#
##
def get_count_column_index(table_key, column_order_dict):
    """
    Get index of child table record count reference column.
    :param table_key: table for which to get index
    :param column_order_dict: dict containing column indexes
    :return: count column start idx position
    """
    table_id_key = get_fg_id_name(API_PARAMS, table_key)
    id_column_index = column_order_dict[table_key + '.' + table_id_key]

    field_groups = API_PARAMS['TABLE_ORDER']
    id_index_gap = len(field_groups) - 1

    return id_column_index + id_index_gap

'''
def get_case_id_index(table_key, column_orders):
    """
    Get case_id's position index for given table
    :param table_key: table in which to lookup case_id
    :param column_orders: dict of {field names: position indexes}
    :return: case_id index for provided table
    """
    return get_count_column_index(table_key, column_orders) - 1
'''


def generate_id_schema_entry(column, parent_table, program):
    """
    Create schema entry for inserted parent reference id.
    :param program: program name
    :param column: parent id column
    :param parent_table: parent table name
    :return: schema entry dict for new reference id field
    """
    field_name = get_field_name(column)

    if field_name == 'case_id':
        bq_col_name = 'case_id'
        # source_table = 'main'
        source_table = get_full_table_name(program, 'cases')
    else:
        bq_col_name = get_bq_name(API_PARAMS, column)
        source_table = get_full_table_name(program, parent_table)

    return {
        "name": get_field_name(column),
        "type": 'STRING',
        "description": ("Reference to ancestor {}, located in {}."
                        .format(bq_col_name, source_table)),
        "mode": 'REQUIRED'
    }


def generate_count_schema_entry(count_id_key, parent_table):
    """
    Create schema entry for one-to-many record count field.
    :param count_id_key: count field name
    :param parent_table: parent table name
    :return: schema entry dict for new one-to-many record count field
    """
    description = ("Total child record count (located in {} table).".format(parent_table))

    return {
        "name": get_field_name(count_id_key),
        "type": 'INTEGER',
        "description": description,
        "mode": 'NULLABLE'
    }


def add_ref_id_to_table(schema, columns, column_order, table, id_tuple):
    # add parent id to one-to-many table

    id_index, id_col_name, program = id_tuple
    parent_fg = get_parent_field_group(table)
    schema[id_col_name] = generate_id_schema_entry(id_col_name, parent_fg, program)
    columns[table].add(id_col_name)
    column_order[table][id_col_name] = id_index


def add_count_col_to_parent_table(schema, columns, column_order, table):
    # add one-to-many record count column to parent table
    parent_table = get_parent_table(columns.keys(), table)
    count_field = get_count_field(table)

    schema[count_field] = generate_count_schema_entry(count_field, parent_table)
    columns[parent_table].add(count_field)
    count_column_index = get_count_column_index(parent_table, column_order[parent_table])
    column_order[parent_table][count_field] = count_column_index


def add_reference_columns(columns, record_counts, schema=None,
                          program=None, is_webapp=False):
    """
    Add reference columns generated by separating and flattening data.

    Possible types:

    - _count column representing # of child records found in supplemental table
    - case_id, used to reference main table records
    - pid, used to reference nearest un-flattened ancestor table

    :param program:
    :param record_counts:
    :param columns: dict containing table column keys
    :param schema: dict containing schema records
    :param is_webapp: if caller is part of webapp table build logic, True. Bypasses
    the insertion of irrelevant reference columns
    :return: table_columns, schema_dict, column_order_dict
    """
    column_orders = dict()

    if not is_webapp and (not program or not schema):
        has_fatal_error("invalid arguments for add_reference_columns. if not is_webapp, "
                        "schema and program are required.", ValueError)

    for table, depth in get_sorted_fg_depths(record_counts):
        # get ordering for table by only including relevant column indexes
        column_orders[table] = get_column_order(table)

        if depth == 1 or table not in columns:
            continue

        curr_index = get_id_index(table, column_orders[table]) + 1

        pid_field = '.'.join([get_parent_field_group(table),
                              get_fg_id_name(API_PARAMS,
                                             get_parent_field_group(table))])
        if is_webapp:
            columns[table].add(pid_field)
            column_orders[table][pid_field] = curr_index
            curr_index += 1
        else:
            # for former doubly-nested tables, ancestor id precedes case_id in table
            if depth > 2:
                add_ref_id_to_table(schema, columns, column_orders, table,
                                    (curr_index, pid_field, program))
                curr_index += 1

            case_id_name = get_case_id_field(table)

            add_ref_id_to_table(schema, columns, column_orders, table,
                                (curr_index, case_id_name, program))

            add_count_col_to_parent_table(schema, columns, column_orders, table)

    return column_orders


def merge_column_orders(schema, columns, record_counts, column_orders, is_webapp=False):
    merged_column_orders = dict()

    for table, depth in get_sorted_fg_depths(record_counts, reverse=True):

        schema_key = '.'.join([table, get_fg_id_name(API_PARAMS, table)])

        if table in columns:
            merged_order_key = table
            schema[schema_key]['mode'] = 'REQUIRED'
        else:
            # not a standalone table, merge
            merged_order_key = get_parent_table(columns.keys(), table)
            # if merging key into parent table, that key is no longer required, might
            # not exist in some cases
            schema[schema_key]['mode'] = 'NULLABLE'

        if merged_order_key not in merged_column_orders:
            merged_column_orders[merged_order_key] = dict()

        merged_column_orders[merged_order_key].update(column_orders[table])

    return merged_column_orders


def remove_null_fields(table_columns, merged_orders):
    for table, columns in table_columns.items():
        table_cols_set = columns
        merged_orders_set = merged_orders[table].keys()
        null_fields_set = merged_orders_set - table_cols_set

        for field in null_fields_set:
            merged_orders[table].pop(field)


def create_webapp_schema_lists(schema, record_counts, merged_orders):
    schema_field_lists = dict()

    for table in get_tables(record_counts):
        schema_field_lists[table] = list()

        if table not in merged_orders:
            has_fatal_error("record counts and merged orders disagree on program's "
                            "table architecture")
        print("\nTable: {} \nmerged orders: {}\n".format(table, merged_orders))

        for field in merged_orders[table]:
            schema_field_lists[table].append(schema[field])

    return schema_field_lists


def create_schema_lists(schema, record_counts, merged_orders):
    # add bq abbreviations to schema field dicts
    for entry in schema:
        field = get_field_name(entry)
        # if is_renamed(API_PARAMS, field):
        #    field = get_new_name(API_PARAMS, field)
        #    schema[entry]['name'] = get_bq_name(API_PARAMS, field)
        if field != 'case_id':
            schema[entry]['name'] = get_bq_name(API_PARAMS, entry)

    schema_field_lists = dict()

    for table in get_tables(record_counts):
        # this is just alphabetizing the count columns
        counts_idx = get_count_column_index(table, merged_orders[table])
        count_cols = [col for col, i in merged_orders[table].items() if i == counts_idx]

        for count_column in sorted(count_cols):
            merged_orders[table][count_column] = counts_idx
            counts_idx += 1

        schema_field_lists[table] = list()

        # sort merged table columns by index
        for column in [col for col, idx in sorted(merged_orders[table].items(),
                                                  key=lambda i: i[1])]:
            if column not in schema:
                print("{} not found in src table, excluding schema field.".format(column))
                continue
            schema_field_lists[table].append(to_bq_schema_obj(schema[column]))

    return schema_field_lists


def remove_excluded_fields(record, table):
    """
    Remove columns with only None values, as well as those excluded.
    :param record: table record to parse.
    :param table: name of destination table.
    :return: Trimmed down record dict.
    """
    excluded_fields = get_excluded_fields(table)

    if isinstance(record, set):
        return {field for field in record if field not in excluded_fields}
    if isinstance(record, dict):
        excluded_fields = {get_bq_name(API_PARAMS, field, table)
                           for field in excluded_fields}
        for field in record.copy().keys():
            if field in excluded_fields or not record[field]:
                record.pop(field)
        return record

    return [field for field in record if field not in excluded_fields]


####
#
# Functions used for parsing and loading data into BQ tables
#
##
def flatten_case_entry(record, field_group, flat_case, case_id, pid, pid_field, is_webapp):
    """
    Recursively traverse the case json object, creating dict of format:
     {field_group: [records]}
    :param record:
    :param field_group:
    :param flat_case: partially-built flattened case dict
    :param case_id: case id
    :param pid: parent field group id
    :param pid_field: parent field group id key
    :return: flattened case dict, format: { 'field_group': [records] }
    """
    # entry represents a field group, recursively flatten each record
    if not is_valid_fg(API_PARAMS, field_group):
        return flat_case

    if isinstance(record, list):
        # flatten each record in field group list
        for entry in record:
            flat_case = flatten_case_entry(entry, field_group, flat_case,
                                           case_id, pid, pid_field, is_webapp)
    else:
        row_dict = dict()
        id_field = get_fg_id_name(API_PARAMS, field_group, is_webapp)

        for field, field_val in record.items():
            if isinstance(field_val, list):
                flat_case = flatten_case_entry(
                    record=field_val,
                    field_group=field_group + '.' + field,
                    flat_case=flat_case,
                    case_id=case_id,
                    pid=record[id_field],
                    pid_field=id_field,
                    is_webapp=is_webapp)
            else:
                if id_field != pid_field:
                    parent_fg = get_parent_field_group(field_group)

                    if not is_webapp:
                        pid_column = get_bq_name(API_PARAMS, pid_field, parent_fg)
                    else:
                        pid_column = pid_field

                    row_dict[pid_column] = pid

                if id_field != 'case_id':
                    row_dict['case_id'] = case_id
                # Field converted bq column name
                if not is_webapp:
                    column = get_bq_name(API_PARAMS, field, field_group)
                else:
                    column = field

                row_dict[column] = field_val
        if field_group not in flat_case:
            flat_case[field_group] = list()

        excluded_columns = get_all_excluded_columns()

        if row_dict:
            for field in row_dict.copy().keys():
                if field in excluded_columns or not row_dict[field]:
                    row_dict.pop(field)
        flat_case[field_group].append(row_dict)

    return flat_case


def flatten_case(case, is_webapp):
    """
    Converts nested case object into a flattened representation of its records.
    :param case: dict containing case data
    :return: flattened case dict
    """

    case_id_key = API_PARAMS['TABLE_METADATA']['cases']['table_id_key']

    if is_webapp and case_id_key in API_PARAMS['RENAME_FIELDS']:
        case_id_key = API_PARAMS['RENAME_FIELDS'][case_id_key]

    return flatten_case_entry(record=case,
                              field_group='cases',
                              flat_case=dict(),
                              case_id=case[case_id_key],
                              pid=case[case_id_key],
                              pid_field=case_id_key,
                              is_webapp=is_webapp)


def get_record_idx(flattened_case, field_group, record_id, is_webapp=False):
    """
    Get index of record associated with record_id from flattened_case
    :param flattened_case: dict containing {field group names: list of record dicts}
    :param field_group: field group containing record_id
    :param record_id: id of record for which to retrieve position
    :return: position index of record in field group's record list
    """
    fg_id_name = get_fg_id_name(API_PARAMS, field_group)

    if is_webapp:
        fg_id_key = fg_id_name
    else:
        fg_id_key = get_bq_name(API_PARAMS, fg_id_name, field_group)

    idx = 0

    for record in flattened_case[field_group]:
        if record[fg_id_key] == record_id:
            return idx
        idx += 1

    return has_fatal_error("id {} not found by get_record_idx.".format(record_id))


def merge_single_entry_fgs(flattened_case, record_counts, is_webapp=False):
    """
    # Merge flatten-able field groups.
    :param flattened_case: flattened case dict
    :param record_counts: field group count dict
    """
    tables = get_tables(record_counts)

    flattened_fg_parents = dict()

    for field_group in record_counts:
        if field_group == 'cases':
            continue
        if record_counts[field_group] == 1:
            if field_group in flattened_case:
                # create list of flattened field group destination tables
                flattened_fg_parents[field_group] = get_parent_table(tables, field_group)

    for field_group, parent in flattened_fg_parents.items():
        fg_id_name = get_fg_id_name(API_PARAMS, parent)

        if is_webapp:
            bq_parent_id_key = fg_id_name
        else:
            bq_parent_id_key = get_bq_name(API_PARAMS, fg_id_name, parent)

        for record in flattened_case[field_group]:
            parent_id = record[bq_parent_id_key]
            parent_idx = get_record_idx(flattened_case, parent, parent_id, is_webapp)
            flattened_case[parent][parent_idx].update(record)

        flattened_case.pop(field_group)


def get_record_counts(flattened_case, record_counts, is_webapp=False):
    """
    # Get record counts for field groups in case record
    :param flattened_case: flattened dict containing case record entries
    :param record_counts: field group count dict
    """
    # initialize dict with field groups that can't be flattened
    # record_count_dict = {fg: 0 for fg in record_counts if record_counts[fg] > 1}
    record_count_dict = {fg: dict() for fg in record_counts if record_counts[fg] > 1}
    tables = get_tables(record_counts)

    for field_group in record_count_dict.copy().keys():
        parent_table = get_parent_table(tables, field_group)
        fg_id_name = get_fg_id_name(API_PARAMS, parent_table)

        if is_webapp:
            bq_parent_id_key = fg_id_name
        else:
            bq_parent_id_key = get_bq_name(API_PARAMS, fg_id_name, parent_table)

        # initialize record counts for parent id
        if parent_table in flattened_case:
            for parent_record in flattened_case[parent_table]:
                parent_table_id = parent_record[bq_parent_id_key]
                record_count_dict[field_group][parent_table_id] = 0

        # count child records
        if field_group in flattened_case:
            for record in flattened_case[field_group]:
                parent_id = record[bq_parent_id_key]
                record_count_dict[field_group][parent_id] += 1

    # insert record count into flattened dict entries
    for field_group, parent_ids_dict in record_count_dict.items():
        parent_table = get_parent_table(tables, field_group)
        count_col_name = get_count_column_name(field_group)

        for parent_id, count in parent_ids_dict.items():
            parent_record_idx = get_record_idx(flattened_case, parent_table,
                                               parent_id, is_webapp)

            flattened_case[parent_table][parent_record_idx][count_col_name] = count


def merge_or_count_records(flattened_case, record_counts, is_webapp=False):
    """
    If program field group has max record count of 1, flattens into parent table.
    Otherwise, counts record in one-to-many table and adds count field to parent record
    in flattened_case
    :param flattened_case: flattened dict containing case record's values
    :param record_counts: max counts for program's field group records
    :return: modified version of flattened_case
    """
    merge_single_entry_fgs(flattened_case, record_counts, is_webapp)
    # initialize counts for parent_ids for every possible child table (some child tables
    # won't actually have records, and this initialization adds 0 counts in that case)
    if not is_webapp:
        get_record_counts(flattened_case, record_counts, is_webapp)


def create_and_load_tables(program_name, cases, schemas, record_counts, is_webapp=False):
    """
    Create jsonl row files for future insertion, store in GC storage bucket,
    then insert the new table schemas and data.
    :param record_counts:
    :param program_name: program for which to create tables
    :param cases: case records to insert into BQ for program
    :param schemas: dict of schema lists for all of this program's tables
    """

    tables = get_tables(record_counts)

    print("\nInserting case records...")
    for table in tables:
        jsonl_file_path = get_temp_filepath(program_name, table, is_webapp)
        # delete last jsonl scratch file so we don't append to it
        if os.path.exists(jsonl_file_path):
            os.remove(jsonl_file_path)

    for case in cases:
        rename_case_fields(case, API_PARAMS)

        flattened_case = flatten_case(case, is_webapp)

        merge_or_count_records(flattened_case, record_counts, is_webapp)

        for table in flattened_case.keys():
            if table not in tables:
                has_fatal_error("Table {} not found in table keys".format(table))

            jsonl_fp = get_temp_filepath(program_name, table, is_webapp)

            with open(jsonl_fp, 'a') as jsonl_file:
                for row in flattened_case[table]:
                    json.dump(obj=row, fp=jsonl_file)
                    jsonl_file.write('\n')

    for table in tables:
        jsonl_file = get_jsonl_filename(program_name, table, is_webapp)
        table_id = get_full_table_name(program_name, table)

        upload_to_bucket(BQ_PARAMS, API_PARAMS, jsonl_file)
        create_and_load_table(BQ_PARAMS, jsonl_file, schemas[table], table_id)


####
#
# Modify existing tables
#
##

def update_table_metadata():
    metadata_path = (BQ_PARAMS['BQ_REPO'] + '/' + BQ_PARAMS['TABLE_METADATA_DIR'] + '/' +
                     get_gdc_rel(BQ_PARAMS) + '/')

    files = get_dir_files(metadata_path)

    for json_file in files:
        table_name = transform_json_name_to_table(json_file)
        table_id = get_working_table_id(BQ_PARAMS, table_name)

        if not exists_bq_table(table_id):
            print('No table found for file (skipping): ' + json_file)
            continue

        with open(get_filepath(metadata_path, json_file)) as json_file_output:
            metadata = json.load(json_file_output)

            update_bq_table(table_id, metadata)


def update_schema():
    fields_path = (BQ_PARAMS['BQ_REPO'] + '/' + BQ_PARAMS['FIELD_DESC_DIR'])
    fields_file = BQ_PARAMS['FIELD_DESC_FILE_PREFIX'] + '_' + \
                  get_gdc_rel(BQ_PARAMS) + '.json'

    with open(get_filepath(fields_path, fields_file)) as json_file_output:
        descriptions = json.load(json_file_output)

    metadata_path = (BQ_PARAMS['BQ_REPO'] + '/' + BQ_PARAMS['TABLE_METADATA_DIR'] + '/' +
                     get_gdc_rel(BQ_PARAMS) + '/')

    files = get_dir_files(metadata_path)

    for json_file in files:
        table_name = transform_json_name_to_table(json_file)
        table_id = get_working_table_id(BQ_PARAMS, table_name)

        update_table_schema(table_id, descriptions)


def transform_json_name_to_table(json_name):
    # json file name 'isb-cgc-bq.HCMI.clinical_follow_ups_gdc_r24.json'
    # def table name 'r24_HCMI_clinical_follow_ups'

    json_name_split = json_name.split('.')
    program_name = json_name_split[1]
    split_table_name = json_name_split[2].split('_')
    partial_table_name = '_'.join(split_table_name[0:-2])
    return '_'.join([get_gdc_rel(BQ_PARAMS), program_name, partial_table_name])


def copy_tables_into_public_project():
    metadata_path = (BQ_PARAMS['BQ_REPO'] + '/' + BQ_PARAMS['TABLE_METADATA_DIR'] + '/' +
                     get_gdc_rel(BQ_PARAMS) + '/')

    files = get_dir_files(metadata_path)

    for json_file in files:
        table_name = transform_json_name_to_table(json_file)

        split_table_id = json_file.split('.')[:-1]

        project = split_table_id[0]

        dataset = split_table_id[1]
        versioned_dataset = dataset + '_versioned'

        versioned_table = split_table_id[2]
        current_table = '_'.join(versioned_table.split('_')[:-1])
        current_table += '_current'

        source_table_id = get_working_table_id(BQ_PARAMS, table_name)
        curr_table_id = '.'.join([project, dataset, current_table])
        versioned_table_id = '.'.join([project, versioned_dataset, versioned_table])

        if not exists_bq_table(source_table_id):
            print('No table found for file (skipping): ' + json_file)
            continue

        print(source_table_id)
        print(curr_table_id)
        print(versioned_table_id)

        # copy_bq_table(source_table_id, curr_table_id, BQ_PARAMS['PUBLIC_PROJECT'])
        # copy_bq_table(source_table_id, versioned_table_id, BQ_PARAMS['PUBLIC_PROJECT'])
        # modify_friendly_name(API_PARAMS, versioned_table_id)


####
#
# Web App specific functions
#
##

def make_biospecimen_stub_tables(program):
    query = ("""
        SELECT proj, case_gdc_id, case_barcode, sample_gdc_id, sample_barcode
        FROM
          (SELECT proj, case_gdc_id, case_barcode, 
            SPLIT(sample_ids, ',') as s_gdc_ids, 
            SPLIT(submitter_sample_ids, ', ') as s_barcodes
            FROM
                (SELECT case_id as case_gdc_id, 
                    submitter_id as case_barcode, 
                    sample_ids, submitter_sample_ids, 
                    SPLIT((SELECT project_id
                           FROM UNNEST(project)), '-')[OFFSET(0)] AS proj
                FROM `isb-project-zero.GDC_Clinical_Data.r25_clinical`)), 
        UNNEST(s_gdc_ids) as sample_gdc_id WITH OFFSET pos1, 
        UNNEST(s_barcodes) as sample_barcode WITH OFFSET pos2
        WHERE pos1 = pos2
        AND proj = '{}'
    """).format(program)

    table_name = build_table_name([str(program), BQ_PARAMS['BIOSPECIMEN_SUFFIX']])
    table_id = get_working_table_id(BQ_PARAMS, table_name)
    table = load_table_from_query(table_id, query)

    print("{}: {} rows inserted.".format(table.table_id, table.num_rows))


####
#
# Script execution
#
##
def print_final_report(start, steps):
    """
    Outputs a basic report of script's results, including total processing
    time and which steps were specified in YAML.
    :param start: float representing script's start time.
    :param steps: set of steps to be performed (configured in YAML)
    """
    seconds = time.time() - start
    minutes = math.floor(seconds / 60)
    seconds -= minutes * 60

    print("Programs script executed in {} min, {:.0f} sec\n".format(minutes, seconds))
    print("Steps completed: ")

    if 'create_and_load_tables' in steps:
        print('\t - created tables and inserted data')
    if 'update_table_metadata' in steps:
        print('\t - added/updated table metadata')
    if 'update_schema' in steps:
        print('\t - updated table field descriptions')
    if 'copy_tables_into_production' in steps:
        print('\t - copied tables into production (public-facing bq tables)')
    if 'validate_data' in steps:
        print('\t - validated data (tests not considered exhaustive)')
    if 'generate_documentation' in steps:
        print('\t - generated documentation')
    print('\n\n')


def main(args):
    """
    Script execution function.
    :param args: command-line arguments
    """
    start = time.time()
    steps = []

    # Load YAML configuration
    if len(args) != 2:
        has_fatal_error("Usage: {} <configuration_yaml>".format(args[0]), ValueError)

    with open(args[1], mode='r') as yaml_file:
        try:
            global API_PARAMS, BQ_PARAMS
            API_PARAMS, BQ_PARAMS, steps = load_config(yaml_file, YAML_HEADERS)
        except ValueError as err:
            has_fatal_error(str(err), ValueError)

    #programs = get_program_list(BQ_PARAMS)
    programs = ['HCMI']

    for program in programs:
        prog_start = time.time()

        if 'create_biospecimen_stub_tables' in steps:

            print("Creating biospecimen stub tables!")
            make_biospecimen_stub_tables(program)

        if 'create_webapp_tables' in steps or 'create_and_load_table' in steps:

            print("Executing script for program {}...".format(program))
            cases = get_cases_by_program(BQ_PARAMS, program)

            if len(cases) == 0:
                print("No case records found for program {}, skipping.".format(program))
                continue

            # derive the program's table structure by analyzing its case records
            columns, record_counts = find_program_structure(cases)

            # generate table schemas
            schema = create_schema_dict(API_PARAMS, BQ_PARAMS)

            if 'create_webapp_tables' in steps:

                webapp_columns = {k:v for (k,v) in columns.items()}
                webapp_record_counts = {k:v for (k,v) in record_counts.items()}

                if 'WEBAPP_EXCLUDED_FG' not in API_PARAMS:
                    has_fatal_error("WEBAPP_EXCLUDED_FG not found in params.", KeyError)

                excluded_fgs = API_PARAMS['WEBAPP_EXCLUDED_FG']

                if excluded_fgs:
                    for excluded_fg in excluded_fgs:
                        if excluded_fg in webapp_columns:
                            webapp_columns.pop(excluded_fg)
                        if excluded_fg in webapp_record_counts:
                            webapp_record_counts.pop(excluded_fg)
                
                webapp_column_orders = add_reference_columns(webapp_columns,
                                                             webapp_record_counts,
                                                             is_webapp=True)

                webapp_schema = {k:v for (k,v) in schema.items()}

                print()
                print(webapp_column_orders)

                # removes the prefix from schema field name attributes
                # removes the excluded fields/field groups
                modify_fields_for_webapp(webapp_schema, webapp_column_orders, API_PARAMS)

                print()
                print(webapp_column_orders)


                # reassign merged_column_orders to column_orders
                webapp_merged_orders = merge_column_orders(webapp_schema,
                                                           webapp_columns,
                                                           webapp_record_counts,
                                                           webapp_column_orders,
                                                           is_webapp=True)

                remove_null_fields(webapp_columns, webapp_merged_orders)

                webapp_table_schemas = create_webapp_schema_lists(
                    webapp_schema, webapp_record_counts, webapp_merged_orders)

                create_and_load_tables(
                    program, cases, webapp_table_schemas,
                    webapp_record_counts, is_webapp=True)

        if 'create_and_load_table' in steps:

            # modify schema dict, add reference columns for this program
            column_orders = add_reference_columns(columns, record_counts, schema, program)

            # reassign merged_column_orders to column_orders
            merged_orders = merge_column_orders(schema, columns,
                                                record_counts, column_orders)

            remove_null_fields(columns, merged_orders)

            table_schemas = create_schema_lists(schema, record_counts, merged_orders)

            # create tables, flatten and insert data
            create_and_load_tables(program, cases, table_schemas, record_counts)

        if 'create_webapp_tables' in steps or 'create_and_load_table' in steps:

            print("{} processed in {:0.0f}s!\n".format(program, time.time() - prog_start))

    if 'update_table_metadata' in steps:
        update_table_metadata()

    if 'update_schema' in steps:
        update_schema()

    if 'copy_tables_into_production' in steps:
        copy_tables_into_public_project()

    if 'generate_documentation' in steps:
        generate_docs(API_PARAMS, BQ_PARAMS)

    if 'validate_data' in steps:
        pass

    print_final_report(start, steps)


if __name__ == '__main__':
    main(sys.argv)

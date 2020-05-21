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
from common_etl.utils import (
    get_table_prefixes, get_bq_name, has_fatal_error, get_query_results, get_field_name,
    get_tables, get_parent_table, get_parent_field_group, load_config,
    get_cases_by_program, get_table_id, upload_to_bucket, create_and_load_table,
    get_field_depth, get_full_field_name, in_bq_format, create_schema_dict,
    to_SchemaField)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')

# todo include in YAML
TABLE_NAME_PREFIX = 'clin'
MASTER_TABLE_NAME = 'clinical_data'


####
#
# Getter functions, employed for readability/consistency
#
##
def generate_long_name(program_name, table):
    """
    Generate string representing a unique name, constructed from elements of
    the table name, program name and GDC release number. Used for storage
    bucket file and BQ table naming.
    :param program_name: Program to which this table is associated.
    :param table: Table name.
    :return: String representing a unique string identifier.
    """
    prefixes = get_table_prefixes(API_PARAMS)
    prefix = prefixes[table]

    # remove invalid char from program name
    if '.' in program_name:
        program_name = '_'.join(program_name.split('.'))

    file_name_parts = [BQ_PARAMS['GDC_RELEASE'], TABLE_NAME_PREFIX, program_name]

    # if one-to-many table, append suffix
    if prefix:
        file_name_parts.append(prefix)

    return '_'.join(file_name_parts)


def get_jsonl_filename(program_name, table):
    """
    Gets unique (per release) jsonl filename, used for intermediately storing
    the table rows after they're flattened, but before BQ insertion. Allows for
    faster script thanks to minimal BigQuery txns.
    :param program_name: name of the program to with the data belongs
    :param table: future insertion table for flattened data
    :return: String .jsonl filename, of the form
        relXX_TABLE_NAME_FULL_PROGRAM_supplemental-table-name
        (_supplemental-table-name optional)
    """
    return generate_long_name(program_name, table) + '.jsonl'


def get_temp_filepath(program_name, table):
    """
    Get filepath for the temp storage folder.
    :param program_name: Program
    :param table: Program to which this table is associated.
    :return: String representing the temp file path.
    """
    return API_PARAMS['TEMP_PATH'] + '/' + get_jsonl_filename(program_name,
                                                              table)


def get_full_table_name(program_name, table):
    """
    Get the full name used in table_id for a given table.
    :param program_name: name of the program to with the data belongs
    :param table: Name of desired table
    :return: String representing table name used by BQ.
    """
    return generate_long_name(program_name, table)


def get_required_columns(table):
    """
    Get list of required columns. Currently generated, but intended to also
    work if supplied in YAML config file.
    :param table: name of table for which to retrieve required columns.
    :return: list of required columns (currently, only includes the table's id column)
    """
    table_id_field = get_table_id_key(table)
    table_id_name = get_full_field_name(table, table_id_field)
    return [table_id_name]


def get_table_id_key(table_key):
    """
    Retrieves the id key used to uniquely identify a table record.
    :param table_key: Table for which to determine the id key.
    :return: String representing table key.
    """
    if not API_PARAMS['TABLE_METADATA']:
        has_fatal_error("params['TABLE_METADATA'] not found")

    if 'table_id_key' not in API_PARAMS['TABLE_METADATA'][table_key]:
        has_fatal_error("table_id_key not found in "
                        "API_PARAMS['TABLE_METADATA']['{}']".format(table_key))

    return API_PARAMS['TABLE_METADATA'][table_key]['table_id_key']


def get_id_index(table_key, column_order_dict):
    """
    Get the relative order index of the table's id column.
    :param table_key: Table for which to get index
    :param column_order_dict: Dictionary containing column names : indexes
    :return: Int representing relative column position in schema.
    """
    table_id_key = get_table_id_key(table_key)
    return column_order_dict[table_key + '.' + table_id_key]


####
#
# Functions which retrieve preliminary information used for creating table
# schemas / ingesting data
#
##
def get_programs_list():
    """
    Get list of programs represented in GDC API master pull.
    :return: List of GDC programs.
    """
    programs_table_id = BQ_PARAMS['WORKING_PROJECT'] + '.' + BQ_PARAMS[
        'METADATA_DATASET'] + \
                        '.' + BQ_PARAMS['GDC_RELEASE'] + '_caseData'

    programs = set()
    results = get_query_results(
        "SELECT distinct(program_name) FROM `{}`".format(programs_table_id))

    for result in results:
        programs.add(result.program_name)

    return programs


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


def get_table_column_order(table):
    if table not in API_PARAMS['TABLE_METADATA']:
        has_fatal_error("'{}' not found in API_PARAMS['TABLE_METADATA']".format(table))
    elif 'column_order' not in API_PARAMS['TABLE_METADATA'][table]:
        has_fatal_error("no column order provided for {} in yaml config.".format(table))

    ordered_table_fields = API_PARAMS['TABLE_METADATA'][table]['column_order']

    return [table + '.' + field for field in ordered_table_fields]


####
#
# Functions used to determine a program's table structure(s)
#
##
def get_excluded_fields(table):
    """
    Get list of fields to exclude from final BQ tables.
    :param table: table key for which to return excluded fields
    :return: list of excluded fields.
    """
    if not API_PARAMS['TABLE_METADATA']:
        has_fatal_error("params['TABLE_METADATA'] not found")

    if 'excluded_fields' not in API_PARAMS['TABLE_METADATA'][table]:
        has_fatal_error("excluded_fields not found in API_PARAMS for {}".format(table))

    excluded_fields = API_PARAMS['TABLE_METADATA'][table]['excluded_fields']

    # return [get_full_field_name(table, field) for field in excluded_fields]
    return excluded_fields


def get_all_excluded_columns():
    excluded_columns = set()

    if not API_PARAMS['TABLE_METADATA']:
        has_fatal_error("params['TABLE_METADATA'] not found")

    if not API_PARAMS['TABLE_ORDER']:
        has_fatal_error("params['TABLE_ORDER'] not found")

    for table in API_PARAMS['TABLE_ORDER']:
        if 'excluded_fields' not in API_PARAMS['TABLE_METADATA'][table]:
            has_fatal_error("excluded_fields not found in API_PARAMS for {}".format(table))

        excluded_fields = API_PARAMS['TABLE_METADATA'][table]['excluded_fields']

        for field in excluded_fields:
            excluded_columns.add(get_bq_name(API_PARAMS, field, table))

    return excluded_columns


def flatten_tables(field_groups, tables):
    """
    From dict containing table_name keys and sets of column names, remove
    excluded columns and merge into parent table if the field group can be
    flattened for this program.
    :param field_groups: dict of tables and columns
    :param tables: set of table names
    :return: flattened table column dict.
    """
    table_columns = dict()

    fg_depths = {fg: get_field_depth(fg) for fg in field_groups}

    for field_group, depth in sorted(fg_depths.items(), key=lambda i: i[1]):
        field_groups[field_group] = remove_excluded_fields(field_groups[field_group],
                                                           field_group)

        full_field_names = {get_full_field_name(field_group, field)
                            for field in field_groups[field_group]}

        if field_group in tables:
            table_columns[field_group] = full_field_names
        else:
            # field group can be flattened
            parent_table = get_parent_table(tables, field_group)

            table_columns[parent_table] |= full_field_names

    return table_columns


def examine_case(table_columns, field_group, record_counts, fg_name):
    for field, record in field_group.items():
        if isinstance(record, list):
            child_fg = fg_name + '.' + field

            if child_fg not in record_counts:
                table_columns[child_fg] = set()
                record_counts[child_fg] = len(record)
            else:
                record_counts[child_fg] = max(record_counts[child_fg], len(record))

            for entry in record:
                table_columns, record_counts = examine_case(table_columns, entry,
                                                            record_counts, child_fg)
        else:
            if fg_name not in table_columns:
                table_columns[fg_name] = set()
                record_counts[fg_name] = 1

            if isinstance(record, dict):
                for child_field in record:
                    table_columns[fg_name].add(child_field)
            else:
                table_columns[fg_name].add(field)

    return table_columns, record_counts


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
        if case:
            field_groups, record_counts = examine_case(field_groups, case,
                                                        record_counts, fg_name='cases')
    tables = get_tables(record_counts)
    table_columns = flatten_tables(field_groups, tables)

    record_counts = {k: v for k, v in record_counts.items() if record_counts[k] > 0}

    return table_columns, tables, record_counts


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
    table_id_key = get_table_id_key(table_key)
    id_column_index = column_order_dict[table_key + '.' + table_id_key]

    field_groups = API_PARAMS['TABLE_ORDER']
    id_index_gap = len(field_groups) - 1

    return id_column_index + id_index_gap


def get_case_id_index(table_key, column_order_dict):
    return get_count_column_index(table_key, column_order_dict) - 1


def generate_id_schema_entry(column, parent_table):
    parent_fg = get_field_name(parent_table)
    source_table = '*_{}'.format(parent_fg) if parent_table != 'cases' else 'main'
    description = ("Reference to the pid ({}) of the record to which this "
                   "record belongs. Parent record found in the program's {} "
                   "table.").format(column, source_table)
    return {
        "name": get_field_name(column),
        "type": 'STRING',
        "description": description,
        "mode": 'NULLABLE'
    }


def generate_count_schema_entry(count_id_key, parent_table_key):
    description = ("Total count of records associated with this case, "
                   "located in {} table").format(parent_table_key)
    return {
        "name": get_field_name(count_id_key),
        "type": 'INTEGER',
        "description": description,
        "mode": 'NULLABLE'
    }


def add_reference_columns(table_columns, schema, record_counts):
    """
    Add reference columns generated by separating and flattening data.

    Possible types:

    - _count column representing # of child records found in supplemental table
    - case_id, used to reference main table records
    - pid, used to reference nearest un-flattened ancestor table

    :param record_counts:
    :param table_columns: dict containing table column keys
    :param schema: dict containing schema records
    :return: table_columns, schema_dict, column_order_dict
    """
    table_orders = dict()
    table_depths = {table: get_field_depth(table) for table in record_counts}
    indexes = build_column_order_dict()

    for table, depth in sorted(table_depths.items(), key=lambda item: item[1]):
        # get ordering for table by only including relevant column indexes
        table_orders[table] = {k: indexes[k] for k in get_table_column_order(table)}

        if depth == 1 or table not in table_columns:
            continue

        ref_column_index = get_id_index(table, table_orders[table]) + 1

        parent_fg = get_parent_field_group(table)
        parent_table = get_parent_table(table_columns.keys(), table)

        # for formerly doubly-nested tables, ancestor id comes before case_id in schema
        if depth > 2:
            # if the depth > 2 cond. (and the case_id insertion below) is removed,
            # tables will only reference direct ancestor
            ancestor_id = parent_fg + '.' + get_table_id_key(parent_fg)

            # add pid to one-to-many table
            schema[ancestor_id] = generate_id_schema_entry(ancestor_id, parent_fg)
            table_columns[table].add(ancestor_id)
            table_orders[table][ancestor_id] = ref_column_index
            ref_column_index += 1

        # add case_id to one-to-many table
        case_id_name = table + '.case_id'
        # case_id_index = get_case_id_index(table, table_orders[table])
        case_id_index = ref_column_index

        schema[case_id_name] = generate_id_schema_entry(case_id_name, 'main')
        table_columns[table].add(case_id_name)
        table_orders[table][case_id_name] = case_id_index

        count_col_index = get_count_column_index(parent_table, table_orders[parent_table])

        # add one-to-many record count column to parent table
        count_name = table + '.count'
        schema[count_name] = generate_count_schema_entry(count_name, parent_table)
        table_columns[parent_table].add(count_name)
        table_orders[parent_table][count_name] = count_col_index

    merged_table_orders = dict()

    for table, depth in sorted(table_depths.items(), key=lambda item: item[1], reverse=True):
        if table not in merged_table_orders:
            merged_table_orders[table] = dict()

        if table in table_columns:
            merged_key = table
        else:
            merged_key = get_parent_table(table_columns.keys(), table)

        if merged_key not in merged_table_orders:
            merged_table_orders[merged_key] = dict()

        merged_table_orders[merged_key].update(table_orders[table])

    return schema, table_columns, merged_table_orders


def rebuild_bq_name(column):
    """
    Reconstruct full column name after it's been abbreviated.
    :param column: abbreviated bq_column name
    :return: column name in field group format ('.' separators rather than '__')
    """

    def get_abbr_dict_():
        abbr_dict_ = dict()

        for table_key, table_metadata in API_PARAMS['TABLE_METADATA'].items():
            if table_metadata['prefix']:
                abbr_dict_[table_metadata['prefix']] = table_key
        return abbr_dict_

    abbr_dict = get_abbr_dict_()
    split_column = column.split('__')
    prefix = '__'.join(split_column[:-1])

    if prefix and abbr_dict[prefix]:
        return abbr_dict[prefix] + '.' + split_column[-1]
    return 'cases.' + split_column[-1]


def prefix_field_names(schema_dict):
    for entry in schema_dict:
        if schema_dict[entry]['name'] == 'case_id':
            continue
        schema_dict[entry]['name'] = get_bq_name(API_PARAMS, entry)

    return schema_dict


def create_schemas(table_columns, tables, record_counts):
    """
    Create ordered schema lists for final tables.
    :param record_counts:
    :param tables:
    :param table_columns: dict containing table column keys
    :return: lists of BQ SchemaFields.
    """
    schema_dict = create_schema_dict(API_PARAMS, BQ_PARAMS, MASTER_TABLE_NAME)
    # modify schema dict, add reference columns for this program
    schema_dict, table_columns, column_orders = add_reference_columns(table_columns,
                                                                      schema_dict,
                                                                      record_counts)
    # merge flattened column orders
    merged_tables = record_counts.keys() - table_columns.keys()
    merged_depths = {table: get_field_depth(table) for table in merged_tables}

    """
    for table, depth in sorted(merged_depths.items(), key=lambda i: i[1], reverse=True):
        if depth == 1:
            has_fatal_error("cases shouldn't be in merged_table list")
        parent_table = get_parent_table(tables, table)
        column_orders[parent_table] |= column_orders[table]
        column_orders.pop(table)
    """

    # add bq abbreviations to schema field dicts
    schema_dict = prefix_field_names(schema_dict)
    schema_field_lists = dict()

    for table in tables:
        # this is just alphabetizing the count columns
        counts_idx = get_count_column_index(table, column_orders[table])
        count_cols = [col for col, i in column_orders[table].items() if i == counts_idx]

        for count_column in sorted(count_cols):
            column_orders[table][count_column] = counts_idx
            counts_idx += 1

        sorted_column_names = [col for col, idx in sorted(column_orders[table].items(),
                                                          key=lambda i: i[1])]
        schema_list = []

        for column in sorted_column_names:
            if column in schema_dict:
                schema_list.append(to_SchemaField(schema_dict[column]))
            else:
                print("{} not found in src table, excluding schema field.".format(column))

        schema_field_lists[table] = schema_list

    return schema_field_lists, column_orders


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
    elif isinstance(record, dict):
        excluded_fields = {get_bq_name(API_PARAMS, field, table)
                           for field in excluded_fields}
        for field in record.copy():
            if field in excluded_fields or not record[field]:
                record.pop(field)
        return record
    else:
        return [field for field in record if field not in excluded_fields]


####
#
# Functions used for parsing and loading data into BQ tables
#
##
def flatten_case_entry(record, field_group, flat_case, case_id, pid, pid_field):
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
    if isinstance(record, list):
        # flatten each record in field group list
        for entry in record:
            flat_case = flatten_case_entry(entry, field_group, flat_case,
                                           case_id, pid, pid_field)
    else:
        row_dict = dict()
        id_field = get_table_id_key(field_group)

        for field, field_val in record.items():
            if isinstance(field_val, list):
                flat_case = flatten_case_entry(
                    record=field_val,
                    field_group=field_group + '.' + field,
                    flat_case=flat_case,
                    case_id=case_id,
                    pid=record[id_field],
                    pid_field=id_field)
            else:
                if id_field != pid_field:
                    parent_fg = get_parent_field_group(field_group)
                    pid_column = get_bq_name(API_PARAMS, pid_field, parent_fg)
                    row_dict[pid_column] = pid

                if id_field != 'case_id':
                    row_dict['case_id'] = case_id
                # Field converted bq column name
                column = get_bq_name(API_PARAMS, field, field_group)
                row_dict[column] = field_val
        if field_group not in flat_case:
            flat_case[field_group] = list()

        excluded_columns = get_all_excluded_columns()

        if row_dict:
            for field in row_dict.copy():
                if field in excluded_columns or not row_dict[field]:
                    row_dict.pop(field)
        flat_case[field_group].append(row_dict)

    return flat_case


def flatten_case(case):
    """
    Converts nested case object into a flattened representation of its records.
    :param case: dict containing case data
    :return: flattened case dict
    """
    return flatten_case_entry(record=case,
                              field_group='cases',
                              flat_case=dict(),
                              case_id=case['case_id'],
                              pid=case['case_id'],
                              pid_field='case_id')


'''
def merge_single_entry_field_groups(flattened_case, bq_program_tables):
    """
    Merge field groups which have a max of one record for every case in this
    program.
    These columns will be located in parent table.
    :param flattened_case: flattened dictionary for case (used to
    recursively capture the record as it's parsed).
    :param bq_program_tables: list of tables to be created for this program.
    :return: flattened_case_dict with single record tables merged.
    """
    fg_depths = {fg: get_field_depth(fg) for fg in flattened_case.keys()}

    for fg_key, fg_depth in sorted(fg_depths.items(), key=lambda item: item[1], reverse=True):
        # cases is the master table, merged into
        if fg_depth == 1:
            break

        parent_table = get_parent_table(flattened_case.keys(), fg_key)
        pid_key = get_table_id_key(parent_table)
        pid_column = get_bq_name(API_PARAMS, pid_key, parent_table)

        # don't merge
        if fg_key in bq_program_tables:
            max_record_count = dict()
            idx = 0

            for entry in flattened_case[parent_table].copy():
                if pid_key not in entry and pid_column not in entry:
                    has_fatal_error("No id key found, in bq or fg format.")

                entry_id = entry[pid_key] if pid_key in entry else entry[pid_column]

                if entry_id not in max_record_count:
                    max_record_count[entry_id] = {'entry_idx': idx, 'record_count': 0}
                    idx += 1

            field_group = flattened_case[fg_key].copy()

            for record in field_group:
                if pid_column in record:
                    pid = record[pid_column]
                    max_record_count[pid]['record_count'] += 1
            for pid in max_record_count:
                entry_idx = max_record_count[pid]['entry_idx']
                count_id = get_bq_name(API_PARAMS, 'count', fg_key)

                flattened_case[parent_table][entry_idx][count_id] = max_record_count[pid]['record_count']
        # merge
        else:
            field_group = flattened_case.pop(fg_key)[0]

            if len(field_group) == 0:
                continue
            if 'case_id' in field_group:
                field_group.pop('case_id')
            # include keys with values
            for key, fg_val in field_group.items():
                flattened_case[parent_table][0][key] = fg_val
    return flattened_case
'''


def merge_single_entry_field_groups(flattened_case, bq_program_tables):
    """
    Merge field groups which have a max of one record for every case in this
    program.
    These columns will be located in parent table.
    :param flattened_case: flattened dictionary for case (used to
    recursively capture the record as it's parsed).
    :param bq_program_tables: list of tables to be created for this program.
    :return: flattened_case_dict with single record tables merged.
    """
    fg_depths = {fg: get_field_depth(fg) for fg in flattened_case.keys()}

    for fg_key, fg_depth in sorted(fg_depths.items(), key=lambda item: item[1], reverse=True):
        # cases is the master table, merged into
        if fg_depth == 1:
            break

        parent_table = get_parent_table(flattened_case.keys(), fg_key)
        parent_fg = get_parent_field_group(fg_key)
        parent_fg_id_key = get_table_id_key(parent_fg)
        ancestor_column = get_bq_name(API_PARAMS, parent_fg_id_key, parent_fg)

        # merge into parent table record
        if fg_key not in bq_program_tables:
            field_group = flattened_case.pop(fg_key)[0]

            if len(field_group) == 0:
                continue
            if 'case_id' in field_group:
                field_group.pop('case_id')
            if ancestor_column in field_group:
                field_group.pop(ancestor_column)

            # include keys with values
            for key, fg_val in field_group.items():
                flattened_case[parent_table][0][key] = fg_val
    return flattened_case


def assign_record_counts(flattened_case, tables, case_id_counts):
    fg_depths = {fg: get_field_depth(fg) for fg in flattened_case.keys()}

    # todo, start from children or parent?
    fg_entry_counts = dict()
    for fg, depth in sorted(fg_depths.items(), key=lambda i: i[1]):
        if depth == 1:
            case_id = fg[0]['case_id']
            continue

        parent_field_group = get_parent_field_group(fg)
        parent_fg_table_id_key = get_table_id_key(parent_field_group)

        # used to create the ancestor's id field
        parent_fg_id_field = get_bq_name(API_PARAMS, parent_fg_table_id_key)
        # used to insert the count into parent table
        parent_table = get_parent_table(tables, fg)

        # todo delete print
        print("parent_table: {}".format(parent_table))
        parent_fg_id_column = get_bq_name(API_PARAMS, parent_fg_id_field, parent_table)

        # todo delete print
        print("parent_fg_id_column: {}".format(parent_fg_id_column))

        fg_ids = dict()

        for entry in flattened_case[fg]:
            parent_fg_id = entry[parent_fg_id_column]
            if parent_fg_id not in fg_ids:
                fg_ids[parent_fg_id] = 1
            else:
                fg_ids[parent_fg_id] += 1

        fg_entry_counts[fg] = fg_ids

    case_id_counts[case_id] = fg_entry_counts

    return case_id_counts


def create_and_load_tables(program_name, cases, schemas, tables):
    """
    Create jsonl row files for future insertion, store in GC storage bucket,
    then insert the new table schemas and data.
    :param program_name: program for which to create tables
    :param cases: case records to insert into BQ for program
    :param schemas: dict of schema lists for all of this program's tables
    :param tables: set of table names
    """
    print("\nInserting case records...")
    for table in tables:
        jsonl_file_path = get_temp_filepath(program_name, table)
        # delete last jsonl scratch file so we don't append to it
        if os.path.exists(jsonl_file_path):
            os.remove(jsonl_file_path)

    case_id_counts = dict()

    for case in cases:
        flattened_case_dict = flatten_case(case)
        case_id_counts = assign_record_counts(flattened_case_dict, tables, case_id_counts)
        flattened_case_dict = merge_single_entry_field_groups(flattened_case_dict, tables)

        for table in flattened_case_dict.keys():
            if table not in tables:
                has_fatal_error("Table {} not found in table keys".format(table))

            jsonl_fp = get_temp_filepath(program_name, table)

            with open(jsonl_fp, 'a') as jsonl_file:
                for row in flattened_case_dict[table]:
                    json.dump(obj=row, fp=jsonl_file)
                    jsonl_file.write('\n')

    print(case_id_counts)

    for table in tables:
        jsonl_file = get_jsonl_filename(program_name, table)
        table_id = get_full_table_name(program_name, table)

        upload_to_bucket(BQ_PARAMS, API_PARAMS['TEMP_PATH'], jsonl_file)
        create_and_load_table(BQ_PARAMS, jsonl_file, schemas[table], table_id)


####
#
# Functions for creating documentation
#
##
def generate_documentation(documentation_dict):
    """
    Dump gathered documentation data into json file
    :param documentation_dict:
    :return:
    """
    json_doc_file = BQ_PARAMS['GDC_RELEASE'] + '_' + TABLE_NAME_PREFIX
    json_doc_file += '_json_documentation_dump.json'

    with open(API_PARAMS['TEMP_PATH'] + '/' + json_doc_file, 'w') as json_file:
        json.dump(documentation_dict, json_file)

    upload_to_bucket(BQ_PARAMS, API_PARAMS['TEMP_PATH'], json_doc_file)


####
#
# Script execution
#
##
def print_final_report(start, steps):
    """
    Outputs a basic report of script's results, including total processing
    time and which steps
    were specified in YAML.
    :param start: float representing script's start time.
    :param steps: set of steps to be performed (configured in YAML)
    """
    seconds = time.time() - start
    minutes = math.floor(seconds / 60)
    seconds -= minutes * 60

    print("Programs script executed in {} min, {:.0f} sec\n".format(minutes,
                                                                    seconds))
    print("Steps completed: ")

    if 'create_and_load_tables' in steps:
        print('\t - created tables and inserted data')
    if 'validate_data' in steps:
        print('\t - validated data (tests not considered exhaustive)')
    if 'generate_documentation' in steps:
        print('\t - generated documentation')
    print('\n\n')


def main(args):
    """

    :param args:
    :return:
    """
    start = time.time()
    steps = []
    documentation_dict = dict()

    # Load YAML configuration
    if len(args) != 2:
        has_fatal_error("Usage: {} <configuration_yaml>".format(args[0]), ValueError)

    with open(args[1], mode='r') as yaml_file:
        try:
            global API_PARAMS, BQ_PARAMS
            API_PARAMS, BQ_PARAMS, steps = load_config(yaml_file, YAML_HEADERS)
        except ValueError as err:
            has_fatal_error(str(err), ValueError)

    # programs = get_programs_list()
    programs = ['HCMI']

    for program in programs:
        prog_start = time.time()
        print("Executing script for program {}...".format(program))

        cases = get_cases_by_program(BQ_PARAMS, MASTER_TABLE_NAME, program)

        if not cases:
            print("Skipping program {}, no cases found.")
            continue

        # derive the program's table structure by analyzing its case records
        table_columns, tables, record_counts = find_program_structure(cases)

        if 'create_and_load_tables' in steps:
            # generate table schemas
            table_schemas, table_order_lists = create_schemas(table_columns, tables, record_counts)

            # create tables, flatten and insert data
            create_and_load_tables(program, cases, table_schemas, tables)

            print("{} processed in {:0.1f} seconds!\n".
                  format(program, time.time() - prog_start))

            if 'generate_documentation' in steps:
                table_ids = {table: get_table_id(BQ_PARAMS, table) for table in tables}

                # converting to JSON serializable form
                table_column_lists = {t: list(v) for t, v in table_columns.items()}

                documentation_dict[program] = {
                    'table_schemas': str(table_schemas),
                    'table_columns': table_column_lists,
                    'table_ids': table_ids,
                    'table_order_dict': table_order_lists
                }

    if 'generate_documentation' in steps:
        documentation_dict['metadata'] = dict()
        documentation_dict['metadata']['API_PARAMS'] = API_PARAMS
        documentation_dict['metadata']['BQ_PARAMS'] = BQ_PARAMS
        # documentation_dict['metadata']['schema_dict'] = schema_dict

        generate_documentation(documentation_dict)

    if 'validate_data' in steps:
        pass

    print_final_report(start, steps)


if __name__ == '__main__':
    main(sys.argv)

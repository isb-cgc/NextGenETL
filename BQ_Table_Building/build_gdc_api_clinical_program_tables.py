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
import os
import json
import time
import sys

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from common_etl.utils import (get_rel_prefix, has_fatal_error, get_scratch_fp,
                              create_and_load_table_from_jsonl, write_list_to_jsonl, upload_to_bucket,
                              exists_bq_table, construct_table_id, update_table_metadata, get_filepath,
                              create_view_from_query, update_schema, list_bq_tables, format_seconds,
                              load_config, construct_table_name_from_list, publish_table, construct_table_name)

from common_etl.support import compare_two_tables_sql, bq_harness_with_result

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


#   Getter functions, employed for readability/consistency


def make_program_list_query():
    """

    Create program list query.
    :return: program list query string
    """
    case_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.GDC_metadata.rel{API_PARAMS['RELEASE']}_caseData"

    return f"""
        SELECT DISTINCT program_name
        FROM {case_table_id}
        ORDER BY program_name
    """


def get_program_list():
    """

    Get list of the programs which have contributed data to GDC's research program.
    :return: list of research programs participating in GDC data sharing
    """

    return [res[0] for res in bq_harness_with_result(make_program_list_query(), BQ_PARAMS['DO_BATCH'])]


def get_one_to_many_tables(record_counts):
    """

    Get one-to-many tables that will be created for program.
    :param record_counts: dict max field group record counts for program
    :return: set of table names (representing field groups which cannot be flattened)
    """
    table_keys = {API_PARAMS['FG_CONFIG']['base_fg']}

    for table in record_counts:
        if record_counts[table] > 1:
            table_keys.add(table)

    return table_keys


def get_full_table_name(program, table):
    """

    Get the full name used in table_id for a given table.
    :param program: name of the program to with the data belongs
    :param table: Name of desired table
    :return: String representing table name used by BQ.
    """
    table_name = [get_rel_prefix(API_PARAMS), program, BQ_PARAMS['MASTER_TABLE']]

    # if one-to-many table, append suffix
    suffixes = get_table_suffixes()
    suffix = suffixes[table]

    if suffix:
        table_name.append(suffix)

    return construct_table_name_from_list(table_name)


def get_bq_name(field, arg_fg=None):
    """

    Get column name (in bq format) from full field name.
    :param field: if not table_path, full field name; else short field name
    :param arg_fg: field group containing field
    :return: bq column name for given field name
    """

    def get_fgs_and_id_keys():
        """

        Create a dictionary of type { 'field_group' : 'id_key_field'}.
        :return: mapping dict, field group -> id_key_field
        """
        id_key_dict = dict()
        fg_config_entries = API_PARAMS['FIELD_CONFIG']

        for _fg in fg_config_entries:
            id_key_dict[_fg] = fg_config_entries[_fg]['id_key']

        return id_key_dict

    base_fg = API_PARAMS['FG_CONFIG']['base_fg']

    if arg_fg:
        # field group is specified as a function argument
        fg = arg_fg
        field_key = merge_fg_and_field(fg, field)
    elif len(field.split('.')) == 1:
        # no fg delimiter found in field string: cannot be a complete field key
        fg = base_fg
        field_key = merge_fg_and_field(fg, field)
    else:
        # no fg argument, but field contains separator chars; extract the fg and name

        # remove field from period-delimited field group string
        fg = ".".join(field.split('.')[:-1])
        field_key = field

    # derive the key's short field name
    field_name = get_field_name(field_key)

    # get id_key and prefix associated with this fg
    this_fg_id = get_field_group_id_key_name(fg)
    prefix = API_PARAMS['FIELD_CONFIG'][fg]['prefix']

    # create map of {fg_names : id_keys}
    fg_to_id_key_map = get_fgs_and_id_keys()

    # if fg has no prefix, or
    #    field is child of base_fg, or
    #    function called for webapp table building: do not add prefix
    if fg == base_fg or not prefix:
        return field_name

    # if field is an id_key, but is not mapped to this fg: do not add prefix
    if field_name in fg_to_id_key_map.values() and field_name != this_fg_id:
        return field_name

    # if the function reaches this line, return a prefixed field:
    #  - the table is user-facing, and
    #  - this field isn't a foreign id key
    return "__".join([prefix, field_name])


def parse_bq_schema_obj(schema, fg, schema_list=None):
    """

    Recursively construct schema using existing metadata in main clinical table.
    :param schema: dict of flattened schema entries
    :param fg: current field group name
    :param schema_list: schema field entries for field_group
    """

    if fg not in API_PARAMS['FIELD_CONFIG']:
        return

    for i, schema_field in enumerate(schema_list):

        field_key = merge_fg_and_field(fg, schema_field['name'])

        # if has 'fields', then the current obj contains nested objs
        if schema_field['type'] == 'RECORD':
            # if nested, recurse down to the next level
            parse_bq_schema_obj(schema, field_key, schema_field['fields'])

            field_config = API_PARAMS['FIELD_CONFIG']
        else:
            # not a nested field entry--do we need to prefix the schema field name?
            # schema_field['name'] = get_bq_name(field_key)
            schema_field['name'] = field_key.split('.')[-1]
            schema[field_key] = schema_field


def create_schema_dict():
    """

    Create schema dict using master table's bigquery.table.Table.schema attribute.
    :return: flattened schema dict in format:
        {full field name: {name: 'name', type: 'field_type', description: 'description'}}
    """
    client = bigquery.Client()
    bulk_table_name = construct_table_name(API_PARAMS,
                                           prefix=get_rel_prefix(API_PARAMS),
                                           suffix=BQ_PARAMS['MASTER_TABLE'],
                                           include_release=False)
    bulk_table_id = construct_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_DATASET'], bulk_table_name)
    bq_table = client.get_table(bulk_table_id)

    schema_list = []

    for schema_field in bq_table.schema:
        schema_list.append(schema_field.to_api_repr())

    schema = dict()

    parse_bq_schema_obj(schema, API_PARAMS['FG_CONFIG']['base_fg'], schema_list)

    return schema


def get_table_suffixes():
    """Get abbreviations for field groups as designated in yaml config.

    :return: dict of {field_group: abbreviation_suffix}
    """
    suffixes = dict()

    for table, metadata in API_PARAMS['FIELD_CONFIG'].items():
        suffixes[table] = metadata['table_suffix'] if metadata['table_suffix'] else ''

    return suffixes


def build_column_order_dict():
    """
    Using table order provided in YAML, with additional ordering for reference
    columns added during one-to-many table creation.
    :return: dict of str column names : int representing position.
    """
    column_orders = dict()
    # list of fields in order, grouped by field_grp
    field_grp_order_list = API_PARAMS['FG_CONFIG']['order']

    # leave room to add reference columns (foreign ids, one-to-many record counts)
    id_index_gap = ((len(field_grp_order_list) - 1) * 2)

    idx = 0

    # assign indexes to each field, in order of field_grp and field precedence
    for field_grp in field_grp_order_list:
        field_order_list = API_PARAMS['FIELD_CONFIG'][field_grp]['column_order']
        field_grp_id_name = API_PARAMS['FIELD_CONFIG'][field_grp]['id_key']

        for field_name in field_order_list:
            field_key = merge_fg_and_field(field_grp, field_name)

            # assign index to field, then increment
            column_orders[field_key] = idx
            idx += 1 if field_name != field_grp_id_name else id_index_gap

    # is this still necessary? experiment
    for end_field in API_PARAMS['FG_CONFIG']['last_keys_in_table']:
        column_orders[end_field] = idx
        idx += 1

    return column_orders


def flatten_tables(field_groups, record_counts):
    """

    From dict containing table_name keys and sets of column names, remove
    excluded columns and merge into parent table if the field group can be
    flattened for this program.
    :param field_groups: dict of tables and columns
    :param record_counts: set of table names
    :return: flattened table column dict.
    """
    tables = get_one_to_many_tables(record_counts)  # supplemental tables required
    table_columns = dict()

    field_grp_depths = {field_grp: len(field_grp.split('.')) for field_grp in field_groups.keys()}
    excluded_fields = get_excluded_fields_all_fgs(field_groups)

    for field_grp, depth in sorted(field_grp_depths.items(), key=lambda i: i[1]):

        if depth > 3:
            print("\n[INFO] Caution, not confirmed to work with nested depth > 3\n")

        field_groups[field_grp] = remove_excluded_fields(field_groups[field_grp], field_grp, excluded_fields)
        field_keys = {merge_fg_and_field(field_grp, field) for field in field_groups[field_grp]}
        if field_grp in tables:
            table_columns[field_grp] = field_keys
        else:
            # field group can be flattened
            parent_table = get_parent_fg(tables, field_grp)
            table_columns[parent_table] |= field_keys

    return table_columns


def examine_case(set_fields, record_counts, field_grp, field_grp_name):
    """

    Recursively examines case and updates dicts of non-null fields and max record counts.
    :param set_fields: current dict of non-null fields for each field group
    :param record_counts: dict of max field group record counts observed in program so far
    :param field_grp: whole or partial case record json object
    :param field_grp_name: name of currently-traversed field group
    :return: dicts of non-null field lists and max record counts (keys = field groups)
    """
    fgs = API_PARAMS['FIELD_CONFIG'].keys()

    if field_grp_name in fgs:
        for field, record in field_grp.items():

            if isinstance(record, list):

                child_field_grp = field_grp_name + '.' + field

                if child_field_grp not in fgs:
                    continue

                if child_field_grp not in record_counts:
                    set_fields[child_field_grp] = set()
                    record_counts[child_field_grp] = len(record)
                else:
                    record_counts[child_field_grp] = max(record_counts[child_field_grp],
                                                         len(record))

                for entry in record:
                    examine_case(set_fields, record_counts, entry, child_field_grp)
            else:
                if field_grp_name not in set_fields:
                    set_fields[field_grp_name] = set()
                    record_counts[field_grp_name] = 1

                if isinstance(record, dict):
                    for child_field in record:
                        set_fields[field_grp_name].add(child_field)
                else:
                    if record:
                        set_fields[field_grp_name].add(field)


def find_program_structure(cases):
    """

    Determine table structure required for the given program.
    :param cases: dict of program's case records
    :return: dict of tables and columns, dict with maximum record count for this program's field groups.
    """

    fgs = {}
    record_counts = {}

    for case in cases:
        if case:
            examine_case(fgs, record_counts, case, API_PARAMS['FG_CONFIG']['base_fg'])

    for field_grp in fgs:
        if field_grp not in API_PARAMS['FIELD_CONFIG']:
            print(f"{field_grp} not in metadata")
            fgs.pop(field_grp)
            cases.pop(field_grp)

    columns = flatten_tables(fgs, record_counts)

    record_counts = {k: v for k, v in record_counts.items() if record_counts[k] > 0}

    return columns, record_counts


def get_parent_fg(tables, field_name):
    """

    Get field's parent table name.
    :param tables: list of table names for program
    :param field_name: full field name for which to retrieve parent table
    :return: parent table name
    """
    # remove field from period-delimited field group string
    parent_table = ".".join(field_name.split('.')[:-1])

    while parent_table and parent_table not in tables:
        # remove field from period-delimited field group string
        parent_table = ".".join(parent_table.split('.')[:-1])

    if not parent_table:
        has_fatal_error(f"No parent fg found for {field_name}")
    return parent_table


def get_field_name(field_col_key):
    """

    Get short field name from full field or bq column name.
    :param field_col_key: full field or bq column name
    :return: short field name
    """
    if '.' not in field_col_key and '__' not in field_col_key:
        return field_col_key

    split_char = '.' if '.' in field_col_key else '__'

    return field_col_key.split(split_char)[-1]


def get_field_group_id_key_name(field_group):
    """

    Retrieve the id key used to uniquely identify a table record.
    :param field_group: table for which to determine the id key
    :return: str representing table key
    """
    fg_id_key = get_long_field_group_id_key(field_group)
    return get_field_name(fg_id_key)


def merge_fg_and_field(field_group, field):
    """

    Get full field key ("{field_group}.{field_name}"}.
    :param field_group: field group to which the field belongs
    :param field: field name
    :return: full field key string
    """
    return f'{field_group}.{field}'


def get_long_field_group_id_key(field_group):
    """

    Retrieve the id key used to uniquely identify a table record.
    :param field_group: table for which to determine the id key
    :return: str representing table key
    """
    split_fg = field_group.split('.')

    if split_fg[0] != API_PARAMS['FG_CONFIG']['base_fg']:
        split_fg.insert(0, API_PARAMS['FG_CONFIG']['base_fg'])
        field_group = ".".join(split_fg)

    long_fg_id_key = merge_fg_and_field(field_group, API_PARAMS['FIELD_CONFIG'][field_group]['id_key'])

    return long_fg_id_key


def get_sorted_fg_depths(record_counts, reverse=False):
    """

    Return a sorted dict of field groups: depths.
    :param record_counts: dict containing field groups and associated record counts
    :param reverse: if True, sort in DESC order, otherwise sort in ASC order
    :return: tuples composed of field group names and record counts
    """
    table_depths = {table: len(table.split('.')) for table in record_counts}

    return sorted(table_depths.items(), key=lambda item: item[1], reverse=reverse)


#   Schema creation


def get_count_column_index(table_name, column_order_dict):
    """

    Get index of child table record count reference column.
    :param table_name: table for which to get index
    :param column_order_dict: dict containing column indexes
    :return: count column start idx position
    """
    table_id_key = get_field_group_id_key_name(table_name)
    id_column_index = column_order_dict[table_name + '.' + table_id_key]

    field_groups = API_PARAMS['FG_CONFIG']['order']
    id_index_gap = len(field_groups) - 1

    return id_column_index + id_index_gap


def generate_id_schema_entry(column, parent_table, program):
    """

    Create schema entry for inserted parent reference id.
    :param column: parent id column
    :param parent_table: parent table name
    :param program: program name
    :return: schema entry dict for new reference id field
    """
    field_name = get_field_name(column)

    if field_name == 'case_id':
        bq_col_name = 'case_id'
        source_table = get_full_table_name(program, API_PARAMS['FG_CONFIG']['base_fg'])
    else:
        bq_col_name = get_bq_name(column)
        source_table = get_full_table_name(program, parent_table)

    return {
        "name": get_field_name(column),
        "type": 'STRING',
        "description": f"Reference to ancestor {bq_col_name}, located in {source_table}.",
        "mode": 'REQUIRED'
    }


def generate_count_schema_entry(count_id_key, parent_table):
    """

    Create schema entry for one-to-many record count field.
    :param count_id_key: count field name
    :param parent_table: parent table name
    :return: schema entry dict for new one-to-many record count field
    """
    description = f"Total child record count (located in {parent_table} table)."

    return {
        "name": get_field_name(count_id_key),
        "type": 'INTEGER',
        "description": description,
        "mode": 'NULLABLE'
    }


def insert_ref_id_keys(schema, columns, column_order, field_grp, id_tuple):
    """

    Add reference id fields to the tables under construction.
    :param schema: dict containing schema records
    :param columns: dict containing table column keys
    :param column_order: dict containing a list of fields (columns) and the order in
    which they will be displayed in the db.
    :param field_grp: a collection of GDC fields.
    :param id_tuple: the field group id's index, key, and program name
    """
    # add parent id to one-to-many table
    field_grp_id_idx, field_grp_id_key, program = id_tuple

    # remove field from period-delimited field group string
    parent_field_group = ".".join(field_grp.split('.')[:-1])

    # add reference id field to schema
    schema[field_grp_id_key] = generate_id_schema_entry(field_grp_id_key, parent_field_group, program)
    columns[field_grp].add(field_grp_id_key)
    column_order[field_grp][field_grp_id_key] = field_grp_id_idx


def add_record_count_refs(schema, columns, column_order, table):
    """

     Add reference columns containing record counts for associated BQ tables.
    :param schema: dict containing schema records
    :param columns: dict containing table column keys
    :param column_order: dict containing relative position of all fields in  the db
    :param table: Name of a table located in BQ, associated with one or more GDC field
    groups
    """
    # add one-to-many record count column to parent table
    parent_table = get_parent_fg(columns.keys(), table)
    count_field = table + '.count'

    schema[count_field] = generate_count_schema_entry(count_field, parent_table)
    columns[parent_table].add(count_field)
    count_column_index = get_count_column_index(parent_table, column_order[parent_table])
    column_order[parent_table][count_field] = count_column_index


def add_ref_columns(columns, record_counts, schema=None, program=None):
    """

    Add reference columns generated by separating and flattening data.

    Possible types:

    - _count column representing # of child records found in supplemental table
    - case_id, used to reference main table records
    - pid, used to reference nearest un-flattened ancestor table

    :param columns: dict containing table column keys
    :param record_counts: field group count dict
    :param schema: dict containing schema records
    :param program: the program from which the cases originate.
    the insertion of irrelevant reference columns
    :return: table_columns, schema_dict, column_order_dict
    """
    column_orders = dict()

    if not program or not schema:
        has_fatal_error("invalid arguments for add_reference_columns; schema and program are required.", ValueError)

    # get relative index of every field, across tables/field groups, in non-nested dict
    field_indexes = build_column_order_dict()

    for field_grp, depth in get_sorted_fg_depths(record_counts):

        # get ordered list for each field_grp
        fg_params = API_PARAMS['FIELD_CONFIG'][field_grp]
        ordered_field_grp_field_keys = [merge_fg_and_field(field_grp, field) for field in fg_params['column_order']]

        # for a given field_grp, assign each field a global index; insert into
        # segregated column order dict (e.g. column_orders[field_grp][field] = idx)
        column_orders[field_grp] = {f: field_indexes[f] for f in ordered_field_grp_field_keys}

        if depth == 1 or field_grp not in columns:
            continue

        # get id key for current field group, and its index position
        table_id_key = get_long_field_group_id_key(field_grp)
        id_idx = column_orders[field_grp][table_id_key]

        # get parent id key, append to column order dict of child field group, increment
        # index from field_grp_id_key's position, assign to parent_id_key (foreign
        # reference key)

        # remove field from period-delimited field group string
        field_group_key = ".".join(field_grp.split('.')[:-1])

        parent_id_key = get_long_field_group_id_key(field_group_key)

        idx = id_idx
        # if not webapp, there are additional reference columns to insert
        # (count of foreign records associated with the current field_grp)
        if depth > 2:
            insert_ref_id_keys(schema, columns, column_orders, field_grp, (idx, parent_id_key, program))
            idx += 1

        base_field_grp_id_key = get_long_field_group_id_key(API_PARAMS['FG_CONFIG']['base_fg'])

        insert_ref_id_keys(schema, columns, column_orders, field_grp, (idx, base_field_grp_id_key, program))
        idx += 1

        add_record_count_refs(schema, columns, column_orders, field_grp)

    return column_orders


def merge_column_orders(schema, columns, record_counts, column_orders):
    """

    Merge flattenable column order dicts
    :param schema: dict containing schema records
    :param columns: dict containing table column keys
    :param record_counts: field group count dict
    :param column_orders: dict of field groups : and fields with their respective indices
    :return: merged column orders dict
    """
    merged_column_orders = dict()

    for table, depth in get_sorted_fg_depths(record_counts, reverse=True):

        table_id_key = get_long_field_group_id_key(table)

        if table in columns:
            merge_dict_key = table

            schema[table_id_key]['mode'] = 'REQUIRED'
        else:
            # not a standalone table, merge
            merge_dict_key = get_parent_fg(columns.keys(), table)
            # if merging key into parent table, that key is no longer required, might
            # not exist in some cases
            schema[table_id_key]['mode'] = 'NULLABLE'

        if merge_dict_key not in merged_column_orders:
            merged_column_orders[merge_dict_key] = dict()

        merged_column_orders[merge_dict_key].update(column_orders[table])

    return merged_column_orders


def remove_null_fields(columns, merged_orders):
    """

    Remove fields composed of only null values for a program, thus making the tables less sparse.
    :param columns: dict containing table column keys
    :param merged_orders: merged dict of field groups: fields with index position data
    """
    for table, cols in columns.items():
        null_fields_set = set(merged_orders[table].keys()) - cols

        for field in null_fields_set:
            merged_orders[table].pop(field)


def create_schema_lists(schema, record_counts, merged_orders):
    """

    Create smaller schemas for each table, containing only columns contained there.
    :param schema: dict containing schema records
    :param record_counts: field group count dict
    :param merged_orders: merged dict of field groups: fields with index position data
    :return: schema_field_lists_dict, one schema per field group turned into table
    """
    # add bq abbreviations to schema field dicts
    for entry in schema:
        field = get_field_name(entry)

        if field != 'case_id':
            schema[entry]['name'] = get_bq_name(entry)

    schema_field_lists_dict = dict()

    for table in get_one_to_many_tables(record_counts):
        # this alphabetizes the count columns
        counts_idx = get_count_column_index(table, merged_orders[table])
        count_cols = [col for col, i in merged_orders[table].items() if i == counts_idx]

        for count_column in sorted(count_cols):
            merged_orders[table][count_column] = counts_idx
            counts_idx += 1

        schema_field_lists_dict[table] = list()

        # sort merged table columns by index

        for column in [col for col, idx in sorted(merged_orders[table].items(), key=lambda i: i[1])]:
            if column not in schema:
                print(f"{column} not in src table; excluding schema field.")
                continue
            schema_field_lists_dict[table].append(bigquery.SchemaField.from_api_repr(schema[column]))

    return schema_field_lists_dict


def get_excluded_fields_all_fgs(fgs):
    """

    Get a list of fields for each field group to exclude from the tables
    from yaml config (API_PARAMS['FIELD_CONFIG']['excluded_fields'] or
    API_PARAMS['FIELD_CONFIG']['app_excluded_fields'] for the web app).
    :param fgs: list of expand field groups included from API call
    :return: set of fields to exclude
    """
    excluded_list_key = 'excluded_fields'

    exclude_fields = set()

    for fg in fgs:
        if not API_PARAMS['FIELD_CONFIG'][fg] or not API_PARAMS['FIELD_CONFIG'][fg][excluded_list_key]:
            continue

        for field in API_PARAMS['FIELD_CONFIG'][fg][excluded_list_key]:
            exclude_fields.add(merge_fg_and_field(fg, field))

    return exclude_fields


def remove_excluded_fields(case, field_grp, excluded):
    """
    Remove columns with only None values, as well as those excluded.
    :param case: field_grp record to parse.
    :param field_grp: name of destination table.
    :param excluded: set of fields to exclude from the final db tables
    :return: Trimmed down record dict.
    """
    if isinstance(case, dict):
        excluded_fields = {get_bq_name(field, field_grp)
                           for field in excluded}

        for field in case.copy().keys():
            if field in excluded_fields or not case[field]:
                case.pop(field)

        return case

    if isinstance(case, set):
        return {field for field in case if field not in excluded}

    return [field for field in case if field not in excluded]


#   Functions used for parsing and loading data into BQ tables


def flatten_case_entry(record, fg, flat_case, case_id, pid, pid_name):
    """

    Recursively traverse the case json object, creating dict of format: {field_group: [records]}
    :param record: the case data object to recurse and flatten
    :param fg: name of the case's field group currently being processed.
    :param flat_case: partially-built flattened case dict
    :param case_id: case id
    :param pid: parent field group id
    :param pid_name: parent field group id key
    """

    def get_excluded_fields_one_fg():
        excluded_key = 'excluded_fields'
        excluded_list = API_PARAMS['FIELD_CONFIG'][fg][excluded_key]

        if excluded_list:
            return [get_bq_name(f, fg) for f in excluded_list]
        return []

    # entry represents a field group, recursively flatten each record
    if fg not in API_PARAMS['FIELD_CONFIG'].keys():
        return

    base_pid_name = get_field_group_id_key_name(API_PARAMS['FG_CONFIG']['base_fg'])

    if isinstance(record, list):
        # flatten each record in field group list
        for entry in record:
            flatten_case_entry(entry, fg, flat_case, case_id, pid, pid_name)
        return
    else:
        row = dict()

        fg_id_name = get_field_group_id_key_name(fg)

        for field, columns in record.items():
            # if list, possibly more than one entry, recurse over list
            if isinstance(columns, list):
                flatten_case_entry(record=columns, fg=merge_fg_and_field(fg, field), flat_case=flat_case,
                                   case_id=case_id, pid=record[fg_id_name], pid_name=fg_id_name)
                continue
            else:
                if fg_id_name != pid_name:
                    # remove field from period-delimited field group string
                    split_parent_fg = fg.split('.')[:-1]
                    parent_fg = ".".join(split_parent_fg)
                    pid_key = get_bq_name(pid_name, parent_fg)

                    # add parent_id key and value to row
                    row[pid_key] = pid

                if fg_id_name != base_pid_name:
                    row[base_pid_name] = case_id

                column = get_bq_name(field, fg)

                row[column] = columns

            if fg not in flat_case:
                # if this is first row added for fg, create an empty list
                # to hold row objects
                flat_case[fg] = list()

            if row:
                excluded = get_excluded_fields_one_fg()

                for row_field in row.copy().keys():
                    # if field is in the excluded list, or is Null, exclude from flat_case
                    if row_field in excluded or not row[row_field]:
                        row.pop(row_field)

        flat_case[fg].append(row)


def flatten_case(case):
    """

    Convert nested case object into a flattened representation of its records.
    :param case: dict containing case data
    :return: flattened case dict
    """

    base_fg = API_PARAMS['FG_CONFIG']['base_fg']
    get_long_field_group_id_key(base_fg)
    base_id_name = get_field_group_id_key_name(base_fg)

    flat_case = dict()

    flatten_case_entry(record=case,
                       fg=base_fg,
                       flat_case=flat_case,
                       case_id=case[base_id_name],
                       pid=case[base_id_name],
                       pid_name=base_id_name)

    return flat_case


def get_record_idx(flat_case, field_grp, record_id):
    """
    Get index of record associated with record_id from flattened_case
    :param flat_case: dict containing {field group names: list of record dicts}
    :param field_grp: field group containing record_id
    :param record_id: id of record for which to retrieve position
    :return: position index of record in field group's record list
    """
    field_grp_id_name = get_field_group_id_key_name(field_grp)
    field_grp_id_key = get_bq_name(field_grp_id_name, field_grp)
    idx = 0

    # iterate until id found in record--if not found, fatal error
    for record in flat_case[field_grp]:
        if record[field_grp_id_key] == record_id:
            return idx
        idx += 1

    return has_fatal_error(f"id {record_id} not found by get_record_idx.")


def merge_single_entry_fgs(flat_case, record_counts):
    """
    # Merge flatten-able field groups.
    :param flat_case: flattened case dict
    :param record_counts: field group count dict
    """
    tables = get_one_to_many_tables(record_counts)

    flattened_field_grp_parents = dict()

    for field_grp in record_counts:
        if field_grp == API_PARAMS['FG_CONFIG']['base_fg']:
            continue
        if record_counts[field_grp] == 1:
            if field_grp in flat_case:
                # create list of flattened field group destination tables
                flattened_field_grp_parents[field_grp] = get_parent_fg(tables, field_grp)

    for field_grp, parent in flattened_field_grp_parents.items():
        field_grp_id_name = get_field_group_id_key_name(parent)
        bq_parent_id_key = get_bq_name(field_grp_id_name, parent)

        for record in flat_case[field_grp]:
            parent_id = record[bq_parent_id_key]
            parent_idx = get_record_idx(flat_case, parent, parent_id)
            flat_case[parent][parent_idx].update(record)
        flat_case.pop(field_grp)


def get_record_counts(flat_case, record_counts):
    """

    Get record counts for field groups in case record.
    :param flat_case: flattened dict containing case record entries
    :param record_counts: field group count dict
    """
    # initialize dict with field groups that can't be flattened
    record_count_dict = {field_grp: dict() for field_grp in record_counts if record_counts[field_grp] > 1}

    tables = get_one_to_many_tables(record_counts)

    for field_grp in record_count_dict:
        parent_field_grp = get_parent_fg(tables, field_grp)
        field_grp_id_name = get_field_group_id_key_name(parent_field_grp)
        parent_id_key = get_bq_name(field_grp_id_name, parent_field_grp)

        # initialize record counts for parent id
        if parent_field_grp in flat_case:

            last_parent_record_id_key = None

            for parent_record in flat_case[parent_field_grp]:
                parent_id = last_parent_record_id_key = parent_record[parent_id_key]
                record_count_dict[field_grp][parent_id] = 0

            # count child records
            if field_grp in flat_case:
                for record in flat_case[field_grp]:
                    if parent_id_key in record:
                        parent_id = record[parent_id_key]
                    else:
                        parent_id = last_parent_record_id_key

                    record_count_dict[field_grp][parent_id] += 1

    # insert record count into flattened dict entries
    for field_grp, parent_ids in record_count_dict.items():
        parent_field_grp = get_parent_fg(tables, field_grp)
        count_name = get_bq_name('count', field_grp)

        for parent_id, count in parent_ids.items():
            p_key_idx = get_record_idx(flat_case, parent_field_grp, parent_id)
            flat_case[parent_field_grp][p_key_idx][count_name] = count


def merge_or_count_records(flattened_case, record_counts):
    """
    If program field group has max record count of 1, flattens into parent table.
    Otherwise, counts record in one-to-many table and adds count field to parent record
    in flattened_case
    :param flattened_case: flattened dict containing case record's values
    :param record_counts: field group count dict max counts for program's field group
    records
    :return: modified version of flattened_case
    """
    merge_single_entry_fgs(flattened_case, record_counts)
    # initialize counts for parent_ids for every possible child table (some child tables
    # won't actually have records, and this initialization adds 0 counts in that case)
    get_record_counts(flattened_case, record_counts)


def create_and_load_tables(program, cases, schemas, record_counts):
    """
    Create jsonl row files for future insertion, store in GC storage bucket,
    then insert the new table schemas and data.
    :param program: program for which to create tables
    :param cases: case records to insert into BQ for program
    :param schemas: dict of schema lists for all of this program's tables
    :param record_counts: field group count dict
    """
    record_tables = get_one_to_many_tables(record_counts)

    for record_table in record_tables:
        full_table_name = get_full_table_name(program, record_table)
        jsonl_name = f"{full_table_name}.jsonl"
        jsonl_fp = get_scratch_fp(BQ_PARAMS, jsonl_name)

        # If jsonl scratch file exists, delete so we don't append
        if os.path.exists(jsonl_fp):
            os.remove(jsonl_fp)

    for i, case in enumerate(cases):
        flat_case = flatten_case(case)

        # remove excluded field groups
        for fg in flat_case.copy():
            if fg not in record_counts:
                flat_case.pop(fg)

        merge_or_count_records(flat_case, record_counts)

        for bq_table in flat_case:
            if bq_table not in record_tables:
                has_fatal_error(f"Table {bq_table} not found in table keys")

            full_table_name = get_full_table_name(program, bq_table)
            jsonl_name = f"{full_table_name}.jsonl"
            jsonl_fp = get_scratch_fp(BQ_PARAMS, jsonl_name)

            write_list_to_jsonl(jsonl_fp, flat_case[bq_table], 'a')

        if i % 100 == 0:
            print(f"wrote case {i} of {len(cases)} to jsonl")

    for record_table in record_tables:
        full_table_name = get_full_table_name(program, record_table)
        jsonl_name = f"{full_table_name}.jsonl"

        print(f"Upload {jsonl_name} to bucket")

        upload_to_bucket(BQ_PARAMS, get_scratch_fp(BQ_PARAMS, jsonl_name), delete_local=True)

        table_name = get_full_table_name(program, record_table)

        table_id = construct_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_DATASET'], table_name)

        create_and_load_table_from_jsonl(BQ_PARAMS, jsonl_name, table_id, schemas[record_table])

        update_table_schema_from_generic(program, table_id, BQ_PARAMS['SCHEMA_TAGS'])


def update_table_schema_from_generic(program, table_id, schema_tags=dict()):
    if program == "BEATAML1_0":
        schema_tags['program-name-upper'] = "BEATAML1.0"
        schema_tags['program-name-lower'] = "beataml"
    else:
        schema_tags['program-name-upper'] = program.upper()  # should already be upper
        schema_tags['program-name-lower'] = program.lower()

    split_table_id = table_id.split("_")
    clinical_index = split_table_id.index("clinical")
    if not clinical_index:
        has_fatal_error("clinical not found in table id, can't parse id to use as friendly name")

    if len(split_table_id) > clinical_index + 1:
        start_index = clinical_index + 1
        mapping_name = f" -"

        for mapping_name_component in split_table_id[start_index:]:
            mapping_name += f" {mapping_name_component.upper()}"
    else:
        mapping_name = ''

    schema_tags['mapping-name'] = mapping_name

    metadata_dir = f"{BQ_PARAMS['BQ_REPO']}/{BQ_PARAMS['GENERIC_TABLE_METADATA_FILEPATH']}"
    # adapts path for vm
    metadata_fp = get_filepath(metadata_dir)

    with open(metadata_fp) as file_handler:
        table_schema = ''

        for line in file_handler.readlines():
            table_schema += line

        for tag_key, tag_value in schema_tags.items():
            tag = f"{{---tag-{tag_key}---}}"

            table_schema = table_schema.replace(tag, tag_value)

        metadata = json.loads(table_schema)

        print(metadata)

        update_table_metadata(table_id, metadata)
        add_column_descriptions(table_id)


def get_metadata_files():
    """Get all the file names in a directory as a list of as strings.

    :return: list of filenames
    """
    rel_path = '/'.join([BQ_PARAMS['BQ_REPO'], BQ_PARAMS['TABLE_METADATA_DIR'], get_rel_prefix(API_PARAMS)])
    metadata_fp = get_filepath(rel_path)

    return [f for f in os.listdir(metadata_fp) if os.path.isfile(os.path.join(metadata_fp, f))]


def make_and_check_metadata_table_id(json_file):
    def convert_json_to_table_name():
        """Convert json filename (from BQEcosystem repo) into BQ table name.
        json schema files match table ID of BQ table.

        data and metadata; json file naming matches table ID of corresponding BQ table
        :return: BQ table name for which the json acts as a configuration file
        """
        # handles naming for *webapp* tables
        split_name = json_file.split('.')
        program_name = split_name[1]
        split_table = split_name[2].split('_')
        program_table_name = '_'.join(split_table[:-2])
        rel = get_rel_prefix(API_PARAMS)
        return '_'.join([rel, program_name, program_table_name])

    table_name = convert_json_to_table_name()
    table_id = construct_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_DATASET'], table_name)

    if not exists_bq_table(table_id):
        print(f'\t\t- skipping -- no table found for file: {json_file}')
        return None
    else:
        print(f'\t- updating {json_file}')
        return table_id


def add_column_descriptions(table_id):
    """
    Alter an existing table's schema (currently, only field descriptions are mutable
    without a table rebuild, Google's restriction).
    """
    print("\nUpdating schemas (field descriptions)!")

    field_desc_fp = f"{BQ_PARAMS['BQ_REPO']}/{BQ_PARAMS['FIELD_DESCRIPTION_FILEPATH']}"
    field_desc_fp = get_filepath(field_desc_fp)

    with open(field_desc_fp) as field_output:
        descriptions = json.load(field_output)

    update_schema(table_id, descriptions)


def change_status_to_archived(table_id):
    """
    todo
    :param table_id:
    :return:
    """
    client = bigquery.Client()
    current_release_tag = get_rel_prefix(API_PARAMS)
    stripped_table_id = table_id.replace(current_release_tag, "")
    previous_release_tag = API_PARAMS['REL_PREFIX'] + str(int(API_PARAMS['RELEASE']) - 1)
    prev_table_id = stripped_table_id + previous_release_tag

    try:
        prev_table = client.get_table(prev_table_id)
        prev_table.labels['status'] = 'archived'
        print(f"labels: {prev_table.labels}")
        client.update_table(prev_table, ["labels"])

        assert prev_table.labels['status'] == 'archived'
    except NotFound:
        print("Not writing archived label for table that didn't exist in a previous version.")


def copy_tables_into_public_project(publish_table_list):
    """Move production-ready bq tables onto the public-facing production server.

    """
    for table_name in publish_table_list:
        split_table_name = table_name.split('_')
        split_table_name.pop(0)
        public_dataset = split_table_name.pop(0)
        src_table_id = construct_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_DATASET'], table_name)

        publish_table(API_PARAMS, BQ_PARAMS, public_dataset, src_table_id, overwrite=True)


def make_biospecimen_stub_view_query(program):
    """

    Create biospecimen view query.
    :param program: Program name for which to create biospecimen view
    :return: biospecimen view query
    """
    aliquot_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.GDC_metadata.rel{API_PARAMS['RELEASE']}_aliquot2caseIDmap"

    return f"""
        SELECT program_name, project_id as project_short_name, case_gdc_id, case_barcode, sample_gdc_id, sample_barcode
        FROM `{aliquot_table_id}`
        WHERE program_name = '{program}'
    """


def build_biospecimen_stub_view(program):
    """

    Create one-to-many biospecimen view for webapp integration.
    :param program: program to which cases belong
    """
    bio_spec_view_name_list = [get_rel_prefix(API_PARAMS), str(program), BQ_PARAMS['BIOSPECIMEN_SUFFIX']]
    biospec_view_name = construct_table_name_from_list(bio_spec_view_name_list)
    biospec_table_id = construct_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_DATASET'], biospec_view_name)
    biospec_stub_table_query = make_biospecimen_stub_view_query(program)

    create_view_from_query(biospec_table_id, biospec_stub_table_query)


#    Script execution

def make_publish_table_list_query(old_table_id, new_table_id):
    """
    todo
    :param old_table_id:
    :param new_table_id:
    :return:
    """
    return f"""
        SELECT count(*) as row_count
        FROM `{old_table_id}` old
        FULL JOIN `{new_table_id}` curr
            ON old.case_id = curr.case_id
        WHERE old.case_id is null 
        OR curr.case_id is null
    """


def build_publish_table_list():
    """
    todo
    :return:
    """
    old_release = get_rel_prefix(API_PARAMS, return_last_version=True)
    new_release = get_rel_prefix(API_PARAMS)

    old_tables = {table for table in list_bq_tables(BQ_PARAMS['DEV_DATASET'], old_release) if "webapp" not in table}
    new_tables = {table for table in list_bq_tables(BQ_PARAMS['DEV_DATASET'], new_release) if "webapp" not in table}

    publish_table_list = list()

    for new_table_name in new_tables:
        # exclude master table
        if new_table_name == f"{new_release}_{BQ_PARAMS['MASTER_TABLE']}":
            continue

        split_new_table = new_table_name.split('_')
        split_new_table[0] = old_release
        old_table_name = "_".join(split_new_table)
        if old_table_name not in old_tables:
            publish_table_list.append(new_table_name)
        else:
            old_table_id = construct_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_DATASET'], old_table_name)
            new_table_id = construct_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_DATASET'], new_table_name)

            res = bq_harness_with_result(compare_two_tables_sql(old_table_id, new_table_id), BQ_PARAMS['DO_BATCH'])

            if not res:
                publish_table_list.append(new_table_name)

            for row in res:
                if row:
                    publish_table_list.append(new_table_name)
                    break

    return publish_table_list


def create_tables(program, cases, schema):
    """
    Run the overall script which creates schemas, modifies data, prepares it for loading,
    and creates the databases.
    :param program: the source for the inserted cases data
    :param cases: dict representations of clinical case data from GDC
    :param schema:  schema file for BQ table creation
    :return:
    """

    print(" - Creating public BQ table(s).")

    # derive the program's table structure by analyzing its case records
    columns, record_counts = find_program_structure(cases)

    # add the parent id to field group dicts that will create separate tables
    column_orders = add_ref_columns(columns, record_counts, schema, program)

    # reassign merged_column_orders to column_orders
    merged_orders = merge_column_orders(schema, columns, record_counts, column_orders)

    # drop any null fields from the merged column order dicts
    remove_null_fields(columns, merged_orders)

    # creates dictionary of lists of SchemaField objects in json format
    schemas = create_schema_lists(schema, record_counts, merged_orders)

    create_and_load_tables(program, cases, schemas, record_counts)


def make_release_fields_comparison_query(old_rel, new_rel):
    """

    todo
    :param old_rel:
    :param new_rel:
    :return:
    """
    return f"""
        SELECT table_name AS release, field_path AS field
        FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
        WHERE field_path IN (
            SELECT field_path 
            FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
            WHERE table_name='{old_rel}_clinical' 
                OR table_name='{new_rel}_clinical'
           GROUP BY field_path
           HAVING COUNT(field_path) <= 1)
    """


def find_release_changed_data_types_query(old_rel, new_rel):
    """

    todo
    :param old_rel:
    :param new_rel:
    :return:
    """
    return f"""
        SELECT field_path, data_type, COUNT(field_path) AS distinct_data_type_cnt 
        FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
        WHERE (table_name='{old_rel}_clinical' OR table_name='{new_rel}_clinical')
            AND (data_type = 'INT64' OR data_type = 'FLOAT64' OR data_type = 'STRING' OR data_type = 'BOOL')
        GROUP BY field_path, data_type 
        HAVING distinct_data_type_cnt <= 1
    """


def make_field_diff_query(old_rel, new_rel, removed_fields):
    """
    todo
    :param old_rel:
    :param new_rel:
    :param removed_fields:
    :return:
    """
    check_rel = old_rel if removed_fields else new_rel

    return f"""
        SELECT field_path AS field
        FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
        WHERE field_path IN (
            SELECT field_path 
            FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
            WHERE table_name='{old_rel}_clinical' 
                OR table_name='{new_rel}_clinical'
           GROUP BY field_path
           HAVING COUNT(field_path) <= 1)
       AND table_name='{check_rel}_clinical'
    """


def make_datatype_diff_query(old_rel, new_rel):
    """
    todo
    :param old_rel:
    :param new_rel:
    :return:
    """
    return f"""
        WITH data_types as (SELECT field_path, data_type
          FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
          WHERE (table_name='{old_rel}_clinical' OR table_name='{new_rel}_clinical')
            AND (data_type = 'INT64' OR data_type = 'FLOAT64' OR data_type = 'STRING' OR data_type = 'BOOL')
          GROUP BY field_path, data_type)
        SELECT field_path
        FROM data_types
        GROUP BY field_path
        HAVING COUNT(field_path) > 1
    """


def make_removed_case_ids_query(old_rel, new_rel):
    """

    todo
    :param old_rel:
    :param new_rel:
    :return:
    """
    return f"""
        SELECT case_id, project.project_id
        FROM `isb-project-zero.GDC_Clinical_Data.{old_rel}_clinical`
        JOIN UNNEST(project) as project
        WHERE case_id NOT IN (
            SELECT case_id 
            FROM `isb-project-zero.GDC_Clinical_Data.{new_rel}_clinical`)    
    """


def make_added_case_ids_query(old_rel, new_rel):
    """
    todo
    :param old_rel:
    :param new_rel:
    :return:
    """
    return f"""
        SELECT project.project_id, count(case_id) as new_case_cnt
        FROM `isb-project-zero.GDC_Clinical_Data.{new_rel}_clinical`
        JOIN UNNEST(project) as project
        WHERE case_id NOT IN (
            SELECT case_id 
            FROM `isb-project-zero.GDC_Clinical_Data.{old_rel}_clinical`)
        GROUP BY project.project_id
    """


def make_tables_diff_query(old_rel, new_rel):
    """
    todo
    :param old_rel:
    :param new_rel:
    :return:
    """
    return f"""
        WITH old_table_counts AS (
          SELECT program, COUNT(program) AS num_tables 
          FROM (
            SELECT els[OFFSET(1)] AS program
            FROM (
              SELECT SPLIT(table_name, '_') AS els
              FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.TABLES
              WHERE table_name LIKE '{old_rel}%'))
          WHERE program != 'clinical'
          GROUP BY program
        ),
        new_table_counts AS (
          SELECT program, COUNT(program) AS num_tables 
          FROM (
            SELECT els[OFFSET(1)] AS program
            FROM (
              SELECT SPLIT(table_name, '_') AS els
              FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.TABLES
              WHERE table_name LIKE '{new_rel}%'))
          WHERE program != 'clinical'
          GROUP BY program
        )

        SELECT  o.program AS prev_rel_program_name, 
                n.program AS new_rel_program_name, 
                o.num_tables AS prev_table_cnt, 
                n.num_tables AS new_table_cnt
        FROM new_table_counts n
        FULL OUTER JOIN old_table_counts o
          ON o.program = n.program
        WHERE o.num_tables != n.num_tables
          OR o.num_tables IS NULL or n.num_tables IS NULL
        ORDER BY n.num_tables DESC
    """


def make_new_table_list_query(old_rel, new_rel):
    """

    todo
    :param old_rel:
    :param new_rel:
    :return:
    """
    return f"""
        WITH old_tables AS (
          SELECT table_name
          FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.TABLES
          WHERE table_name LIKE '{old_rel}%'
          ORDER BY table_name),
        new_tables AS (
          SELECT table_name
          FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.TABLES
          WHERE table_name LIKE '{new_rel}%'
          ORDER BY table_name)
        
        SELECT table_name
        FROM new_tables 
        WHERE LTRIM(table_name, '{new_rel}_') NOT IN (SELECT LTRIM(table_name, '{old_rel}_') FROM old_tables)
    """


def compare_gdc_releases():
    """
    todo
    :return:
    """
    old_rel = API_PARAMS['REL_PREFIX'] + str(int(API_PARAMS['RELEASE']) - 1)
    new_rel = get_rel_prefix(API_PARAMS)

    print(f"\n\n*** {old_rel} -> {new_rel} GDC Clinical Data Comparison Report ***")

    # which fields have been removed?
    removed_fields_res = bq_harness_with_result(make_field_diff_query(old_rel, new_rel, removed_fields=True),
                                           BQ_PARAMS['DO_BATCH'])
    print("\nRemoved fields:")

    if removed_fields_res.total_rows == 0:
        print("<none>")
    else:
        for row in removed_fields_res:
            print(row[0])

    # which fields were added?
    added_fields_res = bq_harness_with_result(make_field_diff_query(old_rel, new_rel, removed_fields=False), BQ_PARAMS['DO_BATCH'])
    print("\nNew GDC API fields:")

    if added_fields_res.total_rows == 0:
        print("<none>")
    else:
        for row in added_fields_res:
            print(row[0])

    # any changes in field data type?
    datatype_diff_res = bq_harness_with_result(make_datatype_diff_query(old_rel, new_rel), BQ_PARAMS['DO_BATCH'])
    print("\nColumns with data type change:")

    if datatype_diff_res.total_rows == 0:
        print("<none>")
    else:
        for row in datatype_diff_res:
            print(row[0])

    # any case ids removed?
    print("\nRemoved case ids:")
    removed_case_ids_res = bq_harness_with_result(make_removed_case_ids_query(old_rel, new_rel), BQ_PARAMS['DO_BATCH'])

    if removed_case_ids_res.total_rows == 0:
        print("<none>")
    else:
        for row in removed_case_ids_res:
            print(row[0])

    # any case ids added?
    print("\nAdded case id counts:")
    added_case_ids_res = bq_harness_with_result(make_added_case_ids_query(old_rel, new_rel), BQ_PARAMS['DO_BATCH'])

    if added_case_ids_res.total_rows == 0:
        print("<none>")
    else:
        for row in added_case_ids_res:
            print(f"{row[0]}: {row[1]} new case ids")

    # any case ids added?
    print("\nTable count changes: ")
    table_count_res = bq_harness_with_result(make_tables_diff_query(old_rel, new_rel), BQ_PARAMS['DO_BATCH'])

    if table_count_res.total_rows == 0:
        print("<none>")
    else:
        for row in table_count_res:
            program_name = row[0] if row[0] else row[1]
            prev_table_cnt = 0 if not row[2] else row[2]
            new_table_cnt = 0 if not row[3] else row[3]

            print(f"{program_name}: {prev_table_cnt} table(s) in {old_rel}, {new_table_cnt} table(s) in {new_rel}")

    print("\nAdded tables: ")
    added_table_res = bq_harness_with_result(make_new_table_list_query(old_rel, new_rel), BQ_PARAMS['DO_BATCH'])

    if added_table_res.total_rows == 0:
        print("<none>")
    else:
        for row in added_table_res:
            print(row[0])

    print("\n*** End Report ***\n\n")


def output_report(start, steps):
    """
    Outputs a basic report of script's results, including total processing
    time and which steps were specified in YAML.
    :param start: float representing script's start time.
    :param steps: set of steps to be performed (configured in YAML)
    """
    seconds = time.time() - start
    print(f"Script executed in {format_seconds(seconds)}\n")

    print("Steps completed: ")

    if 'create_biospecimen_stub_tables' in steps:
        print('\t - created biospecimen stub tables for webapp use')
    if 'create_and_load_tables' in steps:
        print('\t - created tables and inserted data')
    if 'update_table_metadata' in steps:
        print('\t - added/updated table metadata')
    if 'update_schema' in steps:
        print('\t - updated table field descriptions')
    if 'copy_tables_into_production' in steps:
        print('\t - copied tables into production (public-facing bq tables)')
    if 'validate_data' in steps:
        print('\t - validated data')
    if 'generate_documentation' in steps:
        print('\t - generated documentation')
    print('\n\n')


def get_cases_by_program(program):
    """Get a dict obj containing all the cases associated with a given program.

    :param program: the program from which the cases originate
    :return: cases dict
    """

    cases = []

    bulk_table_name = construct_table_name(API_PARAMS,
                                           prefix=get_rel_prefix(API_PARAMS),
                                           suffix=BQ_PARAMS['MASTER_TABLE'],
                                           include_release=False)
    bulk_table_id = construct_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_DATASET'], bulk_table_name)

    sample_table_name_list = [get_rel_prefix(API_PARAMS), str(program), BQ_PARAMS['BIOSPECIMEN_SUFFIX']]
    sample_table_name = construct_table_name_from_list(sample_table_name_list)
    sample_table_id = construct_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_DATASET'], sample_table_name)

    query = f"""
        SELECT * 
        FROM `{bulk_table_id}` 
        WHERE case_id IN (
            SELECT DISTINCT(case_gdc_id) 
            FROM `{sample_table_id}`
        )
    """

    for case_row in bq_harness_with_result(query, BQ_PARAMS['DO_BATCH']):
        case_items = dict(case_row.items())
        cases.append(case_items)

    return cases


def main(args):
    """Script execution function.

    :param args: command-line arguments
    """
    start = time.time()
    steps = []

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
        if 'FIELD_CONFIG' not in API_PARAMS:
            has_fatal_error("params['FIELD_CONFIG'] not found")
    except ValueError as err:
        has_fatal_error(str(err), ValueError)

    programs = ['BEATAML1.0']
    # programs = get_program_list()

    for orig_program in programs:
        prog_start = time.time()
        if 'create_biospecimen_stub_tables' in steps or 'create_and_load_tables' in steps:
            print(f"\nRunning script for program: {orig_program}...")

        if 'create_biospecimen_stub_tables' in steps:
            # these tables are used to populate the per-program clinical tables and the webapp tables
            print(" - Creating biospecimen stub tables!")
            build_biospecimen_stub_view(orig_program)

        if 'create_and_load_tables' in steps:
            cases = get_cases_by_program(orig_program)

            if not cases:
                print(f"No cases found for program {orig_program}, skipping.")
                continue

            schema = create_schema_dict()
            # replace so that 'BEATAML1.0' doesn't break bq table name
            program = orig_program.replace('.', '_')

            create_tables(program, cases, schema)

            prog_end = time.time() - prog_start
            print(f"{orig_program} processed in {format_seconds(prog_end)}!\n")

    if "build_view_queries" in steps:
        view_queries = {
            "BEATAML1_0": """
            SELECT cl.case_id AS case_gdc_id, cl.submitter_id AS case_barcode, cl.disease_type, cl.primary_site, 
            null as disease_code, meta.program_name, meta.project_id as project_short_name,
            cl.demo__ethnicity AS ethnicity, 
            cl.demo__gender AS gender, 
            cl.demo__race AS race, 
            cl.demo__vital_status AS vital_status,
            cl.diag__morphology AS morphology,
            cl.diag__site_of_resection_or_biopsy AS site_of_resection_or_biopsy,
            cl.diag__tissue_or_organ_of_origin AS tissue_or_organ_of_origin,
            cl.diag__tumor_stage AS tumor_stage,
            cl.diag__age_at_diagnosis AS age_at_diagnosis,
            FROM isb-project-zero.GDC_Clinical_Data.r29_BEATAML1_0_clinical cl
            JOIN isb-project-zero.GDC_metadata.rel29_caseData meta
                ON meta.case_gdc_id = cl.case_id
            """,
            "CGCI": """
            SELECT cl.case_id AS case_gdc_id, cl.submitter_id AS case_barcode, cl.disease_type, cl.primary_site, 
            null as disease_code, meta.program_name, meta.project_id as project_short_name,
            cl.demo__ethnicity AS ethnicity, 
            cl.demo__gender AS gender, 
            cl.demo__race AS race, 
            cl.demo__vital_status AS vital_status,
            d.diag__diagnosis_id AS diagnosis_id,
            d.diag__morphology AS morphology,
            d.diag__site_of_resection_or_biopsy AS site_of_resection_or_biopsy,
            d.diag__tissue_or_organ_of_origin AS tissue_or_organ_of_origin,
            d.diag__tumor_grade AS tumor_grade,
            d.diag__tumor_stage AS tumor_stage,
            d.diag__age_at_diagnosis AS age_at_diagnosis,
            d.diag__prior_malignancy AS prior_malignancy,
            d.diag__ajcc_pathologic_m AS ajcc_pathologic_m,
            d.diag__ajcc_pathologic_n AS ajcc_pathologic_n,
            d.diag__ajcc_pathologic_t AS ajcc_pathologic_t,
            cl.exp__pack_years_smoked AS pack_years_smoked,
            FROM isb-project-zero.GDC_Clinical_Data.r29_CGCI_clinical cl
            JOIN isb-project-zero.GDC_Clinical_Data.r29_CGCI_clinical_diagnoses d 
                ON cl.case_id = d.case_id
            JOIN isb-project-zero.GDC_metadata.rel29_caseData meta
                ON meta.case_gdc_id = cl.case_id
            """,
            "CMI": """
            SELECT cl.case_id AS case_gdc_id, cl.submitter_id AS case_barcode, cl.disease_type, cl.primary_site, 
            null as disease_code, meta.program_name, meta.project_id as project_short_name,
            cl.demo__ethnicity AS ethnicity, 
            cl.demo__gender AS gender, 
            cl.demo__race AS race, 
            cl.demo__vital_status AS vital_status,
            cl.diag__morphology AS morphology,
            cl.diag__site_of_resection_or_biopsy AS site_of_resection_or_biopsy,
            cl.diag__tissue_or_organ_of_origin AS tissue_or_organ_of_origin,
            cl.diag__age_at_diagnosis AS age_at_diagnosis,
            FROM isb-project-zero.GDC_Clinical_Data.r29_CMI_clinical cl
            JOIN isb-project-zero.GDC_metadata.rel29_caseData meta
                ON meta.case_gdc_id = cl.case_id
            """,
            "CPTAC": """
            SELECT cl.case_id AS case_gdc_id, cl.submitter_id AS case_barcode, cl.disease_type, cl.primary_site, 
            null as disease_code, meta.program_name, meta.project_id as project_short_name,
            cl.demo__ethnicity AS ethnicity, 
            cl.demo__gender AS gender, 
            cl.demo__race AS race, 
            cl.demo__vital_status AS vital_status,
            cl.diag__morphology AS morphology,
            cl.diag__site_of_resection_or_biopsy AS site_of_resection_or_biopsy,
            cl.diag__tissue_or_organ_of_origin AS tissue_or_organ_of_origin,
            cl.diag__tumor_stage AS tumor_stage,
            cl.diag__age_at_diagnosis AS age_at_diagnosis,
            cl.diag__prior_malignancy AS prior_malignancy,
            cl.diag__ajcc_pathologic_stage AS ajcc_pathologic_stage,
            cl.diag__ajcc_pathologic_m AS ajcc_pathologic_m,
            cl.diag__ajcc_pathologic_n AS ajcc_pathologic_n,
            cl.diag__ajcc_pathologic_t AS ajcc_pathologic_t,
            cl.exp__pack_years_smoked AS pack_years_smoked,
            cl.exp__alcohol_history AS alcohol_history
            FROM isb-project-zero.GDC_Clinical_Data.r29_CPTAC_clinical cl
            JOIN isb-project-zero.GDC_metadata.rel29_caseData meta
                ON meta.case_gdc_id = cl.case_id
            """,
            "CTSP": """
            SELECT cl.case_id AS case_gdc_id, cl.submitter_id AS case_barcode, cl.disease_type, cl.primary_site, 
            null as disease_code, meta.program_name, meta.project_id as project_short_name,
            cl.demo__ethnicity AS ethnicity, 
            cl.demo__gender AS gender, 
            cl.demo__race AS race, 
            cl.demo__vital_status AS vital_status,
            cl.diag__morphology AS morphology,
            cl.diag__site_of_resection_or_biopsy AS site_of_resection_or_biopsy,
            cl.diag__tissue_or_organ_of_origin AS tissue_or_organ_of_origin,
            cl.diag__tumor_stage AS tumor_stage,
            cl.diag__age_at_diagnosis AS age_at_diagnosis,
            cl.diag__prior_malignancy AS prior_malignancy,
            FROM isb-project-zero.GDC_Clinical_Data.r29_CTSP_clinical cl
            JOIN isb-project-zero.GDC_metadata.rel29_caseData meta
                ON meta.case_gdc_id = cl.case_id
            """,
            "FM": """
            SELECT cl.case_id AS case_gdc_id, cl.submitter_id AS case_barcode, cl.disease_type, cl.primary_site, 
            null as disease_code, meta.program_name, meta.project_id as project_short_name,
            cl.demo__ethnicity AS ethnicity, 
            cl.demo__gender AS gender, 
            cl.demo__race AS race, 
            cl.demo__vital_status AS vital_status,
            cl.diag__morphology AS morphology,
            cl.diag__site_of_resection_or_biopsy AS site_of_resection_or_biopsy,
            cl.diag__tissue_or_organ_of_origin AS tissue_or_organ_of_origin,
            cl.diag__tumor_stage AS tumor_stage,
            cl.diag__age_at_diagnosis AS age_at_diagnosis,
            FROM isb-project-zero.GDC_Clinical_Data.r29_FM_clinical cl
            JOIN isb-project-zero.GDC_metadata.rel29_caseData meta
                ON meta.case_gdc_id = cl.case_id
            """,
            "GENIE": """
            SELECT cl.case_id AS case_gdc_id, cl.submitter_id AS case_barcode, cl.disease_type, cl.primary_site, 
            null as disease_code, meta.program_name, meta.project_id as project_short_name,
            cl.demo__ethnicity AS ethnicity, 
            cl.demo__gender AS gender, 
            cl.demo__race AS race, 
            cl.demo__vital_status AS vital_status,
            cl.diag__morphology AS morphology,
            cl.diag__site_of_resection_or_biopsy AS site_of_resection_or_biopsy,
            cl.diag__tissue_or_organ_of_origin AS tissue_or_organ_of_origin,
            cl.diag__tumor_grade AS tumor_grade,
            cl.diag__tumor_stage AS tumor_stage,
            FROM isb-project-zero.GDC_Clinical_Data.r29_GENIE_clinical cl
            JOIN isb-project-zero.GDC_metadata.rel29_caseData meta
                ON meta.case_gdc_id = cl.case_id
            """,
            "HCMI": """
            SELECT cl.case_id AS case_gdc_id, cl.submitter_id AS case_barcode, cl.disease_type, cl.primary_site, 
            null as disease_code, meta.program_name, meta.project_id as project_short_name,
            cl.demo__ethnicity AS ethnicity, 
            cl.demo__gender AS gender, 
            cl.demo__race AS race, 
            cl.demo__vital_status AS vital_status,
            d.diag__diagnosis_id AS diagnosis_id,
            d.diag__morphology AS morphology,
            d.diag__site_of_resection_or_biopsy AS site_of_resection_or_biopsy,
            d.diag__tissue_or_organ_of_origin AS tissue_or_organ_of_origin,
            d.diag__tumor_grade AS tumor_grade,
            d.diag__age_at_diagnosis AS age_at_diagnosis,
            d.diag__prior_malignancy AS prior_malignancy,
            d.diag__ajcc_pathologic_stage AS ajcc_pathologic_stage,
            d.diag__ajcc_pathologic_m AS ajcc_pathologic_m,
            d.diag__ajcc_pathologic_n AS ajcc_pathologic_n,
            d.diag__ajcc_pathologic_t AS ajcc_pathologic_t,
            cl.exp__pack_years_smoked AS pack_years_smoked,
            FROM isb-project-zero.GDC_Clinical_Data.r29_HCMI_clinical cl
            JOIN isb-project-zero.GDC_Clinical_Data.r29_HCMI_clinical_diagnoses d 
                ON cl.case_id = d.case_id
            JOIN isb-project-zero.GDC_metadata.rel29_caseData meta
                ON meta.case_gdc_id = cl.case_id
            """,
            "MMRF": """
            SELECT cl.case_id AS case_gdc_id, cl.submitter_id AS case_barcode, cl.disease_type, cl.primary_site, 
            null as disease_code, meta.program_name, meta.project_id as project_short_name,
            cl.demo__ethnicity AS ethnicity, 
            cl.demo__gender AS gender, 
            cl.demo__race AS race, 
            cl.demo__vital_status AS vital_status,
            cl.diag__morphology AS morphology,
            cl.diag__site_of_resection_or_biopsy AS site_of_resection_or_biopsy,
            cl.diag__tissue_or_organ_of_origin AS tissue_or_organ_of_origin,
            cl.diag__tumor_stage AS tumor_stage,
            cl.diag__age_at_diagnosis AS age_at_diagnosis,
            FROM isb-project-zero.GDC_Clinical_Data.r29_MMRF_clinical cl
            JOIN isb-project-zero.GDC_metadata.rel29_caseData meta
                ON meta.case_gdc_id = cl.case_id
            """,
            "NCICCR": """
            SELECT cl.case_id AS case_gdc_id, cl.submitter_id AS case_barcode, cl.disease_type, cl.primary_site, 
            null as disease_code, meta.program_name, meta.project_id as project_short_name,
            cl.demo__ethnicity AS ethnicity, 
            cl.demo__gender AS gender, 
            cl.demo__race AS race, 
            cl.demo__vital_status AS vital_status,
            cl.diag__morphology AS morphology,
            cl.diag__site_of_resection_or_biopsy AS site_of_resection_or_biopsy,
            cl.diag__tissue_or_organ_of_origin AS tissue_or_organ_of_origin,
            cl.diag__tumor_stage AS tumor_stage,
            cl.diag__age_at_diagnosis AS age_at_diagnosis,
            FROM isb-project-zero.GDC_Clinical_Data.r29_NCICCR_clinical cl
            JOIN isb-project-zero.GDC_metadata.rel29_caseData meta
                ON meta.case_gdc_id = cl.case_id
            """,
            "OHSU": """
            SELECT cl.case_id AS case_gdc_id, cl.submitter_id AS case_barcode, cl.disease_type, cl.primary_site, 
            null as disease_code, meta.program_name, meta.project_id as project_short_name,
            cl.demo__ethnicity AS ethnicity, 
            cl.demo__gender AS gender, 
            cl.demo__race AS race, 
            cl.demo__vital_status AS vital_status,
            cl.diag__morphology AS morphology,
            cl.diag__site_of_resection_or_biopsy AS site_of_resection_or_biopsy,
            cl.diag__tissue_or_organ_of_origin AS tissue_or_organ_of_origin,
            cl.diag__tumor_stage AS tumor_stage,
            cl.diag__age_at_diagnosis AS age_at_diagnosis,
            FROM isb-project-zero.GDC_Clinical_Data.r29_OHSU_clinical cl
            JOIN isb-project-zero.GDC_metadata.rel29_caseData meta
                ON meta.case_gdc_id = cl.case_id
            """,
            "ORGANOID": """
            SELECT cl.case_id AS case_gdc_id, cl.submitter_id AS case_barcode, cl.disease_type, cl.primary_site, 
            null as disease_code, meta.program_name, meta.project_id as project_short_name,
            cl.demo__ethnicity AS ethnicity, 
            cl.demo__gender AS gender, 
            cl.demo__race AS race, 
            cl.demo__vital_status AS vital_status,
            cl.diag__morphology AS morphology,
            cl.diag__site_of_resection_or_biopsy AS site_of_resection_or_biopsy,
            cl.diag__tissue_or_organ_of_origin AS tissue_or_organ_of_origin,
            cl.diag__tumor_stage AS tumor_stage,
            cl.diag__ajcc_pathologic_stage AS ajcc_pathologic_stage,
            FROM isb-project-zero.GDC_Clinical_Data.r29_ORGANOID_clinical cl
            JOIN isb-project-zero.GDC_metadata.rel29_caseData meta
                ON meta.case_gdc_id = cl.case_id
            """,
            "TARGET": """
            SELECT cl.case_id AS case_gdc_id, cl.submitter_id AS case_barcode, cl.disease_type, cl.primary_site, 
            null as disease_code, meta.program_name, meta.project_id as project_short_name,
            cl.demo__ethnicity AS ethnicity, 
            cl.demo__gender AS gender, 
            cl.demo__race AS race, 
            cl.demo__vital_status AS vital_status,
            cl.diag__morphology AS morphology,
            cl.diag__site_of_resection_or_biopsy AS site_of_resection_or_biopsy,
            cl.diag__tissue_or_organ_of_origin AS tissue_or_organ_of_origin,
            cl.diag__tumor_stage AS tumor_stage,
            cl.diag__age_at_diagnosis AS age_at_diagnosis,
            FROM isb-project-zero.GDC_Clinical_Data.r29_TARGET_clinical cl
            JOIN isb-project-zero.GDC_metadata.rel29_caseData meta
                ON meta.case_gdc_id = cl.case_id
            """,
            "TCGA": """
            SELECT cl.case_id AS case_gdc_id, cl.submitter_id AS case_barcode, cl.disease_type, cl.primary_site, 
            null as disease_code, meta.program_name, meta.project_id as project_short_name,            
            cl.demo__ethnicity AS ethnicity, 
            cl.demo__gender AS gender, 
            cl.demo__race AS race, 
            cl.demo__vital_status AS vital_status,
            cl.diag__morphology AS morphology,
            cl.diag__site_of_resection_or_biopsy AS site_of_resection_or_biopsy,
            cl.diag__tissue_or_organ_of_origin AS tissue_or_organ_of_origin,
            cl.diag__tumor_stage AS tumor_stage,
            cl.diag__age_at_diagnosis AS age_at_diagnosis,
            cl.diag__prior_malignancy AS prior_malignancy,
            cl.diag__ajcc_pathologic_stage AS ajcc_pathologic_stage,
            cl.diag__ajcc_pathologic_m AS ajcc_pathologic_m,
            cl.diag__ajcc_pathologic_n AS ajcc_pathologic_n,
            cl.diag__ajcc_pathologic_t AS ajcc_pathologic_t,
            cl.exp__pack_years_smoked AS pack_years_smoked,
            cl.exp__alcohol_history AS alcohol_history
            FROM isb-project-zero.GDC_Clinical_Data.r29_TCGA_clinical cl
            JOIN isb-project-zero.GDC_metadata.rel29_caseData meta
                ON meta.case_gdc_id = cl.case_id
            """,
            "VAREPOP": """
            SELECT cl.case_id AS case_gdc_id, cl.submitter_id AS case_barcode, cl.disease_type, cl.primary_site, 
            null as disease_code, meta.program_name, meta.project_id as project_short_name,
            cl.demo__ethnicity AS ethnicity, 
            cl.demo__gender AS gender, 
            cl.demo__race AS race, 
            cl.demo__vital_status AS vital_status,
            cl.diag__morphology AS morphology,
            cl.diag__site_of_resection_or_biopsy AS site_of_resection_or_biopsy,
            cl.diag__tissue_or_organ_of_origin AS tissue_or_organ_of_origin,
            cl.diag__tumor_stage AS tumor_stage,
            cl.diag__age_at_diagnosis AS age_at_diagnosis,
            cl.diag__prior_malignancy AS prior_malignancy,
            cl.exp__alcohol_history AS alcohol_history
            FROM isb-project-zero.GDC_Clinical_Data.r29_VAREPOP_clinical cl
            JOIN isb-project-zero.GDC_metadata.rel29_caseData meta
                ON meta.case_gdc_id = cl.case_id
            """,
            "WCDT": """
            SELECT cl.case_id AS case_gdc_id, cl.submitter_id AS case_barcode, cl.disease_type, cl.primary_site, 
            null as disease_code, meta.program_name, meta.project_id as project_short_name,
            cl.demo__ethnicity AS ethnicity, 
            cl.demo__gender AS gender, 
            cl.demo__race AS race, 
            cl.demo__vital_status AS vital_status,
            cl.diag__morphology AS morphology,
            cl.diag__site_of_resection_or_biopsy AS site_of_resection_or_biopsy,
            cl.diag__tissue_or_organ_of_origin AS tissue_or_organ_of_origin,
            cl.diag__tumor_stage AS tumor_stage,
            cl.diag__age_at_diagnosis AS age_at_diagnosis,
            cl.diag__ajcc_pathologic_stage AS ajcc_pathologic_stage,
            FROM isb-project-zero.GDC_Clinical_Data.r29_WCDT_clinical cl
            JOIN isb-project-zero.GDC_metadata.rel29_caseData meta
                ON meta.case_gdc_id = cl.case_id
            """
        }

        for program, view_query in view_queries.items():
            program_view_name = f"webapp_{get_rel_prefix(API_PARAMS)}_{program}"
            view_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['DEV_DATASET']}.{program_view_name}"
            create_view_from_query(view_id, view_query)

    if 'list_tables_for_publication' in steps:
        print("Table changes detected--create schemas for: ")
        for table_name in build_publish_table_list():
            print(table_name)
        print()

    if 'validate_data' in steps:
        compare_gdc_releases()

    if 'copy_tables_into_production' in steps:
        publish_table_list = build_publish_table_list()
        copy_tables_into_public_project(publish_table_list)

    output_report(start, steps)


if __name__ == '__main__':
    main(sys.argv)

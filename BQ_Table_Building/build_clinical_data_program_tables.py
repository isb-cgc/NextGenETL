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
import copy
import os
import json
import time
import sys

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from common_etl.utils import (get_query_results, get_rel_prefix, has_fatal_error, get_scratch_fp,
                              create_and_load_table, load_table_from_query, write_list_to_jsonl,
                              upload_to_bucket, exists_bq_table, get_working_table_id,
                              get_webapp_table_id, build_table_id, update_table_metadata, get_filepath,
                              update_schema, copy_bq_table, update_friendly_name, list_bq_tables, format_seconds,
                              load_config, delete_bq_table, build_table_name)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


#   Getter functions, employed for readability/consistency


def make_program_list_query():
    return """
        SELECT DISTINCT(proj) 
        FROM (
            SELECT SPLIT((
                SELECT project_id
                FROM UNNEST(project)), '-')[OFFSET(0)] AS proj
            FROM `{}`
        )
        ORDER BY proj
        """.format(get_working_table_id(BQ_PARAMS))


def get_program_list():
    """Get list of the programs which have contributed data to GDC's research program.

    :return: list of research programs participating in GDC data sharing
    """
    return {prog.proj for prog in get_query_results(make_program_list_query())}


def get_one_to_many_tables(record_counts):
    """Get one-to-many tables for program.

    :param record_counts: dict max field group record counts for program
    :return: set of table names (representing field groups which cannot be flattened)
    """
    table_keys = {get_base_fg(API_PARAMS)}

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
    gdc_rel = get_rel_prefix(BQ_PARAMS)
    table_name = [gdc_rel, program, BQ_PARAMS['MASTER_TABLE']]

    # if one-to-many table, append suffix
    suffixes = get_table_suffixes()
    suffix = suffixes[table]

    if suffix:
        table_name.append(suffix)

    return build_table_name(table_name)


def build_jsonl_name(program, table, is_webapp=False):
    """
    Derive file name for adding or locating jsonl row file in google cloud bucket.
    :param program: gdc program name
    :param table: table type
    :param is_webapp: is webapp integration table?
    :return: jsonl file name
    """
    app_prefix = BQ_PARAMS['APP_JSONL_PREFIX'] if is_webapp else ''
    gdc_rel = get_rel_prefix(BQ_PARAMS)
    program = program.replace('.', '_')
    base_name = BQ_PARAMS['MASTER_TABLE']
    suffix = get_table_suffixes()[table]

    name_list = [app_prefix, gdc_rel, program, base_name, suffix]

    # remove any blank values in list
    filtered_name_list = [x for x in name_list if x]
    file_name = '_'.join(filtered_name_list)

    return file_name + '.jsonl'


def get_bq_name(field, is_webapp=False, arg_fg=None):
    """Get column name (in bq format) from full field name.

    :param field: if not table_path, full field name; else short field name
    :param arg_fg: field group containing field
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: bq column name for given field name
    """

    def get_fgs_and_id_keys(api_params):
        """ Create a dictionary of type { 'field_group' : 'id_key_field'}.

        :param api_params: api param object from yaml config
        :return: mapping dict, field group -> id_key_field
        """
        id_key_dict = dict()
        fg_config_entries = api_params['FIELD_CONFIG']

        for _fg in fg_config_entries:
            id_key_dict[_fg] = fg_config_entries[_fg]['id_key']

        return id_key_dict

    base_fg = get_base_fg(API_PARAMS)

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
    this_fg_id = get_fg_id_name(fg)
    prefix = API_PARAMS['FIELD_CONFIG'][fg]['prefix']

    # create map of {fg_names : id_keys}
    fg_to_id_key_map = get_fgs_and_id_keys(API_PARAMS)

    # if fg has no prefix, or
    #    field is child of base_fg, or
    #    function called for webapp table building: do not add prefix
    if fg == base_fg or is_webapp or not prefix:
        return field_name

    # if field is an id_key, but is not mapped to this fg: do not add prefix
    if field_name in fg_to_id_key_map.values() and field_name != this_fg_id:
        return field_name

    # if the function reaches this line, return a prefixed field:
    #  - the table is user-facing, and
    #  - this field isn't a foreign id key
    return "__".join([prefix, field_name])


def parse_bq_schema_obj(schema, fg, schema_list=None, is_webapp=False):
    """Recursively construct schema using existing metadata in main clinical table.

    :param schema: dict of flattened schema entries
    :param fg: current field group name
    :param schema_list: schema field entries for field_group
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    """

    if fg not in API_PARAMS['FIELD_CONFIG']:
        return

    for i, schema_field in enumerate(schema_list):

        field_key = merge_fg_and_field(fg, schema_field['name'])

        # if has 'fields', then the current obj contains nested objs
        if schema_field['type'] == 'RECORD':
            # if nested, recurse down to the next level
            parse_bq_schema_obj(schema, field_key, schema_field['fields'], is_webapp)

            field_config = API_PARAMS['FIELD_CONFIG']

            if fg not in field_config or 'id_key' not in field_config[fg]:
                required_field_list = None
            else:
                required_field_list = [merge_fg_and_field(fg, field_config[fg]['id_key'])]

            for field_name in required_field_list:
                schema[field_name]['mode'] = 'REQUIRED'
        else:
            # not a nested field entry--do we need to prefix the schema field name?
            # schema_field['name'] = get_bq_name(field_key, is_webapp)
            schema_field['name'] = field_key.split('.')[-1]
            schema[field_key] = schema_field


def create_schema_dict(is_webapp=False):
    """Creates schema dict using master table's bigquery.table.Table.schema attribute.

    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: flattened schema dict in format:
        {full field name: {name: 'name', type: 'field_type', description: 'description'}}
    """
    client = bigquery.Client()
    bq_table = client.get_table(get_working_table_id(BQ_PARAMS))

    schema_list = []

    for schema_field in bq_table.schema:
        json_schema_field = schema_field.to_api_repr()
        schema_list.append(schema_field.to_api_repr())

    schema = dict()

    parse_bq_schema_obj(schema, get_base_fg(API_PARAMS), schema_list, is_webapp)

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
    Using table order provided in YAML, with add't ordering for reference
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


def flatten_tables(field_groups, record_counts, is_webapp=False):
    """
    From dict containing table_name keys and sets of column names, remove
    excluded columns and merge into parent table if the field group can be
    flattened for this program.
    :param field_groups: dict of tables and columns
    :param record_counts: set of table names
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: flattened table column dict.
    """
    tables = get_one_to_many_tables(record_counts) # supplemental tables required
    table_columns = dict()

    field_grp_depths = {field_grp: len(field_grp.split('.')) for field_grp in field_groups.keys()}
    excluded_fields = get_excluded_fields_all_fgs(field_groups, is_webapp)

    for field_grp, depth in sorted(field_grp_depths.items(), key=lambda i: i[1]):

        if depth > 3:
            print("\n[INFO] Caution, not confirmed to work with nested depth > 3\n")

        if is_webapp and field_grp in excluded_fields:
            continue

        field_groups[field_grp] = remove_excluded_fields(field_groups[field_grp], field_grp, excluded_fields, is_webapp)
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


def find_program_structure(cases, is_webapp=False):
    """
    Determine table structure required for the given program.
    :param cases: dict of program's case records
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: dict of tables and columns, dict with maximum record count for
    this program's field groups.
    """

    fgs = {}
    record_counts = {}

    for case in cases:
        if case:
            examine_case(fgs, record_counts, case, get_base_fg(API_PARAMS))

    for field_grp in fgs:
        if field_grp not in API_PARAMS['FIELD_CONFIG']:
            print("{0} not in metadata".format(field_grp))
            fgs.pop(field_grp)
            cases.pop(field_grp)

    columns = flatten_tables(fgs, record_counts, is_webapp)

    record_counts = {k: v for k, v in record_counts.items() if record_counts[k] > 0}

    if is_webapp:
        excluded_field_groups = API_PARAMS['FG_CONFIG']['excluded_fgs']

        for field_grp in record_counts.copy().keys():
            if field_grp in excluded_field_groups:
                record_counts.pop(field_grp)

        for field_grp in columns.copy().keys():
            if field_grp in excluded_field_groups:
                columns.pop(field_grp)

    return columns, record_counts


def get_field_group(field_name):
    """Gets parent field group (might not be the parent *table*, as the ancestor fg
    could be flattened).
    :param field_name: field name for which to retrieve ancestor field group
    :return: ancestor field group
    """
    return ".".join(field_name.split('.')[:-1])


def get_parent_fg(tables, field_name):
    """
    Get field's parent table name.
    :param tables: list of table names for program
    :param field_name: full field name for which to retrieve parent table
    :return: parent table name
    """
    # remove field from period-delimited field group string
    parent_table = get_field_group(field_name)

    while parent_table and parent_table not in tables:
        # remove field from period-delimited field group string
        parent_table = get_field_group(parent_table)

    if parent_table:
        return parent_table
    return has_fatal_error("No parent fg found for {}".format(field_name))


def get_base_fg(api_params):
    """Get the first-level field group, of which all other field groups are descendents.
    :param api_params: api param object from yaml config
    :return: base field group name
    """
    if 'FG_CONFIG' not in api_params:
        has_fatal_error("FG_CONFIG not set (in api_params) in YAML.", KeyError)
    if 'base_fg' not in api_params['FG_CONFIG'] or not api_params['FG_CONFIG']['base_fg']:
        has_fatal_error("base_fg not set (in api_params['FG_CONFIG']) in YAML.", KeyError)

    return api_params['FG_CONFIG']['base_fg']


def get_fg_id_name(field_group, is_webapp=False):
    """Retrieves the id key used to uniquely identify a table record.

    :param field_group: table for which to determine the id key
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: str representing table key
    """
    fg_id_key = get_field_group_id_key(field_group, is_webapp)
    return get_field_name(fg_id_key)


def get_field_group_id_key(field_group, is_webapp=False):
    """Retrieves the id key used to uniquely identify a table record.

    :param field_group: table for which to determine the id key
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: str representing table key
    """

    split_fg = field_group.split('.')

    if split_fg[0] != API_PARAMS['PARENT_FG']:
        split_fg.insert(0, API_PARAMS['PARENT_FG'])
        field_group = ".".join(split_fg)

    fg_id_name = API_PARAMS['FIELD_CONFIG'][field_group]['id_key']

    fg_id_key = '{}.{}'.format(field_group, fg_id_name)

    if is_webapp:
        if fg_id_key in API_PARAMS['RENAMED_FIELDS']:
            return API_PARAMS['RENAMED_FIELDS'][fg_id_key]

    return fg_id_key


def get_sorted_fg_depths(record_counts, reverse=False):
    """Returns a sorted dict of field groups: depths.

    :param record_counts: dict containing field groups and associated record counts
    :param reverse: if True, sort in DESC order, otherwise sort in ASC order
    :return: tuples composed of field group names and record counts
    """
    table_depths = {table: len(table.split('.')) for table in record_counts}

    return sorted(table_depths.items(), key=lambda item: item[1], reverse=reverse)


def get_field_name(field_col_key):
    """Get short field name from full field or bq column name.

    :param field_col_key: full field or bq column name
    :return: short field name
    """
    if '.' not in field_col_key and '__' not in field_col_key:
        return field_col_key

    split_char = '.' if '.' in field_col_key else '__'

    return field_col_key.split(split_char)[-1]


def merge_fg_and_field(field_group, field):
    """Get full field key ("{field_group}.{field_name}"}.

    :param field_group: field group to which the field belongs
    :param field: field name
    :return: full field key string
    """
    return '{}.{}'.format(field_group, field)


#   Schema creation


def get_count_column_index(table_name, column_order_dict):
    """
    Get index of child table record count reference column.
    :param table_name: table for which to get index
    :param column_order_dict: dict containing column indexes
    :return: count column start idx position
    """
    table_id_key = get_fg_id_name(table_name)
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
        source_table = get_full_table_name(program, get_base_fg(API_PARAMS))
    else:
        bq_col_name = get_bq_name(column)
        source_table = get_full_table_name(program, parent_table)

    return {
        "name": get_field_name(column),
        "type": 'STRING',
        "description": ("Reference to ancestor {}, located in {}.".format(bq_col_name, source_table)),
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
     Add reference columns containing record counts for associated BQ tables
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


def add_ref_columns(columns, record_counts, schema=None, program=None, is_webapp=False):
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
    :param is_webapp: if caller is part of webapp table build logic, True. Bypasses
    the insertion of irrelevant reference columns
    :return: table_columns, schema_dict, column_order_dict
    """
    column_orders = dict()

    if not is_webapp and (not program or not schema):
        has_fatal_error("invalid arguments for add_reference_columns. if not is_webapp, "
                        "schema and program are required.", ValueError)

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
        table_id_key = get_field_group_id_key(field_grp)
        id_idx = column_orders[field_grp][table_id_key]

        # get parent id key, append to column order dict of child field group, increment
        # index from field_grp_id_key's position, assign to parent_id_key (foreign
        # reference key)

        # remove field from period-delimited field group string
        field_group_key = ".".join(field_grp.split('.')[:-1])

        parent_id_key = get_field_group_id_key(field_group_key, is_webapp)

        if is_webapp:
            base_field_grp_id_key = get_field_group_id_key(get_base_fg(API_PARAMS), is_webapp)

            # append parent_id_key to field_grp column list and column order dict
            columns[field_grp].add(parent_id_key)
            column_orders[field_grp][parent_id_key] = id_idx
            idx = id_idx + 1

            # if parent_id_key is not the base_id_key, append it in both places as well
            if parent_id_key != base_field_grp_id_key:
                columns[field_grp].add(base_field_grp_id_key)
                column_orders[field_grp][base_field_grp_id_key] = idx + 1
                idx += 1
        else:
            idx = id_idx
            # if not webapp, there are additional reference columns to insert
            # (count of foreign records associated with the current field_grp)
            if depth > 2:
                insert_ref_id_keys(schema, columns, column_orders, field_grp, (idx, parent_id_key, program))
                idx += 1

            base_field_grp_id_key = get_field_group_id_key(get_base_fg(API_PARAMS))

            insert_ref_id_keys(schema, columns, column_orders, field_grp, (idx, base_field_grp_id_key, program))
            idx += 1

            add_record_count_refs(schema, columns, column_orders, field_grp)

    return column_orders


def merge_column_orders(schema, columns, record_counts, column_orders, is_webapp=False):
    """
    Merge flattenable column order dicts
    :param schema: dict containing schema records
    :param columns: dict containing table column keys
    :param record_counts: field group count dict
    :param column_orders: dict of field groups : and fields with their respective indices
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: merged column orders dict
    """
    merged_column_orders = dict()

    for table, depth in get_sorted_fg_depths(record_counts, reverse=True):

        table_id_key = get_field_group_id_key(table, is_webapp)

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
    Remove fields composed of only null values for a program, thus making the tables less
    sparse.
    :param columns: dict containing table column keys
    :param merged_orders: merged dict of field groups: fields with index position data
    """
    for table, cols in columns.items():
        null_fields_set = set(merged_orders[table].keys()) - cols

        for field in null_fields_set:
            merged_orders[table].pop(field)


def create_app_schema_lists(schema, record_counts, merged_orders):
    """
    Create smaller schemas for each table, containing only columns contained there.
    :param schema: dict containing schema records
    :param record_counts: field group count dict
    :param merged_orders: merged dict of field groups: fields with index position data
    :return: schema_field_lists, one schema per field group turned into table
    """

    schema_field_lists = dict()

    for table in get_one_to_many_tables(record_counts):
        schema_field_lists[table] = list()

        if table not in merged_orders:
            has_fatal_error("record counts and merged orders disagree on program's table architecture")

        for field in merged_orders[table]:
            schema_field_lists[table].append(schema[field])

    return schema_field_lists


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
                print("{0} not in src table; excluding schema field.".format(column))
                continue
            schema_field_lists_dict[table].append(bigquery.SchemaField.from_api_repr(schema[column]))

    return schema_field_lists_dict


def get_excluded_fields_all_fgs(fgs, is_webapp=False):
    """Get a list of fields for each field group to exclude from the tables
    from yaml config (API_PARAMS['FIELD_CONFIG']['excluded_fields'] or
    API_PARAMS['FIELD_CONFIG']['app_excluded_fields'] for the web app).

    :param fgs: list of expand field groups included from API call
    :param is_webapp: is script currently running for 'create_webapp_tables' step?
    :return: set of fields to exclude
    """
    excluded_list_key = 'app_excluded_fields' if is_webapp else 'excluded_fields'

    exclude_fields = set()

    for fg in fgs:
        if not API_PARAMS['FIELD_CONFIG'][fg] or not API_PARAMS['FIELD_CONFIG'][fg][excluded_list_key]:
            continue

        for field in API_PARAMS['FIELD_CONFIG'][fg][excluded_list_key]:
            exclude_fields.add(merge_fg_and_field(fg, field))

    return exclude_fields


def remove_excluded_fields(case, field_grp, excluded, is_webapp):
    """
    Remove columns with only None values, as well as those excluded.
    :param case: field_grp record to parse.
    :param field_grp: name of destination table.
    :param excluded: set of fields to exclude from the final db tables
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: Trimmed down record dict.
    """
    if isinstance(case, dict):
        excluded_fields = {get_bq_name(field, is_webapp, field_grp)
                           for field in excluded}

        for field in case.copy().keys():
            if field in excluded_fields or not case[field]:
                case.pop(field)

        return case

    if isinstance(case, set):
        return {field for field in case if field not in excluded}

    return [field for field in case if field not in excluded]


#   Functions used for parsing and loading data into BQ tables


def flatten_case_entry(record, fg, flat_case, case_id, pid, pid_name, is_webapp):
    """
    Recursively traverse the case json object, creating dict of format:
     {field_group: [records]}
    :param record: the case data object to recurse and flatten
    :param fg: name of the case's field group currently being processed.
    :param flat_case: partially-built flattened case dict
    :param case_id: case id
    :param pid: parent field group id
    :param pid_name: parent field group id key
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    """

    def get_excluded_fields_one_fg():
        excluded_key = 'app_excluded_fields' if is_webapp else 'excluded_fields'
        excluded_list = API_PARAMS['FIELD_CONFIG'][fg][excluded_key]
        return [get_bq_name(f, is_webapp, fg) for f in excluded_list]

    # entry represents a field group, recursively flatten each record
    if fg not in API_PARAMS['FIELD_CONFIG'].keys():
        return

    base_pid_name = get_fg_id_name(get_base_fg(API_PARAMS), is_webapp)

    if isinstance(record, list):
        # flatten each record in field group list
        for entry in record:
            flatten_case_entry(entry, fg, flat_case, case_id, pid, pid_name, is_webapp)
        return
    else:
        row = dict()

        fg_id_name = get_fg_id_name(fg, is_webapp)

        for field, columns in record.items():
            # if list, possibly more than one entry, recurse over list
            if isinstance(columns, list):
                flatten_case_entry(record=columns,
                                   fg=merge_fg_and_field(fg, field),
                                   flat_case=flat_case,
                                   case_id=case_id,
                                   pid=record[fg_id_name],
                                   pid_name=fg_id_name,
                                   is_webapp=is_webapp)
                continue
            else:  # todo is this needed?
                if fg_id_name != pid_name:
                    # remove field from period-delimited field group string
                    parent_fg = ".".join(fg.split('.')[:-1])
                    pid_key = get_bq_name(pid_name, is_webapp, parent_fg)

                    # add parent_id key and value to row
                    row[pid_key] = pid

                if fg_id_name != base_pid_name:
                    row[base_pid_name] = case_id

                column = get_bq_name(field, is_webapp, fg)

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


def flatten_case(case, is_webapp):
    """
    Converts nested case object into a flattened representation of its records.
    :param case: dict containing case data
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: flattened case dict
    """

    base_fg = get_base_fg(API_PARAMS)
    get_field_group_id_key(base_fg, is_webapp)

    if is_webapp:
        for old_key, new_key in API_PARAMS['RENAMED_FIELDS'].items():
            if len(old_key.split('.')) == 2:
                old_name = get_field_name(old_key)
                new_name = get_field_name(new_key)
                if old_name in case:
                    case[new_name] = case[old_name]
                    case.pop(old_name)

    base_id_name = get_fg_id_name(base_fg, is_webapp)

    flat_case = dict()

    flatten_case_entry(record=case,
                       fg=base_fg,
                       flat_case=flat_case,
                       case_id=case[base_id_name],
                       pid=case[base_id_name],
                       pid_name=base_id_name,
                       is_webapp=is_webapp)

    if is_webapp:
        renamed_fields = API_PARAMS['RENAMED_FIELDS']

        base_id_key = get_field_group_id_key(base_fg)

        # if case_id in renamed fields (it is), remove the grandparent addition of case_id to doubly nested tables--
        # naming would be incorrect, and it's unnecessary info for webapp tables.
        if base_id_key in renamed_fields:
            base_id_name = get_field_name(base_id_key)

            fg_keys = list(filter(lambda k: len(k.split('.')) > 2, flat_case.keys()))

            for i, fg_key in enumerate(fg_keys):
                for j, fg_entry in enumerate(flat_case[fg_key]):
                    if base_id_name in fg_entry:
                        flat_case[fg_key][j].pop(base_id_name)

    return flat_case


def get_record_idx(flat_case, field_grp, record_id, is_webapp=False):
    """
    Get index of record associated with record_id from flattened_case
    :param flat_case: dict containing {field group names: list of record dicts}
    :param field_grp: field group containing record_id
    :param record_id: id of record for which to retrieve position
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: position index of record in field group's record list
    """
    field_grp_id_name = get_fg_id_name(field_grp, is_webapp)
    field_grp_id_key = get_bq_name(field_grp_id_name, is_webapp, field_grp)
    idx = 0

    # iterate until id found in record--if not found, fatal error
    for record in flat_case[field_grp]:
        if record[field_grp_id_key] == record_id:
            return idx
        idx += 1

    return has_fatal_error("id {} not found by get_record_idx.".format(record_id))


def merge_single_entry_fgs(flat_case, record_counts, is_webapp=False):
    """
    # Merge flatten-able field groups.
    :param flat_case: flattened case dict
    :param record_counts: field group count dict
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    """
    tables = get_one_to_many_tables(record_counts)

    flattened_field_grp_parents = dict()

    for field_grp in record_counts:
        if field_grp == get_base_fg(API_PARAMS):
            continue
        if record_counts[field_grp] == 1:
            if field_grp in flat_case:
                # create list of flattened field group destination tables
                flattened_field_grp_parents[field_grp] = get_parent_fg(tables, field_grp)

    for field_grp, parent in flattened_field_grp_parents.items():
        field_grp_id_name = get_fg_id_name(parent, is_webapp)
        bq_parent_id_key = get_bq_name(field_grp_id_name, is_webapp, parent)

        for record in flat_case[field_grp]:
            parent_id = record[bq_parent_id_key]
            parent_idx = get_record_idx(flat_case, parent, parent_id, is_webapp)
            flat_case[parent][parent_idx].update(record)
        flat_case.pop(field_grp)


def get_record_counts(flat_case, record_counts, is_webapp=False):
    """
    # Get record counts for field groups in case record
    :param flat_case: flattened dict containing case record entries
    :param record_counts: field group count dict
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    """
    # initialize dict with field groups that can't be flattened
    record_count_dict = {field_grp: dict() for field_grp in record_counts if record_counts[field_grp] > 1}

    tables = get_one_to_many_tables(record_counts)

    for field_grp in record_count_dict:
        parent_field_grp = get_parent_fg(tables, field_grp)
        field_grp_id_name = get_fg_id_name(parent_field_grp, is_webapp)
        parent_id_key = get_bq_name(field_grp_id_name, is_webapp, parent_field_grp)

        # initialize record counts for parent id
        if parent_field_grp in flat_case:
            for parent_record in flat_case[parent_field_grp]:
                parent_id = parent_record[parent_id_key]
                record_count_dict[field_grp][parent_id] = 0

            # count child records
            if field_grp in flat_case:
                for record in flat_case[field_grp]:
                    if parent_id_key in record:
                        parent_id = record[parent_id_key]
                    else:
                        # todo this is weird, but I'm hesitant to mess with it.
                        #  I can probably revisit flatten case functionality entirely, I found a better method
                        #  for this for PDC
                        parent_id = parent_record[parent_id_key]

                    record_count_dict[field_grp][parent_id] += 1

    # insert record count into flattened dict entries
    for field_grp, parent_ids in record_count_dict.items():
        parent_field_grp = get_parent_fg(tables, field_grp)
        count_name = get_bq_name('count', is_webapp, field_grp)

        for parent_id, count in parent_ids.items():
            p_key_idx = get_record_idx(flat_case, parent_field_grp, parent_id, is_webapp)
            flat_case[parent_field_grp][p_key_idx][count_name] = count


def merge_or_count_records(flattened_case, record_counts, is_webapp=False):
    """
    If program field group has max record count of 1, flattens into parent table.
    Otherwise, counts record in one-to-many table and adds count field to parent record
    in flattened_case
    :param flattened_case: flattened dict containing case record's values
    :param record_counts: field group count dict max counts for program's field group
    records
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: modified version of flattened_case
    """
    merge_single_entry_fgs(flattened_case, record_counts, is_webapp)
    # initialize counts for parent_ids for every possible child table (some child tables
    # won't actually have records, and this initialization adds 0 counts in that case)
    if not is_webapp:
        get_record_counts(flattened_case, record_counts, is_webapp)


def create_and_load_tables(program, cases, schemas, record_counts, is_webapp=False):
    """
    Create jsonl row files for future insertion, store in GC storage bucket,
    then insert the new table schemas and data.
    :param program: program for which to create tables
    :param cases: case records to insert into BQ for program
    :param schemas: dict of schema lists for all of this program's tables
    :param record_counts: field group count dict
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    """
    record_tables = get_one_to_many_tables(record_counts)

    for record_table in record_tables:
        jsonl_name = build_jsonl_name(program, record_table, is_webapp)
        jsonl_fp = get_scratch_fp(BQ_PARAMS, jsonl_name)

        # If jsonl scratch file exists, delete so we don't append
        if os.path.exists(jsonl_fp):
            os.remove(jsonl_fp)

    added_age_at_diagnosis_days = False

    for i, case in enumerate(cases):
        if is_webapp:
            # add derived age_at_diagnosis in years (from days)
            if 'diagnoses' in case:
                new_diagnosis_list = []
                for diagnosis in case['diagnoses']:
                    if 'age_at_diagnosis' in diagnosis and diagnosis['age_at_diagnosis']:
                        diagnosis['age_at_diagnosis_days'] = diagnosis['age_at_diagnosis']
                        diagnosis['age_at_diagnosis'] = diagnosis['age_at_diagnosis_days'] // 365
                        added_age_at_diagnosis_days = True
                    new_diagnosis_list.append(diagnosis)
                case['diagnoses'] = new_diagnosis_list

            program_name = program.replace("_", ".")
            case['program_name'] = program_name

        flat_case = flatten_case(case, is_webapp)

        # remove excluded field groups
        for fg in flat_case.copy():
            if fg not in record_counts:
                flat_case.pop(fg)

        merge_or_count_records(flat_case, record_counts, is_webapp)

        if is_webapp:
            if 'project_id' in flat_case['cases'][0]:
                flat_case['cases'][0]['project_short_name'] = flat_case['cases'][0]['project_id']

        for bq_table in flat_case:
            if bq_table not in record_tables:
                has_fatal_error("Table {} not found in table keys".format(bq_table))

            jsonl_name = build_jsonl_name(program, bq_table, is_webapp)
            jsonl_fp = get_scratch_fp(BQ_PARAMS, jsonl_name)

            write_list_to_jsonl(jsonl_fp, flat_case[bq_table], 'a')

        if i % 100 == 0:
            print("wrote case {} of {} to jsonl".format(i, len(cases)))

    if is_webapp:
        if added_age_at_diagnosis_days:
            age_at_diagnosis_days_schema = {
                'mode': 'NULLABLE',
                'name': 'age_at_diagnosis_days',
                'type': 'INTEGER',
                'description': ""
            }

            if 'cases.diagnoses' in schemas and 'age_at_diagnosis_days' not in schemas['cases.diagnoses']:
                schemas['cases.diagnoses'].append(age_at_diagnosis_days_schema)
            elif 'age_at_diagnosis_days' not in schemas['cases']:
                schemas['cases'].append(age_at_diagnosis_days_schema)

        disease_code_schema = {
            'mode': 'NULLABLE',
            'name': 'disease_code',
            'type': 'STRING',
            'description': ""
        }

        program_name_schema = {
            'mode': 'NULLABLE',
            'name': 'program_name',
            'type': 'STRING',
            'description': ""
        }

        project_short_name_schema = {
            'mode': 'NULLABLE',
            'name': 'project_short_name',
            'type': 'STRING',
            'description': ""
        }

        schemas['cases'].append(disease_code_schema)
        schemas['cases'].append(program_name_schema)
        schemas['cases'].append(project_short_name_schema)

    for record_table in record_tables:
        jsonl_name = build_jsonl_name(program, record_table, is_webapp)

        print("Upload {} to bucket".format(jsonl_name))

        upload_to_bucket(BQ_PARAMS, get_scratch_fp(BQ_PARAMS, jsonl_name))

        table_name = get_full_table_name(program, record_table)

        if is_webapp:
            table_id = get_webapp_table_id(BQ_PARAMS, table_name)
        else:
            table_id = get_working_table_id(BQ_PARAMS, table_name)

        create_and_load_table(BQ_PARAMS, jsonl_name, table_id, schemas[record_table])


def get_metadata_files():
    """Get all the file names in a directory as a list of as strings.

    :return: list of filenames
    """
    rel_path = '/'.join([BQ_PARAMS['BQ_REPO'], BQ_PARAMS['TABLE_METADATA_DIR'], get_rel_prefix(BQ_PARAMS)])
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
        rel = get_rel_prefix(BQ_PARAMS)
        return '_'.join([rel, program_name, program_table_name])

    table_name = convert_json_to_table_name()
    table_id = get_working_table_id(BQ_PARAMS, table_name)

    if not exists_bq_table(table_id):
        print('\t\t- skipping -- no table found for file: {}'.format(json_file))
        return None
    else:
        print('\t- updating {}'.format(json_file))
        return table_id


def update_metadata():
    """
    Use .json file in the BQEcosystem repo to update a bq table's metadata
    (labels, description, friendly name)
    """
    print("Updating metadata!")

    for json_file in get_metadata_files():
        table_id = make_and_check_metadata_table_id(json_file)

        if not table_id:
            continue

        metadata_dir = '/'.join([BQ_PARAMS['BQ_REPO'], BQ_PARAMS['TABLE_METADATA_DIR'], get_rel_prefix(BQ_PARAMS)])
        metadata_fp = get_filepath(metadata_dir, json_file)

        with open(metadata_fp) as json_file_output:
            metadata = json.load(json_file_output)
            update_table_metadata(table_id, metadata)


def update_clin_schema():
    """
    Alter an existing table's schema (currently, only field descriptions are mutable
    without a table rebuild, Google's restriction).
    """
    print("\nUpdating schemas (field descriptions)!")

    dir_path = '/'.join([BQ_PARAMS['BQ_REPO'], BQ_PARAMS['FIELD_DESC_DIR']])
    fields_file = "{}_{}.json".format(BQ_PARAMS['FIELD_DESC_FILE_PREFIX'], get_rel_prefix(BQ_PARAMS))
    field_desc_fp = get_filepath(dir_path, fields_file)

    with open(field_desc_fp) as field_output:
        descriptions = json.load(field_output)

    for json_file in get_metadata_files():
        table_id = make_and_check_metadata_table_id(json_file)

        if not table_id:
            continue

        update_schema(table_id, descriptions)


def change_status_to_archived(table_id):
    client = bigquery.Client()
    current_release_tag = get_rel_prefix(BQ_PARAMS)
    stripped_table_id = table_id.replace(current_release_tag, "")
    previous_release_tag = BQ_PARAMS['REL_PREFIX'] + str(int(BQ_PARAMS['RELEASE']) - 1)
    prev_table_id = stripped_table_id + previous_release_tag

    try:
        prev_table = client.get_table(prev_table_id)
        prev_table.labels['status'] = 'archived'
        print("labels: {}".format(prev_table.labels))
        client.update_table(prev_table, ["labels"])

        assert prev_table.labels['status'] == 'archived'
    except NotFound:
        print("Not writing archived label for table that didn't exist in a previous version.")


def copy_tables_into_public_project(publish_table_list):
    """Move production-ready bq tables onto the public-facing production server.

    """

    def get_publish_table_ids(bq_params, src_table_name):
        split_table_name = src_table_name.split('_')
        release = split_table_name.pop(0)
        program = split_table_name.pop(0)
        pub_table_name = '_'.join(split_table_name)

        prod_project = bq_params['PROD_PROJECT']

        curr_dataset = program
        curr_table_name = "_".join([pub_table_name, bq_params['DATA_SOURCE'], 'current'])
        current_table_id = build_table_id(prod_project, curr_dataset, curr_table_name)

        versioned_dataset = program + '_versioned'
        versioned_table_name = "_".join([pub_table_name, bq_params['DATA_SOURCE'], release])
        versioned_table_id = build_table_id(prod_project, versioned_dataset, versioned_table_name)

        return current_table_id, versioned_table_id

    for table_name in publish_table_list:
        src_table_id = get_working_table_id(BQ_PARAMS, table_name)
        curr_table_id, vers_table_id = get_publish_table_ids(BQ_PARAMS, table_name)

        print("Publishing {}".format(vers_table_id))
        copy_bq_table(BQ_PARAMS, src_table_id, vers_table_id, replace_table=True)
        print("Publishing {}".format(curr_table_id))
        copy_bq_table(BQ_PARAMS, src_table_id, curr_table_id, replace_table=True)

        update_friendly_name(BQ_PARAMS, vers_table_id)
        change_status_to_archived(vers_table_id)


#    Webapp specific functions


def make_biospecimen_stub_table_query(main_table_id, program):
    return """
        SELECT program_name, project_short_name, case_gdc_id, case_barcode, sample_gdc_id, sample_barcode
        FROM (
            SELECT program_name, project_short_name, case_gdc_id, case_barcode, 
                SPLIT(sample_ids, ', ') as s_gdc_ids, 
                SPLIT(submitter_sample_ids, ', ') as s_barcodes
            FROM (
                SELECT case_id as case_gdc_id, 
                    submitter_id as case_barcode, 
                    sample_ids, 
                    submitter_sample_ids, 
                    SPLIT(p.project_id, '-')[OFFSET(0)] AS program_name,
                    p.project_id as project_short_name
                FROM `{0}`,
                UNNEST(project) AS p
            )
        ), 
        UNNEST(s_gdc_ids) as sample_gdc_id WITH OFFSET pos1, 
        UNNEST(s_barcodes) as sample_barcode WITH OFFSET pos2
        WHERE pos1 = pos2
        AND program_name = '{1}'
    """.format(main_table_id, program)


def build_biospecimen_stub_tables(program):
    """
    Create one-to-many table referencing case_id (as case_gdc_id),
    submitter_id (as case_barcode), (sample_ids as sample_gdc_ids),
    and sample_submitter_id (as sample_barcode).
    :param program: the program from which the cases originate.
    """
    main_table = build_table_name([get_rel_prefix(BQ_PARAMS), BQ_PARAMS['MASTER_TABLE']])
    main_table_id = get_working_table_id(BQ_PARAMS, main_table)

    biospec_stub_table_query = make_biospecimen_stub_table_query(main_table_id, program)

    biospec_table_name = build_table_name([get_rel_prefix(BQ_PARAMS), str(program), BQ_PARAMS['BIOSPECIMEN_SUFFIX']])
    biospec_table_id = get_webapp_table_id(BQ_PARAMS, biospec_table_name)

    load_table_from_query(BQ_PARAMS, biospec_table_id, biospec_stub_table_query)


#    Script execution

def build_publish_table_list():
    old_release = BQ_PARAMS['REL_PREFIX'] + str(int(BQ_PARAMS['RELEASE']) - 1)
    new_release = BQ_PARAMS['REL_PREFIX'] + BQ_PARAMS['RELEASE']
    old_tables = set(list_bq_tables(BQ_PARAMS['DEV_DATASET'], old_release))
    new_tables = list_bq_tables(BQ_PARAMS['DEV_DATASET'], new_release)

    publish_table_list = list()

    for new_table_name in new_tables:
        if new_table_name == (new_release + '_' + BQ_PARAMS['MASTER_TABLE']):
            continue

        split_new_table = new_table_name.split('_')
        split_new_table[0] = old_release
        old_table_name = "_".join(split_new_table)
        if old_table_name not in old_tables:
            publish_table_list.append(new_table_name)
        else:
            old_table_id = get_working_table_id(BQ_PARAMS, old_table_name)
            new_table_id = get_working_table_id(BQ_PARAMS, new_table_name)

            res = get_query_results("""
                SELECT count(*) as row_count
                FROM `{}` old
                FULL JOIN `{}` curr
                    ON old.case_id = curr.case_id
                WHERE old.case_id is null 
                OR curr.case_id is null
            """.format(old_table_id, new_table_id))

            for row in res:
                if row[0] > 0:
                    publish_table_list.append(new_table_name)
                break

    return publish_table_list


def modify_fields_for_app(schema, column_order_dict, columns):
    """Alter field naming conventions so that they're compatible with those in the
    web app.

    :param schema: dict containing schema records
    :param column_order_dict: dict of {field_groups: column_order set()}
    :param columns: dict containing table column keys
    """
    renamed_fields = dict(API_PARAMS['RENAMED_FIELDS'])
    fgs = column_order_dict.keys()

    excluded_fgs = API_PARAMS['FG_CONFIG']['excluded_fgs']
    excluded_fields = get_excluded_fields_all_fgs(fgs, is_webapp=True)

    for fg in fgs:
        # rename case_id no matter which fg it's in
        for renamed_field in renamed_fields.keys():
            if renamed_field in column_order_dict[fg]:
                new_field = renamed_fields[renamed_field]
                column_order_dict[fg][new_field] = column_order_dict[fg][renamed_field]
                column_order_dict[fg].pop(renamed_field)
            if fg in columns and renamed_field in columns[fg]:
                columns[fg].add(renamed_fields[renamed_field])
                columns[fg].remove(renamed_field)

    # field is fully associated name
    for field in {k for k in schema.keys()}:
        base_fg = ".".join(field.split('.')[:-1])
        field_name = field.split('.')[-1]

        # substitute base field name for prefixed
        schema[field]['name'] = field_name

        # exclude any field groups or fields explicitly excluded in yaml
        if field in excluded_fields or base_fg in excluded_fgs:
            schema.pop(field)
        # field exists in renamed_fields, change its name
        elif field in renamed_fields:
            new_field = renamed_fields[field]

            schema[field]['name'] = new_field.split('.')[-1]
            schema[new_field] = schema[field]
            schema.pop(field)

            # change the field name in the column order dict
            if base_fg in column_order_dict and field in column_order_dict[base_fg]:
                column_order_dict[base_fg][new_field] = column_order_dict[base_fg][field]
                column_order_dict[base_fg].pop(field)

        if field in excluded_fields and base_fg in column_order_dict:
            # remove excluded field from column order lists
            if field in column_order_dict[base_fg]:
                column_order_dict[base_fg].pop(field)


def create_tables(program, cases, schema, is_webapp=False):
    """
    Run the overall script which creates schemas, modifies data, prepares it for loading,
    and creates the databases.
    :param program: the source for the inserted cases data
    :param cases: dict representations of clinical case data from GDC
    :param schema:  schema file for BQ table creation
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return:
    """

    if is_webapp:
        print(" - Creating webapp table(s).")
    else:
        print(" - Creating public BQ table(s).")

    # derive the program's table structure by analyzing its case records
    columns, record_counts = find_program_structure(cases, is_webapp)

    # add the parent id to field group dicts that will create separate tables
    column_orders = add_ref_columns(columns, record_counts, schema, program, is_webapp)

    # removes the prefix from schema field name attributes
    # removes the excluded fields/field groups
    if is_webapp:
        modify_fields_for_app(schema, column_orders, columns)

    # reassign merged_column_orders to column_orders
    merged_orders = merge_column_orders(schema, columns, record_counts, column_orders, is_webapp)

    # drop any null fields from the merged column order dicts
    remove_null_fields(columns, merged_orders)

    # creates dictionary of lists of SchemaField objects in json format
    if is_webapp:
        schemas = create_app_schema_lists(schema, record_counts, merged_orders)
    else:
        schemas = create_schema_lists(schema, record_counts, merged_orders)

    create_and_load_tables(program, cases, schemas, record_counts, is_webapp)


def make_release_fields_comparison_query(old_rel, new_rel):
    return """
        SELECT table_name AS release, field_path AS field
        FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
        WHERE field_path IN (
            SELECT field_path 
            FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
            WHERE table_name='{}_clinical' 
                OR table_name='{}_clinical'
           GROUP BY field_path
           HAVING COUNT(field_path) <= 1)
    """.format(old_rel, new_rel)


def find_release_changed_data_types_query(old_rel, new_rel):
    return """
        SELECT field_path, data_type, COUNT(field_path) AS distinct_data_type_cnt 
        FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
        WHERE (table_name='{}_clinical' OR table_name='{}_clinical')
            AND (data_type = 'INT64' OR data_type = 'FLOAT64' OR data_type = 'STRING' OR data_type = 'BOOL')
        GROUP BY field_path, data_type 
        HAVING distinct_data_type_cnt <= 1
    """.format(old_rel, new_rel)


def make_field_diff_query(old_rel, new_rel, removed_fields):
    check_rel = old_rel if removed_fields else new_rel

    return """
        SELECT field_path AS field
        FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
        WHERE field_path IN (
            SELECT field_path 
            FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
            WHERE table_name='{}_clinical' 
                OR table_name='{}_clinical'
           GROUP BY field_path
           HAVING COUNT(field_path) <= 1)
       AND table_name='{}_clinical'
    """.format(old_rel, new_rel, check_rel)


def make_datatype_diff_query(old_rel, new_rel):
    return """
        WITH data_types as (SELECT field_path, data_type
          FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
          WHERE (table_name='{}_clinical' OR table_name='{}_clinical')
            AND (data_type = 'INT64' OR data_type = 'FLOAT64' OR data_type = 'STRING' OR data_type = 'BOOL')
          GROUP BY field_path, data_type)
        SELECT field_path
        FROM data_types
        GROUP BY field_path
        HAVING COUNT(field_path) > 1
    """.format(old_rel, new_rel)


def make_removed_case_ids_query(old_rel, new_rel):
    return """
        SELECT case_id, project.project_id
        FROM `isb-project-zero.GDC_Clinical_Data.{}_clinical`
        JOIN UNNEST(project) as project
        WHERE case_id NOT IN (
            SELECT case_id 
            FROM `isb-project-zero.GDC_Clinical_Data.{}_clinical`)    
    """.format(old_rel, new_rel)


def make_added_case_ids_query(old_rel, new_rel):
    return """
        SELECT project.project_id, count(case_id) as new_case_cnt
        FROM `isb-project-zero.GDC_Clinical_Data.{}_clinical`
        JOIN UNNEST(project) as project
        WHERE case_id NOT IN (
            SELECT case_id 
            FROM `isb-project-zero.GDC_Clinical_Data.{}_clinical`)
        GROUP BY project.project_id
    """.format(new_rel, old_rel)


def make_tables_diff_query(old_rel, new_rel):
    return """
        WITH old_table_cnts AS (
          SELECT program, COUNT(program) AS num_tables 
          FROM (
            SELECT els[OFFSET(1)] AS program
            FROM (
              SELECT SPLIT(table_name, '_') AS els
              FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.TABLES
              WHERE table_name LIKE '{}%'))
          WHERE program != 'clinical'
          GROUP BY program
        ),
        new_table_cnts AS (
          SELECT program, COUNT(program) AS num_tables 
          FROM (
            SELECT els[OFFSET(1)] AS program
            FROM (
              SELECT SPLIT(table_name, '_') AS els
              FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.TABLES
              WHERE table_name LIKE '{}%'))
          WHERE program != 'clinical'
          GROUP BY program
        )

        SELECT  o.program AS prev_rel_program_name, 
                n.program AS new_rel_program_name, 
                o.num_tables AS prev_table_cnt, 
                n.num_tables AS new_table_cnt
        FROM new_table_cnts n
        FULL OUTER JOIN old_table_cnts o
          ON o.program = n.program
        WHERE o.num_tables != n.num_tables
          OR o.num_tables IS NULL or n.num_tables IS NULL
        ORDER BY n.num_tables DESC
    """.format(old_rel, new_rel)


def make_new_table_list_query(old_rel, new_rel):
    return """
        WITH old_tables AS (
          SELECT table_name
          FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.TABLES
          WHERE table_name LIKE '{0}%'
          ORDER BY table_name),
        new_tables AS (
          SELECT table_name
          FROM `isb-project-zero`.GDC_Clinical_Data.INFORMATION_SCHEMA.TABLES
          WHERE table_name LIKE '{1}%'
          ORDER BY table_name)
        
        SELECT table_name
        FROM new_tables 
        WHERE LTRIM(table_name, '{1}_') NOT IN (SELECT LTRIM(table_name, '{0}_') FROM old_tables)
    """.format(old_rel, new_rel)


def compare_gdc_releases():
    old_rel = BQ_PARAMS['REL_PREFIX'] + str(int(BQ_PARAMS['RELEASE']) - 1)
    new_rel = get_rel_prefix(BQ_PARAMS)

    print("\n\n*** {} -> {} GDC Clinical Data Comparison Report ***".format(old_rel, new_rel))

    # which fields have been removed?
    removed_fields_res = get_query_results(make_field_diff_query(old_rel, new_rel, removed_fields=True))
    print("\nRemoved fields:")

    if removed_fields_res.total_rows == 0:
        print("<none>")
    else:
        for row in removed_fields_res:
            print(row[0])

    # which fields were added?
    added_fields_res = get_query_results(make_field_diff_query(old_rel, new_rel, removed_fields=False))
    print("\nNew GDC API fields:")

    if added_fields_res.total_rows == 0:
        print("<none>")
    else:
        for row in added_fields_res:
            print(row[0])

    # any changes in field data type?
    datatype_diff_res = get_query_results(make_datatype_diff_query(old_rel, new_rel))
    print("\nColumns with data type change:")

    if datatype_diff_res.total_rows == 0:
        print("<none>")
    else:
        for row in datatype_diff_res:
            print(row[0])

    # any case ids removed?
    print("\nRemoved case ids:")
    removed_case_ids_res = get_query_results(make_removed_case_ids_query(old_rel, new_rel))

    if removed_case_ids_res.total_rows == 0:
        print("<none>")
    else:
        for row in removed_case_ids_res:
            print(row[0])

    # any case ids added?
    print("\nAdded case id counts:")
    added_case_ids_res = get_query_results(make_added_case_ids_query(old_rel, new_rel))

    if added_case_ids_res.total_rows == 0:
        print("<none>")
    else:
        for row in added_case_ids_res:
            print("{}: {} new case ids".format(row[0], row[1]))

    # any case ids added?
    print("\nTable count changes: ")
    table_count_res = get_query_results(make_tables_diff_query(old_rel, new_rel))

    if table_count_res.total_rows == 0:
        print("<none>")
    else:
        for row in table_count_res:
            program_name = row[0] if row[0] else row[1]
            prev_table_cnt = 0 if not row[2] else row[2]
            new_table_cnt = 0 if not row[3] else row[3]

            print("{}: {} table(s) in {}, {} table(s) in {}".format(program_name,
                                                                    prev_table_cnt, old_rel,
                                                                    new_table_cnt, new_rel))

    print("\nAdded tables: ")
    added_table_res = get_query_results(make_new_table_list_query(old_rel, new_rel))

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
    print("Script executed in {0}\n".format(format_seconds(seconds)))

    print("Steps completed: ")

    if 'create_biospecimen_stub_tables' in steps:
        print('\t - created biospecimen stub tables for webapp use')
    if 'create_webapp_tables' in steps:
        print('\t - created tables for webapp use')
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

    sample_table_name = build_table_name([get_rel_prefix(BQ_PARAMS), str(program), BQ_PARAMS['BIOSPECIMEN_SUFFIX']])
    sample_table_id = build_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['APP_DATASET'], sample_table_name)

    query = """
        SELECT * 
        FROM `{}` 
        WHERE case_id IN (
            SELECT DISTINCT(case_gdc_id) 
            FROM `{}`
            WHERE program_name = '{}')
    """.format(get_working_table_id(BQ_PARAMS), sample_table_id, program)

    for case_row in get_query_results(query):
        case_items = dict(case_row.items())
        # case_items.pop('project')
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
    except ValueError as err:
        has_fatal_error(str(err), ValueError)

    if not API_PARAMS['FIELD_CONFIG']:
        has_fatal_error("params['FIELD_CONFIG'] not found")

    # programs = ['BEATAML1.0']
    programs = get_program_list()
    programs = sorted(programs)

    for orig_program in programs:
        prog_start = time.time()
        if ('create_biospecimen_stub_tables' in steps or
                'create_webapp_tables' in steps or
                'create_and_load_tables' in steps):
            print("\nRunning script for program: {0}...".format(orig_program))

        if 'create_biospecimen_stub_tables' in steps:
            # these tables are used to populate the per-program clinical tables and the webapp tables
            print(" - Creating biospecimen stub tables!")
            build_biospecimen_stub_tables(orig_program)

        if 'create_webapp_tables' in steps or 'create_and_load_tables' in steps:
            cases = get_cases_by_program(orig_program)

            if not cases:
                print("No cases found for program {0}, skipping.".format(orig_program))
                continue

            # rename so that '1.0' doesn't break bq table name
            program = orig_program.replace('.', '_')

            if 'create_webapp_tables' in steps:  # FOR WEBAPP TABLES
                schema = create_schema_dict(is_webapp=True)
                webapp_cases = copy.deepcopy(cases)
                create_tables(program, webapp_cases, schema, is_webapp=True)

            if 'create_and_load_tables' in steps:
                schema = create_schema_dict()
                create_tables(program, cases, schema)

            prog_end = time.time() - prog_start
            print("{0} processed in {1}!\n".format(program, format_seconds(prog_end)))

    if 'list_tables_for_publication' in steps:
        print("Table changes detected--create schemas for: ")
        for table_name in build_publish_table_list():
            print(table_name)
        print()

    if 'validate_data' in steps:
        compare_gdc_releases()

    if 'update_table_metadata' in steps:
        update_metadata()

    if 'update_schema' in steps:
        update_clin_schema()

    if 'cleanup_tables' in steps:
        for table_id in BQ_PARAMS['DELETE_TABLES']:
            project = table_id.split('.')[0]

            if project != BQ_PARAMS['DEV_PROJECT']:
                has_fatal_error("Can only use cleanup_tables on DEV_PROJECT.")

            delete_bq_table(table_id)
            print("Deleted table: {}".format(table_id))

    if 'copy_tables_into_production' in steps:
        publish_table_list = build_publish_table_list()
        copy_tables_into_public_project(publish_table_list)

    output_report(start, steps)


if __name__ == '__main__':
    main(sys.argv)

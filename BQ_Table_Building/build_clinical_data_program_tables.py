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
import math

from common_etl.utils import *

# from temp.gdc_clinical_resources_OLD.generate_docs import generate_docs

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


#   Getter functions, employed for readability/consistency


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
    suffixes = get_table_suffixes(API_PARAMS)
    suffix = suffixes[table]

    if suffix:
        table_name.append(suffix)

    return build_table_name(table_name)


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
            field_key = get_field_key(field_grp, field_name)

            # assign index to field, then increment
            column_orders[field_key] = idx
            idx += 1 if field_name != field_grp_id_name else id_index_gap

    # is this still necessary? experiment
    for end_field in get_last_fields_in_table(API_PARAMS):
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
    tables = get_one_to_many_tables(API_PARAMS, record_counts)
    table_columns = dict()

    field_grp_depths = {field_grp: len(field_grp.split('.')) for field_grp in
                        field_groups}

    for field_grp, depth in sorted(field_grp_depths.items(), key=lambda i: i[1]):
        if depth > 3:
            console_out("\n[INFO] Caution, not confirmed "
                        "to work with nested depth > 3\n")

        excluded_fields = get_excluded_fields_all_fgs(API_PARAMS, field_groups, is_webapp)

        if is_webapp and field_grp in excluded_fields:
            continue

        field_groups[field_grp] = remove_excluded_fields(field_groups[field_grp],
                                                         field_grp, excluded_fields,
                                                         is_webapp)

        field_keys = {get_field_key(field_grp, field) for field in
                      field_groups[field_grp]}

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
            console_out("{0} not in metadata", (field_grp,))
            fgs.pop(field_grp)
            cases.pop(field_grp)

    columns = flatten_tables(fgs, record_counts, is_webapp)

    record_counts = {k: v for k, v in record_counts.items() if record_counts[k] > 0}

    if is_webapp:
        excluded_field_groups = get_excluded_field_groups(API_PARAMS)

        for field_grp in record_counts.copy().keys():
            if field_grp in excluded_field_groups:
                record_counts.pop(field_grp)

        for field_grp in columns.copy().keys():
            if field_grp in excluded_field_groups:
                columns.pop(field_grp)

    return columns, record_counts


#   Schema creation


def get_count_column_index(table_name, column_order_dict):
    """
    Get index of child table record count reference column.
    :param table_name: table for which to get index
    :param column_order_dict: dict containing column indexes
    :return: count column start idx position
    """
    table_id_key = get_fg_id_name(API_PARAMS, table_name)
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
    console_out(id_tuple)

    field_grp_id_idx, field_grp_id_key, program = id_tuple
    parent_field_grp = get_field_group(field_grp)

    # add reference id field to schema
    schema[field_grp_id_key] = generate_id_schema_entry(field_grp_id_key,
                                                        parent_field_grp, program)
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
        ordered_field_grp_field_keys = get_column_order_one_fg(API_PARAMS, field_grp)

        # for a given field_grp, assign each field a global index; insert into
        # segregated column order dict (e.g. column_orders[field_grp][field] = idx)
        column_orders[field_grp] = {f: field_indexes[f] for f in ordered_field_grp_field_keys}

        if depth == 1 or field_grp not in columns:
            continue

        # get id key for current field group, and its index position
        table_id_key = get_field_group_id_key(API_PARAMS, field_grp)
        id_idx = column_orders[field_grp][table_id_key]

        # get parent id key, append to column order dict of child field group, increment
        # index from field_grp_id_key's position, assign to parent_id_key (foreign
        # reference key)
        parent_id_key = get_field_group_id_key(API_PARAMS, get_field_group(field_grp), is_webapp)

        if is_webapp:
            base_field_grp_id_key = get_field_group_id_key(API_PARAMS, get_base_fg(API_PARAMS), is_webapp)

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

            base_field_grp_id_key = get_field_group_id_key(API_PARAMS, get_base_fg(API_PARAMS))

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

        table_id_key = get_field_group_id_key(API_PARAMS, table, is_webapp)

        if table in columns:
            merge_dict_key = table

            schema[table_id_key]['mode'] = 'REQUIRED'
        else:
            # not a standalone table, merge
            merge_dict_key = get_parent_fg(columns.keys(), table)
            # if merging key into parent table, that key is no longer required, might
            # not exist in some cases
            # schema[table_id_key]['mode'] = 'NULLABLE'

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

    for table in get_one_to_many_tables(API_PARAMS, record_counts):
        schema_field_lists[table] = list()

        if table not in merged_orders:
            has_fatal_error("record counts and merged orders disagree on program's "
                            "table architecture")

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
            schema[entry]['name'] = get_bq_name(API_PARAMS, entry)

    schema_field_lists_dict = dict()

    for table in get_one_to_many_tables(API_PARAMS, record_counts):
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
                console_out("{0} not in src table; excluding schema field.", (column,))
                continue
            schema_field_lists_dict[table].append(to_bq_schema_obj(schema[column]))

    return schema_field_lists_dict


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
        excluded_fields = {get_bq_name(API_PARAMS, field, is_webapp, field_grp)
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
    # entry represents a field group, recursively flatten each record
    if fg not in API_PARAMS['FIELD_CONFIG'].keys():
        return

    base_pid_name = get_fg_id_name(API_PARAMS, get_base_fg(API_PARAMS), is_webapp)

    if isinstance(record, list):
        # flatten each record in field group list
        for entry in record:
            flatten_case_entry(entry, fg, flat_case, case_id, pid, pid_name, is_webapp)
            flatten_case_entry(entry, fg, flat_case, case_id, pid, pid_name, is_webapp)
        return
    else:
        row = dict()

        fg_id_name = get_fg_id_name(API_PARAMS, fg, is_webapp)

        for field, columns in record.items():
            # if list, possibly more than one entry, recurse over list
            if isinstance(columns, list):
                flatten_case_entry(record=columns,
                                   fg=get_field_key(fg, field),
                                   flat_case=flat_case,
                                   case_id=case_id,
                                   pid=record[fg_id_name],
                                   pid_name=fg_id_name,
                                   is_webapp=is_webapp)
                continue

            elif fg_id_name != pid_name:
                parent_fg = get_field_group(fg)

                pid_key = get_bq_name(API_PARAMS, pid_name, is_webapp, parent_fg)

                # add parent_id key and value to row
                row[pid_key] = pid

            elif fg_id_name != base_pid_name:
                row[base_pid_name] = case_id

            column = get_bq_name(API_PARAMS, field, is_webapp, fg)

            row[column] = columns

            if fg not in flat_case:
                # if this is first row added for fg, create an empty list
                # to hold row objects
                flat_case[fg] = list()

            if row:
                excluded = get_excluded_fields_one_fg(API_PARAMS, fg, is_webapp)

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
    get_field_group_id_key(API_PARAMS, base_fg, is_webapp)

    if is_webapp:
        for old_key, new_key in API_PARAMS['RENAMED_FIELDS'].items():
            old_name = get_field_name(old_key)
            new_name = get_field_name(new_key)
            if old_name in case:
                case[new_name] = case[old_name]
                case.pop(old_name)

    base_id_name = get_fg_id_name(API_PARAMS, base_fg, is_webapp)

    flat_case = dict()

    flatten_case_entry(record=case, fg=base_fg, flat_case=flat_case,
                       case_id=case[base_id_name], pid=case[base_id_name],
                       pid_name=base_id_name, is_webapp=is_webapp)

    if is_webapp:
        renamed_fields = API_PARAMS['RENAMED_FIELDS']

        base_id_key = get_field_group_id_key(API_PARAMS, base_fg)

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
    field_grp_id_name = get_fg_id_name(API_PARAMS, field_grp, is_webapp)
    field_grp_id_key = get_bq_name(API_PARAMS, field_grp_id_name, is_webapp, field_grp)
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
    tables = get_one_to_many_tables(API_PARAMS, record_counts)

    flattened_field_grp_parents = dict()

    for field_grp in record_counts:
        if field_grp == get_base_fg(API_PARAMS):
            continue
        if record_counts[field_grp] == 1:
            if field_grp in flat_case:
                # create list of flattened field group destination tables
                flattened_field_grp_parents[field_grp] = get_parent_fg(tables, field_grp)

    for field_grp, parent in flattened_field_grp_parents.items():
        field_grp_id_name = get_fg_id_name(API_PARAMS, parent, is_webapp)
        bq_parent_id_key = get_bq_name(API_PARAMS, field_grp_id_name, is_webapp, parent)

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
    # record_count_dict = {field_grp: 0 for field_grp in record_counts if
    # record_counts[field_grp] > 1}
    record_count_dict = get_max_record_counts(record_counts)

    for field_grp, parent_ids in record_count_dict.copy().items():
        tables = get_one_to_many_tables(API_PARAMS, record_counts)
        parent_field_grp = get_parent_fg(tables, field_grp)

        field_grp_id_name = get_fg_id_name(API_PARAMS, parent_field_grp,
                                           is_webapp)

        parent_id_key = get_bq_name(API_PARAMS, field_grp_id_name, is_webapp,
                                    parent_field_grp)

        # initialize record counts for parent id
        if parent_field_grp in flat_case:
            for parent_record in flat_case[parent_field_grp]:
                parent_id = parent_record[parent_id_key]
                parent_ids[parent_id] = 0

        # count child records
        if field_grp in flat_case:
            for record in flat_case[field_grp]:
                parent_id = record[parent_id_key]
                parent_ids[parent_id] += 1

    # insert record count into flattened dict entries
    for field_grp, parent_ids in record_count_dict.items():
        tables = get_one_to_many_tables(API_PARAMS, record_counts)
        parent_field_grp = get_parent_fg(tables, field_grp)
        count_name = get_bq_name(API_PARAMS, 'count', field_grp)

        for parent_id, count in parent_ids.items():
            p_key_idx = get_record_idx(flat_case, parent_field_grp, parent_id,
                                       is_webapp)

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
    record_tables = get_one_to_many_tables(API_PARAMS, record_counts)

    for record_table in record_tables:
        jsonl_name = build_jsonl_name(API_PARAMS, BQ_PARAMS, program, record_table, is_webapp)
        jsonl_fp = get_scratch_fp(BQ_PARAMS, jsonl_name)

        # If jsonl scratch file exists, delete so we don't append
        if os.path.exists(jsonl_fp):
            os.remove(jsonl_fp)

    for case in cases:
        flat_case = flatten_case(case, is_webapp)

        # remove excluded field groups
        for fg in flat_case.copy():
            if fg not in record_counts:
                flat_case.pop(fg)

        merge_or_count_records(flat_case, record_counts, is_webapp)

        for bq_table in flat_case:
            if bq_table not in record_tables:
                has_fatal_error("Table {} not found in table keys".format(bq_table))

            jsonl_name = build_jsonl_name(API_PARAMS, BQ_PARAMS, program, bq_table, is_webapp)
            jsonl_fp = get_scratch_fp(BQ_PARAMS, jsonl_name)

            write_list_to_jsonl(jsonl_fp, flat_case[bq_table], 'a')

    for record_table in record_tables:
        jsonl_name = build_jsonl_name(API_PARAMS, BQ_PARAMS, program, record_table, is_webapp)

        upload_to_bucket(BQ_PARAMS, get_scratch_fp(BQ_PARAMS, jsonl_name))

        table_name = get_full_table_name(program, record_table)

        if is_webapp:
            table_id = get_webapp_table_id(BQ_PARAMS, table_name)
        else:
            table_id = get_working_table_id(BQ_PARAMS, table_name)

        create_and_load_table(BQ_PARAMS, jsonl_name, schemas[record_table], table_id)


def get_metadata_files():
    """Get all the file names in a directory as a list of as strings.

    :return: list of filenames
    """
    rel_path = '/'.join([BQ_PARAMS['BQ_REPO'], BQ_PARAMS['TABLE_METADATA_DIR'], get_rel_prefix(BQ_PARAMS)])
    metadata_fp = get_filepath(rel_path)

    return [f for f in os.listdir(metadata_fp) if os.path.isfile(os.path.join(metadata_fp, f))]


def get_schema_metadata_fp(repo_dir, filename):
    """ Get filepath to schema and/or metadata file in BQEcosystem repo.

    :param bq_params: bq param object from yaml config
    :param repo_dir: directory in which the schema/metadata file resides
    :param filename: schema/metadata file name
    :return: path to schema/metadata file on VM
    """
    dir_path = '/'.join([BQ_PARAMS['BQ_REPO'], repo_dir])

    return get_filepath(dir_path, filename)


def update_table_metadata():
    """
    Use .json file in the BQEcosystem repo to update a bq table's metadata
    (labels, description, friendly name)
    """
    for json_file in get_metadata_files(BQ_PARAMS):
        table_name = convert_json_to_table_name(BQ_PARAMS, json_file)
        table_id = get_working_table_id(BQ_PARAMS, table_name)

        if not exists_bq_table(table_id):
            console_out('No table found for file (skipping): {0}', (json_file,))
            continue

        metadata_fp = get_schema_metadata_fp(BQ_PARAMS['TABLE_METADATA_DIR'], json_file)

        with open(metadata_fp) as json_file_output:
            metadata = json.load(json_file_output)

            update_table_metadata(table_id, metadata)


def update_schema():
    """
    Alter an existing table's schema (currently, only field descriptions are mutable
    without a table rebuild, Google's restriction).
    """

    fields_file = "{}_{}.json".format(BQ_PARAMS['FIELD_DESC_FILE_PREFIX'], get_rel_prefix(BQ_PARAMS))
    field_desc_fp = get_schema_metadata_fp(BQ_PARAMS['FIELD_DESC_DIR'], fields_file)

    with open(field_desc_fp) as field_output:
        descriptions = json.load(field_output)

    for json_file in get_metadata_files():
        table_name = convert_json_to_table_name(BQ_PARAMS, json_file)
        table_id = get_working_table_id(BQ_PARAMS, table_name)

        update_schema(table_id, descriptions)


def copy_tables_into_public_project():
    """Move production-ready bq tables onto the public-facing production server.

    """
    files = get_metadata_files()

    for json_file in files:
        src_table_id, curr_table_id, vers_table_id = convert_json_to_table_id(BQ_PARAMS,
                                                                              json_file)

        if not exists_bq_table(src_table_id):
            console_out('No table found for file (skipping): {0}', (json_file,))
            continue

        copy_bq_table(BQ_PARAMS, src_table_id, vers_table_id)
        copy_bq_table(BQ_PARAMS, src_table_id, curr_table_id)
        update_friendly_name(BQ_PARAMS, vers_table_id)


#    Webapp specific functions


def make_biospecimen_stub_tables(program):
    """
    Create one-to-many table referencing case_id (as case_gdc_id),
    submitter_id (as case_barcode), (sample_ids as sample_gdc_ids),
    and sample_submitter_id (as sample_barcode).
    :param program: the program from which the cases originate.
    """
    query = ("""
        SELECT project_name, case_gdc_id, case_barcode, sample_gdc_id, sample_barcode
        FROM
          (SELECT project_name, case_gdc_id, case_barcode, 
            SPLIT(sample_ids, ', ') as s_gdc_ids, 
            SPLIT(submitter_sample_ids, ', ') as s_barcodes
            FROM
                (SELECT case_id as case_gdc_id, 
                    submitter_id as case_barcode, 
                    sample_ids, submitter_sample_ids, 
                    SPLIT((SELECT project_id
                           FROM UNNEST(project)), '-')[OFFSET(0)] AS project_name
                FROM `{}.{}.{}{}_{}`)), 
        UNNEST(s_gdc_ids) as sample_gdc_id WITH OFFSET pos1, 
        UNNEST(s_barcodes) as sample_barcode WITH OFFSET pos2
        WHERE pos1 = pos2
        AND project_name = '{}'
    """).format(BQ_PARAMS['DEV_PROJECT'],
                BQ_PARAMS['DEV_DATASET'],
                BQ_PARAMS['REL_PREFIX'],
                BQ_PARAMS['RELEASE'],
                BQ_PARAMS['MASTER_TABLE'],
                program)

    table_id = get_biospecimen_table_id(BQ_PARAMS, program)

    load_table_from_query(BQ_PARAMS, table_id, query)


#    Script execution


def output_report(start, steps):
    """
    Outputs a basic report of script's results, including total processing
    time and which steps were specified in YAML.
    :param start: float representing script's start time.
    :param steps: set of steps to be performed (configured in YAML)
    """
    seconds = time.time() - start
    minutes = math.floor(seconds / 60)
    seconds -= minutes * 60

    console_out("Script executed in {0} min, {1:.0f} sec\n", (minutes, seconds))

    console_out("Steps completed: ")

    if 'create_biospecimen_stub_tables' in steps:
        console_out('\t - created biospecimen stub tables for webapp use')
    if 'create_webapp_tables' in steps:
        console_out('\t - created tables for webapp use')
    if 'create_and_load_tables' in steps:
        console_out('\t - created tables and inserted data')
    if 'update_table_metadata' in steps:
        console_out('\t - added/updated table metadata')
    if 'update_schema' in steps:
        console_out('\t - updated table field descriptions')
    if 'copy_tables_into_production' in steps:
        console_out('\t - copied tables into production (public-facing bq tables)')
    if 'validate_data' in steps:
        console_out('\t - validated data (tests not considered exhaustive)')
    if 'generate_documentation' in steps:
        console_out('\t - generated documentation')
    console_out('\n\n')


def create_tables(program, cases, schema, is_webapp=False):
    """
    Run the overall script which creates schemas, modifies data, prepares it for loading,
    and creates the databases.
    :param program: the source for the inserted cases data
    :param cases: dict representations of clinical case data from GDC
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return:
    """
    # derive the program's table structure by analyzing its case records
    columns, record_counts = find_program_structure(cases, is_webapp)

    # add the parent id to field group dicts that will create separate tables
    column_orders = add_ref_columns(columns, record_counts, schema, program, is_webapp)

    # removes the prefix from schema field name attributes
    # removes the excluded fields/field groups
    if is_webapp:
        modify_fields_for_app(API_PARAMS, schema, column_orders, columns)

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

    # programs = get_program_list(BQ_PARAMS)
    programs = ['BEATAML1.0']

    for program in programs:
        prog_start = time.time()
        console_out("\nRunning script for program: {0}...\n", (program,))

        if 'create_biospecimen_stub_tables' in steps:
            '''
            these tables are used to populate the per-program clinical tables, 
            and are also needed for populating data into the webapp
            '''
            console_out("Creating biospecimen stub tables!")
            make_biospecimen_stub_tables(program)

        if 'create_webapp_tables' in steps or 'create_and_load_tables' in steps:
            cases = get_cases_by_program(BQ_PARAMS, program)

            if not cases:
                console_out("No cases found for program {0}, skipping.", (program,))
                continue

            # rename so that '1.0' doesn't break bq table name
            program = program.replace('.', '_')

            # generate table schemas
            # webapp_schema = copy.deepcopy(schema)

            if 'create_webapp_tables' in steps:  # FOR WEBAPP TABLES
                schema = create_schema_dict(API_PARAMS, BQ_PARAMS, is_webapp=True)
                webapp_cases = copy.deepcopy(cases)
                create_tables(program, webapp_cases, schema, is_webapp=True)

            if 'create_and_load_tables' in steps:
                schema = create_schema_dict(API_PARAMS, BQ_PARAMS)
                create_tables(program, cases, schema)

            prog_end = time.time() - prog_start
            console_out("{0} processed in {1:0.0f}s!\n", (program, prog_end))

    if 'update_table_metadata' in steps:
        update_table_metadata()

    if 'update_schema' in steps:
        update_schema()

    if 'cleanup_tables' in steps:
        for table_id in BQ_PARAMS['DELETE_TABLES']:
            project = get_project_name(table_id)

            if project != BQ_PARAMS['DEV_PROJECT']:
                has_fatal_error("Can only use cleanup_tables on DEV_PROJECT.")

            delete_bq_table(table_id)

    if 'copy_tables_into_production' in steps:
        copy_tables_into_public_project()

    '''
    if 'generate_documentation' in steps:
        generate_docs(API_PARAMS, BQ_PARAMS)
    '''

    if 'validate_data' in steps:
        pass  # todo: integrate the queries in compare_clinical_gdc_api_releases.py

    output_report(start, steps)


if __name__ == '__main__':
    main(sys.argv)

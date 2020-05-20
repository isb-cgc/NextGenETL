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
    get_abbr_dict, get_bq_name, has_fatal_error, get_query_results,
    create_mapping_dict, get_field_name, get_tables, get_parent_table,
    get_parent_field_group, load_config, get_cases_by_program, get_table_id,
    upload_to_bucket, create_and_load_table, make_SchemaField)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


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
    abbr_dict = get_abbr_dict(API_PARAMS)
    abbr = abbr_dict[table]

    # remove invalid char from program name
    if '.' in program_name:
        program_name = '_'.join(program_name.split('.'))

    file_name_parts = [BQ_PARAMS['GDC_RELEASE'], 'clin', program_name]

    # if one-to-many table, append suffix
    if abbr:
        file_name_parts.append(abbr)

    return '_'.join(file_name_parts)


def get_jsonl_filename(program_name, table):
    """
    Gets unique (per release) jsonl filename, used for intermediately storing
    the table rows after they're flattened, but before BQ insertion. Allows for
    faster script thanks to minimal BigQuery txns.
    :param program_name: name of the program to with the data belongs
    :param table: future insertion table for flattened data
    :return: String .jsonl filename, of the form 'relXX_clin_PROGRAM +
    (_supplemental_table_name, optionally)
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


def get_required_columns(table_key):
    """
    Get list of required columns. Currently generated, but intended to also
    work if supplied in YAML config file.
    :param table_key: name of table for which to retrieve required columns.
    :return: list of required columns.
    """
    required_columns = list()

    table_id_key = get_table_id_key(table_key)

    required_columns.append(get_bq_name(API_PARAMS, table_key, table_id_key))

    return required_columns


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


def get_id_column_position(table_key, column_order_dict):
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
    max_reference_cols = len(field_groups)

    idx = 0

    for group in field_groups:
        try:
            column_order_list = API_PARAMS['TABLE_METADATA'][group][
                'column_order']
            id_column = API_PARAMS['TABLE_METADATA'][group]['table_id_key']

            for column in column_order_list:
                column_order_dict[group + '.' + column] = idx

                if id_column == column:
                    # this creates space for reference columns (parent id or
                    # one-to-many
                    # record count columns) leaves a gap for submitter_id
                    idx += max_reference_cols * 2
                else:
                    idx += 1
        except KeyError:
            has_fatal_error("{} found in API_PARAMS['TABLE_ORDER'] but not in "
                            "API_PARAMS['TABLE_METADATA']".format(group))

    column_order_dict['cases.state'] = idx
    column_order_dict['cases.created_datetime'] = idx + 1
    column_order_dict['cases.updated_datetime'] = idx + 2

    return column_order_dict


# todo there's more to optimize here in terms of automation
def lookup_column_types():
    """
    Determine column types for data columns, using master table's schema.
    :return: dict of {column_names: types}
    """

    def split_datatype_array(col_dict, col_string, name_prefix):
        columns = col_string[13:-2].split(', ')

        for column in columns:
            column_type = column.split(' ')
            column_name = name_prefix + column_type[0]
            col_dict[column_name] = column_type[1].strip(',')

        return col_dict

    def generate_base_query(field_groups_):
        exclude_column_query_str = ''
        for fg_ in field_groups_:
            exclude_column_query_str += "AND column_name != '{}' ".format(fg_)

        query = """
        SELECT column_name, data_type FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{}_clinical_data' 
        """.format(BQ_PARAMS["WORKING_PROJECT"], BQ_PARAMS["TARGET_DATASET"],
                   BQ_PARAMS["GDC_RELEASE"])

        return query + exclude_column_query_str

    def generate_field_group_query(field_group_):
        return """
        SELECT column_name, data_type FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{}_clinical_data' and column_name = '{}'
        """.format(BQ_PARAMS["WORKING_PROJECT"], BQ_PARAMS["TARGET_DATASET"],
                   BQ_PARAMS["GDC_RELEASE"], field_group_)

    field_groups = []
    child_field_groups = {}

    for group in API_PARAMS['EXPAND_FIELD_GROUPS']:
        if len(group.split(".")) == 1:
            field_groups.append(group)
        elif len(group.split(".")) == 2:
            parent_fg = group.split(".")[0]
            child_fg = group.split(".")[1]
            if parent_fg not in child_field_groups:
                child_field_groups[parent_fg] = set()
            child_field_groups[parent_fg].add(child_fg)

    column_type_dict = dict()

    base_query = generate_base_query(field_groups)
    follow_ups_query = generate_field_group_query("follow_ups")
    exposures_query = generate_field_group_query("exposures")
    demographic_query = generate_field_group_query("demographic")
    diagnoses_query = generate_field_group_query("diagnoses")
    family_histories_query = generate_field_group_query("family_histories")

    results = get_query_results(base_query)

    for result in results:
        vals = result.values()
        column_type_dict['cases.' + vals[0]] = vals[1]

    single_nested_query_dict = {
        "cases.family_histories": family_histories_query,
        "cases.demographic": demographic_query,
        "cases.exposures": exposures_query
    }

    for key in single_nested_query_dict:
        results = get_query_results(single_nested_query_dict[key])

        for result in results:
            vals = result.values()
            column_type_dict = split_datatype_array(
                column_type_dict, vals[1], key + '.')

    results = get_query_results(follow_ups_query)

    for result in results:
        vals = result.values()
        split_vals = vals[1].split('molecular_tests ')

        column_type_dict = split_datatype_array(
            column_type_dict, split_vals[0] + ' ', 'cases.follow_ups.')

        column_type_dict = split_datatype_array(
            column_type_dict, split_vals[1][:-2],
            'cases.follow_ups.molecular_tests.')

    results = get_query_results(diagnoses_query)

    diagnoses = None
    treatments = None
    annotations = None

    # create field list string
    for result in results:
        vals = result.values()
        split_vals = vals[1].split('treatments ')
        diagnoses = split_vals[0]
        treatments = split_vals[1]

        split_diagnoses = diagnoses.split('annotations ')
        if len(split_diagnoses) > 1:
            diagnoses = split_diagnoses[0]
            annotations = split_diagnoses[1][:-2]
            treatments = treatments[:-2]
        else:
            split_treatments = treatments.split('annotations ')
            treatments = split_treatments[0][:-2]
            annotations = split_treatments[1][:-2]

        diagnoses = diagnoses[:-2] + '>>'

    # parse field list strings
    column_type_dict = split_datatype_array(
        column_type_dict, diagnoses, 'cases.diagnoses.')

    column_type_dict = split_datatype_array(
        column_type_dict, treatments, 'cases.diagnoses.treatments.')
    column_type_dict = split_datatype_array(
        column_type_dict, annotations, 'cases.diagnoses.annotations.')

    return column_type_dict


def create_schema_dict():
    """
    Create dict of schema records for BQ table creation.
    :return: dict of entries with the following keys: {name, type, description}
    """
    column_type_dict = lookup_column_types()
    field_mapping_dict = create_mapping_dict(API_PARAMS['ENDPOINT'])

    schema_dict = {}

    for key in column_type_dict:
        try:
            description = field_mapping_dict[key]['description']
        except KeyError:
            # cases.id not returned by mapping endpoint. In such cases,
            # substitute an empty description string.
            description = ""

        field_type = column_type_dict[key]

        # this is the format for bq schema json object entries
        bq_key = get_bq_name(API_PARAMS, None, key)

        schema_dict[key] = {
            "name": bq_key,
            "type": field_type,
            "description": description
        }

    return schema_dict


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
        has_fatal_error("excluded_fields not found in API_PARAMS for {}".
                        format(table))

    base_column_names = API_PARAMS['TABLE_METADATA'][table]['excluded_fields']

    exclude_field_list = [get_bq_name(API_PARAMS, table, x)
                          for x in base_column_names]
    return exclude_field_list


def flatten_tables(table_columns, record_counts):
    """
    From dict containing table_name keys and sets of column names, remove
    excluded columns, merge into parent table if this field group can be
    flattened for this program.
    :param table_columns: dict of tables and columns
    :param record_counts: dict showing max record count for a case in this
    program, used in table creation.
    :return: flattened table column dict.
    """

    def remove_excluded_fields(record_, table_name):
        excluded_fields = get_excluded_fields(table_name)

        for field_ in record_.copy():
            if field_ in excluded_fields:
                record_.remove(field_)

        return record_

    # record_counts uses fg naming convention
    # sort field group keys by depth
    field_group_depths = dict.fromkeys(record_counts.keys(), 0)
    for fg_key in field_group_depths:
        field_group_depths[fg_key] = len(fg_key.split("."))

    for field_group, depth in sorted(
            field_group_depths.items(), key=lambda item: item[1], reverse=True):
        table_columns[field_group] = remove_excluded_fields(
            table_columns[field_group], field_group)

        # this is cases, already flattened
        if depth == 1:
            break

        # field group can be flattened.
        if record_counts[field_group] == 1:
            tables = get_tables(record_counts)
            parent_key = get_parent_table(tables, field_group)

            if not parent_key:
                has_fatal_error("No parent found for {}".format(field_group))

            if parent_key not in table_columns:
                table_columns[parent_key] = set()

            for field in table_columns.pop(field_group):
                table_columns[parent_key].add(field)

    return table_columns


def retrieve_program_case_structure(program_name, cases):
    """
    Determine table structure required for the given program.
    :param program_name: program for which to determine structure
    :param cases: dict of program's case records
    :return: dict of tables and columns, dict with maximum record count for
    this program's field groups.
    """

    def build_case_structure(tables_, case_, record_counts_, parent_path):
        """
        Recursive function for retrieve_program_data, finds nested fields
        """
        if not case_:
            return tables_, record_counts_

        if parent_path not in tables_:
            tables_[parent_path] = set()
        if parent_path not in record_counts_:
            record_counts_[parent_path] = 1

        for field_key in case_:
            if not case_[field_key]:
                continue
            # Hits for cases
            if isinstance(case_[field_key], list):
                new_path = parent_path + '.' + field_key
                if new_path not in record_counts_:
                    record_counts_[new_path] = 1

                # find needed one-to-many tables
                record_counts_[new_path] = max(record_counts_[new_path],
                                               len(case_[field_key]))

                for entry in case_[field_key]:
                    tables_, record_counts_ = build_case_structure(
                        tables_, entry, record_counts_, new_path)

            elif isinstance(case_[field_key], dict):
                tables_, record_counts_ = build_case_structure(
                    tables_, case_[field_key], record_counts_, parent_path)

            else:
                bq_name = get_bq_name(API_PARAMS, parent_path, field_key)
                table_columns[parent_path].add(bq_name)

        return tables_, record_counts_

    table_columns = {}
    record_counts = {}

    for case in cases:
        table_columns, record_counts = build_case_structure(
            table_columns, case, record_counts, parent_path='cases')

    table_columns = flatten_tables(table_columns, record_counts)

    if not table_columns:
        has_fatal_error("[ERROR] no case structure returned "
                        "for program {}".format(program_name))

    print("Record counts for each field group: {}".format(record_counts))

    return table_columns, record_counts


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
    id_column_position = column_order_dict[table_key + '.' + table_id_key]

    count_columns_position = id_column_position + len(API_PARAMS['TABLE_ORDER'])

    return count_columns_position


def add_reference_columns(table_columns, schema_dict):
    """
    Add reference columns generated by separating and flattening data.

    Possible types:

    - _count column representing # of child records found in supplemental table
    - case_id, used to reference main table records
    - pid, used to reference nearest un-flattened ancestor table

    :param table_columns: dict containing table column keys
    :param schema_dict: dict containing schema records
    :return: table_columns, schema_dict, column_order_dict
    """

    def generate_id_schema_entry(column_name, parent_table_key_):
        parent_field_name = get_field_name(parent_table_key_)

        if parent_table_key_ in table_columns.keys():
            ancestor_table = '*_{}'.format(parent_field_name)
        else:
            ancestor_table = 'main'

        if '__' in column_name:
            column_name = column_name.split('__')[-1]
            ancestor_column_name = get_bq_name(API_PARAMS, parent_table_key_,
                                               get_field_name(column_name))
        else:
            ancestor_column_name = column_name

        description = ("Reference to the pid ({}) of the record to which "
                       "this record belongs. Parent record found in the "
                       "program's {} table.").format(ancestor_column_name,
                                                     ancestor_table)

        return {"name": ancestor_column_name, "type": 'STRING',
                "description": description}

    def generate_count_schema_entry(count_id_key_, parent_table_key_):
        description = ("Total count of records associated with this "
                       "case, located in {} table").format(parent_table_key_)

        return {"name": count_id_key_, "type": 'INTEGER',
                "description": description}

    table_orders = dict()

    table_depths = dict()

    for table in table_columns.keys():
        depth = len(table.split('.'))
        table_depths[table] = depth

    for table, depth in sorted(table_depths.items(), key=lambda item: item[1]):

        table_orders[table] = build_column_order_dict()

        id_column_position = get_id_column_position(table,
                                                    table_orders[table])

        ref_column_index = id_column_position + 1

        if depth == 1:
            continue
        if depth > 2:
            # if the > 2 cond. is removed (and the case_id insertion below)
            # tables will only reference direct ancestor
            # tables with depth > 2 have case_id and pid reference
            parent_fg = get_parent_field_group(table)
            pid_key = get_table_id_key(parent_fg)
            full_pid_name = parent_fg + '.' + pid_key
            parent_bq_name = get_bq_name(API_PARAMS, parent_fg, pid_key)

            # add pid to one-to-many table
            schema_dict[full_pid_name] = generate_id_schema_entry(
                parent_bq_name, parent_fg)
            table_columns[table].add(parent_bq_name)
            table_orders[table][full_pid_name] = ref_column_index

            ref_column_index += 1

        case_id_key = 'case_id'
        case_id_column = table + '.case_id'

        # add case_id to one-to-many table
        schema_dict[case_id_column] = generate_id_schema_entry(
            case_id_key, 'main')

        table_columns[table].add(case_id_key)
        table_orders[table][case_id_column] = ref_column_index

        ref_column_index += 1

        parent_table = get_parent_table(table_columns.keys(), table)

        count_col_index = get_count_column_index(
            parent_table, table_orders[table])

        count_column = table + '.count'

        count_id_key = get_bq_name(API_PARAMS, table, 'count')

        # add one-to-many record count column to parent table
        schema_dict[count_column] = generate_count_schema_entry(
            count_id_key, parent_table)
        table_columns[parent_table].add(count_id_key)
        table_orders[parent_table][count_column] = count_col_index

    return schema_dict, table_columns, table_orders


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


def create_schemas(table_columns):
    """
    Create ordered schema lists for final tables.
    :param table_columns: dict containing table column keys
    :return: lists of BQ SchemaFields.
    """
    schema_field_lists = dict()
    schema_dict = create_schema_dict()

    # modify schema dict, add reference columns for this program
    schema_dict, table_columns, column_orders = add_reference_columns(
        table_columns, schema_dict)

    for table in table_columns:
        # todo filter?
        # this is just alphabetizing the count columns
        for column in table_columns[table]:
            # todo find when this if/else happens
            if '__' in column:
                column = rebuild_bq_name(column)
            else:
                column = table + '.' + column

            if column not in column_orders[table]:
                has_fatal_error("'{}' not in column_orders['{}']: \n"
                                "{}".
                                format(column, table, column_orders[table]))

            count_column_index = get_count_column_index(
                table, column_orders[table])

            count_columns = []

            for column_key, index in column_orders[table].items():
                if index == count_column_index:
                    count_columns.append(column_key)

            # index in alpha order
            count_columns.sort()

            for count_column in count_columns:
                column_orders[table][count_column] = count_column_index
                count_column_index += 1

        required_cols = get_required_columns(table)

        schema_list = []

        for key, v in sorted(column_orders[table].items(), key=lambda i: i[1]):
            if key in schema_dict:
                schema_entry = make_SchemaField(schema_dict, key, required_cols)
                schema_list.append(schema_entry)
            else:
                print("{} not found in master bq table, excluding from schema.".
                      format(key))

        schema_field_lists[table] = schema_list

    print(schema_field_lists.keys())

    return schema_field_lists, column_orders


def remove_dict_fields(record, table_name):
    """
    Remove columns with only None values, as well as those excluded.
    :param record: table record to parse.
    :param table_name: name of destination table.
    :return: Trimmed down record dict.
    """
    excluded_fields = get_excluded_fields(table_name)

    for field in record.copy():
        if field in excluded_fields or not record[field]:
            record.pop(field)

    return record


####
#
# Functions used for parsing and loading data into BQ tables
#
##
'''
def flatten_case(case, prefix, flat_case, case_id=None,
                 new_pid=None, new_pid_key=None):
    """
    Convert nested case object into a flattened representation of its records.
    :param case: dict containing case data
    :param prefix: name of parent field group.
    :param flat_case: flattened dictionary for case (used to recursively
        capture the record as it's parsed).
    :param case_id: case record's id value
    :param new_pid: value of parent id for this record (or duplicate of case_id
        if record doesn't have a more direct ancestor.)
    :param new_pid_key: parent table's key for unique ids.
    :return: flattened case dict
    """
    if isinstance(case, list):
        for entry in case:
            entry_dict = dict()
            for key in entry:
                if not isinstance(entry[key], list):
                    curr_table_id_key = get_table_id_key(prefix)

                    if case_id != new_pid and curr_table_id_key != new_pid_key:
                        entry_dict[new_pid_key] = new_pid

                    if 'case_id' not in entry_dict:
                        entry_dict['case_id'] = case_id

                    # This is where GDC field names are converted to
                    # bq_column names (with table abbr prefixes)
                    bq_col_name = get_bq_name(API_PARAMS, prefix, key)
                    entry_dict[bq_col_name] = entry[key]

            if entry_dict:
                entry_dict = remove_dict_fields(entry_dict, prefix)

                if prefix not in flat_case:
                    flat_case[prefix] = list()
                flat_case[prefix].append(entry_dict)

            for key in entry:
                if isinstance(entry[key], list):
                    new_prefix = prefix + '.' + key
                    new_pid_key = get_table_id_key(prefix)
                    new_pid = entry[new_pid_key]
                    new_p_key = get_bq_name(API_PARAMS, prefix, new_pid_key)
                    flat_case = flatten_case(entry[key], new_prefix, flat_case,
                                             case_id, new_pid, new_p_key)
    else:
        entry_dict = dict()

        for key in case:
            if not isinstance(case[key], list):
                # This is the other place where GDC field names are
                # converted to bq_column names (with table abbr prefixes)
                field = get_bq_name(API_PARAMS, prefix, key)
                entry_dict[field] = case[key]

        if entry_dict:
            entry_dict = remove_dict_fields(entry_dict, prefix)

            if prefix not in flat_case:
                flat_case[prefix] = list()
            flat_case[prefix].append(entry_dict)

        for key in case:
            if isinstance(case[key], list):
                new_prefix = prefix + '.' + key
                new_pid_key = get_table_id_key(prefix)
                new_pid = case[new_pid_key]
                new_p_key = get_bq_name(API_PARAMS, prefix, new_pid_key)
                flat_case = flatten_case(case[key], new_prefix, flat_case,
                                         case_id, new_pid, new_p_key)

    return flat_case
'''
    

def create_entry_dict(entry, prefix, flat_case, case_id, pid, pid_key):
    """

    :param entry:
    :param prefix:
    :param flat_case:
    :param case_id:
    :param pid:
    :param pid_key:
    :return:
    """
    if isinstance(entry, list):
        for sub_entry in entry:
            flat_case = create_entry_dict(
                sub_entry, prefix, flat_case, case_id, pid, pid_key)
        return flat_case

    entry_dict = dict()

    # todo switch to filter
    for key in entry:
        if not isinstance(entry[key], list):
            curr_table_id_key = get_table_id_key(prefix)

            if 'case_id' not in entry_dict:
                entry_dict['case_id'] = case_id

            if case_id != pid and curr_table_id_key != pid_key:
                entry_dict[pid_key] = pid

            # This is where field name is converted to abbr. bq column name
            field = get_bq_name(API_PARAMS, prefix, key)
            entry_dict[field] = entry[key]

    # todo is this duplicated effort?
    if entry_dict:
        entry_dict = remove_dict_fields(entry_dict, prefix)

        if prefix not in flat_case:
            flat_case[prefix] = list()

        flat_case[prefix].append(entry_dict)

    # todo switch to filter
    for key in entry:
        if isinstance(entry[key], list):
            new_prefix = prefix + '.' + key
            pid_field_name = get_table_id_key(prefix)
            pid = entry[pid_field_name]
            pid_key = get_bq_name(API_PARAMS, prefix, pid_field_name)
            flat_case = create_entry_dict(
                entry[key], new_prefix, flat_case, case_id, pid, pid_key)
            
    return flat_case


def flatten_case(case):
    """
    Convert nested case object into a flattened representation of its records.
    :param case: dict containing case data
    :return: flattened case dict
    """
    prefix = 'cases'
    case_id = pid = case['case_id']
    pid_key = case_id
    flat_case = dict()
    
    return create_entry_dict(case, prefix, flat_case, case_id, pid, pid_key)


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
    field_group_depths = dict.fromkeys(flattened_case.keys(), 0)

    # sort field group keys by depth
    for key in field_group_depths:
        field_group_depths[key] = len(key.split("."))

    for fg_key, fg_depth in sorted(field_group_depths.items(),
                                   key=lambda item: item[1],
                                   reverse=True):
        # cases is the master table, merged into
        if fg_depth == 1:
            break

        parent_table = get_parent_table(flattened_case.keys(), fg_key)
        pid_key = get_table_id_key(parent_table)
        pid_column = get_bq_name(API_PARAMS, parent_table, pid_key)

        if fg_key in bq_program_tables:
            record_counts = dict()
            idx = 0
            for entry in flattened_case[parent_table].copy():
                if pid_key not in entry and pid_column not in entry:
                    has_fatal_error("No id key found, in bq or fg format.")

                entry_id = entry[pid_key] if pid_key in entry \
                    else entry[pid_column]

                if entry_id not in record_counts:
                    record_counts[entry_id] = {'entry_idx': idx,
                                               'record_count': 0}
                    idx += 1

            field_group = flattened_case[fg_key].copy()

            for record in field_group:
                if pid_column in record:
                    pid = record[pid_column]
                    record_counts[pid]['record_count'] += 1
            for pid in record_counts:
                entry_idx = record_counts[pid]['entry_idx']
                count_id = get_bq_name(API_PARAMS, fg_key, 'count')

                flattened_case[parent_table][entry_idx][count_id] = \
                    record_counts[pid]['record_count']
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


def create_and_load_tables(program_name, cases, schemas, record_counts):
    """
    Create jsonl row files for future insertion, store in GC storage bucket,
    then insert the new table schemas and data.
    :param program_name: program for which to create tables
    :param cases: case records to insert into BQ for program
    :param schemas: dict of schema lists for all of this program's tables.
    :param record_counts: max counts for field groups in this dataset.
    """
    bq_program_tables = get_tables(record_counts)
    print("Inserting case records... ")
    tables = schemas.keys()

    for table in tables:
        file_path = get_temp_filepath(program_name, table)
        if os.path.exists(file_path):
            os.remove(file_path)

    for case in cases:
        flattened_case_dict = flatten_case(case)

        flattened_case_dict = merge_single_entry_field_groups(
            flattened_case_dict,
            bq_program_tables)

        for table in flattened_case_dict.keys():
            if table not in tables:
                has_fatal_error(
                    "Table {} not found in table keys".format(table))

            jsonl_fp = get_temp_filepath(program_name, table)

            with open(jsonl_fp, 'a') as jsonl_file:
                for row in flattened_case_dict[table]:
                    json.dump(obj=row, fp=jsonl_file)
                    jsonl_file.write('\n')

    for table in schemas:
        jsonl_file = get_jsonl_filename(program_name, table)
        upload_to_bucket(BQ_PARAMS, API_PARAMS['TEMP_PATH'], jsonl_file)
        table_id = get_full_table_name(program_name, table)

        try:
            create_and_load_table(BQ_PARAMS, jsonl_file,
                                  schemas[table], table_id)
        except ValueError as err:
            print("{}, {}".format(jsonl_file, table_id))
            has_fatal_error("{}".format(err))


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
    json_doc_file = BQ_PARAMS['GDC_RELEASE']
    json_doc_file += '_clin_json_documentation_dump.json'

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

    print("Programs script executed in {} min, {:.0f} sec".format(minutes,
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
        has_fatal_error(
            'Usage : {} <configuration_yaml> <column_order_txt>".format(args['
            '0])',
            ValueError)

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

        cases = get_cases_by_program(BQ_PARAMS, program)

        if not cases:
            continue

        # derive the program's table structure by analyzing its case records
        table_columns, record_counts = retrieve_program_case_structure(
            program, cases)

        if 'create_and_load_tables' in steps:
            # generate table schemas
            table_schemas, table_order_lists = create_schemas(table_columns)

            # create tables, flatten and insert data
            create_and_load_tables(program, cases, table_schemas, record_counts)

            print("{} processed in {:0.1f} seconds!\n".
                  format(program, time.time() - prog_start))

            if 'generate_documentation' in steps:
                table_ids = {table: get_table_id(BQ_PARAMS, table)
                             for table in get_tables(record_counts)}

                # converting to JSON serializable form
                table_column_lists = {table: list(vals)
                                      for table, vals in table_columns.items()}

                documentation_dict[program] = {
                    'record_counts': record_counts,
                    'table_schemas': str(table_schemas),
                    'table_columns': table_column_lists,
                    'table_ids': table_ids,
                    'table_order_dict': table_order_lists
                }

    if 'generate_documentation' in steps:
        documentation_dict['metadata'] = dict()
        documentation_dict['metadata']['API_PARAMS'] = API_PARAMS
        documentation_dict['metadata']['BQ_PARAMS'] = BQ_PARAMS
        documentation_dict['metadata']['schema_dict'] = schema_dict

        generate_documentation(documentation_dict)

    if 'validate_data' in steps:
        pass

    print_final_report(start, steps)


if __name__ == '__main__':
    main(sys.argv)

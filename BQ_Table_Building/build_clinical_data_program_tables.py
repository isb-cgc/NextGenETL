"""
Copyright 2020, Institute for Systems Biology

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies
or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
import math
import sys
import json
import os
from gdc_clinical_resources.test_data_integrity import *
from common_etl.utils import *

API_PARAMS = None
BQ_PARAMS = None
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


##
# Getter functions, employed for readability/consistency
##
def generate_long_name(program_name, table):
    """
    Generate string representing a unique name, constructed from elements of the table name, program name and
    GDC release number. Used for storage bucket file and BQ table naming.
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
    file_name_parts.append(abbr) if abbr else None

    return '_'.join(file_name_parts)


def get_jsonl_filename(program_name, table):
    """
    Gets unique (per release) jsonl filename, used for intermediately storing the table rows
    after they're flattened, but before BQ insertion. Allows for faster script thanks to minimal BigQuery txns.
    :param program_name: name of the program to with the data belongs
    :param table: future insertion table for flattened data
    :return: String .jsonl filename, of the form 'relXX_clin_PROGRAM + (_supplemental_table_name, optionally)
    """
    return generate_long_name(program_name, table) + '.jsonl'


def get_temp_filepath(program_name, table):
    """
    Get filepath for the temp storage folder.
    :param program_name: Program
    :param table: Program to which this table is associated.
    :return: String representing the temp file path.
    """
    return API_PARAMS['TEMP_PATH'] + '/' + get_jsonl_filename(program_name, table)


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
    Get list of required columns. Currently generated, but intended to also work if supplied in YAML config file.
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
        has_fatal_error("table_id_key not found in API_PARAMS['TABLE_METADATA']['{}']".format(table_key))
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


###
#   Functions which retrieve preliminary information used for creating table schemas / ingesting data
#
##
def get_programs_list():
    """
    Get list of programs represented in GDC API master pull.
    :return: List of GDC programs.
    """
    programs_table_id = BQ_PARAMS['WORKING_PROJECT'] + '.' + BQ_PARAMS['METADATA_DATASET'] + '.' + \
                        BQ_PARAMS['GDC_RELEASE'] + '_caseData'

    programs = set()
    results = get_query_results(
        """
        SELECT distinct(program_name)
        FROM `{}`
        """.format(programs_table_id))

    for result in results:
        programs.add(result.program_name)

    return programs


def build_column_order_dict():
    """
    Using table order provided in YAML, with add't ordering for reference columns added during one-to-many
    table creation.
    :return: dict of str column names : int representing position.
    """
    column_order_dict = dict()
    field_groups = API_PARAMS['TABLE_ORDER']
    max_reference_cols = len(field_groups)

    idx = 0

    for fg in field_groups:
        try:
            column_order_list = API_PARAMS['TABLE_METADATA'][fg]['column_order']
            id_column = API_PARAMS['TABLE_METADATA'][fg]['table_id_key']

            for column in column_order_list:
                column_order_dict[fg + '.' + column] = idx

                if id_column == column:
                    # this creates space for reference columns (parent id or one-to-many record count columns)
                    # leaves a gap for submitter_id
                    idx += max_reference_cols * 2
                else:
                    idx += 1
        except KeyError:
            has_fatal_error("{} found in API_PARAMS['TABLE_ORDER'] "
                            "but not in API_PARAMS['TABLE_METADATA']".format(fg))

    column_order_dict['state'] = idx
    column_order_dict['created_datetime'] = idx + 1
    column_order_dict['updated_datetime'] = idx + 2

    return column_order_dict


def lookup_column_types():
    """
    Determing column types for data columns, using existing master table's schema.
    :return: dict of column names: types
    """

    def split_datatype_array(col_dict, col_string, name_prefix):
        """

        :param col_dict:
        :param col_string:
        :param name_prefix:
        :return:
        """
        columns = col_string[13:-2].split(', ')

        for column in columns:
            column_type = column.split(' ')

            column_name = name_prefix + column_type[0]
            col_dict[column_name] = column_type[1].strip(',')

        return col_dict

    def generate_base_query(field_groups_):
        """

        :param field_groups_:
        :return:
        """
        exclude_column_query_str = ''
        for fg_ in field_groups_:
            exclude_column_query_str += "AND column_name != '{}' ".format(fg_)

        query = """
        SELECT column_name, data_type FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{}_clinical_data' 
        """.format(BQ_PARAMS["WORKING_PROJECT"], BQ_PARAMS["TARGET_DATASET"], BQ_PARAMS["GDC_RELEASE"])

        return query + exclude_column_query_str

    def generate_field_group_query(field_group_):
        """

        :param field_group_:
        :return:
        """
        return """
        SELECT column_name, data_type FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{}_clinical_data' and column_name = '{}'
        """.format(BQ_PARAMS["WORKING_PROJECT"], BQ_PARAMS["TARGET_DATASET"], BQ_PARAMS["GDC_RELEASE"], field_group_)

    field_groups = []
    child_field_groups = {}

    for fg in API_PARAMS['EXPAND_FIELD_GROUPS']:
        if len(fg.split(".")) == 1:
            field_groups.append(fg)
        elif len(fg.split(".")) == 2:
            parent_fg = fg.split(".")[0]
            child_fg = fg.split(".")[1]
            if parent_fg not in child_field_groups:
                child_field_groups[parent_fg] = set()
            child_field_groups[parent_fg].add(child_fg)

    column_type_dict = dict()

    # todo there's more to optimize here in terms of automation
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

    for key in single_nested_query_dict.keys():
        results = get_query_results(single_nested_query_dict[key])

        for result in results:
            vals = result.values()
            column_type_dict = split_datatype_array(column_type_dict, vals[1], key + '.')

    results = get_query_results(follow_ups_query)

    for result in results:
        vals = result.values()
        split_vals = vals[1].split('molecular_tests ')

        column_type_dict = split_datatype_array(column_type_dict, split_vals[0] + ' ', 'cases.follow_ups.')

        column_type_dict = split_datatype_array(column_type_dict, split_vals[1][:-2],
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
    column_type_dict = split_datatype_array(column_type_dict, diagnoses, 'cases.diagnoses.')
    column_type_dict = split_datatype_array(column_type_dict, treatments, 'cases.diagnoses.treatments.')
    column_type_dict = split_datatype_array(column_type_dict, annotations, 'cases.diagnoses.annotations.')

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
            # cases.id not returned by mapping endpoint. In such cases, substitute an empty description string.
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


##
# Functions used to determine a program's table structure(s)
##
def get_excluded_fields(table_key):
    """
    Get list of fields to exclude from final BQ tables.
    :param table_key: table key for which to return excluded fields
    :return: list of excluded fields.
    """
    if not API_PARAMS['TABLE_METADATA']:
        has_fatal_error("params['TABLE_METADATA'] not found")
    if 'excluded_fields' not in API_PARAMS['TABLE_METADATA'][table_key]:
        has_fatal_error("excluded_fields not found in API_PARAMS['TABLE_METADATA']['{}']".format(table_key))

    base_column_names = API_PARAMS['TABLE_METADATA'][table_key]['excluded_fields']
    exclude_field_list = [get_bq_name(API_PARAMS, table_key, x) for x in base_column_names]
    return exclude_field_list


def flatten_tables(table_columns, record_counts):
    """
    From dict containing table_name keys and sets of column names:
    - remove excluded columns, merge into parent table if this field group can be flattened for this program.
    :param table_columns: dict of tables and columns
    :param record_counts: dict showing max record count for a case in this program, used in table creation.
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

    for field_group, depth in sorted(field_group_depths.items(), key=lambda item: item[1], reverse=True):
        table_columns[field_group] = remove_excluded_fields(table_columns[field_group], field_group)

        # this is cases, already flattened
        if depth == 1:
            break

        # field group can be flattened.
        if record_counts[field_group] == 1:
            parent_key = get_parent_table(get_tables(record_counts), field_group)

            if not parent_key:
                has_fatal_error("No parent found: {}, record_counts: {}".format(field_group, record_counts.keys()))
            if parent_key not in table_columns:
                table_columns[parent_key] = set()

            for field in table_columns.pop(field_group):
                table_columns[parent_key].add(field)

    if len(table_columns.keys()) - 1 != sum(val > 1 for val in record_counts.values()):
        has_fatal_error("Flattened tables dictionary has incorrect number of keys.")
    return table_columns


def retrieve_program_case_structure(program_name, cases):
    """
    Determine table structure required for the given program.
    :param program_name: program for which to determine structure
    :param cases: dict of program's case records
    :return: dict of tables and columns, dict with maximum record count for this program's field groups.
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
            elif isinstance(case_[field_key], list):
                new_path = parent_path + '.' + field_key
                if new_path not in record_counts_:
                    record_counts_[new_path] = 1

                # find needed one-to-many tables
                record_counts_[new_path] = max(record_counts_[new_path], len(case_[field_key]))

                for entry in case_[field_key]:
                    tables_, record_counts_ = build_case_structure(tables_, entry, record_counts_, new_path)
            elif isinstance(case_[field_key], dict):
                tables_, record_counts_ = build_case_structure(tables_, case_[field_key], record_counts_, parent_path)
            else:
                bq_name = get_bq_name(API_PARAMS, parent_path, field_key)
                table_columns[parent_path].add(bq_name)

        return tables_, record_counts_

    table_columns = {}
    record_counts = {}

    for case in cases:
        table_columns, record_counts = build_case_structure(table_columns, case, record_counts, parent_path='cases')

    table_columns = flatten_tables(table_columns, record_counts)

    if not table_columns:
        has_fatal_error("[ERROR] no case structure returned for program {}".format(program_name))

    print("Record counts for each field group: {}".format(record_counts))

    return table_columns, record_counts


##
# Functions used for schema creation
##
def get_count_column_position(table_key, column_order_dict):
    """
    Position of reference columns reprenting child table record counts for a case.

    :param table_key: table for which to get index
    :param column_order_dict: dict containing tables and column position idxs.
    :return: count column start idx position.
    """
    table_id_key = get_table_id_key(table_key)
    id_column_position = column_order_dict[table_key + '.' + table_id_key]

    count_columns_position = id_column_position + len(API_PARAMS['TABLE_ORDER'])

    return count_columns_position


def add_reference_columns(table_columns, schema_dict, column_order_dict):
    """
    Add reference columns generated by separating and flattening data.
    Possible types:
    - _count column representing # of child records found in supplemental table
    - case_id, used to reference main table records
    - parent_id, used to reference nearest un-flattened ancestor table
    :param table_columns: dict containing table column keys
    :param schema_dict: dict containing schema records
    :param column_order_dict: dict containing relative column index positions, used for schema ordering
    :return: table_columns, schema_dict, column_order_dict
    """

    def generate_id_schema_entry(column_name, parent_table_key_):
        """

        :param column_name:
        :param parent_table_key_:
        :return:
        """
        parent_field_name = get_field_name(parent_table_key_)

        if parent_table_key_ in table_columns.keys():
            ancestor_table = '*_{}'.format(parent_field_name)
        else:
            ancestor_table = 'main'

        if '__' in column_name:
            column_name = column_name.split('__')[-1]
            ancestor_column_name = get_bq_name(API_PARAMS, parent_table_key_, get_field_name(column_name))
        else:
            ancestor_column_name = column_name

        description = "Reference to the parent_id ({}) of the record to which this record belongs. " \
                      "Parent record found in the program's {} table.".format(ancestor_column_name,
                                                                              ancestor_table)

        return {"name": ancestor_column_name, "type": 'STRING', "description": description}

    def generate_record_count_schema_entry(record_count_id_key_, parent_table_key_):
        """

        :param record_count_id_key_:
        :param parent_table_key_:
        :return:
        """
        description = "Total count of records associated with this case, located in {} table".format(parent_table_key_)
        return {"name": record_count_id_key_, "type": 'INTEGER', "description": description}

    for table_key in table_columns.keys():
        table_depth = len(table_key.split('.'))

        id_column_position = get_id_column_position(table_key, column_order_dict)
        reference_col_position = id_column_position + 1

        if table_depth == 1:
            # base table references inserted while processing child tables, so skip
            continue
        elif table_depth > 2:
            # if the > 2 cond. is removed (and the case_id insertion below) tables will only reference direct ancestor
            # tables with depth > 2 have case_id reference and parent_id reference
            parent_fg = get_parent_field_group(table_key)
            parent_id_key = get_table_id_key(parent_fg)
            full_parent_id_name = parent_fg + '.' + parent_id_key
            parent_bq_name = get_bq_name(API_PARAMS, parent_fg, parent_id_key)

            # add parent_id to one-to-many table
            schema_dict[full_parent_id_name] = generate_id_schema_entry(parent_bq_name, parent_fg)
            table_columns[table_key].add(parent_bq_name)
            column_order_dict[full_parent_id_name] = reference_col_position

            reference_col_position += 1

        case_id_key = 'case_id'
        case_id_column = table_key + '.case_id'

        # add case_id to one-to-many table
        schema_dict[case_id_column] = generate_id_schema_entry(case_id_key, 'main')
        table_columns[table_key].add(case_id_key)
        column_order_dict[case_id_column] = reference_col_position

        reference_col_position += 1

        parent_table_key = get_parent_table(table_columns.keys(), table_key)
        count_columns_position = get_count_column_position(parent_table_key, column_order_dict)

        count_order_col_name = table_key + '.count'

        count_id_key = get_bq_name(API_PARAMS, table_key, 'count')

        # add one-to-many record count column to parent table
        schema_dict[count_order_col_name] = generate_record_count_schema_entry(count_id_key, parent_table_key)
        table_columns[parent_table_key].add(count_id_key)
        column_order_dict[count_order_col_name] = count_columns_position

    return schema_dict, table_columns, column_order_dict


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


def create_schemas(table_columns, schema_dict, column_order_dict):
    """
    Create ordered schema lists for final tables.
    :param table_columns: dict containing table column keys
    :param schema_dict: dict containing schema records
    :param column_order_dict: dict containing relative column index positions, used for schema ordering
    :return: lists of BQ SchemaFields.
    """
    table_schema_fields = dict()

    # modify schema dict, add reference columns for this program
    schema_dict, table_columns, column_order_dict = add_reference_columns(table_columns, schema_dict, column_order_dict)

    for table_key in table_columns:
        table_order_dict = dict()

        for column in table_columns[table_key]:
            if '__' in column:
                full_column_name = rebuild_bq_name(column)
            else:
                full_column_name = table_key + '.' + column

            count_column_position = get_count_column_position(table_key, column_order_dict)

            if not full_column_name or full_column_name not in column_order_dict:
                has_fatal_error("'{}' not in col_order_dict!\n {}".format(full_column_name, column_order_dict))

            table_order_dict[full_column_name] = column_order_dict[full_column_name]

            count_columns = []

            for key, value in table_order_dict.items():
                if value == count_column_position:
                    count_columns.append(key)

            # index in alpha order
            count_columns.sort()

            for count_column in count_columns:
                table_order_dict[count_column] = count_column_position
                count_column_position += 1

        required_columns = get_required_columns(table_key)
        schema_list = []

        for schema_key, val in sorted(table_order_dict.items(), key=lambda item: item[1]):
            schema_list.append(create_SchemaField(schema_dict, schema_key, required_columns))

        table_schema_fields[table_key] = schema_list

    return table_schema_fields


def remove_dict_fields(record, table_name):
    """
    Remove columns with Null values, as well as those from the exclude list.
    :param record: table record to parse.
    :param table_name: name of destination table.
    :return: Trimmed down record dict.
    """
    excluded_fields = get_excluded_fields(table_name)

    for field in record.copy():
        if field in excluded_fields or not record[field]:
            record.pop(field)

    return record


##
# Functions used for parsing and loading data into BQ tables
##
def flatten_case(case, prefix, flat_case_dict, case_id=None, new_p_id=None, new_p_id_key=None):
    """
    Convert nested case object into a flattened representation of its records.
    :param case: dict containing case data
    :param prefix: name of parent field group.
    :param flat_case_dict: flattened dictionary for case (used to recursively capture the record as it's parsed).
    :param case_id: case record's id value
    :param new_p_id: value of parent id for this record (or duplicate of case_id if record doesn't have a more
    direct ancestor.)
    :param new_p_id_key: parent table's key for unique ids.
    :return: flattened case dict
    """
    if isinstance(case, list):
        for entry in case:
            entry_dict = dict()
            for key in entry:
                if not isinstance(entry[key], list):
                    curr_table_id_key = get_table_id_key(prefix)

                    if case_id != new_p_id and curr_table_id_key != new_p_id_key:
                        entry_dict[new_p_id_key] = new_p_id

                    if 'case_id' not in entry_dict:
                        entry_dict['case_id'] = case_id

                    # This is where GDC field names are converted to bq_column names (with table abbr prefixes)
                    bq_col_name = get_bq_name(API_PARAMS, prefix, key)
                    entry_dict[bq_col_name] = entry[key]

            if entry_dict:
                entry_dict = remove_dict_fields(entry_dict, prefix)

                if prefix not in flat_case_dict:
                    flat_case_dict[prefix] = list()
                flat_case_dict[prefix].append(entry_dict)

            for key in entry:
                if isinstance(entry[key], list):
                    new_prefix = prefix + '.' + key
                    new_p_id_key = get_table_id_key(prefix)
                    new_p_id = entry[new_p_id_key]
                    new_p_key = get_bq_name(API_PARAMS, prefix, new_p_id_key)

                    flat_case_dict = flatten_case(entry[key], new_prefix, flat_case_dict, case_id, new_p_id, new_p_key)
    else:
        entry_dict = dict()

        for key in case:
            if not isinstance(case[key], list):
                # This is the other place where GDC field names are converted to bq_column names (with table abbr prefixes)
                field = get_bq_name(API_PARAMS, prefix, key)
                entry_dict[field] = case[key]

        if entry_dict:
            entry_dict = remove_dict_fields(entry_dict, prefix)

            if prefix not in flat_case_dict:
                flat_case_dict[prefix] = list()
            flat_case_dict[prefix].append(entry_dict)

        for key in case:
            if isinstance(case[key], list):
                new_prefix = prefix + '.' + key
                new_p_id_key = get_table_id_key(prefix)
                new_p_id = case[new_p_id_key]
                new_p_key = get_bq_name(API_PARAMS, prefix, new_p_id_key)

                flat_case_dict = flatten_case(case[key], new_prefix, flat_case_dict, case_id, new_p_id, new_p_key)

    return flat_case_dict


def merge_single_entry_field_groups(flattened_case_dict, bq_program_tables):
    """
    Merge field groups which have a max of one record for every case in this program.
    These columns will be located in parent table.
    :param flattened_case_dict: flattened dictionary for case (used to recursively capture the record as it's parsed).
    :param bq_program_tables: list of tables to be created for this program.
    :return: flattened_case_dict with single record tables merged.
    """
    field_group_depths = dict.fromkeys(flattened_case_dict.keys(), 0)

    # sort field group keys by depth
    for key in field_group_depths:
        field_group_depths[key] = len(key.split("."))

    for fg_key, fg_depth in sorted(field_group_depths.items(), key=lambda item: item[1], reverse=True):
        # cases is the master table, merged into
        if fg_key == 'cases':
            break

        parent_table = get_parent_table(flattened_case_dict.keys(), fg_key)
        parent_id_key = get_table_id_key(parent_table)
        parent_id_column = get_bq_name(API_PARAMS, parent_table, parent_id_key)

        if fg_key in bq_program_tables:
            record_count_dict = dict()
            idx = 0
            for entry in flattened_case_dict[parent_table].copy():
                if parent_id_key in entry:
                    entry_id = entry[parent_id_key]
                elif parent_id_column in entry:
                    entry_id = entry[parent_id_column]
                else:
                    has_fatal_error("No id key found, in bq or fg format.")

                if entry_id not in record_count_dict:
                    record_count_dict[entry_id] = {'entry_idx': idx, 'record_count': 0}
                    idx += 1

            field_group = flattened_case_dict[fg_key].copy()

            for record in field_group:
                if parent_id_column in record:
                    parent_id = record[parent_id_column]
                    record_count_dict[parent_id]['record_count'] += 1
            for parent_id in record_count_dict:
                entry_idx = record_count_dict[parent_id]['entry_idx']
                count_id = get_bq_name(API_PARAMS, fg_key, 'count')
                flattened_case_dict[parent_table][entry_idx][count_id] = record_count_dict[parent_id]['record_count']
        else:
            field_group = flattened_case_dict.pop(fg_key)[0]

            if len(field_group) == 0:
                continue
            if 'case_id' in field_group:
                field_group.pop('case_id')
            # include keys with values
            for key, fg_val in field_group.items():
                flattened_case_dict[parent_table][0][key] = fg_val
    return flattened_case_dict


def create_and_load_tables(program_name, cases, table_schemas, record_counts):
    """
    Create jsonl row files for future insertion, store in GC storage bucket, then insert the new table schemas and data.
    :param program_name: program for which to create tables
    :param cases: case records to insert into BQ for program
    :param table_schemas: dict of schema lists for all of this program's tables.
    :param record_counts: max counts for field groups in this dataset.
    """
    bq_program_tables = get_tables(record_counts)
    print("Inserting case records... ")
    table_keys = table_schemas.keys()

    for table in table_keys:
        fp = get_temp_filepath(program_name, table)
        if os.path.exists(fp):
            os.remove(fp)

    for case in cases:
        flattened_case_dict = flatten_case(case, 'cases', dict(), case['case_id'], case['case_id'], 'case_id')

        flattened_case_dict = merge_single_entry_field_groups(flattened_case_dict, bq_program_tables)

        for table in flattened_case_dict.keys():
            if table not in table_keys:
                has_fatal_error("Table {} not found in table keys".format(table))

            jsonl_fp = get_temp_filepath(program_name, table)

            with open(jsonl_fp, 'a') as jsonl_file:
                for row in flattened_case_dict[table]:
                    json.dump(obj=row, fp=jsonl_file)
                    jsonl_file.write('\n')

    for table in table_schemas:
        jsonl_file = get_jsonl_filename(program_name, table)
        upload_to_bucket(BQ_PARAMS, API_PARAMS['TEMP_PATH'], jsonl_file)
        table_id = get_full_table_name(program_name, table)

        try:
            create_and_load_table(BQ_PARAMS, jsonl_file, table_schemas[table], table_id)
        except ValueError as err:
            print("{}, {}".format(jsonl_file, table_id))
            has_fatal_error("{}".format(err))


##
#  Functions for creating documentation
##
def generate_documentation(documentation_dict):
    print(documentation_dict)
    json_doc_file = BQ_PARAMS['GDC_RELEASE'] + '_clin_json_documentation_dump.json'

    with open(API_PARAMS['TEMP_PATH'] + '/' + json_doc_file, 'w') as json_file:
        json.dump(documentation_dict, json_file)

    upload_to_bucket(BQ_PARAMS, API_PARAMS['TEMP_PATH'], json_doc_file)


##
# Script execution
##
def print_final_report(start, steps):
    """
    Outputs a basic report of script's results, including total processing time and which steps were specified in YAML.
    :param start: float representing script's start time.
    :param steps: set of steps to be performed (configured in YAML)
    """
    seconds = time.time() - start
    minutes = math.floor(seconds / 60)
    seconds -= minutes * 60

    print("Programs script executed in {} min, {:.0f} sec".format(minutes, seconds))
    print("Steps completed: ")

    if 'create_and_load_tables' in steps:
        print('\t - created tables and inserted data')
    if 'validate_data' in steps:
        print('\t - validated data (tests not considered exhaustive)')
    if 'generate_documentation' in steps:
        print('\t - generated documentation')
    print('\n\n')


def main(args):
    start = time.time()

    if len(args) != 2:
        has_fatal_error('Usage : {} <configuration_yaml> <column_order_txt>".format(args[0])', ValueError)

    steps = None

    # Load YAML configuration
    with open(args[1], mode='r') as yaml_file:
        try:
            global API_PARAMS, BQ_PARAMS
            API_PARAMS, BQ_PARAMS, steps = load_config(yaml_file, YAML_HEADERS)
        except ValueError as e:
            has_fatal_error(str(e), ValueError)

    # program_names = get_programs_list()
    program_names = ['VAREPOP']
    column_order_dict = build_column_order_dict()
    schema_dict = create_schema_dict()

    documentation_dict = dict()

    for program_name in program_names:
        program_docs_dict = dict()

        prog_start = time.time()

        print("Executing script for program {}...".format(program_name))
        cases = get_cases_by_program(BQ_PARAMS, program_name)

        if cases:
            # derive the program's table structure by analyzing its case records
            table_columns, record_counts = retrieve_program_case_structure(program_name, cases)

            if 'create_and_load_tables' in steps:
                # create table schemas
                table_schemas = create_schemas(table_columns, schema_dict, column_order_dict.copy())

                table_id_dict = dict()

                for table in get_tables(record_counts):
                    table_id_dict[table] = get_table_id(BQ_PARAMS, table)

                # create tables, flatten and insert data
                create_and_load_tables(program_name, cases, table_schemas, record_counts)

        program_time = time.time() - prog_start
        print("{} processed in {:0.1f} seconds!\n".format(program_name, program_time))

        documentation_dict[program_name] = {
            'record_counts': record_counts,
            'table_schemas': str(table_schemas),
            'table_columns': table_columns,
            'table_ids': table_id_dict,
            'api_params': API_PARAMS,
            'bq_params': BQ_PARAMS
        }

    if 'generate_documentation' in steps:
        generate_documentation(documentation_dict)

    if 'validate_data' in steps:
        test_table_output()

    print_final_report(start, steps)


if __name__ == '__main__':
    main(sys.argv)

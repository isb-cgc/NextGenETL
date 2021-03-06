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
import io
import json
import os
import sys
import time

import requests
import yaml
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage, exceptions


#       GETTERS - YAML CONFIG


def get_field_groups(api_params):
    """Get field group list from build master table yaml config for GDC.

    :param api_params: api param object from yaml config
    :return: list of expand field groups
    """
    if 'FIELD_GROUPS' not in api_params:
        has_fatal_error('FIELD_GROUPS not in api_params (check yaml config file)')
    return ",".join(list(api_params['FIELD_GROUPS']))


def get_required_fields(api_params, fg):
    """Get list of required fields (used to create schema and load values into BQ table).

    :param api_params: api param object from yaml config
    :param fg: name of field group for which to retrieve required fields
    :return: list of required fields (currently only returns fg's id key)
    """
    field_config = api_params['FIELD_CONFIG']

    if fg in field_config and 'id_key' in field_config[fg]:
        # this is a single entry list of the moment
        return [get_field_key(fg, field_config[fg]['id_key'])]

    return None


def get_column_order_one_fg(api_params, fg):
    """Get field/column order list associated with given field group from yaml config.

    :param api_params: api param object from yaml config
    :param fg: field group for which to retrieve field/column order list
    :return: field group's column order list
    """
    if fg not in api_params['FIELD_CONFIG']:
        has_fatal_error("'{}' not found in FIELD_CONFIG in yaml config".format(fg))

    fg_params = api_params['FIELD_CONFIG'][fg]

    if not fg_params or 'column_order' not in fg_params:
        has_fatal_error("No order for field group {} in yaml.".format(fg), KeyError)

    # return full field key, in order, for given field_grp
    return [get_field_key(fg, field) for field in fg_params['column_order']]


def get_excluded_field_groups(api_params):
    """Get a list of field groups (via yaml config) to exclude from the final tables.
    Currently used in order to exclude fgs that would otherwise create duplicate columns
    when merging fgs into a smaller # of tables, WHILE not utilizing fg prefixes
    to create unique names (which is undesirable for web app integration, for instance).

    :param api_params: api param object from yaml config
    :return: list of field groups to exclude
    """
    if 'FG_CONFIG' not in api_params or not api_params['FG_CONFIG']:
        has_fatal_error('FG_CONFIG not in api_params, or is empty', KeyError)
    if 'excluded_fgs' not in api_params['FG_CONFIG']:
        has_fatal_error('excluded_fgs not found in not in FG_CONFIG', KeyError)

    return api_params['FG_CONFIG']['excluded_fgs']


def get_excluded_fields_all_fgs(api_params, fgs, is_webapp=False):
    """Get a list of fields for each field group to exclude from the tables
    from yaml config (api_params['FIELD_CONFIG']['excluded_fields'] or
    api_params['FIELD_CONFIG']['app_excluded_fields'] for the web app).

    :param api_params: api param object from yaml config
    :param fgs: list of expand field groups included from API call
    :param is_webapp: is script currently running for 'create_webapp_tables' step?
    :return: set of fields to exclude
    """
    if 'FIELD_CONFIG' not in api_params or not api_params['FIELD_CONFIG']:
        has_fatal_error('FIELD_CONFIG not in api_params, or is empty', KeyError)

    excluded_list_key = 'app_excluded_fields' if is_webapp else 'excluded_fields'

    exclude_fields = set()

    for fg in fgs:
        if fg not in api_params['FIELD_CONFIG']:
            has_fatal_error('{} not found in not in FIELD_CONFIG'.format(fg), KeyError)
        elif not api_params['FIELD_CONFIG'][fg]:
            continue
        elif excluded_list_key not in api_params['FIELD_CONFIG'][fg]:
            has_fatal_error("One of the excluded params missing from YAML.", KeyError)
        elif not api_params['FIELD_CONFIG'][fg][excluded_list_key]:
            continue

        for field in api_params['FIELD_CONFIG'][fg][excluded_list_key]:
            exclude_fields.add(get_field_key(fg, field))

    return exclude_fields


def get_excluded_fields_one_fg(api_params, fg, is_webapp=False):
    """Get excluded fields for given field group (pulled from yaml config file).

    :param api_params: api param object from yaml config
    :param fg: field group for which to retrieve excluded fields
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: list of excluded fields associated with field group 'fg' in yaml config
    """
    if 'FIELD_CONFIG' not in api_params:
        has_fatal_error("FIELD_CONFIG not set in YAML.", KeyError)
    elif fg not in api_params['FIELD_CONFIG']:
        has_fatal_error("{} not set in YAML.".format(fg), KeyError)
    elif not api_params['FIELD_CONFIG'][fg]:
        has_fatal_error("api_params['FIELD_CONFIG']['{}'] not found".format(fg), KeyError)

    excluded_key = 'app_excluded_fields' if is_webapp else 'excluded_fields'

    if excluded_key not in api_params['FIELD_CONFIG'][fg]:
        has_fatal_error("{}'s {} not found.".format(fg, excluded_key))

    excluded_list = api_params['FIELD_CONFIG'][fg][excluded_key]
    return [get_bq_name(api_params, f, is_webapp, fg) for f in excluded_list]


def get_rel_prefix(bq_params):
    """Get current release number/date (set in yaml config).

    :param bq_params: bq param object from yaml config
    :return: release abbreviation
    """
    rel_prefix = ''

    if 'REL_PREFIX' in bq_params and bq_params['REL_PREFIX']:
        rel_prefix += bq_params['REL_PREFIX']
    if 'RELEASE' in bq_params and bq_params['RELEASE']:
        rel_prefix += bq_params['RELEASE']

    return rel_prefix


def get_fg_prefix(api_params, fg):
    """Get field group abbreviations from yaml config, used to create field prefixes
    in order to prevent BQ column name duplication.

    :param api_params: api param object from yaml config
    :param fg: specific field group for which to retrieve prefix
    :return: str containing the prefix designated in yaml config for given fg
    """
    if 'FIELD_CONFIG' not in api_params or not api_params['FIELD_CONFIG']:
        has_fatal_error('FIELD_CONFIG not in api_params, or is empty', KeyError)

    elif fg not in api_params['FIELD_CONFIG']:
        has_fatal_error('{} not found in not in FIELD_CONFIG'.format(fg), KeyError)

    elif 'prefix' not in api_params['FIELD_CONFIG'][fg]:
        has_fatal_error("prefix not found in FIELD_CONFIG for {}".format(fg), KeyError)

    return api_params['FIELD_CONFIG'][fg]['prefix']


def get_table_suffixes(api_params):
    """Get abbreviations for field groups as designated in yaml config.

    :param api_params: api param object from yaml config
    :return: dict of {field_group: abbreviation_suffix}
    """
    suffixes = dict()

    for table, metadata in api_params['FIELD_CONFIG'].items():
        suffixes[table] = metadata['table_suffix'] if metadata['table_suffix'] else ''

    return suffixes


def build_master_table_name_from_params(bq_params):
    """Get master table name from yaml config.

    :param bq_params: bq param object from yaml config
    :return: master table name
    """
    return "_".join([get_rel_prefix(bq_params), bq_params['MASTER_TABLE']])


#       GETTERS - MISC
#


#   Project and Program Getters


def get_program_list(bq_params):
    """Get list of the programs which have contributed data to GDC's research program.

    :param bq_params: bq param object from yaml config
    :return: list of research programs participating in GDC data sharing
    """
    programs_query = ("""
        SELECT DISTINCT(proj) 
        FROM (
            SELECT SPLIT(
                (SELECT project_id
                 FROM UNNEST(project)), '-')[OFFSET(0)] AS proj
            FROM `{}`)
        ORDER BY proj
    """).format(get_working_table_id(bq_params))

    return {prog.proj for prog in get_query_results(programs_query)}


def get_project_name(table_id):
    """Get the BQ project name for a given table id.

    :param table_id: id in standard SQL format: '{project_id}.{dataset_id}.{table_name}'
    :return: GDC project to which the BQ table belongs
    """
    split_table = table_id.split('.')

    if len(split_table) != 3:
        has_fatal_error("Incorrect naming for table_id: {}".format(table_id))

    return split_table[0]


#   Table Getters


def get_one_to_many_tables(api_params, record_counts):
    """Get one-to-many tables for program.

    :param api_params: api param object from yaml config
    :param record_counts: dict max field group record counts for program
    :return: set of table names (representing field groups which cannot be flattened)
    """
    table_keys = {get_base_fg(api_params)}

    for table in record_counts:
        if record_counts[table] > 1:
            table_keys.add(table)

    return table_keys


def build_table_name(str_list):
    """Constructs a table name (str) from list<str>.

    :param str_list: a list<str> of table name segments
    :return: composed table name string
    """
    table_name = "_".join(str_list)

    # replace '.' with '_' so that the name is valid
    # ('.' chars not allowed -- issue with BEATAML1.0, for instance)
    return table_name.replace('.', '_')


def convert_json_to_table_name(bq_params, json_file):
    """Convert json filename (from BQEcosystem repo) into BQ table name.
    json schema files match table ID of BQ table.

    :param bq_params: bq param object from yaml config
    :param json_file: json file from BQEcosystem repo containing table schema
    data and metadata; json file naming matches table ID of corresponding BQ table
    :return: BQ table name for which the json acts as a configuration file
    """
    # handles naming for *webapp* tables
    split_name = json_file.split('.')
    program_name = split_name[1]
    split_table = split_name[2].split('_')
    table_name = '_'.join(split_table[:-2])
    rel = get_rel_prefix(bq_params)

    return '_'.join([rel, program_name, table_name])


def build_table_id(project, dataset, table):
    """ Build table_id in {project_id}.{dataset_id}.{table_name} format.

    :param project: project id
    :param dataset: dataset id
    :param table: table name
    :return: table_id
    """
    return '{}.{}.{}'.format(project, dataset, table)


def convert_json_to_table_id(bq_params, json_file):
    """Convert json file from BQEcosystem repo into component dataset and table names.
    Naming matches table ID of corresponding production BQ clinical tables.

    :param bq_params: bq param object from yaml config
    :param json_file: json file from BQEcosystem repo, storing table metadata
    :return: names of datasets and tables for production current and versioned
    repositories
    """
    split_json = json_file.split('.')
    dest_table = "_".join(split_json[2].split('_')[:-1])

    dev_project = bq_params['DEV_PROJECT']
    prod_project = bq_params['PROD_PROJECT']

    dev_dataset = bq_params['DEV_DATASET']
    curr_dataset = split_json[1]
    versioned_dataset = "_".join([curr_dataset, bq_params['VERSIONED_SUFFIX']])

    src_table = "_".join(split_json[2].split('_')[:-2])
    src_table = "_".join([get_rel_prefix(bq_params), split_json[1], src_table])
    curr_table = "_".join([dest_table, bq_params['CURRENT_SUFFIX']])
    vers_table = "_".join([dest_table, get_rel_prefix(bq_params)])

    src_table_id = build_table_id(dev_project, dev_dataset, src_table)
    curr_table_id = build_table_id(prod_project, curr_dataset, curr_table)
    vers_table_id = build_table_id(prod_project, versioned_dataset, vers_table)

    return src_table_id, curr_table_id, vers_table_id


def get_biospecimen_table_id(bq_params, program):
    """Builds and retrieves a table ID for the biospecimen stub tables.

    :param bq_params: bq param object from yaml config
    :param program: the program from which the cases originate
    :return: biospecimen table_id
    """
    table_name = build_table_name([get_rel_prefix(bq_params),
                                   str(program),
                                   bq_params['BIOSPECIMEN_SUFFIX']])

    return build_table_id(bq_params['DEV_PROJECT'], bq_params['APP_DATASET'], table_name)


def get_working_table_id(bq_params, table_name=None):
    """Get table id for development version of the db table.

    :param bq_params: bq param object from yaml config
    :param table_name: name of the bq table
    :return: table id
    """
    if not table_name:
        table_name = build_master_table_name_from_params(bq_params)

    return build_table_id(bq_params["DEV_PROJECT"], bq_params["DEV_DATASET"], table_name)


def get_webapp_table_id(bq_params, table_name):
    """Get table id for webapp db table.

    :param bq_params: bq param object from yaml config
    :param table_name: name of the bq table
    :return: table id
    """
    return build_table_id(bq_params['DEV_PROJECT'], bq_params['APP_DATASET'], table_name)


#   Field and Field Group Getters


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


def get_parent_fg(tables, field_name):
    """
    Get field's parent table name.
    :param tables: list of table names for program
    :param field_name: full field name for which to retrieve parent table
    :return: parent table name
    """
    parent_table = get_field_group(field_name)

    while parent_table and parent_table not in tables:
        parent_table = get_field_group(parent_table)

    if parent_table:
        return parent_table
    return has_fatal_error("No parent fg found for {}".format(field_name))


def get_field_group(field_name):
    """Gets parent field group (might not be the parent *table*, as the ancestor fg
    could be flattened).

    :param field_name: field name for which to retrieve ancestor field group
    :return: ancestor field group
    """
    return ".".join(field_name.split('.')[:-1])


def get_field_group_id_key(api_params, field_group, is_webapp=False, return_field_only=False):
    """Retrieves the id key used to uniquely identify a table record.

    :param api_params: api param object from yaml config
    :param field_group: table for which to determine the id key
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: str representing table key
    """

    split_fg = field_group.split('.')
    if split_fg[0] != api_params['FG_CONFIG']['base_fg']:
        split_fg.insert(0, api_params['FG_CONFIG']['base_fg'])
        field_group = ".".join(split_fg)

    if field_group not in api_params['FIELD_CONFIG']:
        console_out("field group {} not in API_PARAMS['FIELD_CONFIG']".format(field_group))
        return None
    if 'id_key' not in api_params['FIELD_CONFIG'][field_group]:
        has_fatal_error("id_key not found in API_PARAMS for {}".format(field_group))

    fg_id_name = api_params['FIELD_CONFIG'][field_group]['id_key']

    if return_field_only:
        return fg_id_name

    fg_id_key = get_field_key(field_group, fg_id_name)

    if is_webapp:
        new_fg_id_key = get_renamed_field_key(api_params, fg_id_key)

        if new_fg_id_key:
            return new_fg_id_key

    return fg_id_key


def get_fg_id_name(api_params, field_group, is_webapp=False):
    """Retrieves the id key used to uniquely identify a table record.

    :param api_params: api param object from yaml config
    :param field_group: table for which to determine the id key
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: str representing table key
    """
    fg_id_key = get_field_group_id_key(api_params, field_group, is_webapp)

    return get_field_name(fg_id_key)
    # todo this should be replaced


def get_field_name(field_col_key):
    """Get short field name from full field or bq column name.

    :param field_col_key: full field or bq column name
    :return: short field name
    """
    if '.' not in field_col_key and '__' not in field_col_key:
        return field_col_key

    split_char = '.' if '.' in field_col_key else '__'

    return field_col_key.split(split_char)[-1]


def get_field_key(field_group, field):
    """Get full field key ("{field_group}.{field_name}"}.

    :param field_group: field group to which the field belongs
    :param field: field name
    :return: full field key string
    """
    return '{}.{}'.format(field_group, field)


def get_bq_name(api_params, field, is_webapp=False, arg_fg=None):
    """Get column name (in bq format) from full field name.

    :param api_params: api params from yaml config file
    :param field: if not table_path, full field name; else short field name
    :param arg_fg: field group containing field
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: bq column name for given field name
    """
    base_fg = get_base_fg(api_params)

    if arg_fg:
        # field group is specified as a function argument
        fg = arg_fg
        field_key = get_field_key(fg, field)
    elif len(field.split('.')) == 1:
        # no fg delimiter found in field string: cannot be a complete field key
        fg = base_fg
        field_key = get_field_key(fg, field)
    else:
        # no fg argument, but field contains separator chars; extract the fg and name
        fg = get_field_group(field)
        field_key = field

    # derive the key's short field name
    field_name = get_field_name(field_key)

    # get id_key and prefix associated with this fg
    this_fg_id = get_fg_id_name(api_params, fg)
    prefix = get_fg_prefix(api_params, fg)

    # create map of {fg_names : id_keys}
    fg_to_id_key_map = get_fgs_and_id_keys(api_params)

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


def get_renamed_field_key(api_params, field_key):
    """Gets the new field name for an existing field.
    Used to rename fields for web app integration.

    :param api_params: api param object from yaml config
    :param field_key: field key ({fg}.{field}) for which to find a alternative field key
    :return: None if no replacement field key, otherwise string containing new field key
    """
    if 'RENAMED_FIELDS' not in api_params:
        has_fatal_error("RENAMED_FIELDS not found in API_PARAMS")

    renamed_fields = api_params['RENAMED_FIELDS']

    if not renamed_fields or (renamed_fields and field_key not in renamed_fields):
        return None

    return renamed_fields[field_key]


def get_renamed_field_keys(api_params):
    """Get renamed fields dict from yaml config.

    :param api_params: api param object from yaml config
    :return: renamed fields dict
    """
    if 'RENAMED_FIELDS' not in api_params:
        has_fatal_error("RENAMED_FIELDS not found in API_PARAMS")

    return api_params['RENAMED_FIELDS']


#   I/O Getters


def build_working_gs_uri(bq_params, filename):
    """Builds an uri reference for file uploaded to Google storage bucket.

    :param bq_params: bq param object from yaml config
    :param filename: file uploaded to google storage bucket
    :return: uri reference for google storage bucket file
    """
    return "gs://{}/{}/{}".format(bq_params['WORKING_BUCKET'],
                                  bq_params['WORKING_BUCKET_DIR'],
                                  filename)


def construct_table_name(bq_params, program='', suffix='', is_webapp=False):
    """
    todo
    :param bq_params:
    :param program:
    :param suffix:
    :param is_webapp:
    :return:
    """
    app_prefix = bq_params['APP_JSONL_PREFIX'] if is_webapp else ''

    name_list = [app_prefix,
                 bq_params['REL_PREFIX'] + bq_params['RELEASE'],
                 program,
                 bq_params['MASTER_TABLE'],
                 suffix]

    file_name = [x for x in name_list if x]
    return '_'.join(file_name)


def build_jsonl_output_filename(bq_params, program='', suffix='', is_webapp=False):
    """
    todo
    :param bq_params:
    :param program:
    :param suffix:
    :param is_webapp:
    :return:
    """
    file_name = construct_table_name(bq_params, program, suffix, is_webapp)

    return file_name + '.jsonl'


def get_suffixed_jsonl_filename(api_params, bq_params, program, table, is_webapp=False):
    """
    todo
    :param api_params:
    :param bq_params:
    :param program:
    :param table:
    :param is_webapp:
    :return:
    """
    suffixes = get_table_suffixes(api_params)
    suffix = suffixes[table]
    program = program.replace('.', '_')

    return build_jsonl_output_filename(bq_params, program, suffix, is_webapp=is_webapp)


def build_jsonl_name(api_params, bq_params, program, table, is_webapp=False):
    """
    todo
    :param api_params:
    :param bq_params:
    :param program:
    :param table:
    :param is_webapp:
    :return:
    """
    app_prefix = bq_params['APP_JSONL_PREFIX'] if is_webapp else ''
    gdc_rel = bq_params['REL_PREFIX'] + bq_params['RELEASE']
    program = program.replace('.', '_')
    base_name = bq_params['MASTER_TABLE']
    suffix = get_table_suffixes(api_params)[table]

    name_list = [app_prefix, gdc_rel, program, base_name, suffix]
    filtered_name_list = [x for x in name_list if x]
    file_name = '_'.join(filtered_name_list)

    return file_name + '.jsonl'


def get_filepath(dir_path, filename=None):
    """Get file path for location on VM.

    :param dir_path: directory portion of the filepath (starting at user home dir)
    :param filename: name of the file
    :return: full path to file
    """
    join_list = [os.path.expanduser('~'), dir_path]

    if filename:
        join_list.append(filename)

    return '/'.join(join_list)


def get_scratch_fp(bq_params, filename):
    """Construct filepath for VM output file.

    :param filename: name of the file
    :param bq_params: bq param object from yaml config
    :return: output filepath for VM
    """
    return get_filepath(bq_params['SCRATCH_DIR'], filename)


#       FILESYSTEM HELPERS


def get_dir(fp):
    """ Get directory component of filepath.

    :param fp: full filepath (dir and file name)
    :return: directory component of fp
    """
    return '/'.join(fp.split('/')[:-1])


def write_list_to_jsonl(jsonl_fp, json_obj_list, mode='w'):
    """ Create a jsonl file for uploading data into BQ from a list<dict> obj.

    :param jsonl_fp: filepath of jsonl file to write
    :param json_obj_list: list<dict> object
    :param mode: 'a' if appending to a file that's being built iteratively
                 'w' if file data is written in a single call to the function
                     (in which case any existing data is overwritten)"""

    with open(jsonl_fp, mode) as file_obj:
        cnt = 0

        for line in json_obj_list:
            json.dump(obj=line, fp=file_obj)
            file_obj.write('\n')
            cnt += 1


def append_list_to_jsonl(file_obj, json_list):
    try:
        for line in json_list:
            json_str = convert_dict_to_string(line)
            json.dump(obj=json_str, fp=file_obj)
            file_obj.write('\n')
    except IOError as err:
        print(str(err), IOError)


def delete_file(fp):
    if os.path.exists(fp):
        os.remove(fp)
        print("{} deleted successfully!".format(fp))
    else:
        print("{} not found!".format(fp))


#       REST API HELPERS (GDC, PDC, ETC)


def create_mapping_dict(endpoint):
    """Creates a dict containing field mappings for given endpoint.
    Note: only differentiates the GDC API's 'long' type (called 'integer' in GDC data
    dictionary) and 'float' type (called 'number' in GDC data dictionary). All others
    typed as string.

    :param endpoint: API endpoint for which to retrieve mapping
    :return: dict of field maps. Each entry contains field name, type, and description
    """
    field_mapping_dict = {}

    # retrieve mappings json object
    res = requests.get(endpoint + '/_mapping')
    field_mappings = res.json()['_mapping']

    for field in field_mappings:
        # convert data types from GDC format to formats used in BQ
        if field_mappings[field]['type'] == 'long':
            field_type = 'INTEGER'
        elif field_mappings[field]['type'] == 'float':
            field_type = 'FLOAT'
        else:
            field_type = 'STRING'

        # create json object of field mapping data
        field_mapping_dict[field] = {
            'name': field.split('.')[-1],
            'type': field_type,
            'description': field_mappings[field]['description']
        }

    return field_mapping_dict


def create_schema_dict(api_params, bq_params, is_webapp=False):
    """Creates schema dict using master table's bigquery.table.Table.schema attribute.

    :param api_params: api param object from yaml config
    :param bq_params: bq params from yaml config file
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: flattened schema dict in format:
        {full field name: {name: 'name', type: 'field_type', description: 'description'}}
    """
    client = bigquery.Client()
    bq_table = client.get_table(get_working_table_id(bq_params))

    schema_list = []

    for schema_field in bq_table.schema:
        schema_list.append(schema_field.to_api_repr())

    schema = dict()

    parse_bq_schema_obj(api_params=api_params,
                        schema=schema,
                        fg=get_base_fg(api_params),
                        schema_list=schema_list,
                        is_webapp=is_webapp)

    return schema


def get_cases_by_program(bq_params, program):
    """Get a dict obj containing all the cases associated with a given program.

    :param bq_params: bq param object from yaml config
    :param program: the program from which the cases originate
    :return: cases dict
    """
    start_time = time.time()
    cases = []

    sample_table_id = get_biospecimen_table_id(bq_params, program)

    query = """
        SELECT * 
        FROM `{}` 
        WHERE case_id IN (
            SELECT DISTINCT(case_gdc_id) 
            FROM `{}`
            WHERE project_name = '{}')
    """.format(get_working_table_id(bq_params), sample_table_id, program)

    for case_row in get_query_results(query):
        case_items = dict(case_row.items())
        case_items.pop('project')
        cases.append(case_items)

    end_time = time.time() - start_time

    return cases


def get_graphql_api_response(api_params, query, fail_on_error=True):
    max_retries = 4

    headers = {'Content-Type': 'application/json'}
    endpoint = api_params['ENDPOINT']

    if not query:
        has_fatal_error("Must specify query for get_graphql_api_response.", SyntaxError)

    req_body = {'query': query}
    api_res = requests.post(endpoint, headers=headers, json=req_body)
    tries = 0

    while not api_res.ok and tries < max_retries:
        console_out("API response status code {}: {};\nRetry {} of {}...",
                    (api_res.status_code, api_res.reason, tries, max_retries))
        time.sleep(3)

        api_res = requests.post(endpoint, headers=headers, json=req_body)

        tries += 1

    if tries > max_retries:
        # give up!
        api_res.raise_for_status()

    json_res = api_res.json()

    if 'errors' in json_res and json_res['errors']:
        if fail_on_error:
            has_fatal_error("Errors returned by {}.\nError json:\n{}".format(endpoint, json_res['errors']))
        return None

    return json_res


#       BIGQUERY API HELPERS


def get_last_fields_in_table(api_params):
    """ Get list of fields to always include at the end of merged tables,
    via the yaml config.

    :param api_params: api param object from yaml config
    :return: fields to include at the end of the table
    """
    if 'FG_CONFIG' not in api_params:
        has_fatal_error("Missing FG_CONFIG in YAML", KeyError)
    elif 'last_keys_in_table' not in api_params['FG_CONFIG']:
        has_fatal_error("Missing last_keys_in_table in FG_CONFIG in YAML", KeyError)

    return api_params['FG_CONFIG']['last_keys_in_table']


def parse_bq_schema_obj(api_params, schema, fg, schema_list=None, is_webapp=False):
    """Recursively construct schema using existing metadata in main clinical table.

    :param api_params: api param object from yaml config
    :param schema: dict of flattened schema entries
    :param fg: current field group name
    :param schema_list: schema field entries for field_group
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    """

    if fg not in api_params['FIELD_CONFIG']:
        return

    for i, schema_field in enumerate(schema_list):

        field_key = get_field_key(fg, schema_field['name'])

        # if has 'fields', then the current obj contains nested objs
        if schema_field['type'] == 'RECORD':
            # if nested, recurse down to the next level
            parse_bq_schema_obj(api_params, schema, field_key, schema_field['fields'], is_webapp)

            required_field_list = get_required_fields(api_params, fg)

            for field_name in required_field_list:
                schema[field_name]['mode'] = 'REQUIRED'
        else:
            # not a nested field entry--do we need to prefix the schema field name?
            schema_field['name'] = get_bq_name(api_params, field_key, is_webapp)
            schema[field_key] = schema_field


def get_fgs_and_id_keys(api_params):
    """ Create a dictionary of type { 'field_group' : 'id_key_field'}.

    :param api_params: api param object from yaml config
    :return: mapping dict, field group -> id_key_field
    """
    id_key_dict = dict()

    fg_config_entries = api_params['FIELD_CONFIG']

    for fg in fg_config_entries:
        id_key_dict[fg] = fg_config_entries[fg]['id_key']

    return id_key_dict


def copy_bq_table(bq_params, src_table, dest_table, replace_table=False):
    """Copy an existing BQ table into a new location.

    :param bq_params: bq param object from yaml config
    :param src_table: Table to copy
    :param dest_table: Table to be created
    """
    client = bigquery.Client()

    job_config = bigquery.CopyJobConfig()

    if replace_table:
        delete_bq_table(dest_table)

    bq_job = client.copy_table(src_table, dest_table, job_config=job_config)

    if await_job(bq_params, client, bq_job):
        console_out("Successfully copied table:")
        console_out("src: {0}\n dest: {1}\n", (src_table, dest_table))


def create_and_load_table(bq_params, jsonl_file, schema, table_id):
    """Creates BQ table and inserts case data from jsonl file.

    :param bq_params: bq param obj from yaml config
    :param jsonl_file: file containing case records in jsonl format
    :param schema: list of SchemaFields representing desired BQ table schema
    :param table_id: id of table to create
    """
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    job_config.schema = schema
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    gs_uri = build_working_gs_uri(bq_params, jsonl_file)

    try:
        load_job = client.load_table_from_uri(gs_uri, table_id, job_config=job_config)
        console_out(' - Inserting into {0}... ', (table_id,), end="")
        await_insert_job(bq_params, client, table_id, load_job)
    except TypeError as err:
        has_fatal_error(err)


def create_and_load_tsv_table(bq_params, tsv_file, schema, table_id, null_marker=''):
    """Creates BQ table and inserts case data from jsonl file.

    :param bq_params: bq param obj from yaml config
    :param tsv_file: file containing case records in tsv format
    :param schema: list of SchemaFields representing desired BQ table schema
    :param table_id: id of table to create
    """
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    job_config.schema = schema
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.field_delimiter = '\t'
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.skip_leading_rows = 1
    job_config.null_marker = null_marker  # todo added this back, is that ok?

    gs_uri = build_working_gs_uri(bq_params, tsv_file)

    try:
        load_job = client.load_table_from_uri(gs_uri, table_id, job_config=job_config)

        console_out(' - Inserting into {0}... ', (table_id,), end="")
        await_insert_job(bq_params, client, table_id, load_job)
    except TypeError as err:
        has_fatal_error(err)


def delete_bq_table(table_id):
    """Permanently delete BQ table located by table_id.

    :param table_id: table id in standard SQL format
    """
    client = bigquery.Client()
    client.delete_table(table_id, not_found_ok=True)

    console_out("deleted table: {0}", (table_id,))


def exists_bq_table(table_id):
    """Determine whether bq_table exists.

    :param table_id: table id in standard SQL format
    :return: True if exists, False otherwise
    """
    client = bigquery.Client()

    try:
        client.get_table(table_id)
    except NotFound:
        return False
    return True


def load_table_from_query(bq_params, table_id, query):
    """Create a new BQ table from the returned results of querying an existing BQ db.

    :param bq_params: bq params from yaml config file
    :param table_id: table id in standard SQL format
    :param query: query which returns data to populate a new BQ table.
    """
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(destination=table_id)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    try:
        query_job = client.query(query, job_config=job_config)
        console_out(' - Inserting into {0}... ', (table_id,), end="")
        await_insert_job(bq_params, client, table_id, query_job)
    except TypeError as err:
        has_fatal_error(err)


def get_bq_table_obj(table_id):
    """Get the bq table referenced by table_id.

    :param table_id: table id in standard SQL format
    :return: bq Table object
    """
    if not exists_bq_table(table_id):
        return None

    client = bigquery.Client()
    return client.get_table(table_id)


def get_query_results(query):
    """Returns BigQuery query result object.

    :param query: query string
    :return: result object
    """
    client = bigquery.Client()
    query_job = client.query(query)
    return query_job.result()


def await_insert_job(bq_params, client, table_id, bq_job):
    """Monitor the completion of BQ Job which does produce some result
    (usually data insertion).

    :param bq_params: bq params from yaml config file
    :param client: BQ api object, allowing for execution of bq lib functions
    :param table_id: table id in standard SQL format
    :param bq_job: A Job object, responsible for executing bq function calls
    """
    last_report_time = time.time()
    location = bq_params['LOCATION']
    job_state = "NOT_STARTED"

    while job_state != 'DONE':
        bq_job = client.get_job(bq_job.job_id, location=location)

        if time.time() - last_report_time > 30:
            console_out('\tcurrent job state: {0}...\t', (bq_job.state,), end='')
            last_report_time = time.time()

        job_state = bq_job.state

        if job_state != 'DONE':
            time.sleep(2)

    bq_job = client.get_job(bq_job.job_id, location=location)

    if bq_job.error_result is not None:
        has_fatal_error(
            'While running BQ job: {}\n{}'.format(bq_job.error_result, bq_job.errors),
            ValueError)

    table = client.get_table(table_id)
    console_out(" done. {0} rows inserted.", (table.num_rows,))


def await_job(bq_params, client, bq_job):
    """Monitor the completion of BQ Job which doesn't return a result.

    :param bq_params: bq params from yaml config file
    :param client: BQ api object, allowing for execution of bq lib functions
    :param bq_job: A Job object, responsible for executing bq function calls
    """
    location = bq_params['LOCATION']
    job_state = "NOT_STARTED"

    while job_state != 'DONE':
        bq_job = client.get_job(bq_job.job_id, location=location)

        job_state = bq_job.state

        if job_state != 'DONE':
            time.sleep(2)

    bq_job = client.get_job(bq_job.job_id, location=location)

    if bq_job.error_result is not None:
        err_res = bq_job.error_result
        errs = bq_job.errors
        has_fatal_error("While running BQ job: {}\n{}".format(err_res, errs))


def from_schema_file_to_obj(bq_params, filename):
    """
    Open table schema file and convert to python dict, in order to pass the data to
    BigQuery for table insertion.

    :param bq_params: bq param object from yaml config
    :param filename: name of the schema file
    :return: schema list, table metadata dict
    """

    fp = get_filepath(bq_params['SCHEMA_DIR'], filename)
    # todo changed this, does it work?

    if not os.path.exists(fp):
        return None, None

    with open(fp, 'r') as schema_file:
        try:
            schema_file = json.load(schema_file)

            schema = schema_file['schema']['fields']

            table_metadata = {
                'description': schema_file['description'],
                'friendlyName': schema_file['friendlyName'],
                'labels': schema_file['labels']
            }
        except FileNotFoundError:
            return None, None

        return schema, table_metadata


def to_bq_schema_obj(schema_field_dict):
    """Convert schema entry dict to SchemaField object.

    :param schema_field_dict: dict containing schema field keys
    (name, field_type, mode, fields, description)
    :return: bigquery.SchemaField object
    """
    return bigquery.SchemaField.from_api_repr(schema_field_dict)


def generate_bq_schema(schema_dict, record_type, expand_fields_list):
    """Generates BigQuery SchemaField list for insertion of case records.

    :param schema_dict: dict of schema fields
    :param record_type: type of field/field group
    :param expand_fields_list: list of field groups included in API request
    :return: list of SchemaFields for case record insertion
    """
    # add fields to a list in order to generate a dict representing nested fields
    field_group_names = [record_type]
    nested_depth = 0

    for field_group in expand_fields_list.split(','):
        nested_field_name = record_type + '.' + field_group
        nested_depth = max(nested_depth, len(nested_field_name.split('.')))
        field_group_names.append(nested_field_name)

    record_lists_dict = {field_grp_name: [] for field_grp_name in field_group_names}
    # add field to correct field grouping list based on full field name
    for field in schema_dict:
        # record_lists_dict key is equal to the parent field components of
        # full field name
        record_lists_dict[get_field_group(field)].append(schema_dict[field])

    temp_schema_field_dict = {}

    while nested_depth >= 1:
        for field_group_name in record_lists_dict:
            split_group_name = field_group_name.split('.')

            # builds from max depth inward to avoid iterating through entire schema obj
            # in order to append child field groups. Skip any shallower field groups.
            if len(split_group_name) != nested_depth:
                continue

            schema_field_sublist = []

            for record in record_lists_dict[field_group_name]:
                schema_field_sublist.append(
                    bigquery.SchemaField(name=record['name'],
                                         field_type=record['type'],
                                         mode='NULLABLE',
                                         description=record['description'],
                                         fields=()))

            parent_name = get_field_group(field_group_name)

            if field_group_name in temp_schema_field_dict:
                schema_field_sublist += temp_schema_field_dict[field_group_name]

            if parent_name:
                if parent_name not in temp_schema_field_dict:
                    temp_schema_field_dict[parent_name] = list()

                temp_schema_field_dict[parent_name].append(
                    bigquery.SchemaField(name=get_field_name(field_group_name),
                                         field_type='RECORD',
                                         mode='REPEATED',
                                         description='',
                                         fields=tuple(schema_field_sublist)))
            else:
                if nested_depth > 1:
                    has_fatal_error("Empty parent_name at level {}"
                                    .format(nested_depth), ValueError)
                return schema_field_sublist

        nested_depth -= 1
    return None


def update_table_metadata(table_id, metadata):
    """Modify an existing BQ table with additional metadata.

    :param table_id: table id in standard SQL format
    :param metadata: metadata containing new field and table attributes
    """
    client = bigquery.Client()
    table = get_bq_table_obj(table_id)

    table.labels = metadata['labels']
    table.friendly_name = metadata['friendlyName']
    table.description = metadata['description']
    client.update_table(table, ["labels", "friendly_name", "description"])

    assert table.labels == metadata['labels']
    assert table.friendly_name == metadata['friendlyName']
    assert table.description == metadata['description']


def update_friendly_name(bq_params, table_id, custom_name=None):
    """Modify a table's friendly name metadata.

    :param bq_params: bq param object from yaml config
    :param table_id: table id in standard SQL format
    :param custom_name: By default, appends "'REL' + bq_params['RELEASE'] + ' VERSIONED'"
    onto the existing friendly name. If custom_name is specified, this behavior is
    overridden, and the table's friendly name is replaced entirely.
    """
    client = bigquery.Client()
    table = get_bq_table_obj(table_id)

    if custom_name:
        new_name = custom_name
    else:
        new_name = table.friendly_name + ' REL' + bq_params['RELEASE'] + ' VERSIONED'

    table.friendly_name = new_name
    client.update_table(table, ["friendly_name"])

    assert table.friendly_name == new_name


def update_schema(table_id, new_descriptions):
    """Modify an existing table's field descriptions.

    :param table_id: table id in standard SQL format
    :param new_descriptions: dict of field names and new description strings
    """
    client = bigquery.Client()
    table = get_bq_table_obj(table_id)

    new_schema = []

    for schema_field in table.schema:
        field = schema_field.to_api_repr()

        if field['name'] in new_descriptions.keys():
            name = field['name']
            field['description'] = new_descriptions[name]
        elif field['description'] == '':
            console_out("Still no description for field: {0}", (field['name']))

        mod_field = bigquery.SchemaField.from_api_repr(field)
        new_schema.append(mod_field)

    table.schema = new_schema

    client.update_table(table, ['schema'])


#       (NON-BQ) GOOGLE CLOUD API HELPERS


def upload_file_to_bucket(project, bucket, blob_dir, fp):
    """
    todo
    :param project:
    :param bucket:
    :param blob_dir:
    :param fp:
    :return:
    """
    try:
        client = storage.Client(project=project)
        bucket = client.get_bucket(bucket)
        blob = bucket.blob(blob_dir)

        blob.upload_from_file(fp)
    except exceptions.GoogleCloudError as err:
        has_fatal_error("Failed to upload to bucket.\n{}".format(err))


def upload_to_bucket(bq_params, scratch_fp):
    """Uploads file to a google storage bucket (location specified in yaml config).

    :param bq_params: bq param object from yaml config
    :param scratch_fp: name of file to upload to bucket
    """

    try:
        storage_client = storage.Client(project="")

        jsonl_output_file = scratch_fp.split('/')[-1]
        blob_name = "{}/{}".format(bq_params['WORKING_BUCKET_DIR'], jsonl_output_file)
        bucket = storage_client.bucket(bq_params['WORKING_BUCKET'])
        blob = bucket.blob(blob_name)

        blob.upload_from_filename(scratch_fp)
    except exceptions.GoogleCloudError as err:
        has_fatal_error("Failed to upload to bucket.\n{}".format(err))


def download_from_bucket(bq_params, filename):
    storage_client = storage.Client(project="")
    blob_name = "{}/{}".format(bq_params['WORKING_BUCKET_DIR'], filename)
    bucket = storage_client.bucket(bq_params['WORKING_BUCKET'])
    blob = bucket.blob(blob_name)

    scratch_fp = get_scratch_fp(bq_params, filename)
    with open(scratch_fp, 'wb') as file_obj:
        blob.download_to_file(file_obj)


#       ANALYZE DATA


def check_value_type(value):
    """Checks value for type (possibilities are string, float and integers).

    :param value: value to type check
    :return: type in BQ column format
    """
    # if has leading zero, then should be considered a string, even if only
    # composed of digits
    val_is_none = value in ('NA', 'null', 'None') or not value
    val_is_bool = value in ('True', 'False', True, False)
    val_is_decimal = value.startswith('0.')
    val_is_id = value.startswith('0') and not val_is_decimal and len(value) > 1

    try:
        float(value)
        val_is_num = True
        # Changing this because google won't accept loss of precision in the
        # data insert job
        # (won't cast 1.0 as 1)
        val_is_float = not value.isdigit()
        # If this is used, a field with only trivial floats will be cast as
        # Integer. However, BQ errors due to loss
        # of precision.
        # val_is_float = True if int(float(value)) != float(value) else False
    except ValueError:
        val_is_num = False
        val_is_float = False

    if val_is_none:
        return None
    if val_is_id:
        return 'STRING'
    if val_is_decimal or val_is_float:
        return 'FLOAT'
    if val_is_num:
        return 'INTEGER'
    if val_is_bool:
        return 'BOOLEAN'

    return 'STRING'


def collect_values(fields, field, parent, field_grp_prefix):
    """Recursively inserts sets of values for a given field into return dict (
    used to infer field data type).

    :param fields: A dict of key:value pairs -- {field_name: set(field_values)}
    :param field: field name
    :param parent: dict containing field and it's values
    :param field_grp_prefix: string representation of current location in field hierarchy
    :return: field_dict containing field names and a set of its values
    """
    # If the value of parent_dict[key] is a list at this level, and a dict at the next
    # (or a dict at this level, as seen in second conditional statement),
    # iterate over each list element's dictionary entries. (Sometimes lists are composed
    # of strings rather than dicts, and those are later converted to strings.)
    field_name = field_grp_prefix + field
    new_prefix = field_name + '.'

    if isinstance(parent[field], list) \
            and len(parent[field]) > 0 and isinstance(parent[field][0], dict):
        for dict_item in parent[field]:
            for dict_key in dict_item:
                fields = collect_values(fields, dict_key, dict_item, new_prefix)
    elif isinstance(parent[field], dict):
        for dict_key in parent[field]:
            fields = collect_values(fields, dict_key, parent[field], new_prefix)
    else:
        if field_name not in fields:
            fields[field_name] = set()

        # This type of list can be converted to a comma-separated value string
        if isinstance(parent[field], list):
            value = ", ".join(parent[field])
        else:
            value = parent[field]

        fields[field_name].add(value)

    return fields


def infer_data_types(flattened_json):
    """Infer data type of fields based on values contained in dataset.

    :param flattened_json: file containing dict of {field name: set of field values}
    :return: dict of field names and inferred type (None if no data in value set)
    """
    data_types = dict()

    for column in flattened_json:
        data_types[column] = None

        for value in flattened_json[column]:
            if data_types[column] == 'STRING':
                break

            # adding this change because organoid submitter_ids look like
            # ints, but they should be str for uniformity
            if column[-2:] == 'id':
                data_types[column] = 'STRING'
                break

            val_type = check_value_type(str(value))

            if not val_type:
                continue
            if val_type in ('FLOAT', 'STRING') or (
                    val_type in ('INTEGER', 'BOOLEAN') and not data_types[column]):
                data_types[column] = val_type

    return data_types


def get_sorted_fg_depths(record_counts, reverse=False):
    """Returns a sorted dict of field groups: depths.

    :param record_counts: dict containing field groups and associated record counts
    :param reverse: if True, sort in DESC order, otherwise sort in ASC order
    :return: tuples composed of field group names and record counts
    """
    table_depths = {table: len(table.split('.')) for table in record_counts}

    return sorted(table_depths.items(), key=lambda item: item[1], reverse=reverse)


#       MISC UTILITIES


def format_seconds(seconds):
    if seconds > 3600:
        return time.strftime("%-H hours, %-M minutes, %-S seconds", time.gmtime(seconds))
    if seconds > 60:
        return time.strftime("%-M minutes, %-S seconds", time.gmtime(seconds))

    return time.strftime("%-S seconds", time.gmtime(seconds))


def convert_dict_to_string(obj):
    """Converts dict/list of primitives or strings to a comma-separated string. Used
    to write data to file.

    :param obj: object to converts
    :return: modified object
    """
    if isinstance(obj, list):
        if not isinstance(obj[0], dict):
            str_list = ', '.join(obj)
            obj = str_list
        else:
            for idx, value in enumerate(obj.copy()):
                obj[idx] = convert_dict_to_string(value)
    elif isinstance(obj, dict):
        for key in obj:
            obj[key] = convert_dict_to_string(obj[key])
    return obj


def load_config(args, yaml_dict_keys):
    """Opens yaml file and retrieves configuration parameters.

    :param args: args param from python bash cli
    :param yaml_dict_keys: tuple of strings representing a subset of the yaml file's
    top-level dict keys
    :return: tuple of dicts from yaml file (as requested in yaml_dict_keys)
    """
    if len(args) != 2:
        has_fatal_error('Usage: {} <configuration_yaml>".format(args[0])', ValueError)

    yaml_file_arg = args[1]

    with open(yaml_file_arg, mode='r') as yaml_file_arg:

        yaml_dict = None

        config_stream = io.StringIO(yaml_file_arg.read())

        try:
            yaml_dict = yaml.load(config_stream, Loader=yaml.FullLoader)
        except yaml.YAMLError as ex:
            has_fatal_error(ex, str(yaml.YAMLError))
        if yaml_dict is None:
            has_fatal_error("Bad YAML load, exiting.", ValueError)

        # Dynamically generate a list of dictionaries for the return statement,
        # since tuples are immutable
        return_dicts = [yaml_dict[key] for key in yaml_dict_keys]

        return tuple(return_dicts)


def has_fatal_error(err, exception=None):
    """Error handling function--outputs error str or list<str>;
    optionally throws Exception as well.

    :param err: error message str or list<str>
    :param exception: Exception type for error (defaults to None)
    """
    err_str_prefix = '[ERROR] '
    err_str = ''

    if isinstance(err, list):
        for item in err:
            err_str += err_str_prefix + str(item) + '\n'
    else:
        err_str = err_str_prefix + err

    console_out(err_str)

    if exception:
        raise exception

    sys.exit(1)


def console_out(output_str, print_vars=None, end='\n'):
    """
    todo
    :param output_str:
    :param print_vars:
    :param end:
    :return:
    """
    if print_vars:
        print(str(output_str).format(*print_vars), end=end)
    else:
        print(output_str, end=end)


def modify_fields_for_app(api_params, schema, column_order_dict, columns):
    """Alter field naming conventions so that they're compatible with those in the
    web app.

    :param api_params: api param object from yaml config
    :param schema: dict containing schema records
    :param column_order_dict: dict of {field_groups: column_order set()}
    :param columns: dict containing table column keys
    """
    renamed_fields = dict(api_params['RENAMED_FIELDS'])
    fgs = column_order_dict.keys()

    excluded_fgs = get_excluded_field_groups(api_params)
    excluded_fields = get_excluded_fields_all_fgs(api_params, fgs, is_webapp=True)

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


def create_tsv_row(row_list, null_marker="None"):
    print_str = ''
    last_idx = len(row_list) - 1

    for i, column in enumerate(row_list):
        if not column:
            column = null_marker

        delimiter = "\t" if i < last_idx else "\n"
        print_str += column + delimiter

    return print_str

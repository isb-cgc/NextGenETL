"""
Copyright 2023, Institute for Systems Biology

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
import os
import sys
import time
import re
from typing import Optional, Union, Any
import yaml

Params = dict[str, Union[str, dict, int]]


def load_config(args: str, yaml_dict_keys: tuple[str, ...]) -> tuple[Any, ...]:
    """
    Open yaml file and retrieves configuration parameters.
    :param args: args param from python bash cli
    :param yaml_dict_keys: tuple of strings representing a subset of the yaml file's
    top-level dict keys
    :return: tuple of dicts from yaml file (as requested in yaml_dict_keys)
    """

    def open_yaml_and_return_dict(yaml_name: str) -> dict:
        """
        Open yaml file and return contents as dict.
        :param yaml_name: name of yaml config file
        :return: dictionary of parameters
        """
        with open(yaml_name, mode='r') as yaml_file:
            yaml_dict = None
            config_stream = io.StringIO(yaml_file.read())

            try:
                yaml_dict = yaml.load(config_stream, Loader=yaml.FullLoader)
            except yaml.YAMLError as ex:
                has_fatal_error(ex, str(yaml.YAMLError))
            if yaml_dict is None:
                has_fatal_error("Bad YAML load, exiting.", ValueError)

            # Dynamically generate a list of dictionaries for the return statement,
            # since tuples are immutable
            return {key: yaml_dict[key] for key in yaml_dict_keys}

    if len(args) < 2 or len(args) > 3:
        has_fatal_error("")
    if len(args) == 2:
        singleton_yaml_dict = open_yaml_and_return_dict(args[1])
        return tuple([singleton_yaml_dict[key] for key in yaml_dict_keys])

    shared_yaml_dict = open_yaml_and_return_dict(args[1])

    data_type_yaml_dict = open_yaml_and_return_dict(args[2])

    merged_yaml_dict = {key: {} for key in yaml_dict_keys}

    for key in yaml_dict_keys:
        if key not in shared_yaml_dict and key not in data_type_yaml_dict:
            has_fatal_error(f"{key} not found in shared or data type-specific yaml config")
        elif not shared_yaml_dict[key] and not data_type_yaml_dict[key]:
            has_fatal_error(f"No values found for {key} in shared or data type-specific yaml config")

        if key in shared_yaml_dict and shared_yaml_dict[key]:
            merged_yaml_dict[key] = shared_yaml_dict[key]

            if key in data_type_yaml_dict and data_type_yaml_dict[key]:
                merged_yaml_dict[key].update(data_type_yaml_dict[key])
        else:
            merged_yaml_dict[key] = data_type_yaml_dict[key]

    return tuple([merged_yaml_dict[key] for key in yaml_dict_keys])


def has_fatal_error(err: Union[str, BaseException], exception: Optional[Any] = None):
    """
    Output error str or list<str>, then exits; optionally throws Exception.
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

    print(err_str)

    if exception:
        raise exception

    sys.exit(1)


def format_seconds(seconds: Union[int, float]) -> str:
    """
    Round seconds to formatted hour, minute, and/or second output.
    :param seconds: int representing time in seconds
    :return: formatted time string
    """
    if seconds > 3600:
        return time.strftime("%-H hours, %-M minutes, %-S seconds", time.gmtime(seconds))
    if seconds > 60:
        return time.strftime("%-M minutes, %-S seconds", time.gmtime(seconds))

    return time.strftime("%-S seconds", time.gmtime(seconds))


def make_string_bq_friendly(string: str) -> str:
    """
    Replace any illegal characters with an underscore. Convert duplicate spaces in a row to single underscore.
    (Only alphanumeric, underscore, or space characters allowed.)
    :param string: string to sanitize
    :return: sanitized string
    """
    string = string.replace('%', 'percent')
    string = re.sub(r'[^A-Za-z0-9_ ]+', ' ', string)
    string = string.strip()
    string = re.sub(r'\s+', '_', string)

    return string


def sanitize_file_prefix(file_prefix: str) -> str:
    """
    Replace any illegal characters in file prefix with underscore.
    (Only alphanumeric and underscore allowed.)
    :param file_prefix: file name (without file extension)
    :return: sanitized file prefix
    """
    return re.sub('[^0-9a-zA-Z_]+', '_', file_prefix)


# todo candidate for removal or merging into get_filename
def construct_table_name(params: Params,
                         prefix: str,
                         suffix: Optional[str] = None,
                         include_release: bool = True,
                         release: Optional[str] = None) -> str:
    """
    Generate BigQuery-safe table name using supplied parameters.
    :param params: params supplied in yaml config
    :param prefix: table prefix or the base table's root name
    :param suffix: table suffix, optionally supplying another word to append to the prefix
    :param include_release: If False, excludes RELEASE value set in yaml config; defaults to True
    :param release: Optionally supply a custom release (useful for external mapping tables, etc.)
    :return: Table name, formatted to be compatible with BigQuery's naming limitations (only: A-Z, a-z, 0-9, _)
    """
    table_name = prefix

    if suffix:
        table_name += '_' + suffix

    if release:
        table_name += '_' + release
    elif include_release:
        table_name += '_' + params['RELEASE']

    return sanitize_file_prefix(table_name)


# todo candidate for removal
def get_filename(params: Params,
                 file_extension: str,
                 prefix: str,
                 suffix: Optional[str] = None,
                 include_release: bool = True,
                 release: Optional[str] = None) -> str:
    """
    Get filename based on common table-naming (see construct_table_name).
    :param params: params from YAML config
    :param file_extension: File extension, e.g. jsonl or tsv
    :param prefix: file name prefix
    :param suffix: file name suffix
    :param include_release: if True, includes release in file name; defaults to True
    :param release: data release version
    :return: file name
    """
    filename = construct_table_name(params, prefix, suffix, include_release, release=release)
    return f"{filename}.{file_extension}"


def get_filepath(dir_path: str, filename: Optional[str] = None) -> str:
    """
    Get file path for location on VM; expands compatibly for local or VM scripts.
    :param dir_path: directory portion of the filepath (starting at user home dir)
    :param filename: name of the file
    :return: full path to file
    """
    join_list = [os.path.expanduser('~'), dir_path]

    if filename:
        join_list.append(filename)

    return '/'.join(join_list)


def get_scratch_fp(params: Params, filename: str) -> str:
    """
    Construct filepath for VM output file.
    :param params: params supplied in yaml config
    :param filename: name of the file
    :return: output filepath for VM
    """
    return get_filepath(params['SCRATCH_DIR'], filename)


def create_dev_table_id(params, table_name) -> str:
    """
    Create table id reference to one of the CDA dev tables used to construct the joined data tables.
    :param table_name: name of the table
    :return: table id string
    """
    return f"`{params['WORKING_PROJECT']}.{params['WORKING_DATASET']}.{params['RELEASE']}_{table_name}`"


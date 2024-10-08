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
import logging
import os
import select
import sys
import time
import re
from typing import Optional, Union, Any
import yaml
import hashlib

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
            config_stream = io.StringIO(yaml_file.read())

            try:
                yaml_dict = yaml.load(config_stream, Loader=yaml.FullLoader)
            except yaml.YAMLError as ex:
                logger.critical(ex, str(yaml.YAMLError))
                sys.exit(-1)

            if yaml_dict is None:
                logger.critical("Bad YAML load, exiting.")
                sys.exit(-1)

            # Dynamically generate a list of dictionaries for the return statement,
            # since tuples are immutable
            return {key: yaml_dict[key] for key in yaml_dict_keys}

    logger = logging.getLogger('base_script.cda_bq_etl.utils')

    if len(args) < 2 or len(args) > 3:
        logger.critical("Incorrect number of args.")
        sys.exit(-1)
    if len(args) == 2:
        singleton_yaml_dict = open_yaml_and_return_dict(args[1])
        return tuple([singleton_yaml_dict[key] for key in yaml_dict_keys])

    shared_yaml_dict = open_yaml_and_return_dict(args[1])

    data_type_yaml_dict = open_yaml_and_return_dict(args[2])

    merged_yaml_dict = {key: {} for key in yaml_dict_keys}

    for key in yaml_dict_keys:
        if key not in shared_yaml_dict and key not in data_type_yaml_dict:
            logger.critical(f"{key} not found in shared or data type-specific yaml config")
            logger = logging.getLogger('base_script.cda_bq_etl.utils')
        elif not shared_yaml_dict[key] and not data_type_yaml_dict[key]:
            logger.critical(f"No values found for {key} in shared or data type-specific yaml config")
            logger = logging.getLogger('base_script.cda_bq_etl.utils')

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


def get_scratch_fp(params: Params, filename: str, scratch_dir: str = None) -> str:
    """
    Construct filepath for VM output file.
    :param params: params supplied in yaml config
    :param filename: name of the file
    :param scratch_dir: optional substitute scratch dir
    :return: output filepath for VM
    """
    if not scratch_dir:
        scratch_dir = params['SCRATCH_DIR']

    return get_filepath(scratch_dir, filename)


def create_dev_table_id(params: Params, table_name: str, release_as_suffix: bool = False) -> str:
    """
    Create table id reference to one of the CDA dev tables used to construct the joined data tables.
    :param params: params supplied in yaml config
    :param table_name: name of the table
    :param release_as_suffix: if True, adds release to end of table name, rather than the beginning; defaults to False
    :return: table id string
    """

    dev_dataset_id = f"{params['DEV_PROJECT']}.{params['DEV_RAW_DATASET']}"

    if release_as_suffix:
        return f"{dev_dataset_id}.{table_name}_{params['RELEASE']}"
    else:
        return f"{dev_dataset_id}.{params['RELEASE']}_{table_name}"


def create_excluded_records_table_id(params: Params, table_name: str) -> str:
    """
    Create table id reference to one of the CDA excluded records tables.
    :param params: params supplied in yaml config
    :param table_name: name of the table
    :return: table id string
    """
    return f"{params['DEV_PROJECT']}.{params['EXCLUDED_RECORDS_DATASET']}.{params['RELEASE']}_{table_name}"


def create_metadata_table_id(params: Params, table_name: str, release: str = None) -> str:
    """
    Create table id reference to one of the CDA metadata tables.
    :param params: params supplied in yaml config
    :param table_name: name of the table
    :param release: optional, supply custom release string
    :return: table id string
    """
    if release is None:
        return f"{params['DEV_PROJECT']}.{params['DEV_METADATA_DATASET']}.{params['RELEASE']}_{table_name}"
    else:
        return f"{params['DEV_PROJECT']}.{params['DEV_METADATA_DATASET']}.{release}_{table_name}"


def create_per_sample_table_id(params: Params, table_name: str) -> str:
    """
    Create table id reference to one of the CDA per sample file tables.
    :param params: params supplied in yaml config
    :param table_name: name of the table
    :return: table id string
    """
    return f"{params['DEV_PROJECT']}.{params['DEV_SAMPLE_DATASET']}.{params['RELEASE']}_{table_name}"


def create_clinical_table_id(params: Params, table_name: str) -> str:
    """
    Create table id reference to one of the CDA clinical tables.
    :param params: params supplied in yaml config
    :param table_name: name of the table
    :return: table id string
    """
    return f"{params['DEV_PROJECT']}.{params['DEV_CLINICAL_DATASET']}.{params['RELEASE']}_{table_name}"


def create_quant_table_id(params: Params, table_name: str, is_final: bool) -> str:
    """
    Create table id reference to one of the PDC quant data matrix tables.
    :param params: params supplied in yaml config
    :param table_name: name of the table
    :param is_final: if True, use final dataset, else use raw dataset; defaults to raw
    :return: table id string
    """

    if is_final:
        return f"{params['DEV_PROJECT']}.{params['DEV_QUANT_FINAL_DATASET']}.{table_name}"
    else:
        return f"{params['DEV_PROJECT']}.{params['DEV_QUANT_RAW_DATASET']}.{table_name}"


def input_with_timeout(seconds: int) -> Union[str, None]:
    """
    Wait for user response. Continue automatically after n seconds.
    :param seconds: Number of seconds to wait before continuing automatically.
    :return: keyboard input
    """
    input_poll = select.poll()
    input_poll.register(sys.stdin.fileno(), select.POLLIN)

    while True:
        events = input_poll.poll(seconds * 1000)  # milliseconds

        if not events:
            return None

        for fileno, event in events:
            if fileno == sys.stdin.fileno():
                return input()


def calculate_md5sum(file_path: str) -> str:
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

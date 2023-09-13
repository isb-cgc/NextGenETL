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
import sys
from typing import Union, Any, Optional

import json
import re
import datetime
import time
import logging
import csv
from distutils import util

from cda_bq_etl.gcs_helpers import upload_to_bucket
from cda_bq_etl.utils import sanitize_file_prefix, get_scratch_fp, make_string_bq_friendly

ColumnTypes = Union[None, str, float, int, bool]
RowDict = dict[str, Union[None, str, float, int, bool]]
JSONList = list[RowDict]
Params = dict[str, Union[str, dict, int]]


def write_list_to_jsonl(jsonl_fp: str, json_obj_list: JSONList, mode: str = 'w'):
    """
    Create a jsonl file for uploading data into BigQuery from a list<dict> obj.
    :param jsonl_fp: local VM jsonl filepath
    :param json_obj_list: list of dicts representing json objects
    :param mode: 'a' if appending to a file that's being built iteratively;
                 'w' if file data is written in a single call to the function
                 (in which case any existing data is overwritten)
    """
    with open(jsonl_fp, mode) as file_obj:
        for line in json_obj_list:
            json.dump(obj=line, fp=file_obj, default=json_datetime_to_str_converter)
            file_obj.write('\n')


def write_list_to_jsonl_and_upload(params: Params,
                                   prefix: str,
                                   record_list: JSONList,
                                   local_filepath: Optional[str] = None):
    """
    Write joined_record_list to file name specified by prefix and uploads to scratch Google Cloud bucket.
    :param params: params supplied in yaml config
    :param prefix: string representing base file name (release string is appended to generate filename)
    :param record_list: list of record objects to insert into jsonl file
    :param local_filepath: VM path where jsonl file is stored prior to upload
    """
    if not local_filepath:
        jsonl_filename = f"{sanitize_file_prefix(prefix)}_{params['RELEASE']}.jsonl"
        local_filepath = get_scratch_fp(params, jsonl_filename)

    write_list_to_jsonl(local_filepath, record_list)
    upload_to_bucket(params, local_filepath, delete_local=True)


def recursively_detect_object_structures(nested_obj: Union[JSONList, RowDict]) -> Union[JSONList, RowDict]:
    """
    Traverse a dict or list of objects, analyzing the structure. Order not guaranteed (if anything, it'll be
    backwards)--Not for use with TSV data. Works for arbitrary nesting, even if object structure varies from record to
    record; use for lists, dicts, or any combination therein.
    If nested_obj is a list, function will traverse every record in order to find all possible fields.
    :param nested_obj: object to traverse
    :return data types dict--key is the field name, value is the set of BigQuery column data types returned
    when analyzing data using check_value_type ({<field_name>: {<data_type_set>}})
    """
    # stores the dict of {fields: value types}
    data_types_dict = dict()

    def recursively_detect_object_structure(_obj, _data_types_dict):
        """
        Recursively explore a part of the supplied object. Traverses parent nodes, adding to data_types_dict
        as repeated (RECORD) field objects. Adds child nodes parent's "fields" list.
        :param _obj: object in current location of recursion
        :param _data_types_dict: dict of fields and type sets
        """
        for k, v in _obj.items():
            if isinstance(_obj[k], dict):
                if k not in _data_types_dict:
                    # this is a dict, so use dict to nest values
                    _data_types_dict[k] = dict()

                recursively_detect_object_structure(_obj[k], _data_types_dict[k])
            elif isinstance(_obj[k], list) and len(_obj[k]) > 0 and isinstance(_obj[k][0], dict):
                if k not in _data_types_dict:
                    # this is a dict, so use dict to nest values
                    _data_types_dict[k] = dict()

                for _record in _obj[k]:
                    recursively_detect_object_structure(_record, _data_types_dict[k])
            elif not isinstance(_obj[k], list) or (isinstance(_obj[k], list) and len(_obj[k]) > 0):
                # create set of Data type values
                if k not in _data_types_dict:
                    _data_types_dict[k] = set()

                _obj[k] = normalize_value(_obj[k])
                val_type = check_value_type(_obj[k])

                if val_type:
                    _data_types_dict[k].add(val_type)

    if isinstance(nested_obj, dict):
        recursively_detect_object_structure(nested_obj, data_types_dict)
    elif isinstance(nested_obj, list):
        for record in nested_obj:
            recursively_detect_object_structure(record, data_types_dict)

    return data_types_dict


def get_column_list_tsv(header_list: Optional[list[str]] = None,
                        tsv_fp: Optional[str] = None,
                        header_row_index: Optional[int] = None) -> list[str]:
    """
    Return a list of column headers using header_list OR using a header_row index to retrieve column names from tsv_fp.
        NOTE: Specifying both header_list and header_row in parent function triggers a fatal error.
    :param header_list: Optional ordered list of column headers corresponding to columns in dataset tsv file
    :param tsv_fp: Optional string filepath; provided if column names are being obtained directly from tsv header
    :param header_row_index: Optional header row index, if deriving column names from tsv file
    :return list of columns with BQ-compatible names
    :rtype list
    """
    logger = logging.getLogger('base_script.cda_bq_etl.data_helpers')

    if not header_list and not header_row_index and not isinstance(header_row_index, int):
        logger.critical("Must supply either the header row index or header list for tsv schema creation.")
        sys.exit(-1)
    if header_row_index and header_list:
        logger.critical("Can't supply both a header row index and header list for tsv schema creation.")
        sys.exit(-1)

    column_list = list()

    if header_list:
        for column in header_list:
            column = make_string_bq_friendly(column)
            column_list.append(column)
    else:
        with open(tsv_fp, 'r') as tsv_file:
            if header_row_index:
                for index in range(header_row_index):
                    tsv_file.readline()

            column_row = tsv_file.readline()
            columns = column_row.split('\t')

            if len(columns) == 0:
                logger.critical("No column name values supplied by header row index")
                sys.exit(-1)

            for column in columns:
                column = make_string_bq_friendly(column)
                column_list.append(column)

    return column_list


def aggregate_column_data_types_tsv(tsv_fp: str,
                                    column_headers: list[str],
                                    skip_rows: int,
                                    sample_interval: int = 1) -> dict[str, set[str]]:
    """
    Open tsv file and aggregate data types for each column.
    :param tsv_fp: tsv dataset filepath used to analyze the data types
    :type tsv_fp: str
    :param column_headers: list of ordered column headers
    :type column_headers: list
    :param skip_rows: number of (header) rows to skip before starting analysis
    :type skip_rows: int
    :param sample_interval: sampling interval, used to skip rows in large datasets; defaults to checking every row
        ex.: sample_interval == 10 will sample every 10th row
    :type sample_interval: int
    :return dict of column keys, with value sets representing all data types found for that column
    :rtype dict[str, set]
    """
    data_types_dict = dict()

    for column in column_headers:
        data_types_dict[column] = set()

    with open(tsv_fp, 'r') as tsv_file:
        for i in range(skip_rows):
            tsv_file.readline()

        count = 0

        while True:
            row = tsv_file.readline()

            if not row:
                break

            if count % sample_interval == 0:
                row_list = row.split('\t')

                for idx, value in enumerate(row_list):
                    value = value.strip()
                    # convert non-standard null or boolean value to None, "True" or "False", otherwise return original
                    value = normalize_value(value)
                    value_type = check_value_type(value)
                    data_types_dict[column_headers[idx]].add(value_type)

            count += 1

    return data_types_dict


def resolve_type_conflicts(types_dict: dict[str, set[Any]]) -> dict[str, str]:
    """
    Iteratively resolve data type conflicts for non-nested type dicts (e.g. if there is more than one data type found,
    select the superseding type.)
    :param types_dict: dict containing columns and all detected data types
    :type types_dict: dict {str: set}
    :return dict containing the column name and its BigQuery data type.
    :rtype dict[str, str]
    """
    type_dict = dict()

    for field, types_set in types_dict.items():
        type_dict[field] = resolve_type_conflict(field, types_set)

    return type_dict


def resolve_type_conflict(field: str, types_set: Union[set[str], ColumnTypes]):
    """
    Resolve BigQuery column data type precedence, where multiple types are detected. Rules for type conversion based on
    BigQuery's implicit conversion behavior.
    See https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules#coercion
    :param field: field name
    :param types_set: Set of BigQuery data types in string format
    :return: BigQuery data type with the highest precedence
    """

    datetime_types = {"TIMESTAMP", "DATE", "TIME"}
    number_types = {"INT64", "FLOAT64", "NUMERIC"}

    # remove null type value from set
    none_set = {None}
    types_set = types_set - none_set

    # fix to make even proper INT64 ids into STRING ids
    if "_id" in field:
        return "STRING"

    if len(types_set) == 0:
        # fields with no type values default to string--this would still be safe for skip-row analysis of a data file
        return "STRING"
    if len(types_set) == 1:
        # only one data type for field, return it
        return list(types_set)[0]

    # From here, the field's types_set contains at least two values; select based on BigQuery's implicit conversion
    # rules; when in doubt, declare a string to avoid risk of data loss

    if "ARRAY" in types_set or "RECORD" in types_set:
        # these types cannot be implicitly converted to any other, exit
        logger = logging.getLogger('base_script.cda_bq_etl.data_helpers')
        logging.critical(f"Invalid datatype combination for {field}: {types_set}")
        sys.exit(-1)

    if "STRING" in types_set:
        # if it's partly classified as a string, it has to be a string--other data type values are converted
        return "STRING"

    if len(types_set) == 2 and "INT64" in types_set and "BOOL" in types_set:
        # 1 or 0 are labelled bool by type checker; any other ints are labelled as ints.
        # If both 1 and 0 occur, AND there are traditional Boolean values in the column, then it'll be declared a BOOL;
        # otherwise, it should be INT64
        return "INT64"

    has_datetime_type = False

    # are any of the data types datetime types -- {"TIMESTAMP", "DATE", "TIME"}?
    for datetime_type in datetime_types:
        if datetime_type in types_set:
            has_datetime_type = True
            break

    has_number_type = False

    # are any of the data types number types -- {"INT64", "FLOAT64", "NUMERIC"}?
    for number_type in number_types:
        if number_type in types_set:
            has_number_type = True
            break

    # What, data source?! Okay, fine, be a string
    if has_datetime_type and has_number_type:
        # another weird edge case that really shouldn't happen
        return "STRING"

    # Implicitly convert to inclusive datetime format
    if has_datetime_type:
        if "TIME" in types_set:
            # TIME cannot be implicitly converted to DATETIME
            return "STRING"
        # DATE and TIMESTAMP *can* be implicitly converted to DATETIME
        return "DATETIME"

    # Implicitly convert to inclusive number format
    if has_number_type:
        # only number types remain
        # INT64 and NUMERIC can be implicitly converted to FLOAT64
        # INT64 can be implicitly converted to NUMERIC
        if "FLOAT64" in types_set:
            return "FLOAT64"
        elif "NUMERIC" in types_set:
            return "NUMERIC"

    # No BOOL, DATETIME combinations allowed, or whatever other randomness occurs--return STRING
    return "STRING"


def is_int_value(value: Any) -> bool:
    """
    Verify whether this value is of int type.
    :param value: the value to evaluate
    :return: True if value is int type, False otherwise
    """
    def is_valid_decimal(val):
        try:
            float(val)
        except ValueError:
            return False
        except TypeError:
            return False
        else:
            return True

    def should_be_string(val):
        val = str(val)
        # todo should I make this regex?
        if val.startswith("0") and len(val) > 1 and ':' not in val and '-' not in val and '.' not in val:
            return True

    if should_be_string(value):
        return False

    if is_valid_decimal(value):
        try:
            if float(value) == int(float(value)):
                return True
        except OverflowError:
            return False

    try:
        int(value)
        return True
    except ValueError:
        return False
    except TypeError:
        return False


def normalize_value(value: Any, is_tsv: bool = False) -> Any:
    """
    - If value is string, but is NoneType-like or boolean-like, then converts to correct form (None, True, False).
    - If value is a trivial float (e.g. 100.0), converts to int. This is safe to do, because if any of the values in a
    given column end up being floats, BigQuery will re-convert the ints back into float format.
    otherwise returns original value.
    :param value: value to convert
    :param is_tsv:
    :return: normalized (or original) value
    """

    if value is None:
        return value

    if isinstance(value, str):
        value = value.strip()

        test_value = value.lower()

        if test_value in ('na', 'n/a', 'none', '', '--', '-', 'null', 'not reported', 'unknown'):
            # can't use None in the TSV files, it's interpreted as a string
            return '' if is_tsv else None
        elif test_value in ('false', 'no'):
            return "False"
        elif test_value in ('true', 'yes'):
            return "True"

    if is_int_value(value):
        try:
            cast_value = int(float(value))
            return cast_value
        except OverflowError:
            pass
    else:
        return value


def normalize_header_row(header_row: list[str]) -> list[str]:
    new_header_row = list()

    for value in header_row:
        value = value.lower()
        test_value = value
        suffix_value = 1

        # if column header is a duplicate, append numeric suffix
        while test_value in new_header_row:
            test_value = f"{value}_{str(suffix_value)}"
            suffix_value += 1

        if value != test_value:
            logger = logging.getLogger('base_script.cda_bq_etl.data_helpers')
            logger.warning(f"Changing header value {value} to {test_value} (due to encountering duplicate header).")

        new_header_row.append(test_value)

    return new_header_row


def create_normalized_tsv(raw_tsv_fp: str, normalized_tsv_fp: str):
    """
    Opens a raw tsv file, normalizes its data, then writes to new tsv file.
    :param raw_tsv_fp: path to non-normalized data file
    :param normalized_tsv_fp: destination file for normalized data
    """

    logger = logging.getLogger('base_script.cda_bq_etl.data_helpers')

    with open(normalized_tsv_fp, mode="w", newline="") as normalized_tsv_file:
        tsv_writer = csv.writer(normalized_tsv_file, delimiter="\t")

        with open(raw_tsv_fp, mode="r", newline="") as tsv_file:
            tsv_reader = csv.reader(tsv_file, delimiter="\t")

            raw_row_count = 0

            for row in tsv_reader:
                normalized_record = list()

                if raw_row_count == 0:
                    header_row = normalize_header_row(row)
                    tsv_writer.writerow(header_row)
                    raw_row_count += 1
                    continue

                for value in row:
                    new_value = normalize_value(value, is_tsv=True)
                    normalized_record.append(new_value)

                tsv_writer.writerow(normalized_record)
                raw_row_count += 1
                if raw_row_count % 500000 == 0:
                    logger.info(f"Normalized {raw_row_count} rows.")

            logger.info(f"Normalized {raw_row_count} rows.")

    with open(normalized_tsv_fp, mode="r", newline="") as normalized_tsv_file:
        tsv_reader = csv.reader(normalized_tsv_file, delimiter="\t")

        normalized_row_count = sum(1 for _ in tsv_reader)

    if normalized_row_count != raw_row_count:
        logger.critical(f"ERROR: Row count changed. Original: {raw_row_count}; Normalized: {normalized_row_count}")
        sys.exit()


def normalize_flat_json_values(records: JSONList) -> JSONList:
    normalized_json_list = list()

    for record in records:
        normalized_record = dict()
        for key in record.keys():
            value = normalize_value(record[key])
            normalized_record[key] = value
        normalized_json_list.append(normalized_record)

    return normalized_json_list


# todo this works, but could it be tightened up?
def check_value_type(value: Any):
    """
    Check value for corresponding BigQuery type. Evaluates the following BigQuery column data types:
        - datetime formats: DATE, TIME, TIMESTAMP
        - number formats: INT64, FLOAT64, NUMERIC
        - misc formats: STRING, BOOL, ARRAY, RECORD
    :param value: value on which to perform data type analysis
    :return: data type in BigQuery Standard SQL format
    """
    def is_valid_decimal(val):
        try:
            float(val)
        except ValueError:
            return False
        except TypeError:
            return False
        else:
            return True

    if isinstance(value, bool):
        return "BOOL"
    # currently not working for tsv because we don't normalize those files prior to upload yet
    if is_valid_decimal(value):
        # If you don't cast a string to float before casting to int, it will throw a TypeError
        try:
            str_val = str(value)

            if str_val.startswith("0") and len(str_val) > 1 and ':' not in str_val \
                    and '-' not in str_val and '.' not in str_val:
                return "STRING"

            if float(value) == int(float(value)):
                return "INT64"
        except OverflowError:
            # can't cast float infinity to int
            pass
    if isinstance(value, float):
        return "FLOAT64"
    if value != value:  # NaN case
        return "FLOAT64"
    if isinstance(value, list):
        return "ARRAY"
    if isinstance(value, dict):
        return "RECORD"
    if not value:
        return None
    if isinstance(value, datetime.datetime):
        return "TIMESTAMP"
    if isinstance(value, datetime.date):
        return "DATE"
    if isinstance(value, datetime.time):
        return "TIME"

    # A sequence of numbers starting with a 0 represents a string id,
    # but int() check will pass and data loss would occur.
    if isinstance(value, str):
        if value.startswith("0") and len(value) > 1 and ':' not in value and '-' not in value and '.' not in value:
            return "STRING"

    # check to see if value is numeric, float or int;
    # differentiates between these types and datetime or ids, which may be composed of only numbers or symbols
    if '.' in value and ':' not in value and "E+" not in value and "E-" not in value:
        try:
            int(value)
            return "INT64"
        except ValueError:
            try:
                float(value)
                decimal_val = int(value.split('.')[1])

                # if digits right of decimal place are all zero, float can safely be cast as an int
                if not decimal_val:
                    return "INT64"
                return "FLOAT64"
            except ValueError:
                return "STRING"

    # numeric values are numbers with special encoding, like an exponent or sqrt symbol
    elif value.isnumeric() and not value.isdigit() and not value.isdecimal():
        return "NUMERIC"

    # no point in performing regex for this, it's just a string
    if value.count("-") > 2:
        return "STRING"

    """
    BIGQUERY'S CANONICAL DATE/TIME FORMATS:
    (see https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)
    """

    if value.count("-") == 2 or value.count(":") == 2:
        # Check for BigQuery DATE format: 'YYYY-[M]M-[D]D'
        date_re_str = r"[0-9]{4}-(0[1-9]|1[0-2]|[0-9])-(0[1-9]|[1-2][0-9]|[3][0-1]|[1-9])"
        date_pattern = re.compile(date_re_str)
        if re.fullmatch(date_pattern, value):
            return "DATE"

        # Check for BigQuery TIME format: [H]H:[M]M:[S]S[.DDDDDD]
        time_re_str = r"([0-1][0-9]|[2][0-3]|[0-9]{1}):([0-5][0-9]|[0-9]{1}):([0-5][0-9]|[0-9]{1}])(\.[0-9]{1,6}|)"
        time_pattern = re.compile(time_re_str)
        if re.fullmatch(time_pattern, value):
            return "TIME"

        # Check for BigQuery TIMESTAMP format: YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]][time zone]
        timestamp_re_str = date_re_str + r'( |T)' + time_re_str + r"([ \-:A-Za-z0-9]*)"
        timestamp_pattern = re.compile(timestamp_re_str)
        if re.fullmatch(timestamp_pattern, value):
            return "TIMESTAMP"

        return "STRING"

    try:
        util.strtobool(value)
        return "BOOL"
    except ValueError:
        pass

    # Final check for int and float values.
    # This will catch simple integers or edge case float values (infinity, scientific notation, etc.)
    try:
        int(value)
        return "INT64"
    except ValueError:
        try:
            float(value)
            return "FLOAT64"
        except ValueError:
            return "STRING"


def json_datetime_to_str_converter(datetime_obj: datetime) -> str:
    """
    Convert python datetime object to string (necessary for json serialization).
    :param datetime_obj: python datetime object
    :return: datetime cast as string
    """
    if isinstance(datetime_obj, datetime.datetime):
        return str(datetime_obj)
    if isinstance(datetime_obj, datetime.date):
        return str(datetime_obj)
    if isinstance(datetime_obj, datetime.time):
        return str(datetime_obj)


def initialize_logging(log_filepath: str) -> logging.Logger:
    # initialize Logger object
    logger = logging.getLogger(name='base_script')
    logger.setLevel(logging.DEBUG)

    # emit logger output to a file
    log_file_handler = logging.FileHandler(log_filepath)
    log_file_handler.setLevel(logging.DEBUG)

    # emit logger output to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # format log output: levelname is the severity level, e.g. INFO, WARNING
    # name is the location from which the message was emitted; lineno is the line
    formatter = logging.Formatter('[%(levelname)s][%(name)s:%(lineno)s] %(message)s')
    log_file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(log_file_handler)
    logger.addHandler(console_handler)

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    logger.info(f"Logging started: {start_time}")

    return logger

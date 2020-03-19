import io
import yaml
import sys
import pprint


def load_config(yaml_file, yaml_dict_keys):
    """
    Opens yaml file and retrieves configuration parameters.
    :param yaml_file: yaml config file name
    :param yaml_dict_keys: tuple of strings representing a subset of the yaml file's top-level dictionary keys.
    file
    :return: tuple of dicts from yaml file (as requested in yaml_dict_keys)
    """
    yaml_dict = None

    with open(yaml_file, mode='r') as yaml_file:
        config_stream = io.StringIO(yaml_file.read())

        try:
            yaml_dict = yaml.load(config_stream, Loader=yaml.FullLoader)
        except yaml.YAMLError as ex:
            print(ex)

        if yaml_dict is None:
            sys.exit("Bad YAML load, exiting.")

        # Dynamically generate a list of dictionaries for the return statement, since tuples are immutable
        return_dicts = [yaml_dict[key] for key in yaml_dict_keys]

        return tuple(return_dicts)


def check_value_type(value):
    # if has leading zero, then should be considered a string, even if only composed of digits
    val_is_none = value == '' or value == 'NA' or value == 'null' or value is None or value == 'None'
    val_is_decimal = value.startswith('0.')
    val_is_id = value.startswith('0') and not val_is_decimal and len(value) > 1

    try:
        float(value)
        val_is_num = True
    except ValueError:
        val_is_num = False
        val_is_float = False

    if val_is_num:
        val_is_float = True if int(float(value)) != float(value) else False

    if val_is_none:
        return None
    elif val_is_id:
        return 'string'
    elif val_is_decimal or val_is_float:
        return 'float'
    elif val_is_num:
        return 'integer'
    else:
        return 'string'

    print("ERROR NOT FINDING TYPE, value: {}".format(value))


def infer_data_types(flattened_json):
    data_types = dict()
    for column in flattened_json:
        data_types[column] = None

        for value in flattened_json[column]:
            if data_types[column] == 'string':
                break

            val_type = check_value_type(str(value))

            if not val_type:
                continue
            elif val_type == 'float' or val_type == 'string':
                data_types[column] = val_type
            elif val_type == 'integer':
                if not data_types[column]:
                    data_types[column] = 'integer'
            else:
                print("[ERROR] NO TYPE SET FOR val {}, type {}".format(value, val_type))

    return data_types

def pprint_json(data):
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(data)
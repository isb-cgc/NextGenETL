import io
import yaml
import sys


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

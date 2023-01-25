"""
Config Parser function to use throughout the project.
"""
import json


def parse_config_file(config_file: str) -> dict:
    """
    Parses a given JSON file.
    :param config_file: For the file to parse.
    :return: The parsed JSON file as a dictionary.
    """
    with open(config_file, 'r') as cf:
        return json.load(cf)


def get_configuration(line: str, config_type: str = None):
    """
    Gets the wanted configuration according to a given type.
    :param line: For the configuration line.
    :param config_type: For the config type to parse, None as default.
    :return: THe configuration line.
    """
    data = None
    try:
        data = parse_config_file(r"config.json")
    except (FileNotFoundError, ValueError) as err:
        print(f"[!] Unable to parse config file, Error: {err}")

    if config_type == "logger":
        return data[config_type][line]
    else:
        return data[line]



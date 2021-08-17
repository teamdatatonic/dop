import json

import yaml

# Use LibYAML bindings if installed, much faster than the pure Python version
# See https://pyyaml.org/wiki/PyYAMLDocumentation for details
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader  # type: ignore


def dict_to_yaml(dct: dict) -> str:
    """
    Convert a dictionary to a YAML serialised string

    :param dct: Dictionary to be converted
    :return: serialised dictionary in YAML format
    """
    yml: dict = yaml.load(json.dumps(dct), Loader=Loader)
    return yaml.dump(yml)


def yaml_to_dict(yml: str) -> dict:
    """
    Convert a YAML serialised string to a dictionary

    Using unsafe_load to allow complex python types in YAML.
    For example in an airflow sensor operator we can define as param
    execution_date: !!python/object/apply:datetime.timedelta [1]

    As we are defining the yaml and it cannot be modified by third parties,
    there is no risk of injection and execution of arbitrary code

    :param yml: YAML serialised string to be converted
    :return: Converted dictionary
    """
    return yaml.unsafe_load(yml)

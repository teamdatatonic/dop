import json

import yaml

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper  # noqa: F401


def dict_to_yaml(d):
    yml = yaml.load(json.dumps(d), Loader=Loader)
    return yaml.dump(yml)


def yaml_to_dict(y):
    yml = yaml.load(y, Loader=Loader)
    return yml

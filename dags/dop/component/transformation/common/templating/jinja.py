import os
import logging

from jinja2 import FileSystemLoader, Environment
from dop import definitions


def log(msg, info=False):
    if info:
        logging.info(msg)
    else:
        logging.debug(msg)
    return ""


def raise_error(error):
    raise RuntimeError(error)


class RunnerEnvironment:
    def __init__(self, runner):
        if runner not in definitions.SUPPORTED_TRANSFORMATION_RUNNERS:
            raise NotImplementedError(
                f'Transformation Runner "{runner}" is not supported'
            )

        self._runner = runner

    def _get_runner_template_paths(self):
        common_template_path = os.path.join(
            definitions.ROOT_DIR,
            "component",
            "transformation",
            "common",
            "templating",
            "template",
        )

        runner_base_path = os.path.join(
            definitions.ROOT_DIR,
            "component",
            "transformation",
            "runner",
            self._runner,
            "template",
            "macro",
        )

        if not os.path.isdir(runner_base_path):
            raise RuntimeError(f"Path `{runner_base_path}` does not exist")

        if not os.path.isdir(common_template_path):
            raise RuntimeError(f"Path `{common_template_path}` does not exist")

        return [common_template_path, runner_base_path]

    def get_env(self):
        template_loader = FileSystemLoader(self._get_runner_template_paths())
        runner_env = Environment(loader=template_loader, extensions=["jinja2.ext.do"])

        runner_env.globals["log"] = log
        runner_env.globals["raise"] = raise_error

        return runner_env


def get_runner_environment(runner):
    return RunnerEnvironment(runner=runner).get_env()

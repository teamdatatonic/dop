import logging
import os

from typing import List, Dict

from airflow.operators.bash_operator import BashOperator
from airflow.sensors.base_sensor_operator import apply_defaults
from dop.component.configuration.env import env_config
from dop.airflow_module.operator import dbt_operator_helper


class DbtOperator(BashOperator):
    template_fields = (
        "action",
        "target",
        "dbt_project_path",
        "dbt_version",
        "dbt_arguments",
        "bash_command",
    )
    ui_color = "#FF694B"

    @apply_defaults
    def __init__(
        self,
        dbt_project_name: str,
        dbt_version: str,
        dbt_arguments: List[Dict],
        *args,
        **kwargs,
    ):
        """
        :param dbt_project_name: the name for the dbt project name inline with what's defined in `.dbt-project-repos.json`
        :param dbt_version: a supported DBT version, version must be >= 0.18.1
        :param args:
        :param kwargs: must contain the Task entity
        """

        task = kwargs["task"]
        self.dbt_project_path = os.path.sep.join(
            [env_config.dbt_projects_path, dbt_project_name]
        )
        self.dbt_project_name = dbt_project_name
        self.dbt_version = dbt_version
        self.action = task.kind.action
        self.target = task.kind.target
        self.dbt_arguments = dbt_arguments

        self._full_refresh = (
            False  # used to trigger DBT full refresh, modified via execute() override
        )

        super(DbtOperator, self).__init__(
            bash_command=self.parse_bash_command(), *args, **kwargs
        )

    def execute(self, context):
        """
        Override the parent method to ingest required contexts
        """
        dag_run_conf = context["dag_run"].conf if context["dag_run"].conf else {}
        full_refresh = dag_run_conf.get("full_refresh", False)

        self._full_refresh = full_refresh

        logging.info(f"### IS FULL REFRESH ENABLED: {self._full_refresh}")

        self.bash_command = self.parse_bash_command(context=context)
        super(DbtOperator, self).execute(context=context)

    def parse_bash_command(self, context=None):
        """
        Create a virtualenv and run DBT. Virtualenv is removed regardless if the script is successful or not
        """

        full_refresh_cmd = ""
        if self.target != "run":
            full_refresh_cmd = ""
        elif self.dbt_arguments:
            if self._full_refresh and "--full-refresh" not in [
                arg.get("option") for arg in self.dbt_arguments
            ]:
                full_refresh_cmd = "--full-refresh"
        elif self._full_refresh:
            full_refresh_cmd = "--full-refresh"

        set_err_handling = "set -xe"
        trap = """
        trap 'catch $? $LINENO' ERR
        catch() {
          echo "Script errored, removing virtualenv"
          rm -rf $TMP_DIR
          exit 1
        }
        """
        cmd_for_tmp_dir = "export TMP_DIR=$(mktemp -d)"
        cmd_to_print_tmp_dir = "echo TMP_DIR is: $TMP_DIR"
        cmd_for_virtualenv = "virtualenv -p python3 $TMP_DIR"
        dbt_init = f"PYTHONPATH={env_config.dag_path} python {env_config.dag_path}/dop/component/helper/dbt_init.py --tmp_dir=$TMP_DIR --project_name={self.dbt_project_name}"
        cmd_for_activating_virtualenv = "source $TMP_DIR/bin/activate"
        install_pip_deps = f"pip install dbt=={self.dbt_version}"

        cmd_for_additional_arguments = ""

        if self.dbt_arguments:
            cmd_for_additional_arguments = dbt_operator_helper.implode_arguments(
                dbt_arguments=self.dbt_arguments
            )

        cmd_to_run_dbt = (
            f"dbt clean --project-dir {self.dbt_project_path} --profiles-dir $TMP_DIR/.dbt"
            f" && dbt deps --project-dir {self.dbt_project_path}"
            f" && dbt --no-use-colors {self.target} --project-dir {self.dbt_project_path}"
            f" --profiles-dir $TMP_DIR/.dbt"
            f" --vars {dbt_operator_helper.parsed_cmd_airflow_context_vars(context=context)}"
            f" {cmd_for_additional_arguments}"
            f" {full_refresh_cmd}"
        )

        cmd_to_remove_tmp_dir = "rm -rf $TMP_DIR"

        return "\n".join(
            [
                set_err_handling,
                trap,
                cmd_for_tmp_dir,
                cmd_to_print_tmp_dir,
                cmd_for_virtualenv,
                dbt_init,  # setup dbt profiles.yml & service account secret from Secret Manager
                cmd_for_activating_virtualenv,
                install_pip_deps,
                cmd_to_run_dbt,
                cmd_to_remove_tmp_dir,
            ]
        )

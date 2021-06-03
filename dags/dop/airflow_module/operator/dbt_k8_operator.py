import logging
import os

from typing import List, Dict

from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.sensors.base_sensor_operator import apply_defaults
from dop.component.configuration.env import env_config
from dop.airflow_module.operator import dbt_operator_helper

# See: https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity
node_pool_affinity = {
    "nodeAffinity": {
        # requiredDuringSchedulingIgnoredDuringExecution means in order
        # for a pod to be scheduled on a node, the node must have the
        # specified labels. However, if labels on a node change at
        # runtime such that the affinity rules on a pod are no longer
        # met, the pod will still continue to run on the node.
        "requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [
                {
                    "matchExpressions": [
                        {
                            # When nodepools are created in Google Kubernetes
                            # Engine, the nodes inside of that nodepool are
                            # automatically assigned the label
                            # 'cloud.google.com/gke-nodepool' with the value of
                            # the nodepool's name.
                            "key": "cloud.google.com/gke-nodepool",
                            "operator": "In",
                            "values": ["kubernetes-task-pool"],
                        }
                    ]
                }
            ]
        }
    }
}


def retrieve_commit_hash():
    with open(
        os.path.sep.join([env_config.service_project_path, ".commit-hash"])
    ) as fp:
        return fp.read()


class DbtK8Operator(KubernetesPodOperator):
    template_fields = (
        "action",
        "target",
        "dbt_project_name",
        "image_tag",
        "dbt_arguments",
        "gcr_pull_secret_name",
        "arguments",
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
        :param dbt_version: Not used
        :param args:
        :param kwargs: must contain the Task entity
        """

        task = kwargs["task"]
        self.dbt_project_name = dbt_project_name
        self.dbt_version = "N/A, this is fixed in the docker image"
        self.action = task.kind.action
        self.target = task.kind.target
        self.dbt_arguments = dbt_arguments
        self.gcr_pull_secret_name = env_config.gcr_pull_secret_name
        self.image_tag = retrieve_commit_hash()

        self._full_refresh = (
            False  # used to trigger DBT full refresh, modified via execute() override
        )

        self.arguments = [self.parse_bash_command()]

        super(DbtK8Operator, self).__init__(
            name=kwargs["task_id"],
            cmds=["/bin/bash", "-c"],
            arguments=self.arguments,
            get_logs=True,
            namespace="default",
            image=f"eu.gcr.io/{env_config.infra_project_id}/dop-dbt:{self.image_tag}",
            is_delete_operator_pod=True,
            env_vars={
                "DOP_PROJECT_ID": env_config.project_id,
                "DOP_LOCATION": env_config.location,
            },
            image_pull_secrets=self.gcr_pull_secret_name,
            affinity=node_pool_affinity,
            *args,
            **kwargs,
        )

    def execute(self, context):
        """
        Override the parent method to ingest required contexts
        """
        dag_run_conf = context["dag_run"].conf if context["dag_run"].conf else {}
        full_refresh = dag_run_conf.get("full_refresh", False)

        self._full_refresh = full_refresh

        logging.info(f"### IS FULL REFRESH ENABLED: {self._full_refresh}")

        self.arguments = [self.parse_bash_command(context=context)]

        logging.info(f"### Updated arguments: {self.arguments}")

        super(DbtK8Operator, self).execute(context=context)

    def parse_bash_command(self, context=None):
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

        cmd_for_additional_arguments = ""

        if self.dbt_arguments:
            cmd_for_additional_arguments = dbt_operator_helper.implode_arguments(
                dbt_arguments=self.dbt_arguments
            )

        cmd_to_run_dbt = (
            f"pipenv run dbt --no-use-colors {self.target} --project-dir ./{self.dbt_project_name}"
            f" --vars {dbt_operator_helper.parsed_cmd_airflow_context_vars(context=context)}"
            f" {cmd_for_additional_arguments}"
            f" {full_refresh_cmd}"
        )

        return cmd_to_run_dbt

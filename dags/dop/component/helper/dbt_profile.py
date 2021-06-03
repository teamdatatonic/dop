import os
import logging

from dop.component.transformation.common.parser.yaml_parser import (
    yaml_to_dict,
    dict_to_yaml,
)
from dop.component.configuration.env import env_config
from dop.component.configuration import env


def setup_profiles(project_name):
    project_id = env_config.project_id
    location = env_config.location
    # the profile is generated dynamically at runtime, therefore multiple target profiles are not required
    target = "all"
    target_type = "bigquery"
    path_to_dbt_project_yml = os.path.sep.join(
        [env_config.dbt_projects_path, project_name, "dbt_project.yml"]
    )

    bq_profile = {}

    if not os.path.isfile(path_to_dbt_project_yml):
        raise RuntimeError(
            f"Profile yaml `{path_to_dbt_project_yml}` does not exist, "
            f"was the DBT option `project` in the orchestration config set correctly?"
        )

    with open(path_to_dbt_project_yml) as fp:
        profile_id = yaml_to_dict(fp.read())["profile"]
        bq_profile[profile_id] = {
            "target": target,
            "outputs": {
                target: {
                    "type": target_type,
                    "method": "oauth",
                    "project": project_id,
                    "schema": str(project_name).replace("-", "_"),
                    # TODO: Is this the right default, should `schema` be passed in via an environment variable?
                    "threads": 1,
                    "timeout_seconds": 300,
                    "location": location,
                    "priority": "interactive",
                    "impersonate_service_account": f"{env.DOP_DBT_USER}@{project_id}.iam.gserviceaccount.com",
                }
            },
        }

    profile = dict_to_yaml(bq_profile)
    logging.info(f"DBT Profile: {profile}")

    return profile


def setup_and_save_profiles(project_name, profile_path):
    profile_yml = setup_profiles(project_name=project_name)
    path_to_profile_yml = os.path.sep.join([profile_path, ".dbt", "profiles.yml"])
    logging.info(f"Updating: {path_to_profile_yml}")
    with open(path_to_profile_yml, "w+") as fp:
        fp.write(profile_yml)

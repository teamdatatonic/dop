import os
import yaml
import json
import shutil
import subprocess

DOP_DBT_USER = 'dop-dbt-user'

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


def copy_and_overwrite(from_path, to_path):
    if os.path.exists(to_path):
        shutil.rmtree(to_path)
    shutil.copytree(from_path, to_path)


def yaml_to_dict(y):
    yml = yaml.load(y, Loader=Loader)
    return yml


def dict_to_yaml(d):
    yml = yaml.load(json.dumps(d), Loader=Loader)
    return yaml.dump(yml)


def build_profile_file_content(profile_ids):
    # the profile is generated dynamically at runtime, therefore multiple target profiles are not required
    target = 'all'
    target_type = 'bigquery'

    bq_profile = {}

    for profile_id in profile_ids:
        bq_profile[profile_id] = {
            'target': target,
            'outputs': {
                target: {
                    'type': target_type,
                    'method': 'oauth',
                    'project': '{{ env_var("DOP_PROJECT_ID") }}',
                    'schema': '{{ env_var("DOP_DBT_SCHEMA", "' + str(profile_id).replace('-', '_') + '") }}',
                    'threads': 1,
                    'timeout_seconds': 300,
                    'location': '{{ env_var("DOP_LOCATION") }}',
                    'priority': 'interactive',
                    'impersonate_service_account': f'{DOP_DBT_USER}'
                                                   + '@{{ env_var("DOP_PROJECT_ID") }}.iam.gserviceaccount.com'
                }
            }
        }

    profile = dict_to_yaml(bq_profile)

    return profile


def save_profile_yml(dbt_home, file_content):
    with open(os.path.sep.join([dbt_home, '.dbt', 'profiles.yml']), 'w+') as fp:
        fp.write(file_content)


dbt_home = os.environ['DBT_HOME']
build_dir = os.environ['BUILD_DIR']

print(f'DBT_HOME: {dbt_home}')
print(f'BUILD_DIR: {build_dir}')

dop_config_path = os.path.sep.join([build_dir, 'embedded_dop', 'executor_config', 'dbt', 'config.yaml'])

with open(dop_config_path) as fp_config:
    dop_config = yaml_to_dict(fp_config.read())
    # validation
    if not dop_config.get('dbt_projects'):
        raise RuntimeError('The `dbt_projects` section must be defined')

    dbt_configs = dop_config.get('dbt_projects')
    profile_ids = []
    dbt_projects_path = []
    for dbt_config in dbt_configs:
        # validation
        if not dbt_config.get('project_path'):
            raise RuntimeError('`project_path` must be defined for DBT')

        project_path = dbt_config.get('project_path')

        project_yml_path = os.path.sep.join([build_dir, project_path, 'dbt_project.yml'])
        with open(project_yml_path) as fp_yml:
            profile_ids.append(yaml_to_dict(fp_yml.read()).get('profile'))

        # copy dbt projects to the dbt home location
        to_path = os.path.sep.join([dbt_home, project_path])
        copy_and_overwrite(
            from_path=os.path.sep.join([build_dir, project_path]),
            to_path=to_path
        )

        dbt_projects_path.append(to_path)

    # create the profiles yml file for all dbt projects
    if profile_ids:
        file_content = build_profile_file_content(
            profile_ids=profile_ids
        )

        print(f'profiles.yml: \n{file_content}')

        save_profile_yml(dbt_home=dbt_home, file_content=file_content)

    for dbt_project_path in dbt_projects_path:
        for dbt_cmd in ['clean', 'deps']:
            proc = subprocess.Popen(['pipenv', 'run', 'dbt', dbt_cmd], stdout=subprocess.PIPE, cwd=dbt_project_path)
            while True:
                line = proc.stdout.readline()
                if not line:
                    break
                print(line.rstrip())

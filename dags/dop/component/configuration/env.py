import os

DOP_DBT_USER = 'dop-dbt-user'
DOP_DOCKER_USER = 'dop-docker-user'


class EnvConfig:
    def __init__(self):
        pass

    @property
    def environment(self):
        return os.environ['DOP_ENVIRONMENT']

    @property
    def project_id(self):
        return os.environ['DOP_PROJECT_ID']

    @property
    def dag_path(self):
        return os.path.sep.join([self.service_project_path, 'embedded_dop', 'source', 'dags'])

    @property
    def location(self):
        return os.environ['DOP_LOCATION']

    @property
    def orchestration_path(self):
        return os.path.sep.join([self.service_project_path, 'embedded_dop', 'orchestration'])

    @property
    def is_sandbox_environment(self):
        return bool(os.environ.get('DOP_SANDBOX_ENVIRONMENT', False))

    @property
    def service_project_path(self):
        return os.environ['DOP_SERVICE_PROJECT_PATH']

    @property
    def infra_project_id(self):
        return os.environ['DOP_INFRA_PROJECT_ID']

    @property
    def gcr_pull_secret_name(self):
        return os.environ.get('DOP_GCR_PULL_SECRET_NAME', None)

    @property
    def dbt_projects_path(self):
        """
        An Alias of service_project_path because it is also the DBT project paths by convention.
        This may however be changed to have its own environment variable in the future if it is required to
        differentiate the DBT projects setup from the service project path itself
        :return:
        """
        return self.service_project_path


env_config = EnvConfig()

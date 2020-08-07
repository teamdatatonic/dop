from google.auth import impersonated_credentials, default


class ServiceAccountImpersonationCredentialManager:
    target_scopes = ['https://www.googleapis.com/auth/cloud-platform']

    def __init__(self, source_sa_name, project_id):
        self._source_sa_name = source_sa_name
        self._project_id = project_id

    def get_target_credentials(self):
        impersonated_sa = f'{self._source_sa_name}@{self._project_id}.iam.gserviceaccount.com'
        source_credentials, _ = default()

        target_credentials = impersonated_credentials.Credentials(
            source_credentials=source_credentials,
            target_principal=impersonated_sa,
            target_scopes=self.target_scopes,
            delegates=[],
            lifetime=500
        )

        return target_credentials

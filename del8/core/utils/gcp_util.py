"""TODO: Add title."""
import os
from google.cloud import storage as gcp_storage
from google.oauth2 import service_account


def connect_to_bucket(bucket, private_key_filepath):
    private_key_filepath = os.path.expanduser(private_key_filepath)

    credentials = service_account.Credentials.from_service_account_file(
        private_key_filepath,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = gcp_storage.Client(credentials=credentials, project=credentials.project_id)
    return client.get_bucket(bucket)

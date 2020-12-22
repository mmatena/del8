"""TODO: Add title."""
import contextlib
import json
import os
import tempfile

from google.cloud import storage as gcp_storage
from google.oauth2 import service_account
import psycopg2

from del8.core import data_class
from del8.core import serialization
from del8.core.di import executable
from del8.core.storage import storage

SerializationType = serialization.SerializationType


# GROUPS_TABLE = "Groups"
ITEMS_TABLE = "Items"
BLOBS_TABLE = "Blobs"


@data_class.data_class()
class GcpStorageParams(storage.StorageParams):
    # The defaults are for prod and a vast executor.
    def __init__(
        self,
        # Where all the stuff is organized.
        client_directory="~/del8/gcp/vast",
        # Stuff for Cloud SQL.
        server_ca_pem="server-ca.pem",
        client_cert_pem="client-cert.pem",
        client_key_pem="client-key.pem",
        config_file="config.json",
        # Stuff for Cloud Storage.
        private_key_file="storage-private-key.json",
        bucket_name="del8_blobs",
    ):
        pass

    def instantiate_storage(self):
        return GcpStorage.from_params(self)

    def get_storage_cls(self):
        return GcpStorage

    @property
    def _directory(self):
        # NOTE: `os.path.expanduser` returns different results depending
        # on which system we are evaluating on. Thus, we have to expand
        # it lazily and cannot do it in __init__.
        return os.path.expanduser(self.client_directory)

    def get_server_ca_pem(self):
        return os.path.join(self._directory, self.server_ca_pem)

    def get_client_cert_pem(self):
        return os.path.join(self._directory, self.client_cert_pem)

    def get_client_key_pem(self):
        return os.path.join(self._directory, self.client_key_pem)

    def get_config_file(self):
        return os.path.join(self._directory, self.config_file)

    def get_private_key_file(self):
        return os.path.join(self._directory, self.private_key_file)


@executable.executable(
    pip_packages=[
        "google-auth",
        "google-cloud-storage",
        "psycopg2-binary",
    ],
)
class GcpStorage(storage.Storage):
    # NOTE: Going to have to do a little refactor here and probably in the general storage interface.
    def __init__(
        self,
        gcp_params,
        # NOTE: You won't be able to use all of the methods in this class if you don't provide
        # these arguments.
        group=None,
        experiment=None,
        run_uuid=None,
    ):
        # Need to set these probably through some context manager.
        self._group = group
        self._experiment = experiment
        self._run_uuid = run_uuid
        self._gcp_params = self.params_by_environment_mode(gcp_params)

        self._conn = None
        self._bucket = None

    def call(self):
        # NOTE: This is kind of a hack that comes from making this @executable.
        return self

    def params_by_environment_mode(
        self,
        prod_params,
        environment_mode="prod",
        dev_client_directory="~/del8/dev/gcp/vast",
        dev_bucket_name="dev_del8_blobs",
    ):
        if environment_mode == "prod":
            return prod_params
        elif environment_mode == "dev":
            return prod_params.copy(
                # Be sure that entries in the dev directory point to the
                # dev resources.
                client_directory=dev_client_directory,
                bucket_name=dev_bucket_name,
            )
        else:
            raise ValueError(f"Unrecognized environment mode: {environment_mode}.")

    #################

    @classmethod
    def from_params(cls, gcp_params):
        return GcpStorage(gcp_params)

    #################

    def _commit(self):
        self._conn.commit()

    @contextlib.contextmanager
    def _cursor(self):
        # To be used as `with self._cursor() as c: ...`
        cursor = self._conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
            self._commit()

    #################

    @property
    def private_key_file(self):
        return self._gcp_params.get_private_key_file()

    def _initialize_cloud_sql(self):
        server_ca_pem = self._gcp_params.get_server_ca_pem()
        client_cert_pem = self._gcp_params.get_client_cert_pem()
        client_key_pem = self._gcp_params.get_client_key_pem()

        os.chmod(server_ca_pem, 0o600)
        os.chmod(client_cert_pem, 0o600)
        os.chmod(client_key_pem, 0o600)

        config_file = self._gcp_params.get_config_file()
        with open(config_file, "r") as f:
            config = json.load(f)

        hostaddr = config["hostaddr"]
        port = config["port"]
        user = config["user"]
        password = config["password"]
        dbname = config["dbname"]

        # NOTE: No escaping going on here. Make sure your file
        # paths are nice.
        args = [
            "sslmode=verify-ca",
            f"sslrootcert={server_ca_pem}",
            f"sslcert={client_cert_pem}",
            f"sslkey={client_key_pem}",
            f"hostaddr={hostaddr}",
            f"port={port}",
            f"user={user}",
            f"password={password}",
            f"dbname={dbname}",
        ]
        args = " ".join(args)
        return psycopg2.connect(args)

    def _initialize_cloud_storage(self):
        key_path = self._gcp_params.get_private_key_file()
        credentials = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        client = gcp_storage.Client(
            credentials=credentials, project=credentials.project_id
        )
        return client.get_bucket(self._gcp_params.bucket_name)

    def initialize(self):
        self._conn = self._initialize_cloud_sql()
        self._bucket = self._initialize_cloud_storage()

    def close(self):
        if self._conn:
            self._conn.close()

    #################

    @property
    def run_uuid(self):
        return self._run_uuid

    @property
    def experiment_uuid(self):
        return self._experiment.uuid

    @property
    def group_uuid(self):
        return self._group.uuid

    #################

    def store_item(self, item):
        item_uuid = self.new_uuid()
        ser_item = serialization.serialize(item)
        with self._cursor() as c:
            c.execute(
                f"INSERT INTO {ITEMS_TABLE} VALUES (%s, %s, %s, %s, %s)",
                (
                    item_uuid,
                    self.group_uuid,
                    self.experiment_uuid,
                    self.run_uuid,
                    ser_item,
                ),
            )
        return item_uuid

    def replace_item(self, item_uuid, new_item):
        assert item_uuid, "Update needs a non-empty item_uuid."
        ser_item = serialization.serialize(new_item)
        with self._cursor() as c:
            c.execute(
                f"UPDATE {ITEMS_TABLE} SET data = %s WHERE uuid = %s",
                (ser_item, item_uuid),
            )
        return item_uuid

    def retrieve_item(self, uuid):
        with self._cursor() as c:
            c.execute(f"SELECT data FROM {ITEMS_TABLE} WHERE uuid=%s", (uuid,))
            row = c.fetchone()
            if not row:
                raise ValueError(f"Experiment run with uuid {uuid} not found.")
            (data,) = row
            return serialization.deserialize(data)

    #################

    def retrieve_items_by_class(
        self,
        item_cls,
        group_uuid=None,
        experiment_uuid=None,
        run_uuid=None,
    ):
        # All of the uuids are optional are should restrict the returns
        # to only items associated with the respective group/experiment/run.
        cls_name = item_cls.__name__
        cls_module = item_cls.__module__

        json_value = {
            "name": cls_name,
            "module": cls_module,
            "__type__": SerializationType.CLASS_OBJECT,
        }
        json_value = {"__class__": json_value}
        json_value = json.dumps(json_value)

        query = [f"SELECT data FROM {ITEMS_TABLE} WHERE data @> %s"]
        bindings = [json_value]
        # NOTE: There is probably someway to automate this query construction process.
        if group_uuid:
            query.append("AND group_uuid=%s")
            bindings.append(group_uuid)
        if experiment_uuid:
            query.append("AND experiment_uuid=%s")
            bindings.append(experiment_uuid)
        if run_uuid:
            query.append("AND run_uuid=%s")
            bindings.append(run_uuid)
        query = " ".join(query)
        bindings = tuple(bindings)

        with self._cursor() as c:
            c.execute(query, bindings)
            rows = c.fetchall()

        items = [serialization.deserialize(row[0]) for row in rows]
        return items

    #################

    def store_model_weights(self, model):
        """Returns UUID."""
        blob_uuid = self.new_uuid()
        extension = "h5"
        gcp_storage_object_name = f"{blob_uuid}.{extension}"

        # NOTE: We write to a temporary local file and then upload to
        # Cloud Storage. It might also be possible to directly save to
        # Cloud Storage. I'm not sure of the advantages and disadvantages
        # each of the methods.
        with tempfile.NamedTemporaryFile(suffix=f".{extension}") as f:
            model.save_weights(f.name)
            blob = self._bucket.blob(gcp_storage_object_name)
            blob.upload_from_filename(f.name)

        with self._cursor() as c:
            c.execute(
                f"INSERT INTO {BLOBS_TABLE} VALUES (%s, %s, %s, %s, %s)",
                (
                    blob_uuid,
                    self.group_uuid,
                    self.experiment_uuid,
                    self.run_uuid,
                    gcp_storage_object_name,
                ),
            )
        # TODO: Maybe delete the GCP storage object if inserting into the
        # database fails. Then probably re-raise the exception.
        return blob_uuid

    def retrieve_blob_as_file(self, blob_uuid, dst_dir):
        object_name = self.retrieve_blob_path(blob_uuid)

        filename = os.path.basename(object_name)
        filepath = os.path.join(dst_dir, filename)

        blob = self._bucket.blob(object_name)
        blob.download_to_filename(filepath)

        return filepath

    # def store_tensors(self, tensors):
    #     """Returns UUID."""
    #     if isinstance(tensors, tf.Tensor):
    #         tensors = [tensors]
    #     # Maybe try something like an h5 file for storing structs of tensors.
    #     raise NotImplementedError("TODO")

    def retrieve_blob_path(self, blob_uuid):
        with self._cursor() as c:
            c.execute(
                f"SELECT gcp_storage_object_name FROM {BLOBS_TABLE} WHERE uuid=%s",
                (blob_uuid,),
            )
            row = c.fetchone()
            if not row:
                raise ValueError(f"Blob with uuid {blob_uuid} not found.")
            return row[0]

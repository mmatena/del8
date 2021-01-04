"""TODO: Add title."""
import contextlib
import json
import os
import shutil
import tempfile
import time

from absl import logging

from google.cloud import storage as gcp_storage
from google.oauth2 import service_account
import requests
import psycopg2

from del8.core import data_class
from del8.core import serialization
from del8.core.di import executable
from del8.core.experiment import runs
from del8.core.storage import storage
from del8.core.utils import backoffs
from del8.core.utils import file_util


# 30 min timeout for loading from gcp.
TIMEOUT = 30 * 60

SerializationType = serialization.SerializationType


RUN_STATES_TABLE = "RunStates"
ITEMS_TABLE = "Items"
BLOBS_TABLE = "Blobs"


# Should only be set to true for debugging.
PERSISTENT_CACHE = False


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
        # Stuff for caching.
        blob_read_cache_dir="~/.del8_gcp_storage_blob_read_cache",
        preloading_params=None,
    ):
        pass

    def instantiate_storage(
        self,
        group=None,
        experiment=None,
        run_uuid=None,
    ):
        return GcpStorage.from_params(
            self, group=group, experiment=experiment, run_uuid=run_uuid
        )

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
    only_wrap_methods=["params_by_environment_mode"],
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

        self._context_depth = 0
        self._conn = None
        self._bucket = None

        self._preloader = None

        # Using an int instead of a bool to enable an idempotent context manager.
        self._use_blob_read_cache_depth = 0
        self._blob_uuid_to_name = {}

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
    def from_params(cls, gcp_params, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        return GcpStorage(gcp_params, **kwargs)

    #################

    def _commit(self):
        self._conn.commit()

    def _get_cursor(self, tries=5, wait_secs=3):
        # TODO: Make the backoff, and perhaps other features, more sophisticated.
        try:
            c = self._conn.cursor()
            # Make sure that we can execute commands.
            c.execute("SELECT 1")
            return c
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            logging.info(
                f"Encountered exception '{e}' when connecting to Cloud SQL. Trying again."
            )
            if tries <= 1:
                raise e
            # NOTE: We assume that the connection has been closed.
            # TODO: Check if the connection has been closed. If not, then close it.
            time.sleep(wait_secs)
            self._conn = self._initialize_cloud_sql()
            return self._get_cursor(tries=tries - 1)

    @contextlib.contextmanager
    def _cursor(self):
        # To be used as `with self._cursor() as c: ...`
        cursor = self._get_cursor()
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

    def get_bucket_from_new_client(self):
        key_path = self._gcp_params.get_private_key_file()
        credentials = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        client = gcp_storage.Client(
            credentials=credentials, project=credentials.project_id
        )
        return client.get_bucket(self._gcp_params.bucket_name)

    def _initialize_cloud_storage(self):
        return self.get_bucket_from_new_client()

    def initialize(self):
        self._context_depth += 1

        if not self._conn:
            self._conn = self._initialize_cloud_sql()
        if not self._bucket:
            self._bucket = self._initialize_cloud_storage()
        if not self._preloader and self.can_preload_blobs():
            self._preloader = self._gcp_params.preloading_params.instantiate_preloader(
                self
            )
            self._preloader.initialize()

    def close(self):
        self._context_depth -= 1

        if not self._context_depth:
            if self._conn:
                self._conn.close()
            if self._preloader:
                self._preloader.close()
            self._conn = None
            self._bucket = None
            self._preloader = None

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

    def can_preload_blobs(self):
        return bool(self._gcp_params.preloading_params)

    def preload_blobs(self, blob_uuids):
        self._preloader.preload_blobs(blob_uuids)

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

    def set_run_state(self, run_state):
        with self._cursor() as c:
            c.execute(
                f"INSERT INTO {RUN_STATES_TABLE} VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (run_uuid) DO UPDATE SET state = EXCLUDED.state",
                (
                    self.group_uuid,
                    self.experiment_uuid,
                    self.run_uuid,
                    run_state,
                ),
            )

    def get_run_state(self, run_uuid):
        with self._cursor() as c:
            c.execute(
                f"SELECT state FROM {RUN_STATES_TABLE} WHERE run_uuid=%s",
                [run_uuid],
            )
            row = c.fetchone()
            if not row:
                raise ValueError(f"Run state not found for run with uuid: {run_uuid}.")
            (state,) = row
            return state

    #################

    def _create_uuid_query_terms(
        self,
        *,
        group_uuid=None,
        experiment_uuid=None,
        run_uuid=None,
    ):
        query = []
        bindings = []
        if group_uuid:
            query.append("group_uuid=%s")
            bindings.append(group_uuid)
        if experiment_uuid:
            query.append("exp_uuid=%s")
            bindings.append(experiment_uuid)
        if run_uuid:
            query.append("run_uuid=%s")
            bindings.append(run_uuid)
        query = " AND ".join(query)
        if not query:
            query = "TRUE"
        bindings = tuple(bindings)
        return query, bindings

    def _create_item_cls_query_term(self, item_cls):
        cls_name = item_cls.__name__
        cls_module = item_cls.__module__

        json_value = {
            "name": cls_name,
            "module": cls_module,
            "__type__": SerializationType.CLASS_OBJECT,
        }
        json_value = {"__class__": json_value}
        json_value = json.dumps(json_value)

        term = "data @> %s"
        bindings = (json_value,)

        return term, bindings

    def retrieve_items_by_class(
        self,
        item_cls,
        group_uuid=None,
        experiment_uuid=None,
        run_uuid=None,
    ):
        # All of the uuids are optional are should restrict the returns
        # to only items associated with the respective group/experiment/run.
        cls_term, cls_bindings = self._create_item_cls_query_term(item_cls)
        uuid_terms, uuid_bindings = self._create_uuid_query_terms(
            group_uuid=group_uuid,
            experiment_uuid=experiment_uuid,
            run_uuid=run_uuid,
        )
        query = f"SELECT data FROM {ITEMS_TABLE} WHERE {cls_term} AND {uuid_terms}"
        bindings = cls_bindings + uuid_bindings

        with self._cursor() as c:
            c.execute(query, bindings)
            rows = c.fetchall()

        items = [serialization.deserialize(row[0]) for row in rows]
        return items

    def retrieve_single_item_by_class(
        self,
        item_cls,
        group_uuid=None,
        experiment_uuid=None,
        run_uuid=None,
    ):
        items = self.retrieve_items_by_class(
            item_cls,
            group_uuid=group_uuid,
            experiment_uuid=experiment_uuid,
            run_uuid=run_uuid,
        )
        # NOTE: Maybe log the rest of the parameters. Log the query?
        if not items:
            raise ValueError(f"Unable to find an item with class {item_cls}.")
        elif len(items) > 1:
            raise ValueError(f"Found multiple items with class {item_cls}.")
        return items[0]

    def run_keys_from_partial_values(
        self,
        run_key_values,
        group_uuid=None,
        experiment_uuid=None,
    ):
        # NOTE: This probably does not work well with arrays.

        # All of the uuids are optional are should restrict the returns
        # to only items associated with the respective group/experiment.
        cls_term, cls_bindings = self._create_item_cls_query_term(runs.RunKey)
        uuid_terms, uuid_bindings = self._create_uuid_query_terms(
            group_uuid=group_uuid,
            experiment_uuid=experiment_uuid,
        )
        run_key_binding = {"attributes": {"key_values": run_key_values}}
        run_key_binding = serialization.serialize(run_key_binding)

        query = (
            f"SELECT data FROM {ITEMS_TABLE} WHERE "
            f"data @> %s AND {cls_term} AND {uuid_terms}"
        )
        bindings = (run_key_binding,) + cls_bindings + uuid_bindings

        with self._cursor() as c:
            c.execute(query, bindings)
            rows = c.fetchall()

        items = [serialization.deserialize(row[0]) for row in rows]
        return items

    def retrieve_run_uuids(
        self, *, group_uuid=None, experiment_uuid=None, run_state=None
    ):
        terms, bindings = self._create_uuid_query_terms(
            group_uuid=group_uuid,
            experiment_uuid=experiment_uuid,
        )
        if run_state is not None:
            terms += " AND state=%s::integer"
            bindings += (run_state,)

        query = f"SELECT run_uuid FROM {RUN_STATES_TABLE} WHERE {terms}"
        with self._cursor() as c:
            c.execute(query, bindings)
            rows = c.fetchall()

        return [row[0] for row in rows]

    #################

    @backoffs.linear_to_exp_backoff(
        exceptions_to_catch=[requests.exceptions.ReadTimeout]
    )
    def _upload_file(self, blob, filename):
        blob.upload_from_filename(filename, timeout=TIMEOUT)

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
            self._upload_file(blob, f.name)

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

    def store_blob_from_file(self, filepath):
        # NOTE: ext will start with a dot if it is non-empty.
        ext = file_util.get_file_suffix(filepath)
        blob_uuid = self.new_uuid()
        gcp_storage_object_name = f"{blob_uuid}{ext}"

        blob = self._bucket.blob(gcp_storage_object_name)
        blob.upload_from_filename(filepath, timeout=TIMEOUT)

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

    def _should_cache_blobs(self):
        return bool(PERSISTENT_CACHE or self._use_blob_read_cache_depth)

    def _is_blob_preloaded(self, blob_uuid):
        return bool(self._preloader and self._preloader.has_blob(blob_uuid))

    def retrieve_blob_as_file(self, blob_uuid, dst_dir):
        if self._should_cache_blobs() and blob_uuid in self._blob_uuid_to_name:
            object_name = self._blob_uuid_to_name[blob_uuid]

            cached_path = os.path.join(self.blob_read_cache_dir, object_name)

            filename = os.path.basename(object_name)
            filepath = os.path.join(dst_dir, filename)

            if cached_path != filepath:
                shutil.copyfile(cached_path, filepath)

            return filepath

        elif self._is_blob_preloaded(blob_uuid):
            preloaded_path = self._preloader.get_blob_filepath(blob_uuid)

            filename = os.path.basename(preloaded_path)
            filepath = os.path.join(dst_dir, filename)

            if preloaded_path != filepath:
                shutil.copyfile(preloaded_path, filepath)

            return filepath

        object_name = self.retrieve_blob_name(blob_uuid)

        if PERSISTENT_CACHE:
            cached_path = os.path.join(self.blob_read_cache_dir, object_name)
            if os.path.exists(cached_path):
                self._blob_uuid_to_name[blob_uuid] = object_name
                return self.retrieve_blob_as_file(blob_uuid, dst_dir)

        filename = os.path.basename(object_name)
        filepath = os.path.join(dst_dir, filename)

        blob = self._bucket.blob(object_name)
        blob.download_to_filename(filepath, timeout=TIMEOUT)

        if self._should_cache_blobs():
            self._blob_uuid_to_name[blob_uuid] = object_name
            cached_path = os.path.join(self.blob_read_cache_dir, object_name)
            if filepath != cached_path:
                shutil.copyfile(filepath, cached_path)

        return filepath

    @contextlib.contextmanager
    def retrieve_blob_as_tempfile(self, blob_uuid, flags="r"):
        use_cache = bool(self._use_blob_read_cache_depth)

        if use_cache:
            dst_dir = self.blob_read_cache_dir
        else:
            dst_dir = tempfile.mkdtemp()
        file = None
        try:
            filepath = self.retrieve_blob_as_file(blob_uuid, dst_dir)
            file = open(filepath, flags)
            yield file
        finally:
            if file:
                file.close()
            if not use_cache:
                shutil.rmtree(dst_dir)

    # def store_tensors(self, tensors):
    #     """Returns UUID."""
    #     if isinstance(tensors, tf.Tensor):
    #         tensors = [tensors]
    #     # Maybe try something like an h5 file for storing structs of tensors.
    #     raise NotImplementedError("TODO")

    def retrieve_blob_name(self, blob_uuid):
        with self._cursor() as c:
            c.execute(
                f"SELECT gcp_storage_object_name FROM {BLOBS_TABLE} WHERE uuid=%s",
                (blob_uuid,),
            )
            row = c.fetchone()
            if not row:
                raise ValueError(f"Blob with uuid {blob_uuid} not found.")
            return row[0]

    def retrieve_blob_names(self, blob_uuids):
        if not blob_uuids:
            return {}

        with self._cursor() as c:
            c.execute(
                f"SELECT uuid, gcp_storage_object_name FROM {BLOBS_TABLE} WHERE uuid IN %s",
                (tuple(blob_uuids),),
            )
            uuid_to_name = {r[0]: r[1] for r in c.fetchall()}
        return uuid_to_name

    #################

    @property
    def blob_read_cache_dir(self):
        return os.path.expanduser(self._gcp_params.blob_read_cache_dir)

    @contextlib.contextmanager
    def blob_read_cache(self):
        if not self._use_blob_read_cache_depth:
            self._blob_uuid_to_name = {}
            if not os.path.isdir(self.blob_read_cache_dir):
                os.mkdir(self.blob_read_cache_dir)

        self._use_blob_read_cache_depth += 1
        try:
            yield
        finally:
            self._use_blob_read_cache_depth -= 1
            if not self._use_blob_read_cache_depth and not PERSISTENT_CACHE:
                self._blob_uuid_to_name = {}
                shutil.rmtree(self.blob_read_cache_dir)

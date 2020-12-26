"""TODO: Add title."""
import os

from absl import logging

from google.cloud import storage as gcp_storage
from google.oauth2 import service_account
import tensorflow_datasets as tfds

from del8.core.di import executable
from del8.core.utils import gcp_util


###############################################################################


@executable.executable(pip_packages=["tensorflow-datasets"])
class tfds_dataset(object):
    def call(
        self,
        dataset_name,
        split,
        tfds_dir=None,
    ):
        return tfds.load(dataset_name, split=split, data_dir=tfds_dir)


###############################################################################


@executable.executable(pip_packages=["tensorflow-datasets"])
def private_key_filepath_from_storage(storage):
    # Assume storage is an instance of `GcpStorage`.
    return storage.private_key_file


@executable.executable(
    # TODO: Add all of the cloud dependencies to pip_packages as well.
    pip_packages=["tensorflow-datasets"],
    default_bindings={
        "private_key_filepath": private_key_filepath_from_storage,
    },
)
class gcp_tfds_dataset(object):
    # NOTE: I'm not putting the most logic into handling data set versions. If that
    # becomes important, I'll almost certainly have to make changes.
    #
    # Raises exception if the dataset was not found.

    def dataset_name_to_subpath(self, dataset_name):
        # NOTE: Not aggressively checking correct format.
        return dataset_name.replace(":", "/")

    def connect_to_bucket(self, tfds_bucket, private_key_filepath):
        return gcp_util.connect_to_bucket(tfds_bucket, private_key_filepath)

    def call(
        self,
        dataset_name,
        split,
        download_all_records=True,
        tfds_bucket="del8_tfds",
        # This is where stuff will be copied locally.
        tfds_dir="~/tensorflow_datasets",
    ):
        # NOTE: That the split argument does not affect what is downloaded from gcp.
        if not download_all_records:
            raise NotImplementedError(
                "TODO: Support downloading tfrecords as needed. Bested suited for large datasets."
            )

        name_subpath = self.dataset_name_to_subpath(dataset_name)
        tfds_dir = os.path.expanduser(tfds_dir)
        local_records_path = os.path.join(tfds_dir, name_subpath)

        # If the directory already exists locally for the dataset, assume that the
        # data set exists locally and load from it.
        if os.path.isdir(local_records_path):
            logging.info(
                f"Using records found locally at {local_records_path} for tfds data set {dataset_name}."
            )
            return tfds.load(dataset_name, split=split, data_dir=tfds_dir)
        else:
            logging.info(
                f"Downloading records from gs://{tfds_bucket} for tfds data set {dataset_name}."
            )

        bucket = self.connect_to_bucket(tfds_bucket)

        prefix = name_subpath
        # NOTE: I think the prefix has to end with a slash.
        if not prefix.endswith("/"):
            prefix = prefix + "/"

        blobs = list(bucket.list_blobs(prefix=prefix))
        if not blobs:
            raise ValueError(
                f"The tfds dataset {dataset_name} had no matching files on gcp bucket {tfds_bucket}."
            )

        # NOTE: For large datasets, this can be done much faster using multithreading. However,
        # it appears good enough for GLUE datasets, so I'll hold off on that.
        for blob in blobs:
            download_filepath = os.path.join(tfds_dir, blob.name)
            os.makedirs(os.path.dirname(download_filepath), exist_ok=True)
            blob.download_to_filename(download_filepath)

        return tfds.load(dataset_name, split=split, data_dir=tfds_dir)

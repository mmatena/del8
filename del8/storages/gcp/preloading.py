"""TODO: Add title."""
import datetime
from concurrent import futures
import json
import os
import shutil
import time


from absl import logging

import google.resumable_media.common

from del8.core import data_class
from del8.core.utils import backoffs

# 30 min timeout for loading from gcp.
TIMEOUT = 30 * 60

_BLOB_UUID_TO_NAME_FILENAME = "blob_uuid_to_name.json"


@data_class.data_class()
class GcpPreloadingParams(object):
    DELETE_ALL = "DELETE_ALL"
    DELETE_NONE = "DELETE_NONE"
    DELETE_UNUSED = "DELETE_UNUSED"

    def __init__(
        self,
        preload_dir="~/.del8_gcp_preload_dir",
        max_parallel_downloads=256,
        clear_style=DELETE_UNUSED,
    ):
        pass

    def get_preload_dir(self):
        return os.path.expanduser(self.preload_dir)

    def instantiate_preloader(self, storage):
        return GcpPreloader(params=self, storage=storage)


class GcpPreloader(object):
    def __init__(self, params, storage):
        self._params = params
        self._storage = storage

        self._blob_uuid_to_filename = {}

    @property
    def preload_dir(self):
        return self._params.get_preload_dir()

    @property
    def max_parallel_downloads(self):
        return self._params.max_parallel_downloads

    @property
    def clear_style(self):
        return self._params.clear_style

    @property
    def blob_uuid_to_name_filename(self):
        return os.path.join(self.preload_dir, _BLOB_UUID_TO_NAME_FILENAME)

    @property
    def _bucket(self):
        return self._storage._bucket

    ############################################

    def initialize(self):
        if not os.path.isdir(self.preload_dir):
            os.mkdir(self.preload_dir)
        if os.path.exists(self.blob_uuid_to_name_filename):
            with open(self.blob_uuid_to_name_filename) as f:
                self._blob_uuid_to_filename.update(json.load(f))

    def preload_blobs(self, blob_uuids):
        # Make sure that they are unique.
        blob_uuids = set(blob_uuids)

        if self.clear_style == GcpPreloadingParams.DELETE_UNUSED:
            removed_uuids = self._remove_difference_from_cache(blob_uuids)
            logging.info(f"Remove {len(removed_uuids)} unused blobs from cache.")

        blob_uuids_to_load = blob_uuids - set(self._blob_uuid_to_filename.keys())
        uuid_to_name = self._retrieve_blob_names(blob_uuids_to_load)

        logging.info(f"Starting download of {len(blob_uuids_to_load)} blobs")
        start_time = time.time()

        with futures.ThreadPoolExecutor(
            max_workers=self.max_parallel_downloads
        ) as executor:
            list(executor.map(self._preload_blob, uuid_to_name.items()))

        elapsed_seconds = time.time() - start_time
        elapsed_nice = str(datetime.timedelta(seconds=elapsed_seconds))
        logging.info(f"Downloaded {len(blob_uuids_to_load)} blobs in {elapsed_nice}")

    def close(self):
        if self.clear_style == GcpPreloadingParams.DELETE_ALL:
            self._blob_uuid_to_filename = {}
            shutil.rmtree(self.preload_dir)
            logging.info("Cleared preloading cache.")
        else:
            with open(self.blob_uuid_to_name_filename, "w") as f:
                json.dump(self._blob_uuid_to_filename, f)

    ############################################

    def has_blob(self, blob_uuid):
        return blob_uuid in self._blob_uuid_to_filename

    def get_blob_filepath(self, blob_uuid):
        if not self.has_blob(blob_uuid):
            return None
        filename = self._blob_uuid_to_filename[blob_uuid]
        return os.path.join(self.preload_dir, filename)

    ############################################

    @backoffs.linear_to_exp_backoff(
        exceptions_to_catch=[google.resumable_media.common.DataCorruption],
        linear_backoff_steps=3,
        exp_backoff_steps=0,
    )
    def _preload_blob(self, item):
        blob_uuid, blob_name = item
        filepath = os.path.join(self.preload_dir, blob_name)

        if os.path.exists(filepath):
            self._blob_uuid_to_filename[blob_uuid] = filepath
            logging.info(f"Using blob {blob_uuid} cached at {filepath}")
            return

        start_time = time.time()

        logging.info(f"Starting download of {blob_uuid}")

        blob = self._storage.get_bucket_from_new_client().blob(blob_name)
        blob.download_to_filename(filepath, timeout=TIMEOUT)

        elapsed_seconds = time.time() - start_time
        elapsed_nice = str(datetime.timedelta(seconds=elapsed_seconds))
        logging.info(f"Downloaded blob {blob_uuid} in {elapsed_nice}")

        self._blob_uuid_to_filename[blob_uuid] = filepath

    def _remove_difference_from_cache(self, blob_uuids):
        difference = set(self._blob_uuid_to_filename.keys()) - set(blob_uuids)
        for uuid in difference:
            filepath = self.get_blob_filepath(uuid)
            del self._blob_uuid_to_filename[uuid]
            try:
                os.remove(filepath)
            except FileNotFoundError:
                # Already deleted.
                pass
        return difference

    def _retrieve_blob_names(self, blob_uuids):
        uuid_to_name = self._storage.retrieve_blob_names(blob_uuids)
        return uuid_to_name

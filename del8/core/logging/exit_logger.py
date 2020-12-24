"""TODO: Add title.

All the logs go to GCP. Might want to configure this later.
"""
import base64
import os
import sys
import tarfile
import tempfile

from del8.core import data_class
from del8.core.utils import gcp_util


@data_class.data_class()
class ExitLoggerParams(object):
    def __init__(
        self,
        logs_dir,
        private_key_filepath,
        log_object_name,
        logs_bucket="del8_logs",
    ):
        pass


def log(logger_params):
    logs_dir = os.path.expanduser(logger_params.logs_dir)

    bucket = gcp_util.connect_to_bucket(
        logger_params.logs_bucket, logger_params.private_key_filepath
    )

    sys.stdout.flush()
    sys.stderr.flush()

    with tempfile.NamedTemporaryFile() as tmp:
        with tarfile.open(tmp.name, "w:gz") as tar:
            tar.add(logs_dir, arcname="logs")
        blob = bucket.blob(logger_params.log_object_name)
        blob.upload_from_filename(tmp.name)


def log_from_base64(logger_params, tar_base64):
    # The `logger_params.logs_dir` will be ignored here.
    bucket = gcp_util.connect_to_bucket(
        logger_params.logs_bucket, logger_params.private_key_filepath
    )
    contents = base64.b64decode(tar_base64.encode("utf-8"))
    blob = bucket.blob(logger_params.log_object_name)
    blob.upload_from_string(contents)


def postprocess_stored_logs(logger_params):
    # Stored logs saved as .tar.gz files. We extract so we can view on GCP
    # without having to download and extract the archives.
    gcp_dir = logger_params.log_object_name

    bucket = gcp_util.connect_to_bucket(
        logger_params.logs_bucket, logger_params.private_key_filepath
    )

    # NOTE: I think the prefix has to end with a slash.
    prefix = gcp_dir + "/"

    blobs = list(bucket.list_blobs(prefix=prefix))

    for blob in blobs:
        if not blob.name.endswith(".tar.gz"):
            continue
        with tempfile.TemporaryDirectory() as tmpdir:
            with tempfile.NamedTemporaryFile(suffix=".tar.gz") as tmp:
                blob.download_to_filename(tmp.name)
                with tarfile.open(tmp.name) as tar:
                    tar.extractall(tmpdir)
            extracted_dir = os.path.join(tmpdir, "logs")
            for logfile in os.listdir(extracted_dir):
                logfilepath = os.path.join(extracted_dir, logfile)

                logs_dir, *rest = blob.name[: -len(".tar.gz")].split("/")
                rest = "-".join(rest)

                object_name = os.path.join(
                    logs_dir,
                    f"{rest}-{logfile}",
                )

                new_blob = bucket.blob(object_name)
                new_blob.upload_from_filename(logfilepath, content_type="text/plain")
        blob.delete()

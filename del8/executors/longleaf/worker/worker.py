"""TODO: Add title."""
import os

from del8.core import data_class
from del8.core import serialization


# NOTE: I'm not sure how this will work if we use this as a pip package. In the
# worst case, we can tar and send the worker folder separately.
DEFAULT_WORKER_MAIN = "~/del8/del8/executors/longleaf/worker/main.py"


@data_class.data_class()
class WorkerParams(object):
    def __init__(
        self,
        slurm_params,
        worker_main=DEFAULT_WORKER_MAIN,
        #
        apt_get_packages=(),
        pip_packages=(),
        #
        image="tensorflow_2.3.0-gpu.sif",
    ):
        pass

    def get_worker_dir(self, longleaf_params, launch_id, worker_id):
        launch_dir = longleaf_params.get_launch_dir(launch_id)
        return os.path.join(launch_dir, "del8_workers", worker_id)

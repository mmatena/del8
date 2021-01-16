"""TODO: Add title."""
import os
import uuid as uuidlib

from absl import logging

from del8.core import data_class
from del8.core import serialization
from del8.core.utils import project_util

from . import longleaf_util
from .supervisor import launcher

DEFAULT_EXCLUDES = project_util.ProjectParams.DEFAULT_EXCLUDES


@data_class.data_class()
class LongleafParams(object):
    def __init__(
        self,
        user,
        project_params,
        supervisor_params,
        worker_params,
        storage_params,
        slurm_interface_params,
    ):
        # Ensure that project params is stored as a sequence.
        assert self.project_params
        if not isinstance(self.project_params, (list, tuple)):
            self.project_params = [self.project_params]

    @property
    def ssh_host(self):
        return f"{self.user}@longleaf.unc.edu"

    @property
    def pine_root_dir(self):
        user = self.user
        return f"/pine/scr/{user[0]}/{user[1]}/{user}/del8_launches"

    @property
    def user_nas_dir(self):
        return f"/nas/longleaf/home/{self.user}"

    @property
    def images_dir(self):
        return os.path.join(self.user_nas_dir, "del8/images")

    def get_launch_dir(self, launch_id):
        return os.path.join(self.pine_root_dir, launch_id)


def _change_storage_params(execution_item, longleaf_params: LongleafParams):
    worker_run_kwargs = execution_item.worker_run_kwargs.copy()
    worker_run_kwargs["storage_params"] = longleaf_params.storage_params
    return execution_item.copy(worker_run_kwargs=worker_run_kwargs)


def launch(execution_items, longleaf_params: LongleafParams):
    launch_id = uuidlib.uuid4().hex

    # We serialize each execution item so that we can pass the string to the
    # worker on the supervisor. Thus we do not need to download experiment's
    # dependencies on the supervisor.
    execution_items = [
        serialization.serialize(_change_storage_params(item, longleaf_params))
        for item in execution_items
    ]

    launcher.launch(execution_items, longleaf_params, launch_id)

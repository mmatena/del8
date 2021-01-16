"""TODO: Add title.

Much of this was copied from the `onstart_util` of vastai.
"""
import base64
import os
import re
import subprocess
import tarfile
import tempfile

from absl import logging

from del8.core.utils import project_util

from del8.storages.gcp import gcp


PIP = "pip3"
PYTHON = "python3"


def _add_storage_startup(storage_params):
    if not isinstance(storage_params, gcp.GcpStorageParams):
        return ""

    source_dir = storage_params.client_directory
    source_parent_dir = os.path.dirname(source_dir)

    # NOTE: The gcp stuff on the worker ends up being like ~/~/..., so fix it.
    return project_util.folder_to_bash_command(
        source_dir, unzip_directory=source_parent_dir
    )


def create_setup_command(import_params, project_params, storage_params):
    script = [
        "export TMPDIR=~/tmp",
        "mkdir $TMPDIR",
    ]

    # apt-get stuff
    script.append("apt-get update")
    if import_params.apt_get_packages:
        args = " ".join(import_params.apt_get_packages)
        script.append(f"apt-get -y install {args}")

    # pip stuff
    if import_params.pip_packages:
        script.append(
            project_util.pip_packages_to_bash_command(
                import_params.pip_packages, pip=PIP
            )
        )

    # storage stuff
    if storage_params:
        script.append(_add_storage_startup(storage_params))

    # project stuff
    for project in project_params:
        script.append(project_util.python_project_to_pythonpath_command(project))

    # script.append(_add_start_worker_script(job_params))

    return "\n".join(script)


def create_copy_within_longleaf_command(project_params, storage_params, dst):
    script = []

    for project in project_params:
        src = f"~/{project.get_folder_name()}"
        src = os.path.expanduser(src)
        script.append(f"cp -r {src} {dst}")

    if isinstance(storage_params, gcp.GcpStorageParams):
        src = storage_params.client_directory
        src = os.path.expanduser(src)
        script.append(f"cp -r {src} {dst}")

    return "\n".join(script)


def move_within_longleaf(project_params, storage_params, dst):
    cmd = create_copy_within_longleaf_command(project_params, storage_params, dst)
    for line in cmd.split("\n"):
        try:
            subprocess.run(line.split(), check=True)
        except subprocess.CalledProcessError as e:
            logging.error(e.output)
            raise e

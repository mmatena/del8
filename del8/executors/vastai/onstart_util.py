"""Code for creating the onstart_cmd."""
import base64
import os
import re
import tarfile
import tempfile

from del8.core.utils import project_util

from del8.storages.gcp import gcp


def _add_storage_startup(storage_params):
    if not isinstance(storage_params, gcp.GcpStorageParams):
        return ""

    source_dir = storage_params.client_directory
    source_parent_dir = os.path.dirname(source_dir)

    return project_util.folder_to_bash_command(
        source_dir, unzip_directory=source_parent_dir
    )


def _add_start_worker_script(vast_params):
    instance_params = vast_params.instance_params

    logs_dir = instance_params.worker_logs_dir
    stdout = os.path.join(logs_dir, "logs.stdout")
    stderr = os.path.join(logs_dir, "logs.stderr")

    cmd = [
        instance_params.python_binary,
        instance_params.worker_main,
        f"--port={instance_params.remote_port}",
        f"1>{stdout} 2>{stderr} &",
    ]
    cmd = " ".join(cmd)
    script = [f"mkdir -p {logs_dir}", cmd]
    return "\n".join(script)


def create_onstart_cmd(vast_params):
    instance_params = vast_params.instance_params
    storage_params = vast_params.storage_params

    pip = instance_params.pip_binary

    script = ["#!/bin/bash", "touch ~/.no_auto_tmux"]

    # apt-get stuff
    script.append("apt-get update")
    if instance_params.apt_get_packages:
        args = " ".join(instance_params.apt_get_packages)
        script.append(f"apt-get -y install {args}")

    # pip stuff
    script.append(
        project_util.pip_packages_to_bash_command(instance_params.pip_packages, pip=pip)
    )

    # storage stuff
    if storage_params:
        script.append(_add_storage_startup(storage_params))

    # project stuff
    for project in instance_params.project_params:
        script.append(project_util.python_project_to_bash_command(project))

    if instance_params.extra_onstart_cmd:
        script.append(instance_params.extra_onstart_cmd)

    script.append(_add_start_worker_script(vast_params))

    return "\n".join(script)

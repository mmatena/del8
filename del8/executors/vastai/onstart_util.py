"""Code for creating the onstart_cmd."""
import base64
import os
import re
import tarfile
import tempfile

from del8.storages.gcp import gcp


def _add_storage_startup(storage_params):
    if not isinstance(storage_params, gcp.GcpStorageParams):
        return ""

    params = storage_params.extra_params

    source_dir = os.path.expanduser(params.client_directory)
    source_name = os.path.basename(source_dir)
    source_parent_dir = os.path.dirname(params.client_directory)

    with tempfile.NamedTemporaryFile() as tmp:
        with tarfile.open(tmp.name, "w:gz") as tar:
            tar.add(source_dir, arcname=source_name)
        with open(tmp.name, "rb") as f:
            content = f.read()
    client_tar = base64.b64encode(content).decode("utf-8")

    script = [
        f"GCP_CLIENT_TAR='{client_tar}'",
        "TMP_TAR_FILE=$(mktemp)",
        "echo $GCP_CLIENT_TAR | base64 -d > $TMP_TAR_FILE",
        f"mkdir -p {source_parent_dir}",
        f"tar -xvzf $TMP_TAR_FILE -C {source_parent_dir}",
        "rm $TMP_TAR_FILE",
    ]

    return "\n".join(script)


def _add_project_startup(project_params):
    source_dir = os.path.expanduser(project_params.folder_path)
    source_name = project_params.get_folder_name()

    excludes = project_params.get_excludes()

    def filter_fn(tarinfo):
        for pattern in excludes:
            if re.match(pattern, tarinfo.name):
                return None
        return tarinfo

    with tempfile.NamedTemporaryFile() as tmp:
        with tarfile.open(tmp.name, "w:gz") as tar:
            tar.add(source_dir, arcname=source_name, filter=filter_fn)
        with open(tmp.name, "rb") as f:
            content = f.read()
    code_tar = base64.b64encode(content).decode("utf-8")

    script = [
        f"CODE_TAR='{code_tar}'",
        "TMP_TAR_FILE=$(mktemp)",
        "echo $CODE_TAR | base64 -d > $TMP_TAR_FILE",
        "tar -xvzf $TMP_TAR_FILE -C ./",
        "rm $TMP_TAR_FILE",
        f"export PYTHONPATH=$PYTHONPATH:~/{source_name}",
    ]

    return "\n".join(script)


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

    script = ["#!/bin/bash"]

    # apt-get stuff
    script.append("apt-get update")
    if instance_params.apt_get_packages:
        args = " ".join(instance_params.apt_get_packages)
        script.append(f"apt-get -y install {args}")

    # pip stuff
    script.append(f"{pip} install --upgrade pip")
    if instance_params.pip_packages:
        args = " ".join(instance_params.pip_packages)
        script.append(f"{pip} install {args}")

    # storage stuff
    if storage_params:
        script.append(_add_storage_startup(storage_params))

    # project stuff
    for project in instance_params.project_params:
        script.append(_add_project_startup(project))

    if instance_params.extra_onstart_cmd:
        script.append(instance_params.extra_onstart_cmd)

    script.append(_add_start_worker_script(vast_params))

    return "\n".join(script)

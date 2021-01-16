"""TODO: Add title."""
import os
import subprocess

from del8.core.utils import project_util


def execute_cmd_on_longleaf(longleaf_params, cmd):
    # NOTE: Need to have password-free access to longleaf set up. See
    # https://serverfault.com/questions/2429 for how to do this.

    # NOTE: Think about blocking, non-blocking, futures.
    local_cmd = [
        "ssh",
        longleaf_params.ssh_host,
        cmd,
    ]
    output = subprocess.check_output(local_cmd)
    return output


def copy_folder_to_longleaf(longleaf_params, folderpath, dst, excludes=[]):
    folderpath = os.path.expanduser(folderpath)

    local_cmd = ["rsync", "-ra", "-e", "ssh"]
    for s in excludes:
        local_cmd += ["--exclude", s]
    local_cmd += [folderpath, f"{longleaf_params.ssh_host}:{dst}"]

    try:
        output = subprocess.check_output(local_cmd)
    except subprocess.CalledProcessError as e:
        print(e.output)
        raise e

    return output

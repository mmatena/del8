"""TODO: Add title."""
import base64
import os
import re
import tarfile
import tempfile

from typing import Sequence

from absl import logging

from del8.core import data_class


@data_class.data_class()
class ProjectParams(object):
    DEFAULT_EXCLUDES = (
        r".*/__pycache__$",
        r".*/\.git$",
    )

    def __init__(
        self,
        folder_path: str,
        extra_excludes: Sequence[str] = (),
    ):
        pass

    def get_excludes(self):
        return tuple(self.extra_excludes) + self.DEFAULT_EXCLUDES

    def get_folder_name(self):
        source_dir = os.path.expanduser(self.folder_path)
        return os.path.basename(source_dir)


def python_project_to_bash_command(project_params):
    # NOTE: This assumes that we have a top-level python project and wish
    # to add it to the path.
    source_dir = project_params.folder_path
    source_name = project_params.get_folder_name()
    excludes = project_params.get_excludes()

    script = [
        folder_to_bash_command(source_dir, excludes=excludes),
        f"export PYTHONPATH=$PYTHONPATH:~/{source_name}",
    ]

    return "\n".join(script)


def file_to_bash_command(filepath, dst_directory="./"):
    # NOTE: Does no compression or encryption.
    filepath = os.path.expanduser(filepath)
    with open(filepath, "rb") as f:
        content = f.read()
    file_content = base64.b64encode(content).decode("utf-8")

    logging.info(f"{filepath} base64 has been created.")

    filename = os.path.basename(filepath)
    dst_file = os.path.join(dst_directory, filename)

    script = [
        f"FILE_CONTENT='{file_content}'",
        f"mkdir -p {dst_directory}",
        f"echo $FILE_CONTENT | base64 -d > {dst_file}",
    ]

    return "\n".join(script)


def folder_to_bash_command(folder, excludes=[], unzip_directory="./"):
    def filter_fn(tarinfo):
        for pattern in excludes:
            if re.match(pattern, tarinfo.name):
                return None
        return tarinfo

    source_dir = os.path.expanduser(folder)
    source_name = os.path.basename(source_dir)

    with tempfile.NamedTemporaryFile() as tmp:
        with tarfile.open(tmp.name, "w:gz") as tar:
            tar.add(source_dir, arcname=source_name, filter=filter_fn)
        with open(tmp.name, "rb") as f:
            content = f.read()
    folder_tar = base64.b64encode(content).decode("utf-8")

    logging.info(f"{folder} base64 has been created.")

    script = [
        f"FOLDER_TAR='{folder_tar}'",
        "TMP_TAR_FILE=$(mktemp)",
        "echo $FOLDER_TAR | base64 -d > $TMP_TAR_FILE",
        f"mkdir -p {unzip_directory}",
        f"tar -xvzf $TMP_TAR_FILE -C {unzip_directory}",
        "rm $TMP_TAR_FILE",
    ]

    return "\n".join(script)


def pip_packages_to_bash_command(pip_packages, pip="pip3", sudo=False):
    script = [f"{pip} install --upgrade pip"]
    if pip_packages:
        args = " ".join(pip_packages)
        if sudo:
            script.append(f"sudo {pip} install {args}")
        else:
            script.append(f"{pip} install {args}")
    return "\n".join(script)


###############################################################################


DEL8_PROJECT = ProjectParams(
    # TODO: Set this using __file__ and os.path.dirname
    folder_path="~/Desktop/projects/del8"
)

"""TODO: Add title."""
import base64
import os
import uuid as uuidlib

from del8.core import data_class
from del8.core import serialization
from del8.core.not_sync import events

from .. import longleaf_util
from .. import slurm
from . import singularity
from ..worker import setup_util
from ..slurm_interface import slurm_interface


PIP = "pip3"
PYTHON = "python3"

EXCLUDES = (
    "*/__pycache__",
    "*/.git",
)


def launch(execution_items, longleaf_params, launch_id):
    supervisor_params = longleaf_params.supervisor_params
    supervisor_dir = supervisor_params.get_supervisor_dir(longleaf_params, launch_id)

    print(f"Supervisor directory: {supervisor_dir}")

    # Make the supervisor directory.
    longleaf_util.execute_cmd_on_longleaf(longleaf_params, f"mkdir -p {supervisor_dir}")

    # Copy over the projects to the supervisor directory on longleaf.
    for project in longleaf_params.project_params:
        if project.extra_excludes:
            raise ValueError(
                "Currently not supporting extra_excludes when copying files to longleaf."
            )
        longleaf_util.copy_folder_to_longleaf(
            longleaf_params, project.folder_path, dst=supervisor_dir, excludes=EXCLUDES
        )

    cmd = _create_slurm_command(
        longleaf_params, supervisor_params, launch_id, execution_items
    )
    cmd = " ".join(cmd)

    longleaf_util.execute_cmd_on_longleaf(longleaf_params, cmd)

    print(f"tail -f {supervisor_dir}/logs*")


def _create_slurm_command(
    longleaf_params, supervisor_params, launch_id, execution_items
):
    supervisor_dir = supervisor_params.get_supervisor_dir(longleaf_params, launch_id)
    singularity_cmd = singularity.create_exec_command(
        _create_singularity_command(
            longleaf_params, supervisor_params, launch_id, execution_items
        ),
        singularity.get_image_path(supervisor_params.image, longleaf_params.images_dir),
        gpu=False,
        home=supervisor_dir,
    )

    cmd = [
        slurm_interface.create_startup_command(longleaf_params, supervisor_dir),
        singularity_cmd,
    ]
    cmd = "\n".join(cmd)

    return slurm.create_launch_command(
        cmd, supervisor_params.slurm_params, logs_dir=supervisor_dir
    )


def _to_base64(data):
    ser = serialization.serialize(data)
    return base64.b64encode(ser.encode("utf-8")).decode("utf-8")


def _create_singularity_command(
    longleaf_params, supervisor_params, launch_id, execution_items
):
    setup_cmd = setup_util.create_setup_command(
        supervisor_params,
        longleaf_params.project_params,
        longleaf_params.storage_params,
    )

    params_b64 = serialization.serialize(longleaf_params)
    params_b64 = base64.b64encode(params_b64.encode("utf-8")).decode("utf-8")

    python_cmd = [
        PYTHON,
        supervisor_params.supervisor_main,
        f"--longleaf_params_base64={_to_base64(longleaf_params)}",
        f"--execution_items_base64={_to_base64(execution_items)}",
        f"--launch_id={launch_id}",
    ]
    cmd = [
        setup_cmd,
        " ".join(python_cmd),
    ]
    return "\n".join(cmd)

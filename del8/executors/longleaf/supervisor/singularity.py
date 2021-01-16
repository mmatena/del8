"""TODO: Add title."""
import os
import shlex

# singularity shell --contain -B /usr/bin:/original_usr/bin -B /pine -B /proj --home=/pine/scr/m/m/mmatena/del8_launches/8ea64073822e45858806fe5add014d8b:/root ~/del8/images/tensorflow_2.3.0-gpu.sif


def create_exec_command(cmd, simg, gpu, home=None, extra_bindings=None):
    if extra_bindings is None:
        extra_bindings = {}

    singularity = "singularity"

    ret = [
        f"{singularity} exec",
        "--contain",
        f"--home={shlex.quote(home)}:/root" if home else "",
        "--nv" if gpu else "",
        "-B /pine -B /proj",
        " ".join([f"-B {src}:{dst}" for src, dst in extra_bindings.items()]),
        simg,
        "bash -c",
        shlex.quote(cmd),
    ]
    return " ".join(ret)


def get_image_path(image, images_dir):
    return os.path.join(images_dir, image)

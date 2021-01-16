"""Utilities for SLURM."""
import os
import re
import shlex
import subprocess

from del8.core import data_class


GPU_PARTITIONS = {"gpu", "volta-gpu"}


@data_class.data_class()
class SlurmParams(object):
    def __init__(
        self,
        ram_gb,
        partition,
        num_cpus,
        duration,
        # NOTE: Should only be None if we are running on a non-gpu partition.
        num_gpus=None,
    ):
        pass


def create_launch_command(cmd, slurm_params, logs_dir, bin_dir=None, quote=True):
    stdout = os.path.join(logs_dir, "logs.stdout")
    stderr = os.path.join(logs_dir, "logs.stderr")
    ret = [
        os.path.join(bin_dir, "sbatch") if bin_dir else "sbatch",
        f"--error={stdout}",
        f"--output={stderr}",
        f"--ntasks={slurm_params.num_cpus}",
        f"--mem={slurm_params.ram_gb}g",
        f"--time={slurm_params.duration}",
        f"--partition={slurm_params.partition}",
    ]
    if slurm_params.partition in GPU_PARTITIONS:
        ret += [
            f"--gres=gpu:{slurm_params.num_gpus}",
            "--qos=gpu_access",
        ]
    ret += [f"--wrap={shlex.quote(cmd) if quote else cmd}"]
    return ret


def parse_launch_stdout(output):
    match = re.search(r"^Submitted batch job (\d+)$", output, re.MULTILINE)
    if not match:
        raise ValueError(f"Error launching job. Received output:\n {output}")
    return match.group(1)


def launch_job(launch_cmd):
    # Return the id of the job.
    try:
        output = subprocess.check_output(launch_cmd)
    except subprocess.CalledProcessError as e:
        print(e.output)
        raise e
    return parse_launch_stdout(output)


#######################################


def cancel_job(job_id):
    subprocess.check_output(["scancel", job_id])


#######################################


def create_job_states_command(user):
    return ["squeue", "-u", user, "-o", R"%.18i %.2t"]


def parse_job_states_stdout(output):
    lines = output.split("\n")
    job_id_to_state = {}
    # We skip the first line as it is the header.
    for line in lines[1:]:
        job_id = line[:18].strip()
        state = line[19:21].strip()
        if job_id:
            job_id_to_state[job_id] = state
    return job_id_to_state


def get_job_states(user):
    # Do a ctrl-f for "JOB STATE CODES" at https://slurm.schedmd.com/squeue.html to get a list and
    # description of the state codes.
    cmd = create_job_states_command(user)
    output = subprocess.check_output(cmd)
    return parse_job_states_stdout(output)

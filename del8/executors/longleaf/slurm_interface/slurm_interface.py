"""TODO: Add title."""
from multiprocessing import connection
import os
import threading

from del8.core import data_class
from del8.executors.longleaf import slurm

MAIN_RELATIVE_PATH = "del8/del8/executors/longleaf/slurm_interface/main.py"

PYTHON = "python3"


@data_class.data_class()
class SlurmInterfaceParams(object):
    def __init__(
        self,
        slurm_interface_main=MAIN_RELATIVE_PATH,
        port=4646,
    ):
        pass


class SlurmInterface(object):
    def __init__(self, longleaf_params):
        self._params = longleaf_params
        self._conn = None
        self._conn_lock = threading.RLock()

    @property
    def slurm_interface_params(self):
        return self._params.slurm_interface_params

    def _send_to_connection(self, cmd):
        # For thread-safety ONLY interact with the connection via this method.
        # cmd is a list of str
        with self._conn_lock:
            if not self._conn:
                self._conn = connection.Client(
                    ("127.0.0.1", self.slurm_interface_params.port)
                )
            self._conn.send(cmd)
            output = self._conn.recv()
        return output.decode("utf-8")

    def launch_job(self, launch_cmd):
        output = self._send_to_connection(launch_cmd)
        return slurm.parse_launch_stdout(output)

    def cancel_job(self, job_id):
        self._send_to_connection(["scancel", job_id])

    def get_job_states(self, user):
        cmd = slurm.create_job_states_command(user)
        output = self._send_to_connection(cmd)
        return slurm.parse_job_states_stdout(output)


def create_startup_command(longleaf_params, root_dir):
    p = longleaf_params.slurm_interface_params
    main = os.path.join(root_dir, p.slurm_interface_main)
    # The & makes us run in the background.
    return f"{PYTHON} {main} --port={p.port} &"

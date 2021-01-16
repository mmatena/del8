"""TODO: Add title."""
from multiprocessing import connection
import os
import queue
import socket
import time
import threading
import uuid as uuidlib

from absl import logging

from del8.core import data_class
from del8.core import serialization
from del8.core.not_sync import events

from del8.executors.longleaf import slurm
from del8.executors.longleaf.slurm_interface import slurm_interface

from .. import messages
from ..worker import setup_util
from . import singularity

event_generator = events.event_generator
event_handler = events.event_handler
event_listener = events.event_listener
event_exception_handler = events.event_exception_handler

dfsa_transition = events.dfsa_transition
fixed_transition = events.fixed_transition


# NOTE: I'm not sure how this will work if we use this as a pip package. In the
# worst case, we can tar and send the supervisor folder separately.
DEFAULT_SUPERVISOR_MAIN = "~/del8/del8/executors/longleaf/supervisor/main.py"

SUPERVISOR_PIP_PACKAGES = [
    "absl-py",
    "google-auth",
    "google-cloud-storage",
    "overload==1.1",
    "pinject",
    "psycopg2-binary",
    "requests",
    "sshtunnel",
]

PYTHON = "python3"


###############################################################################


@data_class.data_class()
class SupervisorParams(object):
    def __init__(
        self,
        target_num_workers,
        num_buffer_workers,
        max_buffer_queue_size,
        #
        slurm_params,
        #
        image="tensorflow_2.3.0-gpu.sif",
        #
        supervisor_main=DEFAULT_SUPERVISOR_MAIN,
        #
        apt_get_packages=(),
        pip_packages=SUPERVISOR_PIP_PACKAGES,
    ):
        pass

    def get_supervisor_dir(self, longleaf_params, launch_id):
        launch_dir = longleaf_params.get_launch_dir(launch_id)
        return os.path.join(launch_dir, "del8_supervisor")


###############################################################################


class Event(object):
    #
    START_SUPERVISOR = "START_SUPERVISOR"
    UPDATE_JOB_STATES = "UPDATE_JOB_STATES"
    #
    LAUNCH_WORKER = "LAUNCH_WORKER"
    JOB_STATE_CHANGE = "JOB_STATE_CHANGE"
    WORKER_STARTED_RUNNING = "WORKER_STARTED_RUNNING"
    ACCEPTING = "ACCEPTING"
    KILL_WORKER = "KILL_WORKER"


#######################################


class LongleafSupervisor(events.EventItemWithUuid):
    def __init__(self, longleaf_params, launch_id, execution_items):
        super().__init__()
        self._params = longleaf_params
        self._launch_id = launch_id
        self._execution_items = _as_queue(execution_items)

        self._slurm = slurm_interface.SlurmInterface(longleaf_params)

        self._ip_address = _get_ip_address()

        self._workers = {}
        self._job_states = {}
        self._worker_knowledge_of_job_states = {}

        self._job_state_ping_interval = 5  # seconds

        self.context.add_event(self.uuid, Event.START_SUPERVISOR, None)

    @property
    def slurm(self):
        return self._slurm

    @property
    def launch_id(self):
        return self._launch_id

    @property
    def supervisor_params(self):
        return self._params.supervisor_params

    @property
    def ip_address(self):
        return self._ip_address

    ###############

    def get_execution_item(self):
        return self._execution_items.get_nowait()

    ###############

    @event_handler(Event.START_SUPERVISOR)
    @event_generator(Event.UPDATE_JOB_STATES)
    def on_start(self, data=None):
        # TODO: Change how the worker management is done. Maybe create own class for it.
        for _ in range(self.supervisor_params.target_num_workers):
            handle = LongleafWorkerHandle(self._params, supervisor=self)
            self._workers[handle.uuid] = handle
            self.context.add_event(handle.uuid, Event.LAUNCH_WORKER, None)

    @event_handler(Event.UPDATE_JOB_STATES)
    @event_generator(Event.UPDATE_JOB_STATES)
    def on_update_job_states(self, data=None):
        self._job_states = self.slurm.get_job_states(self._params.user)

        logging.info(self._job_states)

        self._send_job_state_changes_to_workers()

        # TODO: Need a way to break this loop somehow.
        time.sleep(self._job_state_ping_interval)

    ###############

    def _get_worker_by_job_id(self, job_id):
        for worker in self._workers.values():
            if worker.job_id == job_id:
                return worker
        return None

    def _send_job_state_changes_to_workers(self):
        new_job_states = self._job_states
        old_job_states = self._worker_knowledge_of_job_states

        new_knowledge_of_job_states = old_job_states.copy()
        all_job_ids = set(new_job_states.keys()) | set(old_job_states.keys())
        for job_id in all_job_ids:
            old_job_state = old_job_states.get(job_id, None)
            new_job_state = new_job_states.get(job_id, None)
            if new_job_state != old_job_state:
                worker = self._get_worker_by_job_id(job_id)
                if worker:
                    new_knowledge_of_job_states[job_id] = new_job_state
                    self.context.add_event(
                        worker.uuid, Event.JOB_STATE_CHANGE, new_job_state
                    )

        self._worker_knowledge_of_job_states = new_knowledge_of_job_states


#######################################


class LongleafWorkerHandle(events.EventItemWithUuid):
    def __init__(self, longleaf_params, supervisor):
        super().__init__()
        self._params = longleaf_params
        self._supervisor = supervisor
        self._launch_id = supervisor.launch_id

        self._listener = self._create_listener()
        self._conn = None

        self._slurm_job_id = None

    @property
    def slurm(self):
        return self._supervisor.slurm

    @property
    def launch_id(self):
        return self._launch_id

    @property
    def job_id(self):
        return self._slurm_job_id

    @property
    def worker_dir(self):
        return self._params.worker_params.get_worker_dir(
            self._params, launch_id=self.launch_id, worker_id=self.uuid
        )

    @property
    def listener_host(self):
        return self._listener.address[0]

    @property
    def listener_port(self):
        return self._listener.address[1]

    ###############

    @event_handler(Event.LAUNCH_WORKER)
    def launch_job(self, data=None):
        worker_params = self._params.worker_params

        setup_command = setup_util.create_setup_command(
            worker_params, self._params.project_params, storage_params=None
        )

        os.makedirs(self.worker_dir)

        logging.info(f"Worker dir: {self.worker_dir}")
        logging.info(f"tail -f {self.worker_dir}/logs*")

        setup_util.move_within_longleaf(
            self._params.project_params,
            self._params.storage_params,
            dst=self.worker_dir,
        )

        startup_cmd = [setup_command, self._create_worker_start_command()]
        startup_cmd = "\n".join(startup_cmd)

        sing_cmd = singularity.create_exec_command(
            startup_cmd,
            simg=singularity.get_image_path(
                worker_params.image, self._params.images_dir
            ),
            gpu=True,
            home=self.worker_dir,
        )

        launch_cmd = slurm.create_launch_command(
            sing_cmd,
            worker_params.slurm_params,
            self.worker_dir,
            quote=False,
        )

        logging.info(" ".join(launch_cmd))

        job_id = self.slurm.launch_job(launch_cmd)
        self._slurm_job_id = job_id

    @event_handler(Event.JOB_STATE_CHANGE)
    def on_job_state_change(self, new_state):
        if new_state is None:
            # Worker has been killed.
            # TODO: Add something like the following line, make sure not to get in infinite loop.
            # self.context.add_event(self.uuid, Event.KILL_WORKER, self.uuid)
            pass
        elif new_state == "R":
            # Worker job is running. Note that it still might be setting up, and
            # thus the worker python program might not be running yet.
            self.context.add_event(self.uuid, Event.WORKER_STARTED_RUNNING, None)
        elif new_state == "PD":
            # Worker is pending. Do nothing.
            pass
        else:
            logging.info(f"Unknown job state {new_state}. Doing nothing.")

    @event_handler(Event.WORKER_STARTED_RUNNING)
    @event_generator(Event.ACCEPTING)
    def on_worker_started_running(self, data=None):
        conn = self._listener.accept()
        conn.__enter__()
        recv = conn.recv()
        logging.info(recv)
        assert recv == "CONNECTED"

        # I need to send something for whatever reason.
        conn.send("IDK WHY I HAVE TO DO THIS")

        self._conn = conn

    @event_handler(Event.ACCEPTING)
    def on_accepting(self, data=None):
        try:
            item = self._supervisor.get_execution_item()
            self._process_execution_item(item)
            self.context.add_event(self.uuid, Event.ACCEPTING, None)
        except queue.Empty:
            self.context.add_event(self.uuid, Event.KILL_WORKER, self.uuid)

    ###############

    def _create_listener(self):
        ip = self._supervisor.ip_address
        # Port of 0 means that the contooter chooses it 4 u.
        return connection.Listener((ip, 0))

    def _create_worker_start_command(self):
        cmd = [
            PYTHON,
            self._params.worker_params.worker_main,
            f"--listener_host={self.listener_host}",
            f"--listener_port={self.listener_port}",
        ]
        return " ".join(cmd)

    def _process_execution_item(self, item):
        # TODO: Add some nice logging and handle some failures (see Vast AI for example).
        msg = messages.ProcessItem.from_execution_item(item)
        ser_msg = serialization.serialize(msg)

        self._conn.send(ser_msg)
        logging.info("sent")

        response = self._conn.recv()
        self._assert_good_response_status(response)

        logging.info("Successfully processed an item.")
        return response

    def _assert_good_response_status(self, response):
        if not isinstance(response, messages.Message):
            # Assume we are being passed the serialized JSON string.
            response = serialization.deserialize(response)

        if response.content.status != messages.ResponseStatus.SUCCESS:
            # TODO: Handle failures.
            raise ValueError(
                f"Unsuccessful item processing with status {response.status}."
            )


###############################################################################


def _as_queue(lst):
    q = queue.Queue()
    for item in lst:
        q.put_nowait(item)
    return q


def _get_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip_address = s.getsockname()[0]
    s.close()
    return ip_address

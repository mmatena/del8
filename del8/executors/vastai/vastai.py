"""TODO: Add title."""
from concurrent import futures
import logging as pylogging
from multiprocessing import connection
import queue
import time

from typing import Sequence

from absl import logging
import sshtunnel

from del8.core import data_class
from del8.core import serialization
from del8.core.execution import executor
from del8.core.utils import backoffs

from . import api_wrapper
from . import onstart_util
from . import messages


# Do this to suppress messages that spam the logs when trying
# to connect repeatedly via the tunnel.
#
# It might be a good idea to remove these when we are debugging
# the ssh tunnel.
pylogging.getLogger("sshtunnel.SSHTunnelForwarder").addFilter(lambda *x, **y: False)
pylogging.getLogger("paramiko.transport").addFilter(lambda *x, **y: False)


###############################################################################

# NOTE: I'm not sure how this will work if we use this as a pip package. In the
# worst case, we can tar and send the worker folder separately.
DEFAULT_WORKER_MAIN = "~/del8/del8/executors/vastai/worker/main.py"


@data_class.data_class()
class VastExecutorParams(object):
    def __init__(
        self,
        offer_query,
        instance_params,
        num_workers: int,
        storage_params=None,
        # NOTE: This will be the entire worker on start command. It will not be
        # added to some other on start command; again, it will form the entire
        # on start command.
        entire_on_start_cmd=None,
    ):
        pass

    def get_queries_str(self):
        return self.offer_query.get_queries_str(self.instance_params)

    def create_onstart_cmd(self):
        if self.entire_on_start_cmd:
            return self.entire_on_start_cmd
        return onstart_util.create_onstart_cmd(self)

    def create_supervisor(self):
        return VastSupervisor.from_params(self)


@data_class.data_class()
class OfferQuery(object):
    def __init__(
        self,
        queries_str="",
        order_str="dlperf_usd,dph",
        # TODO: Support interuptable instances.
        offer_type="on-demand",
    ):
        pass

    def get_queries_str(self, instance_params):
        return f"disk_space >= {instance_params.disk_gb} {self.queries_str}"


@data_class.data_class()
class InstanceParams(object):
    def __init__(
        self,
        disk_gb: int,
        project_params=None,
        extra_onstart_cmd: str = None,
        # Usually should not be set. Will use the default if None.
        worker_main=DEFAULT_WORKER_MAIN,
        remote_port: int = 6464,
        worker_logs_dir="~/del8_worker_logs",
        apt_get_packages: Sequence[str] = (),
        pip_packages: Sequence[str] = (),
        pip_binary="pip3",
        python_binary="python3",
        # TODO: Make the image configurable somewhere
        #
        # The vast ai images are very out of date. Do not use them.
        # Use the tensorflow/pytorch ones instead.
        #
        # Also don't use the latest- tagged images. I think they change,
        # and I've gotten stuff broken as new ones are pushed. Also the
        # vast ai workers have to download the images when they change,
        # which takes time and costs money in downloads.
        image="tensorflow/tensorflow:2.3.0-gpu",
    ):
        # Ensure that project params is stored as a sequence.
        if not self.project_params:
            self.project_params = ()
        elif not isinstance(self.project_params, (list, tuple)):
            self.project_params = [self.project_params]


###############################################################################


def create_supervisor_params(experiment, *, num_workers, offer_query, disk_gb):
    execution_items = experiment.create_all_execution_items()

    all_binding_specs = set()
    for exe_item in execution_items:
        # NOTE: Not including anything from the init_kwargs or call_kwargs. It
        # is the user's job to include that in the experiment or group's
        # extra_apt_get_packages and extra_pip_packages properties.
        all_binding_specs |= set(exe_item.worker_run_kwargs["global_binding_specs"])
    all_binding_specs = list(all_binding_specs)

    return VastExecutorParams(
        num_workers=num_workers,
        storage_params=experiment.get_storage_params(),
        offer_query=offer_query,
        instance_params=InstanceParams(
            disk_gb=disk_gb,
            project_params=experiment.get_all_project_params(),
            **experiment.get_all_package_kwargs(all_binding_specs),
        ),
    )


def launch_experiment(experiment, *, num_workers, offer_query, disk_gb):
    execution_items = experiment.create_all_execution_items()

    vast_params = create_supervisor_params(
        experiment,
        num_workers=num_workers,
        offer_query=offer_query,
        disk_gb=disk_gb,
    )

    sup = VastSupervisor.from_params(vast_params)
    sup.run(execution_items)
    return sup


###############################################################################


def _list_to_queue(lst):
    q = queue.Queue()
    for item in lst:
        q.put_nowait(item)
    return q


###############################################################################


class _WorkerStates(object):
    UNSTARTED = "UNSTARTED"
    INITIALIZING = "INITIALIZING"
    ACCEPTING = "ACCEPTING"
    PROCESSING = "PROCESSING"
    KILLED = "KILLED"


class VastSupervisor(executor.Supervisor):
    def __init__(self, vast_params):
        self._vast_params = vast_params
        self._worker_launcher = VastWorkerLauncher(vast_params)

    @classmethod
    def from_params(cls, executor_params):
        return cls(executor_params)

    def initialize(self):
        # NOTE: I chose the 2 pretty arbitarily here.
        max_pool_workers = round(2 * self._vast_params.num_workers)
        self._pool = futures.ThreadPoolExecutor(max_workers=max_pool_workers)
        self._worker_handles = set()
        self._worker_launcher.prepare_for_launches()

    def close(self):
        self._pool.shutdown()
        for handle in self._worker_handles:
            handle.close()

    def _launch_worker(self):
        handle = self._worker_launcher.launch()
        self._worker_handles.add(handle)
        return handle

    def _run(self, execution_items):
        not_done = set()

        def submit_to_pool(fn, *args, **kwargs):
            not_done.add(self._pool.submit(fn, *args, **kwargs))

        execution_items = _list_to_queue(execution_items)

        for _ in range(self._vast_params.num_workers):
            submit_to_pool(self._launch_worker)

        while not_done:
            done, not_done = futures.wait(not_done, return_when=futures.FIRST_COMPLETED)
            # TODO: Handle failures.
            if len(done) != 1:
                raise Exception(
                    # TODO: Make sure printing the items in done is clean. I don't
                    # know what they'll look like as they are now.
                    "Was expecting to have a single done item returned. "
                    f"Instead we had {len(done)} items with values {done}."
                )
            handle = list(done)[0].result()
            state = handle.state

            logging.info(f"Worker state: {state}")

            if state == _WorkerStates.INITIALIZING:
                submit_to_pool(handle.wait_for_connection)

            elif state == _WorkerStates.ACCEPTING:
                try:
                    item = execution_items.get_nowait()
                    submit_to_pool(handle.accept_item, item)
                except queue.Empty:
                    submit_to_pool(handle.kill)

            elif state == _WorkerStates.KILLED:
                continue

            else:
                raise Exception(
                    f"State {state} not recognized in the supervisor for VastWorkerHandle."
                )


class VastWorkerLauncher(executor.WorkerLauncher):
    def __init__(
        self,
        vast_params,
    ):
        self._vast_params = vast_params
        self._offers = None

    def prepare_for_launches(self):
        offers = api_wrapper.query_offers(self._vast_params)
        self._offers = _list_to_queue(offers)

    def launch(self):
        # Note that this is safe as we do not add any offers to the queue after
        # we havve created it. Otherwise, probably use something like a blocking get.
        if self._offers.empty():
            # TODO: Add some configuration on what to do in this case.
            raise Exception("Ran out of Vast AI offers when trying to create workers.")
        offer = self._offers.get_nowait()
        handle = VastWorkerHandle(vast_params=self._vast_params, offer=offer)
        return handle.start()


class _ConnectFailedException(Exception):
    pass


class VastWorkerHandle(executor.WorkerHandle):
    def __init__(self, vast_params, offer):
        self._vast_params = vast_params
        self._offer = offer

        self._uuid = None
        self._instance = None
        self._tunnel = None
        self._listener = None
        self._conn = None

        self.state = _WorkerStates.UNSTARTED

    def start(self):
        self.state = _WorkerStates.INITIALIZING
        self._instance = None
        self._tunnel = None

        instance_id = self._offer.get_instance_id()
        self._uuid = api_wrapper.create_instance(
            instance_id, vast_params=self._vast_params
        )

        return self

    def kill(self):
        # logging.fatal("NOT KILLING WORKER FOR TESTING. UNCOMMENT ASAP.")
        if self._instance:
            api_wrapper.destroy_instance_by_vast_instance_id(
                self._instance.get_instance_id()
            )
        elif self._uuid:
            api_wrapper.destroy_instance_by_uuid(self._uuid)
        self.state = _WorkerStates.KILLED
        logging.info("Killed worker.")
        return self

    def wait_for_connection(self):
        assert self.state == _WorkerStates.INITIALIZING
        # NOTE: Not sure if this sleep is necessary.
        time.sleep(5)
        if self._can_connect():
            self._connect()
            self.state = _WorkerStates.ACCEPTING
        return self

    def _can_connect(self):
        assert self.state == _WorkerStates.INITIALIZING
        self._instance = api_wrapper.get_instance_by_uuid(self._uuid)
        return self._instance.is_running() and self._instance.get_ssh_address()

    def _is_tunnel_good_to_go(self):
        if not self._tunnel:
            return False
        self._tunnel.check_tunnels()
        if not self._tunnel.is_active or not self._tunnel.is_alive:
            return False
        if not self._tunnel.tunnel_is_up[
            (self._tunnel.local_bind_host, self._tunnel.local_bind_port)
        ]:
            logging.debug("Tunnel is active and alive but does not appear to be up.")
            return False
        return True

    @backoffs.linear_to_exp_backoff(
        exceptions_to_catch=(_ConnectFailedException,),
        should_retry_on_exception_fn=lambda e: isinstance(e, _ConnectFailedException),
        linear_backoff_steps=60,
    )
    def _connect(self):
        assert self.state == _WorkerStates.INITIALIZING

        ssh_address = self._instance.get_ssh_address()
        remote_port = self._vast_params.instance_params.remote_port

        self._tunnel = sshtunnel.SSHTunnelForwarder(
            ssh_address,
            remote_bind_address=("127.0.0.1", remote_port),
            ssh_username="root",
            compression=True,
            mute_exceptions=True,
            # NOTE: If I have issues with the connections dying, try setting
            # the `set_keepalive` argument.
            # set_keepalive,
        )

        self._tunnel.start()

        if not self._is_tunnel_good_to_go():
            raise _ConnectFailedException(
                "Failed to create SSH tunnel to VastAI worker."
            )

        self._conn = connection.Client(("127.0.0.1", self._tunnel.local_bind_port))

    def accept_item(self, item):
        assert self.state == _WorkerStates.ACCEPTING
        assert item is not None
        self.state = _WorkerStates.PROCESSING

        msg = messages.Message(
            type=messages.MessageType.PROCESS_ITEM,
            content=messages.ProcessItem(execution_item=item),
        )

        ser_msg = serialization.serialize(msg)
        self._conn.send(ser_msg)

        response = self._conn.recv()
        response = serialization.deserialize(response)

        if response.content.status != messages.ResponseStatus.SUCCESS:
            # TODO: Handle failures.
            raise ValueError(
                f"Unsuccessful item processing with status {response.status}."
            )

        logging.info("Successfully processed an item.")

        self.state = _WorkerStates.ACCEPTING

        return self

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
        if self._tunnel:
            self._tunnel.stop()
            self._tunnel = None
        if self.state != _WorkerStates.KILLED:
            self.kill()

"""Wrapper around the Vast API for better use within the executor code."""
import functools
import random
import time
import uuid as uuidlib

from absl import logging
import requests

from del8.core.utils import backoffs
from del8.core.utils import decorator_util as dec_util
from .vast_api import api as vast_api


##############################################################################


# def http_backoff(
#     status_code,
#     *,
#     # These just provide the default values. You can override on each function
#     # call if needed.
#     linear_backoff_steps=5,
#     linear_interval_secs=5,
#     exp_backoff_steps=4,
#     exp_start_interval_secs=5,
#     exp_backoff_base=2,
#     backoff_noise_ms=50,
# ):

#     def decorator(fn):
#         @functools.wraps(fn)
#         def inner(
#             *args,
#             # The `no_backoff` arg is useful when we put this wrapper on a
#             # function that calls another function with its own backoff and
#             # don't want to double up the backoffs while being able to control
#             # the backoffs via passed arguments for the outer function.
#             no_backoff=False,
#             linear_backoff_steps=linear_backoff_steps,
#             linear_interval_secs=linear_interval_secs,
#             exp_backoff_steps=exp_backoff_steps,
#             exp_start_interval_secs=exp_start_interval_secs,
#             exp_backoff_base=exp_backoff_base,
#             backoff_noise_ms=backoff_noise_ms,
#             **kwargs,
#         ):
#             if no_backoff:
#                 intervals = []
#             else:
#                 intervals = [
#                     linear_interval_secs * i for i in range(linear_backoff_steps)
#                 ] + [
#                     exp_start_interval_secs * exp_backoff_base ** i
#                     for i in range(exp_backoff_steps)
#                 ]

#             attempts = 0
#             total_secs_waited = 0

#             while True:
#                 try:
#                     return fn(*args, **kwargs)

#                 except requests.exceptions.HTTPError as e:
#                     attempts += 1
#                     if not intervals:
#                         if not no_backoff:
#                             logging.warning(
#                                 f"Unable to call {fn.__name__} without producing "
#                                 f"an HTTPError with status code {status_code}. We "
#                                 f"had a total of {attempts} attempts with a cumulative "
#                                 f"total of {total_secs_waited} seconds waited between attempts."
#                             )
#                         raise e

#                     if e.response.status_code == status_code:
#                         interval = intervals.pop(0)
#                         logging.info(
#                             f"Received HTTPError with status code {status_code} from a call "
#                             f"to {fn.__name__}. Trying again in {interval} seconds."
#                         )
#                         total_secs_waited += interval

#                         noise = (random.random() - 0.5) * backoff_noise_ms / 1000
#                         time.sleep(interval + noise)
#                     else:
#                         raise e
#             raise Exception("If we get here, then there is a bug in the code.")

#         return inner

#     return decorator


_bad_gateway_backoff = functools.partial(
    backoffs.linear_to_exp_backoff,
    exceptions_to_catch=(requests.exceptions.HTTPError,),
    should_retry_on_exception_fn=lambda e: e.response.status_code == 502,
)


##############################################################################


def _new_uuid():
    return uuidlib.uuid4().hex


def _json_wrapper(cls):
    @dec_util.wraps_class(cls)
    class JsonWrapper(cls):
        def __init__(self, json, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._json = json

        def __getitem__(self, key):
            return self._json[key]

        def __str__(self):
            return str(self._json)

        def __repr__(self):
            return repr(self._json)

    return JsonWrapper


@_json_wrapper
class VastOffer(object):
    def get_instance_id(self):
        return self["id"]


@_json_wrapper
class VastInstance(object):
    def get_instance_id(self):
        return self["id"]

    # The ssh host and ports won't be available until the instance is running.
    def get_ssh_host(self):
        return self["ssh_host"]

    def get_ssh_port(self):
        return self["ssh_port"]

    def get_ssh_address(self):
        # Ports can't be 0, right?
        if self.get_ssh_host() and self.get_ssh_port():
            return (self.get_ssh_host(), self.get_ssh_port())
        return None

    def get_uuid(self):
        return self["label"]

    def is_running(self):
        return self["actual_status"] == "running"


##############################################################################


@_bad_gateway_backoff()
def query_offers(vast_params):
    query_str = vast_params.get_queries_str()
    order_str = vast_params.offer_query.order_str

    results = vast_api.search_offers(
        query_str=query_str,
        order_str=order_str,
        offer_type=vast_params.offer_query.offer_type,
        disable_bundling=True,
    )

    return [VastOffer(r) for r in results]


@_bad_gateway_backoff()
def create_instance(
    instance_id,
    vast_params,
) -> str:
    """Returns the uuid of the instance. Note this is different than the instance id."""
    # It looks like the instance id changes from the offer to the instance,
    # so we use the label for tracking.
    uuid = _new_uuid()

    onstart_cmd = vast_params.create_onstart_cmd()

    response = vast_api.create_instance(
        instance_id,
        disk_gb=vast_params.instance_params.disk_gb,
        image=vast_params.instance_params.image,
        onstart_cmd=onstart_cmd,
        label=uuid,
    )

    # The API response is something like
    #     {'success': True, 'new_contract': 697522}
    # if successful. I think it throws an error if it fails,
    # but I'm adding positive verification of success just
    # to be safe.
    if not response["success"]:
        raise Exception(f"Failed to create VastAI instance with id {instance_id}.")

    return uuid


@_bad_gateway_backoff()
def get_all_instances():
    # Instances means that I'm the one running them.
    instances = vast_api.get_instances()
    return [VastInstance(instance) for instance in instances]


@_bad_gateway_backoff()
def get_instance_by_uuid(instance_uuid):
    # Instance means that I'm running it.
    for instance in get_all_instances(no_backoff=True):
        if instance.get_uuid() == instance_uuid:
            return instance
    raise Exception(f"Instance with uuid {instance_uuid} not found.")


@_bad_gateway_backoff()
def destroy_instance_by_vast_instance_id(vast_instance_id):
    response = vast_api.destroy_instance(vast_instance_id)
    if not response["success"]:
        raise Exception(
            f"Failed to destroy VastAI instance with id {vast_instance_id}."
        )
    return response


def destroy_instance_by_uuid(instance_uuid, **backoff_kwargs):
    instance = get_instance_by_uuid(instance_uuid, **backoff_kwargs)
    return destroy_instance_by_vast_instance_id(
        instance.get_instance_id(), **backoff_kwargs
    )

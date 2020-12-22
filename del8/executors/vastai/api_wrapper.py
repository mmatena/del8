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


_http_500_backoff = functools.partial(
    backoffs.linear_to_exp_backoff,
    exceptions_to_catch=(requests.exceptions.HTTPError,),
    should_retry_on_exception_fn=lambda e: 500 <= e.response.status_code < 600,
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


@_http_500_backoff()
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


@_http_500_backoff()
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


@_http_500_backoff()
def get_all_instances():
    # Instances means that I'm the one running them.
    instances = vast_api.get_instances()
    return [VastInstance(instance) for instance in instances]


@_http_500_backoff()
def get_instance_by_uuid(instance_uuid):
    # Instance means that I'm running it.
    for instance in get_all_instances(no_backoff=True):
        if instance.get_uuid() == instance_uuid:
            return instance
    raise Exception(f"Instance with uuid {instance_uuid} not found.")


@_http_500_backoff()
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

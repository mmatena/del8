"""Main executable program for a GCE supervisor."""
import base64
import json
import requests

from absl import app
from absl import flags
from absl import logging

from del8.core import serialization

FLAGS = flags.FLAGS

flags.DEFINE_string("execution_items_base64", None, "")
flags.DEFINE_string("executor_params_base64", None, "")

flags.mark_flag_as_required("execution_items_base64")
flags.mark_flag_as_required("executor_params_base64")


def kill_vm():
    # TODO: Looks like this code wasn't working.
    pass
    # # From https://stackoverflow.com/a/52811140.
    # # based on https://stackoverflow.com/q/52748332/321772

    # # get the token
    # r = json.loads(
    #     requests.get("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
    #                  headers={"Metadata-Flavor": "Google"})
    #         .text)

    # token = r["access_token"]

    # # get instance metadata
    # # based on https://cloud.google.com/compute/docs/storing-retrieving-metadata
    # project_id = requests.get("http://metadata.google.internal/computeMetadata/v1/project/project-id",
    #                           headers={"Metadata-Flavor": "Google"}).text

    # name = requests.get("http://metadata.google.internal/computeMetadata/v1/instance/name",
    #                     headers={"Metadata-Flavor": "Google"}).text

    # zone_long = requests.get("http://metadata.google.internal/computeMetadata/v1/instance/zone",
    #                          headers={"Metadata-Flavor": "Google"}).text
    # zone = zone_long.split("/")[-1]

    # # shut ourselves down
    # logging.info("Calling API to delete this VM, {zone}/{name}".format(zone=zone, name=name))

    # requests.delete("https://www.googleapis.com/compute/v1/projects/{project_id}/zones/{zone}/instances/{name}"
    #                 .format(project_id=project_id, zone=zone, name=name),
    #                 headers={"Authorization": "Bearer {token}".format(token=token)})


# NOTE: There is probably a cleaner way to do this than a
# bunch of nested contexts and loops. Also maybe try seeing
# if some light server framework could be used.
def main(_):
    try:
        exe_items = base64.b64decode(
            FLAGS.execution_items_base64.encode("utf-8")
        ).decode("utf-8")
        exe_params = base64.b64decode(
            FLAGS.executor_params_base64.encode("utf-8")
        ).decode("utf-8")

        exe_items = serialization.deserialize(exe_items)
        exe_params = serialization.deserialize(exe_params)

        supervisor = exe_params.create_supervisor()

        supervisor.run(exe_items)
    finally:
        kill_vm()


if __name__ == "__main__":
    app.run(main)

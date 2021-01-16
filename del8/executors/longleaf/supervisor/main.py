"""Main executable program for a longleaf supervisor."""
import base64
import json
import os
import requests

from absl import app
from absl import flags
from absl import logging

from del8.core import serialization
from del8.executors.longleaf.supervisor import supervisor as ll_supervisor
from del8.core.not_sync import events


FLAGS = flags.FLAGS

flags.DEFINE_string("launch_id", None, "")
flags.DEFINE_string("longleaf_params_base64", None, "")
flags.DEFINE_string("execution_items_base64", None, "")

flags.mark_flag_as_required("launch_id")
flags.mark_flag_as_required("longleaf_params_base64")
flags.mark_flag_as_required("execution_items_base64")


MAX_THREADS = 256


def parse_base64_flag(value):
    serialized = base64.b64decode(value).decode("utf-8")
    return serialization.deserialize(serialized)


def main(_):
    launch_id = FLAGS.launch_id
    longleaf_params = parse_base64_flag(FLAGS.longleaf_params_base64)
    execution_items = parse_base64_flag(FLAGS.execution_items_base64)

    with events.MultiThreadedContext(max_workers=MAX_THREADS) as ctx:
        supervisor = ll_supervisor.LongleafSupervisor(
            longleaf_params, launch_id, execution_items
        )
        del supervisor
        ctx.execute()

    # Add worker start up commands to sbatch queue.
    # Connect to the GCE supervisor (if present, may enforce this at first).


if __name__ == "__main__":
    app.run(main)

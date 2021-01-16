"""Main executable program for a longleaf worker."""
from multiprocessing import connection
import sys
import time

from absl import app
from absl import flags
from absl import logging

import tensorflow as tf

from del8.core import serialization
from del8.core.execution import entrypoint
from del8.executors.vastai import messages

FLAGS = flags.FLAGS

flags.DEFINE_string("listener_host", None, "")
flags.DEFINE_integer("listener_port", None, "")

flags.mark_flag_as_required("listener_host")
flags.mark_flag_as_required("listener_port")


def main(_):
    logging.info("Longleaf worker started.")
    logging.info(f"Waiting to connect to {FLAGS.listener_host}:{FLAGS.listener_port}")

    conn = connection.Client((FLAGS.listener_host, FLAGS.listener_port))
    logging.info(f"Connected to {FLAGS.listener_host}:{FLAGS.listener_port}")

    conn.send("CONNECTED")

    while True:
        msg = conn.recv()
        try:
            logging.info("Waiting for message from supervisor.")
            msg = conn.recv()
            logging.info("Message received.")
        except EOFError:
            logging.warning("[NOT FATAL] EOFError on conn.recv()")
            break

        msg = serialization.deserialize(msg)
        logging.info(f"Incoming msg: {msg}")

        if msg.type == messages.MessageType.PROCESS_ITEM:
            exe_item = msg.content.execution_item
            if isinstance(exe_item, str):
                exe_item = serialization.deserialize(exe_item)

            logging.info(
                f"Processing execution item: {serialization.serialize(exe_item, indent=2)}"
            )
            entrypoint.worker_run(**exe_item.worker_run_kwargs)

            response = messages.Message(
                type=messages.MessageType.PROCESS_ITEM,
                content=messages.ItemProcessed(status=messages.ResponseStatus.SUCCESS),
            )
            logging.info("Successfully processed execution item")

            ser_res = serialization.serialize(response)
            logging.info("Sending response to supervisor.")
            conn.send(ser_res)

            logging.info("Clearing keras session.")
            tf.keras.backend.clear_session()

        else:
            raise ValueError(f"Message received with unknown type {msg.type}.")


if __name__ == "__main__":
    app.run(main)

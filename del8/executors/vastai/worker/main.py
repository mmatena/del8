"""Main executable program for a Vast AI worker."""
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

flags.DEFINE_integer("port", None, "")

flags.mark_flag_as_required("port")


# NOTE: There is probably a cleaner way to do this than a
# bunch of nested contexts and loops. Also maybe try seeing
# if some light server framework could be used.
def main_loop():
    address = ("127.0.0.1", FLAGS.port)
    with connection.Listener(address) as listener:
        while True:
            with listener.accept() as conn:
                while True:
                    # Try to keep these flushed for exit logging.
                    sys.stdout.flush()
                    sys.stderr.flush()

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
                            content=messages.ItemProcessed(
                                status=messages.ResponseStatus.SUCCESS
                            ),
                        )
                        logging.info("Successfully processed execution item")

                        ser_res = serialization.serialize(response)
                        logging.info("Sending response to supervisor.")
                        conn.send(ser_res)

                        logging.info("Clearing keras session.")
                        tf.keras.backend.clear_session()

                    # NOTE: I don't I support this, so commenting out.
                    # elif msg.type == messages.MessageType.KILL:
                    #     return

                    else:
                        raise ValueError(
                            f"Message received with unknown type {msg.type}."
                        )


def main(_):
    main_loop()


if __name__ == "__main__":
    app.run(main)

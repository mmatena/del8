"""Main executable program for a Vast AI worker."""
from multiprocessing import connection
import time

from absl import app
from absl import flags
from absl import logging

from del8.core import serialization
from del8.core.execution import entrypoint
from del8.executors.vastai import messages

FLAGS = flags.FLAGS

flags.DEFINE_integer("port", None, "")

flags.mark_flag_as_required("port")


# NOTE: There is probably a cleaner way to do this than a
# bunch of nested contexts and loops. Also maybe try seeing
# if some light server framework could be used.
def main(_):
    address = ("127.0.0.1", FLAGS.port)
    with connection.Listener(address) as listener:
        while True:
            with listener.accept() as conn:
                while True:
                    try:
                        msg = conn.recv()
                    except EOFError:
                        logging.warning("EOFError on conn.recv()")
                        break

                    msg = serialization.deserialize(msg)
                    logging.info(f"Incoming msg: {msg}")

                    if msg.type == messages.MessageType.PROCESS_ITEM:
                        exe_item = msg.content.execution_item
                        if isinstance(exe_item, str):
                            exe_item = serialization.deserialize(exe_item)

                        # TODO: Exception handling
                        # logging.warning("Not processing anything for testing purposes. Uncomment the line.")
                        entrypoint.worker_run(**exe_item.worker_run_kwargs)

                        response = messages.Message(
                            type=messages.MessageType.PROCESS_ITEM,
                            content=messages.ItemProcessed(
                                status=messages.ResponseStatus.SUCCESS
                            ),
                        )

                        ser_res = serialization.serialize(response)
                        conn.send(ser_res)

                    elif msg.type == messages.MessageType.KILL:
                        return

                    else:
                        raise ValueError(
                            f"Message received with unknown type {msg.type}."
                        )


if __name__ == "__main__":
    app.run(main)

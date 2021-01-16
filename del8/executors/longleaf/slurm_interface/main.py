"""Main executable program used for launching SLURM jobs.

We run this outside of singularity as I couldn't figure out how
to call SLURM commands from within it.
"""
import json
from multiprocessing import connection
import subprocess

from absl import app
from absl import flags
from absl import logging


FLAGS = flags.FLAGS

flags.DEFINE_integer("port", None, "")

flags.mark_flag_as_required("port")


def main(_):
    address = ("127.0.0.1", FLAGS.port)
    with connection.Listener(address) as listener:
        while True:
            # TODO: Enable multi-threading for connections.
            with listener.accept() as conn:
                while True:
                    try:
                        msg = conn.recv()
                    except EOFError:
                        logging.warning("[NOT FATAL] EOFError on conn.recv()")
                        break

                    cmd = msg
                    # logging.info(cmd)
                    output = subprocess.check_output(cmd)
                    conn.send(output)


if __name__ == "__main__":
    app.run(main)

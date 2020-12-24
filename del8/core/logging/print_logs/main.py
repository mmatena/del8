"""TODO: Add title."""
import base64
import os
import sys
import tarfile
import tempfile

from absl import app
from absl import flags

from del8.core.logging.print_logs import common

FLAGS = flags.FLAGS

flags.DEFINE_string("logs_dir", None, "")

flags.mark_flag_as_required("logs_dir")


def main(_):
    logs_dir = os.path.expanduser(FLAGS.logs_dir)

    with tempfile.NamedTemporaryFile() as tmp:
        with tarfile.open(tmp.name, "w:gz") as tar:
            tar.add(logs_dir, arcname="logs")
        with open(tmp.name, "rb") as f:
            content = f.read()
    folder_tar = base64.b64encode(content).decode("utf-8")

    print_str = common.create_print_str(folder_tar)
    print(print_str, file=sys.stdout)
    sys.stdout.flush()


if __name__ == "__main__":
    app.run(main)

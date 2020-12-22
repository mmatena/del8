"""TODO: Add title.

```bash
# Useful monitoring command once you have ssh-ed into GCE VM.
sudo su -
tail -f del8_supervisor_logs/log*
```
"""
import base64
import os
import uuid as uuidlib

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
from libcloud.compute.deployment import ScriptDeployment

from del8.core import data_class
from del8.core import serialization
from del8.core.utils import project_util


# NOTE: Trying to put the basic packages stuff here to run executors and stuff.
SUPERVISOR_PIP_PACKAGES = [
    "absl-py",
    "google-auth",
    "google-cloud-storage",
    "pinject",
    "psycopg2-binary",
    "requests",
    "sshtunnel",
]

# NOTE: I'm not sure how this will work if we use this as a pip package. In the
# worst case, we can tar and send the supervisor folder separately.
DEFAULT_SUPERVISOR_MAIN = "~/del8/del8/executors/gce/supervisor/main.py"

SSH_PERMISSIONS_SET_UP_CMD = (
    R"find .ssh/ -type f -exec chmod 600 {} \;; "
    R"find .ssh/ -type d -exec chmod 700 {} \;; "
    R'find .ssh/ -type f -name "*.pub" -exec chmod 644 {} \;'
)


@data_class.data_class()
class GceParams(object):
    def __init__(
        self,
        service_account="del8-227@durable-tangent-298714.iam.gserviceaccount.com",
        datacenter="us-central1-b",
        project_id="durable-tangent-298714",
        credentials_path="~/del8/gcp/vast/storage-private-key.json",
        private_ssh_key_path="~/.ssh/id_rsa",
        public_ssh_key_path="~/.ssh/id_rsa.pub",
        pip_binary="pip3",
        python_binary="python3",
        vast_api_key_file="~/.vast_api_key",
        supervisor_main=DEFAULT_SUPERVISOR_MAIN,
        supervisor_logs_dir="~/del8_supervisor_logs",
    ):
        self.private_ssh_key_path = os.path.expanduser(self.private_ssh_key_path)
        self.public_ssh_key_path = os.path.expanduser(self.public_ssh_key_path)

    def create_ex_metadata(self):
        with open(self.public_ssh_key_path, "r") as fp:
            public_key_content = fp.read().strip()
        return {
            "items": [
                {
                    "key": "ssh-keys",
                    "value": f"root: {public_key_content}",
                }
            ]
        }

    def create_ex_service_accounts(self):
        return [
            {
                "email": self.service_account,
                "scopes": ["compute", "storage-full"],
            }
        ]


def _add_start_supervisor_script(
    execution_items, executor_params, launch_params, instance_name
):
    on_start_cmd = executor_params.create_onstart_cmd()
    executor_params = executor_params.copy(entire_on_start_cmd=on_start_cmd)

    # We serialize each execution item so that we can pass the string to the
    # worker on the supervisor. Thus we do not need to download experiment's
    # dependencies on the supervisor.
    execution_items = [serialization.serialize(item) for item in execution_items]

    execution_items = serialization.serialize(execution_items).encode("utf-8")
    executor_params = serialization.serialize(executor_params).encode("utf-8")

    execution_items = base64.b64encode(execution_items).decode("utf-8")
    executor_params = base64.b64encode(executor_params).decode("utf-8")

    logs_dir = launch_params.supervisor_logs_dir
    stdout = os.path.join(logs_dir, "logs.stdout")
    stderr = os.path.join(logs_dir, "logs.stderr")

    cmd = [
        launch_params.python_binary,
        launch_params.supervisor_main,
        f"--gce_instance_name='{instance_name}'",
        f"--zone='{launch_params.datacenter}'",
        f"--execution_items_base64='{execution_items}'",
        f"--executor_params_base64='{executor_params}'",
        f"1>{stdout} 2>{stderr} &",
    ]
    cmd = " ".join(cmd)
    script = [f"mkdir -p {logs_dir}", cmd]
    return "\n".join(script)


def launch(execution_items, executor_params, launch_params):
    # NOTE: Maybe make this include the experiment name.
    instance_name = f"del8-{uuidlib.uuid4().hex}"

    script = [
        "#!/bin/bash",
        # I think we need the sudo apt-get update twice for whatever reason.
        "sudo apt-get update",
        "sudo apt-get update",
        "sudo apt-get -y install python3-pip",
        project_util.pip_packages_to_bash_command(
            SUPERVISOR_PIP_PACKAGES, pip=launch_params.pip_binary
        ),
        #
        project_util.file_to_bash_command(
            launch_params.public_ssh_key_path, dst_directory="~/.ssh"
        ),
        project_util.file_to_bash_command(
            launch_params.private_ssh_key_path, dst_directory="~/.ssh"
        ),
        SSH_PERMISSIONS_SET_UP_CMD,
        "eval `ssh-agent -s`",
        f"ssh-add ~/.ssh/{os.path.basename(launch_params.private_ssh_key_path)}",
        project_util.file_to_bash_command(launch_params.vast_api_key_file),
        #
        project_util.python_project_to_bash_command(project_util.DEL8_PROJECT),
        #
        _add_start_supervisor_script(
            execution_items, executor_params, launch_params, instance_name=instance_name
        ),
    ]
    deploy = ScriptDeployment("\n".join(script))

    ComputeEngine = get_driver(Provider.GCE)
    driver = ComputeEngine(
        launch_params.service_account,
        launch_params.credentials_path,
        datacenter=launch_params.datacenter,
        project=launch_params.project_id,
    )

    image = [
        im for im in driver.list_images() if "ubuntu" in im.name and "focal" in im.name
    ][0]

    # f1-micro
    size = [s for s in driver.list_sizes() if s.name == "e2-micro"][0]

    node = driver.deploy_node(
        name=instance_name,
        image=image,
        size=size,
        deploy=deploy,
        ssh_username="root",
        ex_metadata=launch_params.create_ex_metadata(),
        ssh_key=launch_params.private_ssh_key_path,
        ex_service_accounts=launch_params.create_ex_service_accounts(),
    )

    return node, deploy

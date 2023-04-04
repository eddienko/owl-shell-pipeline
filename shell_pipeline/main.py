import os
import subprocess
from pathlib import Path

import dask
from distributed import Client
from owl_dev import pipeline
from owl_dev.logging import logger

CMD_SCRIPT = "/tmp/cmd.sh"
CMD_TEMPLATE = """#!/bin/bash
set -e

{command}
"""


def run_command(command, env=None, cwd=None):
    with open(CMD_SCRIPT, "w") as fh:
        fh.write(CMD_TEMPLATE.format(command=command))
    os.chmod(CMD_SCRIPT, 0o744)

    res = subprocess.run(
        CMD_SCRIPT,
        env=env,
        cwd=cwd or Path.home(),
        shell=True,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )

    return res


@pipeline
def main(
    *,
    command: str,
    use_dask: bool,
    input_path: Path = None,
    output_path: Path = None,
    **kwargs,
):
    logger.info("Pipeline started")
    client: Client = Client.current()

    if output_path is None:
        output_path = str(Path("~").expanduser())

    if use_dask:
        scheduler = client._scheduler_identity["address"]
        logger.info("Using Dask scheduler at %s", scheduler)
        env = {"DASK_SCHEDULER_ADDRESS": scheduler}
        res = run_command(command, env=env, cwd=output_path)
    else:
        with dask.annotate(executor="processes", retries=2):
            fut = client.submit(run_command, command, cwd=output_path)
        res = client.gather(fut)

    if res.returncode == 0:
        logger.info("Command successful : %s", res.stdout.decode())
    else:
        logger.error("Command failed : %s", res.stderr.decode())
        raise Exception("Command failed")

    return res.returncode

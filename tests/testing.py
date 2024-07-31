import subprocess
import logging
import time

# NOTE: The image has to be available locally, it won't be pulled out from the registry, 
# so if running the tests for the first time, the docker image containing kvs service should be built
_KVS_SERVICE_IMAGE = "kvs:latest"

_logger = logging.getLogger(__name__)

def _is_kvs_docker_container_running() -> bool:
    """_summary_
    """
    container_names = subprocess.check_output(
        ["docker", "ps", "--format", "{{.Names}}"],
        universal_newlines=True
    )

    for cont in container_names.split("\n"):
        if cont == "kvs-service-emulator":
            return True

    return False

def start_kvs_docker_container(port: int=4040) -> tuple[subprocess.Popen, str]:
    """_summary_

    :param port: 
    :returns
    """
    # Keep forgetting that it's HOST_PORT:CONTAINER_PORT so leaving the comment here...
    process = subprocess.Popen(
        ["docker", "run", "--rm", "--name",
         "kvs-service-emulator", "-p", f"{port}:8080", _KVS_SERVICE_IMAGE],
        stdout=subprocess.PIPE,
    )

    # Wait for docker container running kvs-service to start up properly
    while _is_kvs_docker_container_running():
        time.sleep(0.3)

    _logger.info("running kvs service inside docker container")

    return process, f"http://localhost:{port}"


def kill_kvs_docker_container(process: subprocess.Popen) -> None:
    """_summary_
    
    :param process:
    """
    _logger.info("killing docker container")
    if ret_code := subprocess.call(["docker", "rm", "--force", "kvs-service-emulator"]) != 0:
        _logger.error("failed to kill docker container, status %s", ret_code)
        
    process.kill()
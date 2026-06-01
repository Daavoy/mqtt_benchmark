# adapted from GPT UiO
import argparse
import subprocess
import time
import os
from datetime import datetime


def run_docker_compose(compose_file, environment_variables):
    try:
        with subprocess.Popen(
            ["docker-compose", "-f", compose_file, "up", "-d"],
            env=environment_variables
        ) as proc:
            proc.wait()
            print("Docker Compose started successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e.returncode}")
        print(e.output)


def stop_docker_compose(compose_file, environment_variables):
    try:
        with subprocess.Popen(
            ["docker-compose", "-f", compose_file, "down"],
            env=environment_variables
        ) as proc:
            proc.wait()
            print("Docker Compose stopped successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e.returncode}")
        print(e.output)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, default=1, help="Topic structure config: 1 or 2")
    parser.add_argument("--qos", required=True, default=0, help="MQTT QoS: 0, 1 or 2")

    args = parser.parse_args()

    config  = args.config
    qos     = args.qos

    # Verify input benchmark parameters
    assert (config == 1 or config == 2)
    assert (0 <= qos <= 2)

    monitoring_file = 'monitoring-docker-compose.yml'
    sut_file = 'docker-compose-sop-sut-config1.yml'

    if not os.path.isfile(monitoring_file):
        print(f"Error monitoring docker compose file not found. Make sure the paths are correct.")
        return

    # Define environment variables
    environment_variables = dict()
    environment_variables["QOS"] = qos

    # Config 1
    if config == 1:
        logfile_prefix = '-'.join((datetime.now().strftime("%d%m%Y%H%M%S"),"config1","qos",str(qos)))
        logfile_name = '.'.join((logfile_prefix,"log"))
        logfile_path = '/'.join(("/app","logs",logfile_name))

        uwsn_file = 'docker-compose-data-provider-consumer-config1.yml'

        # Verify input file
        if not os.path.isfile(uwsn_file) or not os.path.isfile(sut_file):
            print(f"Error docker compose file not found. Make sure the paths are correct.")
            return

        environment_variables["PUBLISHER_FILE1"] = "configs/data-provider-1.yml"
        environment_variables["PUBLISHER_FILE2"] = "configs/data-provider-2.yml"

        environment_variables["SUBSCRIBER_FILE"] = "configs/subscriber.yml"
        environment_variables["LOG_FILE"] = logfile_path

        environment_variables["TRANSFORMER_FILE1"] = "deploy_wsensenodetransformer.sh"
        environment_variables["TRANSFORMER_CONFIGFILE1"] = "configs/config-provider2.yml"

        environment_variables["TRANSFORMER_FILE2"] = "deploy_aadinodetransformer.sh"
        environment_variables["TRANSFORMER_CONFIGFILE2"] = "configs/config-provider1.yml"

    # Config 2
    else:
        uwsn_file = 'docker-compose-data-provider-consumer-config2.yml'

        # Verify input file
        if not os.path.isfile(uwsn_file) or not os.path.isfile(sut_file):
            print(f"Error docker compose file not found. Make sure the paths are correct.")
            return

        logfile_prefix = '-'.join((datetime.now().strftime("%d%m%Y%H%M%S"), "config2", "qos", str(qos)))
        logfile_name = '.'.join((logfile_prefix, "log"))
        logfile_path = '/'.join(("/app", "logs", logfile_name))

        environment_variables["PUBLISHER_FILE11"] = "configs/data-provider-1-sensor-hub-1.yml"
        environment_variables["PUBLISHER_FILE12"] = "configs/data-provider-1-sensor-hub-2.yml"
        environment_variables["PUBLISHER_FILE21"] = "configs/data-provider-2-sensor-hub-1.yml"
        environment_variables["PUBLISHER_FILE22"] = "configs/data-provider-2-sensor-hub-2.yml"

        environment_variables["SUBSCRIBER_FILE"] = "configs/subscriber.yml"
        environment_variables["LOG_FILE"] = logfile_path

        environment_variables["TRANSFORMER_FILE11"] = "deploy_wsensenodetransformer.sh"
        environment_variables["TRANSFORMER_CONFIGFILE11"] = "configs/config-provider2-hub1.yml"

        environment_variables["TRANSFORMER_FILE12"] = "deploy_wsensenodetransformer.sh"
        environment_variables["TRANSFORMER_CONFIGFILE12"] = "configs/config-provider2-hub2.yml"

        environment_variables["TRANSFORMER_FILE21"] = "deploy_aadinodetransformer.sh"
        environment_variables["TRANSFORMER_CONFIGFILE21"] = "configs/config-provider1-hub1.yml"

        environment_variables["TRANSFORMER_FILE22"] = "deploy_aadinodetransformer.sh"
        environment_variables["TRANSFORMER_CONFIGFILE22"] = "configs/config-provider1-hub2.yml"

    # Run monitoring system
    run_docker_compose(monitoring_file, environment_variables)
    time.sleep(60)

    # Run SUT
    run_docker_compose(sut_file, environment_variables)
    time.sleep(60)

    # Run Virtual UWSN + subscriber
    run_docker_compose(uwsn_file, environment_variables)

    # Add logic here if you need to run tasks while services are up
    input("Press Enter to stop the Docker Compose setup...")

    stop_docker_compose(uwsn_file, environment_variables)
    stop_docker_compose(sut_file, environment_variables)
    stop_docker_compose(monitoring_file, environment_variables)
    

if __name__ == "__main__":
    main()

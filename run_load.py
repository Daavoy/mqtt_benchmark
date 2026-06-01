import argparse
import time
import subprocess

def run_docker_compose(compose_file):
    try:
        with subprocess.Popen(
            ["docker", "compose", "-f", compose_file, "up",
             "--force-recreate"]
        ) as proc:
            proc.wait()
            print("Docker Compose started successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e.returncode}")
        print(e.output)
        stop_docker_compose(compose_file)
        proc.kill()
    except Exception as e:
        proc.kill()
        print(f'Error executing experiment docker compose: {e}')


def stop_docker_compose(compose_file):
    try:
        with subprocess.Popen(
            ["docker", "compose", "-f", compose_file, "down"]
        ) as proc:
            proc.wait()
            print("Docker Compose stopped successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e.returncode}")
        print(e.output)

    except Exception as e:
        proc.kill()
        print(f'Error executing experiment docker compose: {e}')


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--numexecs", default=10, type=int, help="Number of executions for parameter")
    parser.add_argument("--stats", required=True, help="stats file")

    args = parser.parse_args()

    numexecs = args.numexecs

    # Verify input benchmark parameters
    assert (numexecs >= 1)

    print(f"Running {numexecs} repetitions. . .")

    # clients docker compose file
    load_docker_compose = "docker-compose-clients.yml"

    # Run docker stats
    try:
        with subprocess.Popen(
            ["/bin/bash", "record_stats.sh", args.stats]
        ) as proc:
            print("Docker stats recording started successfully.")

            for _exec in range(numexecs):
                # Run MQTT Clients
                print(f'Running execution {_exec}. . .')
                run_docker_compose(load_docker_compose)
                time.sleep(60)  # time between load repetition run -- KEEP ALIVE
            proc.kill()

    except Exception as e:
        stop_docker_compose(load_docker_compose)
        print(f'Error executing experiment docker compose: {e}')

    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e.returncode}")
        print(e.output)

    finally:
        print(f'Finished execution {exec}')


if __name__ == "__main__":
    main()
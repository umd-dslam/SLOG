#!/usr/bin/python3
"""Admin tool

This tool is used to control a cluster of SLOG servers. For example,
starting a cluster, stopping a cluster, getting status, and more.
"""
import docker
import ipaddress
import json
import logging
import os

import google.protobuf.text_format as text_format

from argparse import ArgumentParser
from datetime import datetime
from threading import Thread
from typing import Dict, List, Tuple

from docker.models.containers import Container
from paramiko.ssh_exception import PasswordRequiredException

from gen_data import add_exported_gen_data_arguments
from proto.configuration_pb2 import Configuration

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(process)d - %(levelname)s: %(message)s'
)
LOG = logging.getLogger("admin")

USER = "ubuntu"
CONTAINER_DATA_DIR = "/var/tmp"
HOST_DATA_DIR = "/var/tmp"

SLOG_IMG = "ctring/slog"
SLOG_CONTAINER_NAME = "slog"
SLOG_DATA_MOUNT = docker.types.Mount(
    target=CONTAINER_DATA_DIR,
    source=HOST_DATA_DIR,
    type="bind",
)
SLOG_CONFIG_FILE_NAME = "slog.conf"
CONTAINER_SLOG_CONFIG_FILE_PATH = os.path.join(
    CONTAINER_DATA_DIR,
    SLOG_CONFIG_FILE_NAME
)

BENCHMARK_CONTAINER_NAME = "benchmark"

def cleanup_container(
    client: docker.DockerClient,
    name: str,
    addr="",
) -> None:
    """
    Cleans up a container with a given name.
    """
    try:
        c = client.containers.get(name)
        c.remove(force=True)
        LOG.info(
            "%sCleaned up container \"%s\"",
            f"{addr}: " if addr else "",
            name,
        )
    except:
        pass


def get_container_status(client: docker.DockerClient, name: str) -> str:
    if client is None:
        return "network unavailable"
    else:
        try:
            c = client.containers.get(name)
            return c.status
        except docker.errors.NotFound:
            return "container not started"
        except:
            pass
    return "unknown"


def parse_envs(envs: List[str]) -> Dict[str, str]:
    '''Parses a list of environment variables

    This function transform a list of strings such as ["env1=a", "env2=b"] into:
    {
        env: a,
        env: b,
    }

    '''
    if envs is None:
        return {}
    env_var_tuples = [env.split('=') for env in envs]
    return {env[0]: env[1] for env in env_var_tuples}


class Command:
    """Base class for a command.

    This class contains the common implementation of all commands in this tool
    such as parsing of common arguments, loading config file, and pulling new
    SLOG image from docker repository.

    All commands must extend from this class. A command may or may not override
    any method but it must override and implement the `do_command` method as
    well as all class variables to describe the command.
    """

    NAME = "<not_implemented>"
    HELP = ""
    CONFIG_FILE_REQUIRED = True

    def __init__(self):
        self.config = None

    def create_subparser(self, subparsers):
        parser = subparsers.add_parser(self.NAME, help=self.HELP)
        parser.set_defaults(run=self.__initialize_and_do_command)
        self.add_arguments(parser)
        return parser

    def add_arguments(self, parser):
        parser.add_argument(
            "config",
            nargs='?',
            default="",
            metavar="config_file",
            help="Path to a config file",
        )
        parser.add_argument(
            "--no-pull",
            action="store_true",
            help="Skip image pulling step"
        )
        parser.add_argument(
            "--image",
            default=SLOG_IMG,
            help="Name of the Docker image to use"
        )
        parser.add_argument(
            "--user", "-u",
            default=USER,
            help="Username of the target machines"
        )

    def __initialize_and_do_command(self, args):
        # The initialization phase is broken down into smaller methods so
        # that subclasses can override them with different behaviors
        self.load_config(args)
        self.init_clients(args)
        self.pull_slog_image(args)
        # Perform the command
        self.do_command(args)

    def load_config(self, args):
        if not os.path.isfile(args.config):
            if self.CONFIG_FILE_REQUIRED:
                raise FileNotFoundError(f'Config file does not exist: "{args.config}"')
            else:
                return
        with open(args.config, "r") as f:
            self.config = Configuration()
            text_format.Parse(f.read(), self.config)

    
    def init_clients(self, args):
        # The rep_to_client variable is a list of lists of Docker clients.
        # The replica and partition numbers match to the indices in the lists.
        # For example, given the following machines 
        # in the format (replica, partition, address):
        #   (0, 0, 192.168.1.1), (0, 1, 192.168.1.2), (1, 0, 192.168.1.3)
        # The rep_to_clients list will be:
        #   [
        #       [
        #           (<Docker client>, 192.168.1.1),
        #           (<Docker client>, 192.168.1.2),
        #       ],
        #       [
        #           (<Docker client>, 192.168.1.3),
        #       ]
        #   ]
        self.rep_to_clients = []
        self.num_clients = 0
        # Create a docker client for each node
        for rep in self.config.replicas:
            rep_clients = []
            for addr in rep.addresses:
                addr_str = addr.decode()
                try:
                    client = self.new_client(args.user, addr_str)
                    rep_clients.append((client, addr_str))
                    self.num_clients += 1
                    LOG.info("Connected to %s", addr_str)
                except PasswordRequiredException as e:
                    rep_clients.append((None, addr_str))
                    LOG.error(
                        "Failed to authenticate when trying to connect to %s. "
                        "Check username or key files",
                        f"{args.user}@{addr_str}",
                    )
                except Exception as e:
                    rep_clients.append((None, addr_str))
                    LOG.exception(e)

            self.rep_to_clients.append(rep_clients)

    def pull_slog_image(self, args):
        if self.num_clients == 0 or args.no_pull:
            LOG.info(
                "Skipped image pulling. Using the local version of \"%s\"", 
                args.image,
            )
            return
        LOG.info(
            "Pulling SLOG image for each node. "
            "This might take a while."
        )

        threads = []
        for client, addr in self.clients():
            LOG.info(
                "%s: Pulling latest docker image \"%s\"...",
                addr,
                args.image,
            )
            th = Thread(target=client.images.pull, args=(args.image,))
            th.start()
            threads.append(th)

        for th in threads:
            th.join()

    def do_command(self, args):
        raise NotImplementedError

    ##############################
    #       Helper methods
    ##############################
    def new_client(self, user, addr):
        """
        Gets a new Docker client for a given address.
        """
        return docker.DockerClient(
            base_url=f'ssh://{user}@{addr}',
        )

    def clients(self) -> Tuple[docker.DockerClient, str]:
        """
        Generator to iterate through all available docker clients.
        """
        for clients in self.rep_to_clients:
            for (client, addr) in clients:
                if client is not None:
                    yield (client, addr)
    
    def wait_for_containers(
        self,
        containers: List[Tuple[Container, str]]
    ) -> None:
        """
        Waits until all given containers stop.
        """
        for c, addr in containers:
            res = c.wait()
            if res['StatusCode'] == 0:
                LOG.info("%s: Done", addr)
            else:
                LOG.error(
                    "%s: Finished with non-zero status (%d). "
                    "Check the logs of the container \"%s\" for more details",
                    addr,
                    res['StatusCode'],
                    c.name,
                )


class GenDataCommand(Command):

    NAME = "gen_data"
    HELP = "Generate data for one or more SLOG servers"

    def add_arguments(self, parser):
        super().add_arguments(parser)
        add_exported_gen_data_arguments(parser)

    def do_command(self, args):
        shell_cmd = (
            f"tools/gen_data.py {CONTAINER_DATA_DIR} "
            f"--num-replicas {len(self.config.replicas)} "
            f"--num-partitions {self.config.num_partitions} "
            f"--partition-bytes {self.config.hash_partitioning.partition_key_num_bytes} "
            f"--partition {args.partition} "
            f"--size {args.size} "
            f"--size-unit {args.size_unit} "
            f"--record-size {args.record_size} "
            f"--max-jobs {args.max_jobs} "
        )
        containers = []
        for client, addr in self.clients():
            cleanup_container(client, self.NAME, addr=addr)
            LOG.info(
                "%s: Running command: %s",
                addr,
                shell_cmd
            )
            c = client.containers.create(
                args.image,
                name=self.NAME,
                command=shell_cmd,
                # Mount a directory on the host into the container
                mounts=[SLOG_DATA_MOUNT],
            )
            c.start()
            containers.append((c, addr))
        
        self.wait_for_containers(containers)
        

class StartCommand(Command):

    NAME = "start"
    HELP = "Start an SLOG cluster"

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "-e",
            nargs="*",
            help="Environment variables to pass to the container. For example, "
                 "use -e GLOG_v=1 to turn on verbose logging at level 1."
        )
        
    def do_command(self, args):
        # Prepare a command to update the config file
        config_text = text_format.MessageToString(self.config)
        sync_config_cmd = (
            f"echo '{config_text}' > {CONTAINER_SLOG_CONFIG_FILE_PATH}"
        )

        # Clean up everything first so that the old running session does not
        # mess up with broker synchronization of the new session
        for rep, clients in enumerate(self.rep_to_clients):
            for client, addr in clients:
                cleanup_container(client, SLOG_CONTAINER_NAME, addr=addr)

        for rep, clients in enumerate(self.rep_to_clients):
            for part, (client, addr) in enumerate(clients):
                if client is None:
                    # Skip this node because we cannot
                    # initialize a docker client
                    continue
                shell_cmd = (
                    f"slog "
                    f"--config {CONTAINER_SLOG_CONFIG_FILE_PATH} "
                    f"--address {addr} "
                    f"--replica {rep} "
                    f"--partition {part} "
                    f"--data-dir {CONTAINER_DATA_DIR} "
                )
                client.containers.run(
                    args.image,
                    name=SLOG_CONTAINER_NAME,
                    command=[
                        "/bin/sh", "-c",
                        f"{sync_config_cmd} && {shell_cmd}",
                    ],
                    # Mount a directory on the host into the container
                    mounts=[SLOG_DATA_MOUNT],
                    # Expose all ports from container to host
                    network_mode="host",
                    # Avoid hanging this tool after starting the server
                    detach=True,
                    environment=parse_envs(args.e),
                )
                LOG.info(
                    "%s: Synced config and ran command: %s",
                    addr,
                    shell_cmd
                )


class StopCommand(Command):

    NAME = "stop"
    HELP = "Stop an SLOG cluster"

    def pull_slog_image(self, args):
        """
        Override this method to skip the image pulling step.
        """
        pass
        
    def do_command(self, args):
        for (client, addr) in self.clients():
            try:
                LOG.info("Stopping SLOG on %s...", addr)
                c = client.containers.get(SLOG_CONTAINER_NAME)
                # Set timeout to 0 to kill the container immediately
                c.stop(timeout=0)
            except docker.errors.NotFound:
                pass


class StatusCommand(Command):

    NAME = "status"
    HELP = "Show the status of an SLOG cluster"

    def pull_slog_image(self, args):
        """
        Override this method to skip the image pulling step.
        """
        pass
        
    def do_command(self, args):
        for rep, clients in enumerate(self.rep_to_clients):
            print(f"Replica {rep}:")
            for part, (client, addr) in enumerate(clients):
                status = get_container_status(client, SLOG_CONTAINER_NAME)
                print(f"\tPartition {part} ({addr}): {status}")


class LogsCommand(Command):

    NAME = "logs"
    HELP = "Stream logs from a server"
    CONFIG_FILE_REQUIRED = False

    def add_arguments(self, parser):
        super().add_arguments(parser)
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            "-a",
            metavar='ADDRESS',
            help="Address of the machine to stream logs from"
        )
        group.add_argument(
            "-rp",
            nargs=2,
            type=int,
            metavar=('REPLICA', 'PARTITION'),
            help="Two numbers representing the replica and"
                 "partition of the machine to stream log from."
        )
        parser.add_argument(
            "-f", "--follow", 
            action="store_true",
            help="Follow log output"
        )
        parser.add_argument(
            "--container",
            default=SLOG_CONTAINER_NAME,
            help="Name of the Docker container to show logs from"
        )

    def init_clients(self, args):
        """
        Override this method because we only need one client
        """
        self.client = None
        self.addr = None
        try:
            if args.a is not None:
                if self.config is None:
                    self.addr = args.a
                else:
                    # Check if the given address is specified in the config
                    addr_is_in_replica = (
                        args.a.encode() in rep.addresses
                        for rep in self.config.replicas
                    )
                    if any(addr_is_in_replica):
                        self.addr = args.a
                    else:
                        LOG.error(
                            "Address \"%s\" is not specified in the config", args.a
                        )
                        return
            else:
                if self.config is None:
                    raise Exception('The "-rp" flag requires a valid config file')
                r, p = args.rp
                self.addr = self.config.replicas[r].addresses[p].decode()

            self.client = self.new_client(args.user, self.addr)
            LOG.info("Connected to %s", self.addr)
        except PasswordRequiredException as e:
            LOG.error(
                "Failed to authenticate when trying to connect to %s. "
                "Check username or key files",
                f"{args.user}@{self.addr}",
            )

    def pull_slog_image(self, args):
        """
        Override this method to skip the image pulling step.
        """
        pass

    def do_command(self, args):
        if self.client is None:
            return

        try:
            c = self.client.containers.get(args.container)
        except docker.errors.NotFound:
            LOG.error("Cannot find container \"%s\"", args.container)
            return

        if args.follow:
            try:
                for log in c.logs(stream=True, follow=True):
                    print(log.decode(), end="")
            except KeyboardInterrupt:
                print()
        else:
            print(c.logs().decode(), end="")


class LocalCommand(Command):

    NAME = "local"
    HELP = "Control a cluster that run on the local machine"

    # Local network
    NETWORK_NAME = "slog_nw"
    SUBNET = "172.28.0.0/16"
    IP_RANGE = "172.28.5.0/24"

    def add_arguments(self, parser):
        super().add_arguments(parser)
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            "--start",
            action='store_true',
            help="Start the local cluster"
        )
        group.add_argument(
            "--stop",
            action='store_true',
            help="Stop the local cluster"
        )
        group.add_argument(
            "--remove",
            action="store_true",
            help="Remove all containers of the local cluster"
        )
        group.add_argument(
            "--status",
            action="store_true",
            help="Get status of the local cluster",
        )
        parser.add_argument(
            "-e",
            nargs="*",
            help="Environment variables to pass to the container. For example, "
                 "use -e GLOG_v=1 to turn on verbose logging at level 1."
        )

    def load_config(self, args):
        super().load_config(args)
        # Replace the addresses in the config with auto-generated addresses
        address_generator = ipaddress.ip_network(self.IP_RANGE).hosts()
        for rep in self.config.replicas:
            del rep.addresses[:]
            for p in range(self.config.num_partitions):
                ip_address = str(next(address_generator))
                rep.addresses.append(ip_address.encode())

    def init_clients(self, args):
        """
        Override this method because we only need one client
        """
        self.client = docker.from_env()

    def pull_slog_image(self, args):
        """
        Override this method because we only pull image for one client
        """
        if self.client is None or args.no_pull:
            LOG.info(
                "Skipped image pulling. Using the local version of \"%s\"", 
                args.image,
            )
            return
        LOG.info("Pulling latest docker image \"%s\"...", args.image)
        self.client.images.pull(args.image)

    def do_command(self, args):
        if self.client is None:
            return

        if args.start:
            self.__start()
        elif args.stop:
            self.__stop()
        elif args.remove:
            self.__remove()
        elif args.status:
            self.__status()

    def __start(self):
        #
        # Create a local network if one does not exist
        #
        nw_list = self.client.networks.list(names=[self.NETWORK_NAME])
        if nw_list:
            slog_nw = nw_list[0]
            LOG.info("Reused network \"%s\"", self.NETWORK_NAME)
        else:
            ipam_pool = docker.types.IPAMPool(
                subnet=self.SUBNET,
                iprange=self.IP_RANGE
            )
            ipam_config = docker.types.IPAMConfig(pool_configs=[ipam_pool])
            slog_nw = self.client.networks.create(
                name=self.NETWORK_NAME,
                driver="bridge",
                check_duplicate=True,
                ipam=ipam_config,
            )
            LOG.info("Created network \"%s\"", self.NETWORK_NAME)

        #
        # Spin up local Docker containers and connect them to the network
        #
        config_text = text_format.MessageToString(self.config)
        sync_config_cmd = (
            f"echo '{config_text}' > {CONTAINER_SLOG_CONFIG_FILE_PATH}"
        )

        # Clean up everything first so that the old running session does not
        # mess up with broker synchronization of the new session
        for r, rep in enumerate(self.config.replicas):
            for p, _ in enumerate(rep.addresses):
                container_name = f"slog_{r}_{p}"
                cleanup_container(self.client, container_name)

        for r, rep in enumerate(self.config.replicas):
            for p, addr_b in enumerate(rep.addresses):
                addr = addr_b.decode()
                shell_cmd = (
                    f"slog "
                    f"--config {CONTAINER_SLOG_CONFIG_FILE_PATH} "
                    f"--address {addr} "
                    f"--replica {r} "
                    f"--partition {p} "
                    f"--data-dir {CONTAINER_DATA_DIR} "
                )
                container_name = f"slog_{r}_{p}"
                # Create and run the container
                container = self.client.containers.create(
                    args.image,
                    name=container_name,
                    command=[
                        "/bin/sh", "-c",
                        f"{sync_config_cmd} && {shell_cmd}",
                    ],
                    mounts=[SLOG_DATA_MOUNT],
                    environment=parse_envs(args.e)
                )

                # Connect the container to the custom network.
                # This has to happen before we start the container.
                slog_nw.connect(container, ipv4_address=addr)

                # Actually start the container
                container.start()

                LOG.info(
                    "%s: Synced config and ran command: %s",
                    addr,
                    shell_cmd,
                )
    
    def __stop(self):
        for r in range(len(self.config.replicas)):
            for p in range (self.config.num_partitions):
                try:
                    container_name = f"slog_{r}_{p}"
                    LOG.info("Stopping \"%s\"", container_name)
                    c = self.client.containers.get(container_name)
                    c.stop(timeout=0)
                except docker.errors.NotFound:
                    pass
    
    def __remove(self):
        for r in range(len(self.config.replicas)):
            for p in range (self.config.num_partitions):
                container_name = f"slog_{r}_{p}"
                cleanup_container(self.client, container_name)

    def __status(self):
        for r, rep in enumerate(self.config.replicas):
            print(f"Replica {r}:")
            for p, addr in enumerate(rep.addresses):
                container_name = f"slog_{r}_{p}"
                status = get_container_status(self.client, container_name)
                print(f"\tPartition {p} ({addr.decode()}): {status}")


class BenchmarkCommand(Command):

    NAME = "benchmark"
    HELP = "Spawn distributed clients to run benchmark"

    def add_arguments(self, parser):
        super().add_arguments(parser)
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            "--num-txns", type=int,
            help="Number of transactions sent per client"
        )
        group.add_argument(
            "--duration", type=int,
            help="How long the benchmark is run in seconds"
        )
        parser.add_argument(
            "--clients",
            required=True,
            help="Path to a json file containing lists of clients"
        )
        parser.add_argument(
            "--workload", "-wl",
            default="basic",
            help="Name of the workload to run benchmark with"
        )
        parser.add_argument(
            "--params",
            default="",
            help="Parameters of the workload"
        )
        parser.add_argument(
            "--rate", type=int,
            default=1000,
            help="Maximum number of transactions sent per second"
        )
        parser.add_argument(
            "-e",
            nargs="*",
            help="Environment variables to pass to the container. For example, "
                 "use -e GLOG_v=1 to turn on verbose logging at level 1."
        )

    def init_clients(self, args):
        """
        Override this method because the clients of this command are created from
        the addresses specified in a json file instead of from the configuration file
        """
        # This rep_to_clients variable is slightly different in this benchmark command
        # than the global default. Here, only the indices of the replica have any
        # significance while the indices of the elements of each inner list do not
        # have any meaning because each benchmarking client does not correspond to a partition
        self.rep_to_clients = []
        self.num_clients = 0
        with open(args.clients, 'r') as f:
            # Load the list of clients from the json file
            benchmark_clients = json.load(f)
            for rep, clients_in_rep in enumerate(benchmark_clients):
                rep_clients = []
                for addr_str in clients_in_rep:
                    try:
                        docker_client = self.new_client(args.user, addr_str)
                        rep_clients.append((docker_client, addr_str))
                        self.num_clients += 1
                        LOG.info("Connected to %s", addr_str)
                    except PasswordRequiredException as e:
                        rep_clients.append((None, addr_str))
                        LOG.error(
                            "Failed to authenticate when trying to connect to %s. "
                            "Check username or key files",
                            f"{args.user}@{addr_str}",
                        )
                    except Exception as e:
                        rep_clients.append((None, addr_str))
                        LOG.exception(e)
                
                self.rep_to_clients.append(rep_clients)


    def do_command(self, args):
        # Prepare a command to update the config file
        config_text = text_format.MessageToString(self.config)
        sync_config_cmd = (
            f"echo '{config_text}' > {CONTAINER_SLOG_CONFIG_FILE_PATH}"
        )

        tag = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        output_dir = os.path.join(CONTAINER_DATA_DIR, f"slog-benchmark-{tag}")
        mkdir_cmd = f"mkdir -p {output_dir}"

        # Clean up everything
        for rep, clients in enumerate(self.rep_to_clients):
            for client, addr in clients:
                cleanup_container(client, BENCHMARK_CONTAINER_NAME, addr=addr)

        for rep, clients in enumerate(self.rep_to_clients):
            for client, addr in clients:
                if client is None:
                    # Skip this node because we cannot
                    # initialize a docker client
                    continue
                shell_cmd = (
                    f"benchmark "
                    f"--config {CONTAINER_SLOG_CONFIG_FILE_PATH} "
                    f"--r {rep} "
                    f"--data-dir {CONTAINER_DATA_DIR} "
                    f"--out-dir {output_dir} "
                    f"--wl {args.workload} "
                    f'--params="{args.params}" '
                    f"--rate {args.rate} "
                )
                if args.num_txns:
                    shell_cmd += f"--num_txns {args.num_txns} "
                else:
                    shell_cmd += f"--duration {args.duration} "
                client.containers.run(
                    args.image,
                    name=BENCHMARK_CONTAINER_NAME,
                    command=[
                        "/bin/sh", "-c",
                        f"{sync_config_cmd} && {mkdir_cmd} && {shell_cmd}"
                    ],
                    # Mount a directory on the host into the container
                    mounts=[SLOG_DATA_MOUNT],
                    # Expose all ports from container to host
                    network_mode="host",
                    # Avoid hanging this tool after starting the server
                    detach=True,
                    environment=parse_envs(args.e),
                )
                LOG.info(
                    "%s: Synced config and ran command: %s",
                    addr,
                    shell_cmd
                )

        LOG.info("Output dir: %s", output_dir)


class CollectCommand(Command):

    NAME = "collect"
    HELP = "Collect benchmark data from the clients"

    def add_arguments(self, parser):
        parser.add_argument(
            "name",
            help="Name of the directory containing benchmark data"
        )
        parser.add_argument(
            "clients",
            help="Path to a json file containing lists of clients"
        )
        parser.add_argument(
            "--out-dir",
            default='',
            help="Directory to put the collected data"
        )
        parser.add_argument(
            "--user", "-u",
            default=USER,
            help="Username of the target machines"
        )

    def load_config(self, args):
        pass

    def init_clients(self, args):
        pass

    def pull_slog_image(self, args):
        pass

    def do_command(self, args):
        data_path = os.path.join(HOST_DATA_DIR, args.name, '*.csv')
        out_path = os.path.join(args.out_dir, args.name)
        
        if not os.path.exists(out_path):
            os.mkdir(out_path)
            LOG.info(f"Created directory: {out_path}")

        commands = []
        with open(args.clients, 'r') as f:
            # Load the list of clients from the json file
            benchmark_clients = json.load(f)
            for addr_list in benchmark_clients:
                for addr in addr_list:
                    out_path_per_client = os.path.join(out_path, addr)
                    
                    if not os.path.exists(out_path_per_client):
                        os.mkdir(out_path_per_client)
                        LOG.info(f"Created directory: {out_path_per_client}")

                    commands.append(
                        f'scp {args.user}@{addr}:{data_path} {out_path_per_client}'
                    )
        
        LOG.info("Executing commands %s", ';'.join(commands))
        os.system(';'.join(commands))


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Controls deployment and experiment of SLOG"
    )
    subparsers = parser.add_subparsers(dest="command name")
    subparsers.required = True

    COMMANDS = [
        BenchmarkCommand,
        CollectCommand,
        GenDataCommand,
        StartCommand,
        StopCommand,
        StatusCommand,
        LogsCommand,
        LocalCommand,
    ]
    for command in COMMANDS:
        command().create_subparser(subparsers)

    args = parser.parse_args()
    args.run(args)
#!/usr/bin/python3
"""Admin tool

This tool is used to automate controlling a cluster of SLOG servers with tasks
such as starting a cluster, stopping a cluster, getting status, and more.
"""
import docker
import logging
import os

import google.protobuf.text_format as text_format

from argparse import ArgumentParser
from typing import List, Tuple

from docker.models.containers import Container

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
SLOG_CONFIG_FILE_PATH = os.path.join(
    CONTAINER_DATA_DIR,
    SLOG_CONFIG_FILE_NAME
)


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
        LOG.info("%s: Cleaned up container \"%s\"", addr, name)
    except:
        pass


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

    def __init__(self):
        self.rep_to_clients = []
        self.num_clients = 0
        self.config = None

    def create_subparser(self, subparsers):
        parser = subparsers.add_parser(self.NAME, help=self.HELP)
        parser.add_argument(
            "config",
            metavar="config_file",
            help="Path to a config file"
        )
        parser.add_argument(
            "--local",
            action='store_true',
            help="Run the command on the local machine"
        )
        parser.set_defaults(run=self.__initialize_and_do_command)
        return parser

    def __initialize_and_do_command(self, args):
        self.load_config(args)
        self.init_clients(args)
        self.pull_slog_image(args)

        self.do_command(args)

    def load_config(self, args):
        with open(args.config, "r") as f:
            self.config = Configuration()
            text_format.Parse(f.read(), self.config)
    
    def init_clients(self, args):
        self.rep_to_clients = []
        self.num_clients = 0
        for rep in self.config.replicas:
            rep_clients = []
            for addr in rep.addresses:
                addr_str = addr.decode()
                try:
                    client = self.new_client(addr_str)
                    rep_clients.append((client, addr_str))
                    self.num_clients += 1
                    LOG.info("Connected to %s", addr_str)
                except Exception as e:
                    rep_clients.append((None, addr_str))
                    LOG.error(str(e))

            self.rep_to_clients.append(rep_clients)

    def pull_slog_image(self, args):
        if self.num_clients == 0:
            return
        LOG.info(
            "Pulling SLOG image for each node. "
            "This might take a while on first run."
        )
        # TODO(ctring): Use multiprocessing to parallelizing
        #               image pulling across machines.
        for client, addr in self.clients():
            LOG.info(
                "%s: Pulling latest docker image \"%s\"...",
                addr,
                SLOG_IMG,
            )
            client.images.pull(SLOG_IMG)

    def do_command(self, args):
        raise NotImplementedError

    ##############################
    #       Helper methods
    ##############################
    def new_client(self, addr):
        """
        Gets a new Docker client for a given address.
        """
        return docker.DockerClient(
            base_url=f'ssh://{USER}@{addr}',
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
                self.NAME,
            )


class GenDataCommand(Command):

    NAME = "gen_data"
    HELP = "Generate data for one or more SLOG servers"

    def create_subparser(self, subparsers):
        parser = super().create_subparser(subparsers)
        add_exported_gen_data_arguments(parser)

    def do_command(self, args):
        shell_cmd = (
            f"tools/gen_data.py {CONTAINER_DATA_DIR} "
            f"--num-replicas {len(self.config.replicas)} "
            f"--num-partitions {self.config.num_partitions} "
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
                SLOG_IMG,
                name=self.NAME,
                command=shell_cmd,
                mounts=[SLOG_DATA_MOUNT],
            )
            c.start()
            containers.append((c, addr))
        
        self.wait_for_containers(containers)
        

class StartCommand(Command):

    NAME = "start"
    HELP = "Start an SLOG cluster"
        
    def do_command(self, args):
        config_text = text_format.MessageToString(self.config)
        sync_config_cmd = (
            f"echo '{config_text}' > {SLOG_CONFIG_FILE_PATH}"
        )
        broker_port = self.config.broker_port
        server_port = self.config.server_port
        port_mapping = {
            broker_port: broker_port,
            server_port: server_port,
        }
        for rep, clients in enumerate(self.rep_to_clients):
            for part, (client, addr) in enumerate(clients):
                if client is None:
                    # Skip this node because we cannot
                    # initialize a docker client
                    continue
                shell_cmd = (
                    f"slog "
                    f"--config {SLOG_CONFIG_FILE_PATH} "
                    f"--address {addr} "
                    f"--replica {rep} "
                    f"--partition {part} "
                    f"--data-dir {CONTAINER_DATA_DIR} "
                )
                cleanup_container(client, SLOG_CONTAINER_NAME, addr=addr)
                client.containers.run(
                    SLOG_IMG,
                    name=SLOG_CONTAINER_NAME,
                    command=[
                        "/bin/sh", "-c",
                        sync_config_cmd + " && " +
                        shell_cmd
                    ],
                    mounts=[SLOG_DATA_MOUNT],
                    ports=port_mapping,
                    detach=True,
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
                status = "unknown"
                if client is None:
                    status = "network unavailable"
                else:
                    try:
                        c = client.containers.get(SLOG_CONTAINER_NAME)
                        status = c.status
                    except docker.errors.NotFound:
                        status = "container not started"
                    except:
                        pass
                print(f"\tPartition {part} ({addr}): {status}")


class LogsCommand(Command):

    NAME = "logs"
    HELP = "Stream logs from a server"

    def create_subparser(self, subparsers):
        parser = super().create_subparser(subparsers)
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

    def init_clients(self, args):
        """
        Override this method because we only need one client
        """
        self.client = None
        self.addr = None
        try:
            if args.a is not None:
                addr_is_in_replica = (
                    args.a.encode() in rep.addresses
                    for rep in self.config.replicas
                )
                if all(addr_is_in_replica):
                    self.addr = args.a
                else:
                    LOG.error(
                        "Address \"%s\" is not specified in the config", args.a
                    )
                    return
            else:
                r, p = args.rp
                self.addr = self.config.replicas[r].addresses[p].decode()

            self.client = self.new_client(self.addr)
            LOG.info("Connected to %s", self.addr)
        except Exception as e:
            LOG.error(str(e))

    def do_command(self, args):
        if self.client is None:
            return

        try:
            c = self.client.containers.get(SLOG_CONTAINER_NAME)
        except docker.errors.NotFound:
            LOG.error("Cannot find container \"%s\"", SLOG_CONTAINER_NAME)
            return

        if args.follow:
            try:
                for log in c.logs(stream=True, follow=True):
                    print(log.decode(), end="")
            except KeyboardInterrupt:
                print()
        else:
            print(c.logs().decode(), end="")


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Controls deployment and experiment of SLOG"
    )
    subparsers = parser.add_subparsers(dest="command name")
    subparsers.required = True

    COMMANDS = [
        GenDataCommand,
        StartCommand,
        StopCommand,
        StatusCommand,
        LogsCommand,
    ]
    for command in COMMANDS:
        command().create_subparser(subparsers)

    args = parser.parse_args()
    args.run(args)
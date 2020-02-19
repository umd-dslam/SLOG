#!/usr/bin/python3

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


class Command:

    NAME = "<not_implemented>"
    HELP = ""

    def __init__(self):
        self.rep_to_clients = []
        self.config = None

    def create_subparser(self, subparsers):
        parser = subparsers.add_parser(self.NAME, help=self.HELP)
        parser.add_argument("config", help="Path to a config file")
        parser.add_argument(
            "--local",
            action='store_true',
            help="Run the command on the local machine"
        )
        parser.set_defaults(func=self.__initialize_and_do_command)
        return parser

    def __initialize_and_do_command(self, args):
        self.initialize(args)
        self.do_command(args)

    def initialize(self, args):
        with open(args.config, "r") as f:
            self.config = Configuration()
            text_format.Parse(f.read(), self.config)
        
        # Initialize Docker clients
        self.rep_to_clients = []
        for rep in self.config.replicas:
            rep_clients = []
            for addr in rep.addresses:
                addr_str = addr.decode()
                try:
                    client = docker.DockerClient(
                        base_url=f'ssh://{USER}@{addr_str}',
                    )
                    rep_clients.append((client, addr_str))
                    LOG.info("Connected to %s", addr_str)
                except:
                    LOG.exception("Unable to connect to %s", addr_str)

            self.rep_to_clients.append(rep_clients)

        LOG.info(
            "Pulling SLOG image for each node. "
            "This might take a while on first run."
        )
        # TODO(ctring): Use multiprocessing here to parallelizing image pulling
        for client, addr in self.clients():
            LOG.info(
                "Pulling latest docker image \"%s\" for %s...",
                SLOG_IMG,
                addr,
            )
            client.images.pull(SLOG_IMG)

    def do_command(self, args):
        raise NotImplementedError

    ##############################
    #       Helper methods
    ##############################
    def clients(self):
        '''
        Generator to iterate through all docker clients
        '''
        for clients in self.rep_to_clients:
            for client in clients:
                yield client
    
    def cleanup_container(
        self,
        client: docker.DockerClient,
        addr: str,
    ) -> None:
        '''
        Cleans up the container for a client
        '''
        try:
            c = client.containers.get(self.NAME)
            c.remove(force=True)
            LOG.info("Cleaned up container \"%s\" on %s", self.NAME, addr)
        except:
            pass
    
    def wait_for_containers(
        self,
        containers: List[Tuple[Container, str]]
    ) -> None:
        for c, addr in containers:
            res = c.wait()
        if res['StatusCode'] == 0:
            LOG.info("%s finished successfully", addr)
        else:
            LOG.error(
                "%s finished with non-zero status (%d). "
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
        LOG.info("Command to run: %s", shell_cmd)
        containers = []
        for client, addr in self.clients():
            self.cleanup_container(client, addr)
            try:
                c = client.containers.create(
                    SLOG_IMG,
                    name=self.NAME,
                    command=shell_cmd,
                    mounts=[SLOG_DATA_MOUNT],
                )
                c.start()
                containers.append((c, addr))
                LOG.info(
                    "%s: ran command: %s",
                    addr,
                    shell_cmd
                )
            except:
                LOG.exception("Unable to run command on %s", addr)
        
        self.wait_for_containers(containers)
        

class StartCommand(Command):

    NAME = "start"
    HELP = "Start an SLOG cluster"
        
    def do_command(self, args):
        config_text = text_format.MessageToString(self.config)
        sync_config_cmd = (
            f"echo '{config_text}' > {SLOG_CONFIG_FILE_PATH}"
        )
        for rep, clients in enumerate(self.rep_to_clients):
            for part, (client, addr) in enumerate(clients):
                shell_cmd = (
                    f"slog "
                    f"--config {SLOG_CONFIG_FILE_PATH} "
                    f"--address {addr} "
                    f"--replica {rep} "
                    f"--partition {part} "
                    f"--data-dir {CONTAINER_DATA_DIR} "
                )
                self.cleanup_container(client, addr)
                try:
                    client.containers.run(
                        SLOG_IMG,
                        name=self.NAME,
                        command=[
                            "/bin/sh", "-c",
                            sync_config_cmd + " && " +
                            shell_cmd
                        ],
                        mounts=[SLOG_DATA_MOUNT],
                        detach=True,
                    )
                    LOG.info(
                        "%s: synced config and ran command: %s",
                        addr,
                        shell_cmd
                    )
                except:
                    LOG.exception(
                        "Unable to run command on %s", addr
                    )


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Controls deployment and experiment of SLOG"
    )
    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True

    COMMANDS = [
        GenDataCommand,
        StartCommand,
    ]
    for command in COMMANDS:
        command().create_subparser(subparsers)

    args = parser.parse_args()
    args.func(args)
import boto3
import json
import logging
import time

from subprocess import Popen, PIPE

from common import Command, initialize_and_run_commands
from tabulate import tabulate

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(process)d - %(levelname)s: %(message)s'
)
LOG = logging.getLogger("spot_cluster")
MAX_RETRIES = 6

INSTALL_DOCKER_COMMAND = (
  "curl -fsSL https://get.docker.com -o get-docker.sh && "
  "sh get-docker.sh && "
  "sudo usermod -aG docker ubuntu "
)


def shorten_output(out):
    MAX_LINES = 10
    lines = out.split('\n')
    if len(lines) < MAX_LINES:
        return out
    removed_lines = len(lines) - MAX_LINES
    return '\n'.join(
        [f'... {removed_lines} LINES ELIDED ...'] + lines[:MAX_LINES]
    )


def install_docker(instance_ips):
    # Install Docker
    installer_procs = []
    for region, ips in instance_ips.items():
        for ip in ips:
            command = [
                "ssh", "-o", "StrictHostKeyChecking no",
                f"ubuntu@{ip}",
                INSTALL_DOCKER_COMMAND
            ]
            LOG.info(
                '%s [%s]: Running command "%s"', region, ip, command
            )
            process = Popen(command, stdout=PIPE, stderr=PIPE)
            installer_procs.append((region, ip, process))
    
    for region, ip, p in installer_procs:
        p.wait()
        out, err = p.communicate()
        out = shorten_output(out.decode().strip())
        err = shorten_output(err.decode().strip())
        message = f"{region} [{ip}]: Done"
        if out:
            message += f'\nSTDOUT:\n\t{out}'
        if err:
            message += f'\nSTDERR:\n\t{err}'
        LOG.info(message)


def print_instance_ips(instance_ips, label="IP ADDRESSES"):
    print(f"\n================== {label} ==================\n")
    print(json.dumps(instance_ips, indent=2))


def print_slog_config_fragment(instance_public_ips, instance_private_ips, num_clients):
    print("\n================== SLOG CONFIG FRAGMENT ==================\n")
    slog_configs = []
    for region in instance_public_ips:
        public_ips = instance_public_ips[region]
        private_ips = instance_private_ips[region]
        private_server_ips = private_ips[num_clients:]
        client_ips, public_server_ips = public_ips[:num_clients], public_ips[num_clients:]

        private_server_ips_str = [f'  addresses: "{ip}"' for ip in private_server_ips]
        public_server_ips_str = [f'  public_addresses: "{ip}"' for ip in public_server_ips]
        clients = [f'  client_addresses: "{ip}"' for ip in client_ips]
        slog_configs.append(
            'replicas: {\n' + 
                '\n'.join(private_server_ips_str) +
                '\n' +
                '\n'.join(public_server_ips_str) +
                '\n' +
                '\n'.join(clients) +
            '\n}'
        )

    print('\n'.join(slog_configs))


class AWSCommand(Command):

    def add_arguments(self, parser):
        parser.add_argument(
            "-r", "--regions", nargs="*", help="Run this script on these regions only"
        )


class CreateSpotClusterCommand(AWSCommand):

    NAME = "spot"
    HELP = "Create a spot clusters from given configurations"

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument("config", help="Configuration file for spot cluster")
        parser.add_argument(
            "--clients", type=int, default=1, help="Number of client machines"
        )
        parser.add_argument(
            "--capacity", type=int, default=None, help="Overwrite target capacity in the config"
        )
        parser.add_argument(
            "--dry-run", action="store_true", help="Run the command without actually creating the clusters"
        )

    def initialize_and_do_command(self, args):
        all_configs = {}
        with open(args.config) as f:
            all_configs = json.load(f)

        if not all_configs:
            LOG.error("Empty config")
            exit()

        regions = [
            r for r in all_configs 
            if (
                r != 'default' and (
                    not args.regions or r in args.regions
                )
            )
        ]

        if args.capacity is not None:
            all_configs['default']['TargetCapacity'] = args.capacity
        
        LOG.info('Requesting %d spot instances at: %s', all_configs['default']['TargetCapacity'], regions)

        if args.dry_run:
            return

        # Request spot fleets
        spot_fleet_requests = {}
        for region in regions:        
            # Apply region-specific configs to the default config
            config = all_configs['default'].copy()
            config.update(all_configs[region])

            ec2 = boto3.client('ec2', region_name=region)
        
            response = ec2.request_spot_fleet(SpotFleetRequestConfig=config)
            request_id = response['SpotFleetRequestId']
            LOG.info("%s: Spot fleet requested. Request ID: %s", region, request_id)

            spot_fleet_requests[region] = {
                'request_id': request_id,
                'config': config,
            }
            
        # Wait for the fleets to come up
        instance_ids = {}
        for region in regions:
            if region not in spot_fleet_requests:
                LOG.warn(
                    "%s: No spot fleet request found. Skipping", region
                )
                continue

            ec2 = boto3.client('ec2', region_name=region)

            request_id = spot_fleet_requests[region]['request_id']
            config = spot_fleet_requests[region]['config']
            target_capacity = config['TargetCapacity']

            region_instances = []
            wait_time = 4
            attempted = 0
            while len(region_instances) < target_capacity and attempted < MAX_RETRIES:
                try:
                    response = ec2.describe_spot_fleet_instances(
                        SpotFleetRequestId=request_id
                    )
                    region_instances = response['ActiveInstances']
                except Exception as e:
                    LOG.exception(region, e)

                LOG.info(
                    '%s: %d/%d instances started',
                    region,
                    len(region_instances),
                    target_capacity,
                )
                if len(region_instances) >= target_capacity:
                    break
                time.sleep(wait_time)
                wait_time *= 2
                attempted += 1
            
            if len(region_instances) >= target_capacity:
                LOG.info('%s: All instances started', region)
                instance_ids[region] = [
                    instance['InstanceId'] for instance in region_instances
                ]
            else:
                LOG.error('%s: Fleet failed to start', region)

        # Wait until status check is OK then collect IP addresses
        instance_public_ips = {}
        instance_private_ips = {}
        for region in regions:
            if region not in instance_ids:
                LOG.warn('%s: Skip fetching IP addresses', region)
                continue

            ec2 = boto3.client('ec2', region_name=region)
            ids = instance_ids[region]

            try:
                LOG.info("%s: Waiting for OK status from %d instances", region, len(ids))
                status_waiter = ec2.get_waiter('instance_status_ok')
                status_waiter.wait(InstanceIds=ids)

                LOG.info("%s: Collecting IP addresses", region)
                response = ec2.describe_instances(InstanceIds=ids)
                instance_public_ips[region] = []
                instance_private_ips[region] = []
                for r in response['Reservations']:
                    instance_public_ips[region] += [
                        i['PublicIpAddress'].strip() for i in r['Instances']
                    ]
                    instance_private_ips[region] += [
                        i["PrivateIpAddress"].strip() for i in r["Instances"]
                    ]
            except Exception as e:
                LOG.exception(region, e)

        install_docker(instance_public_ips)
        print_instance_ips(instance_public_ips, "PUBLIC IP ADDRESSES")
        print_instance_ips(instance_private_ips, "PRIVATE IP ADDRESSES")
        print_slog_config_fragment(instance_public_ips, instance_private_ips, args.clients)


class DestroySpotClusterCommand(AWSCommand):

    NAME = "stop"
    HELP = "Destroy a spot clusters from given configurations"

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--dry-run", action="store_true", help="Run the command without actually destroying the clusters"
        )

    def initialize_and_do_command(self, args):
        for region in args.regions:
            ec2 = boto3.client('ec2', region_name=region)
            response = ec2.describe_spot_fleet_requests()
            spot_fleet_requests_ids = [
                config['SpotFleetRequestId'] for config in
                response['SpotFleetRequestConfigs']
                if config['SpotFleetRequestState'] in ['submitted', 'active', 'modifying']
            ]
            LOG.info("%s: Cancelling request IDs: %s", region, spot_fleet_requests_ids)
            if not args.dry_run:
                if len(spot_fleet_requests_ids) > 0:
                    ec2.cancel_spot_fleet_requests(
                        SpotFleetRequestIds=spot_fleet_requests_ids, TerminateInstances=True
                    )


class InstallDockerCommand(AWSCommand):
    NAME = "docker"
    HELP = "Install docker on all running instances"

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--clients", type=int, default=1, help="Number of client machines"
        )
        parser.add_argument(
            "--dry-run", action="store_true", help="List the instances to install Docker"
        )

    def initialize_and_do_command(self, args):
        if not args.regions:
            LOG.error("Must provide at least one region with --regions")
            return

        instance_public_ips = {}
        instance_private_ips = {}
        for region in args.regions:
            ec2 = boto3.client('ec2', region_name=region)
            try:
                running_instances = ec2.describe_instances(
                    Filters=[{'Name': 'instance-state-name', 'Values': ['running']}]
                )
                LOG.info("%s: Collecting IP addresses", region)
                instance_public_ips[region] = []
                instance_private_ips[region] = []
                for r in running_instances['Reservations']:
                    instance_public_ips[region] += [
                        i['PublicIpAddress'].strip() for i in r['Instances']
                    ]
                    instance_private_ips[region] += [
                        i["PrivateIpAddress"].strip() for i in r["Instances"]
                    ]
            except Exception as e:
                LOG.exception(region, e)
        
        if not args.dry_run:
            install_docker(instance_public_ips)

        print_instance_ips(instance_public_ips, "PUBLIC IP ADDRESSES")
        print_instance_ips(instance_private_ips, "PRIVATE IP ADDRESSES")
        print_slog_config_fragment(instance_public_ips, instance_private_ips, args.clients)


class ListInstancesCommand(AWSCommand):
    NAME = "ls"
    HELP = "List instances and their states"

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--state", default='',
            choices=['pending', 'running', 'terminated', 'stopping', 'stopped'],
            nargs="*",
            help="Filter instances by state"
        )
        parser.add_argument(
            "-c", type=int,
            help="When this parameter is set, a config fragment is printed. This value is used for number of clients"
        )

    def initialize_and_do_command(self, args):
        if not args.regions:
            LOG.error("Must provide at least one region with --regions")
            return

        filters = []
        if args.state:
            filters.append({'Name': 'instance-state-name', 'Values': args.state})

        if args.c is None:
            info = []
            for region in args.regions:
                ec2 = boto3.client('ec2', region_name=region)
                try:
                    instances = ec2.describe_instances(Filters=filters)
                    for r in instances['Reservations']:
                        for i in r['Instances']:
                            info.append([
                                i.get('PublicIpAddress', ''),
                                i.get('PrivateIpAddress', ''),
                                i['State']['Name'],
                                i['Placement']['AvailabilityZone'],
                                i['InstanceType'],
                                ','.join([sg['GroupName'] for sg in i['SecurityGroups']]),
                                i['KeyName'],
                            ])
                except Exception as e:
                    LOG.exception(region, e)

            print(tabulate(info, headers=[
                "Public IP", "Private IP", "State", "Availability Zone", "Type", "Security group", "Key"
            ]))
        else:
            instance_ips = {}
            for region in args.regions:
                ec2 = boto3.client('ec2', region_name=region)
                try:
                    instances = ec2.describe_instances(Filters=filters)
                    ips = []
                    for r in instances['Reservations']:
                        for i in r['Instances']:
                            ips.append(i.get('PublicIpAddress', ''))
                except Exception as e:
                    LOG.exception(region, e)
                instance_ips[region] = ips

            print_instance_ips(instance_ips)
            print_slog_config_fragment(instance_ips, args.c)


if __name__ == "__main__":
    initialize_and_run_commands("AWS utilities", [
        CreateSpotClusterCommand,
        DestroySpotClusterCommand,
        InstallDockerCommand,
        ListInstancesCommand,
    ])
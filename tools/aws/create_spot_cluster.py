import argparse
import boto3
import json
import logging
import time

from subprocess import Popen, PIPE

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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config", help="Configuration file for spot cluster")
    parser.add_argument(
        "--regions", nargs="*", help="Run this script on these regions only"
    )
    parser.add_argument(
        "--clients", type=int, default=1, help="Number of client machines"
    )
    parser.add_argument(
        "--capacity", type=int, default=None, help="Overwrite target capacity in the config"
    )
    args = parser.parse_args()

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
    
    LOG.info('Requesting %d machines at: %s', all_configs['default']['TargetCapacity'], regions)

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
    instance_ips = {}
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
            instance_ips[region] = []
            for r in response['Reservations']:
                instance_ips[region] += [
                    i['PublicIpAddress'].strip() for i in r['Instances']
                ]
        except Exception as e:
            LOG.exception(region, e)

    # Install Docker
    commands = []
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

    # Output IP addresses
    print("\n================== IP ADDRESSES ==================\n")
    print(json.dumps(instance_ips, indent=2))
    ip_groups = instance_ips.values()

    print("\n================== SLOG CONFIG FRAGMENT ==================\n")
    slog_configs = []
    for rep, ips in enumerate(ip_groups):
        client_ips, server_ips = ips[:args.clients], ips[args.clients:]
        servers = [f'  addresses: "{ip}"' for ip in server_ips]
        clients = [
            '  clients: {\n'
            f'    address: "{ip}"\n'
            '    procs: 1\n'
            '  }'
            for ip in client_ips
        ]
        slog_configs.append(
            'replicas: {\n' + 
                '\n'.join(servers) +
                '\n' +
                '\n'.join(clients) +
            '\n}'
        )

    print('\n'.join(slog_configs))

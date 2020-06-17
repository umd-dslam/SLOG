import boto3
import pprint
import random
from subprocess import call
import time

# TODO: place boto3 in dependencies

'''
Request spot instances for cluster deployment and install Docker

REQUIRED:
- install and configure aws cli v2
- security groups `slog` and `ssh` must exist in all VPCs
'''

##### DEPLOYMENT CONFIG #########
# TODO: ingest from file

INSTANCE_COUNT = 1
SPOT_DURATION = 60
DEFAULT_CONFIG = {
  'instance_type': 'c3.large',
  'key_name': 'johann-slog',
  'availability_zone': '',
}

deployments = [
  {
    'region': 'us-east-1',
    'availability_zone': 'us-east-1b',
    'image_id': 'ami-068663a3c619dd892', # Ubuntu server 20
  },
]

##################################

MAX_RETRIES = 10 # retries for spot instances to be ready
REFRESH_WAIT_SCALE = 2 # base for exponential backoff (seconds)
SSH_DELAY_SECONDS = 5 # how long to wait before sshing

INSTALL_DOCKER_COMMAND = """ \
  curl -fsSL https://get.docker.com -o get-docker.sh && \
  sh get-docker.sh && \
  sudo usermod -aG docker ubuntu && \
  newgrp docker \
""".strip()

SSH_COMMAND = """ \
  ssh -o "StrictHostKeyChecking no" -i zzz_personal/johann-slog.pem \
""".strip()

pp = pprint.PrettyPrinter(indent=2)


def main():
  print('{:=^50}'.format('REQUEST SPOT INSTANCES'))
  request_ids = {}
  for partial_conf in deployments:
    # Send request
    conf = {**DEFAULT_CONFIG, **partial_conf} # overrides defaults
    resp = request_spot_instances(conf)
    print('\nSpot request:')
    pp.pprint(resp)
    
    # get request ids
    requests = resp['SpotInstanceRequests']
    assert len(requests) == INSTANCE_COUNT
    request_ids[conf['region']] = [r['SpotInstanceRequestId'] for r in requests]

  print('\n{:=^50}'.format('WAIT FOR INSTANCES'))
  instance_ips = {}
  for region, ids in request_ids.items():
    ips = []
    retries = 0
    while len(ips) < INSTANCE_COUNT:
      retries += 1
      assert retries < MAX_RETRIES

      wait_time = REFRESH_WAIT_SCALE ** retries # exponential backoff
      print("Waiting " + str(wait_time) + " seconds...")
      time.sleep(wait_time)

      print("Trying to fetch instances...")
      ips = get_instance_ips(region, ids)
      print("Got " + str(len(ips)))

    assert len(ips) == INSTANCE_COUNT
    instance_ips[region] = ips

  print('\nIp addresses:')
  pp.pprint(instance_ips)

  print ('\n{:=^50}'.format('INSTALL DOCKER'))
  time.sleep(SSH_DELAY_SECONDS) # Instances may not be ready for ssh yet
  install_docker(instance_ips)

  print('\nIp addresses:')
  pp.pprint(instance_ips)


def request_spot_instances(conf):
  ec2 = boto3.client('ec2', region_name=conf['region'])
  return ec2.request_spot_instances(
    BlockDurationMinutes=SPOT_DURATION,
    InstanceCount=INSTANCE_COUNT,
    LaunchSpecification={
      'SecurityGroups': [
        'ssh',
        'slog',
      ],
      'ImageId': conf['image_id'],
      'InstanceType': conf['instance_type'],
      'KeyName': conf['key_name'],
      'Placement': {
        'AvailabilityZone': conf['availability_zone'],
      },
    }
  )


def get_instance_ips(region, ids):
  ec2 = boto3.client('ec2', region_name=region)
  resp = ec2.describe_spot_instance_requests(
    SpotInstanceRequestIds = ids
  )
  # TODO: surface if request closed
  instance_ids = []
  for spot_req in resp['SpotInstanceRequests']:
    if 'InstanceId' in spot_req:
      instance_ids.append(spot_req['InstanceId'])
    else:
      print('\nDelayed spot request:')
      pp.pprint(spot_req)

  if not instance_ids:
    return []

  ips = []
  resp = ec2.describe_instances(
    InstanceIds = instance_ids
  )
  for r in resp['Reservations']:
    for i in r['Instances']:
      if 'PublicIpAddress' in i:
        ips.append(i['PublicIpAddress'])
      else:
        print('\nInstance without public ip:')
        pp.pprint(i)
  return ips


def install_docker(instance_ips):
  commands = []
  for _, ips in instance_ips.items():
    for ip in ips:
      command = """ \
        {} ubuntu@{} "{}" &\
        """.format(SSH_COMMAND, ip, INSTALL_DOCKER_COMMAND).strip()
      commands.append(command)
  combined_command = "\n".join(commands) + "&& wait"
  print(combined_command)

  # TODO: this process completes the docker install then hangs
  # the script finishes, then output arrives
  call(combined_command, shell=True)


if __name__ == "__main__":
    # execute only if run as a script
    main()
import boto3
import pprint
import random
import os
import time

# TODO: place boto3 in dependencies

'''
Request spot instances for cluster deployment and install Docker

REQUIRED:
- install and configure aws cli v2
- security groups `slog` and `ssh` must exist in all VPCs
- provide the correct AMI id for each region (cli command below)

If no instances are created in a region, you may need to specifiy an
availability zone. Check the output of the spot request.
'''

##### DEPLOYMENT CONFIG #########
# TODO: ingest from file

INSTANCE_COUNT = 1
SPOT_DURATION = 60
DEFAULT_CONFIG = {
  'instance_type': 'm5d.large',
  'key_name': 'johann-slog',
  'availability_zone': '',
}

'''
Get Ubuntu 20.04 AMIs:

aws ec2 describe-images
  --region <region>
  --owners 099720109477
  --filters 'Name=name,Values=ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-????????' 'Name=state,Values=available'
  --query 'reverse(sort_by(Images, &CreationDate))[:1].ImageId'
  --output text
'''

deployments = [ # only one per region
  {
    'region': 'us-east-1',
    'availability_zone': 'us-east-1b',
    'image_id': 'ami-02ae530dacc099fc9',
  },
  {
    'region': 'eu-central-1',
    'image_id': 'ami-0b90a8636b6f955c1',
  },
  {
    'region': 'ap-southeast-2',
    'image_id': 'ami-01de829e74be5dfad',
  },
]

##################################

MAX_RETRIES = 10 # retries for spot instances to be ready
REFRESH_WAIT_SCALE = 2 # base for exponential backoff (seconds)
SSH_DELAY_SECONDS = 10 # may refuse connection if ssh quickly

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
    print('\nSpot request ' + conf['region'] + ':')
    pp.pprint(resp)
    
    # get request ids
    requests = resp['SpotInstanceRequests']
    assert len(requests) == INSTANCE_COUNT
    request_ids[conf['region']] = [r['SpotInstanceRequestId'] for r in requests]

  print('\n{:=^50}'.format('WAIT FOR INSTANCES'))
  instance_ips = {}
  for region, request_id in request_ids.items():
    print(region)
    ips = []
    retries = 0
    while len(ips) < INSTANCE_COUNT:
      retries += 1
      assert retries < MAX_RETRIES

      wait_time = REFRESH_WAIT_SCALE ** retries # exponential backoff
      print("Waiting " + str(wait_time) + " seconds...")
      time.sleep(wait_time)

      print("Trying to fetch " + str(INSTANCE_COUNT) + " instances...")
      ips = get_instance_ips(region, request_id)
      print("Got " + str(len(ips)))

    assert len(ips) == INSTANCE_COUNT
    instance_ips[region] = ips

  print('\nIp addresses:')
  pp.pprint(instance_ips)

  print ('\n{:=^50}'.format('INSTALL DOCKER'))
  time.sleep(SSH_DELAY_SECONDS)
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
    Filters = [
      {
        'Name': 'instance-state-name',
        'Values' : ['running']
      }
    ],
    InstanceIds = instance_ids,
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
  combined_command = "\n".join(commands) + " wait"
  print(combined_command)

  # TODO: redirect output?
  os.system(combined_command)


if __name__ == "__main__":
    # execute only if run as a script
    main()
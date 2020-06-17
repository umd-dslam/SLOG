
import os

def request_spot_instances(args):
  spec = """
    aws --region {region} ec2 request-spot-instances \\
      --block-duration-minutes {minutes} \\
      --instance-count {instance_count} \\
      --launch-specification \\
        \"{{ \\
          \\"ImageId\\": \\"{image_id}\\", \\
          \\"InstanceType\\": \\"{instance_type}\\", \\
          \\"BlockDeviceMappings\\": [ \\
            {{ \\
              \\"DeviceName\\": \\"/dev/sda1\\", \\
              \\"Ebs\\": {{ \\
                \\"DeleteOnTermination\\": true, \\
                \\"SnapshotId\\": \\"snap-035ee9b0c38e6b45d\\", \\
                \\"VolumeSize\\": 8, \\
                \\"Encrypted\\": false, \\
                \\"VolumeType\\": \\"gp2\\" \\
                }} \\
              }} \\
            ], \\
              \\"KeyName\\": \\"{key_name}\\", \\
              \\"SecurityGroups\\": [ \\
                  \\"slog\\", \\"ssh\\" \\
              ] \\
        }}\"
  """.format(**args)
  print(spec)
  os.system(spec)

MINUTES = 60
INSTANCE_COUNT = 1
INSTANCE_TYPE = 'c3.large'
KEY_NAME = 'johann-slog'

request_spot_instances({
    'region': 'us-east-1',
    'minutes': MINUTES,
    'instance_count': INSTANCE_COUNT,
    'image_id': 'ami-068663a3c619dd892',
    'instance_type': INSTANCE_TYPE,
    'key_name': KEY_NAME
})

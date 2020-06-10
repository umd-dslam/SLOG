# https://github.com/aws/aws-cli/issues/1777#issuecomment-541127605

# List instances across regions. All regions by default, or filter using grep

echo "Filtering regions by \`$1\`"
for region in $(aws ec2 describe-regions --query "Regions[*].RegionName" --output text | tr "\t" "\n" | grep "$1")
  do
    echo "$region':"
    aws ec2 describe-instances --region $region \
      --output table \
      --query "Reservations[*].Instances[*].{
              Instance:InstanceId,
              Type:InstanceType,
              KeyName:KeyName,
              Name:Tags[?Key==\`Name\`]|[0].Value,
              IP:PublicIpAddress,
              State:State.Name
          }"
  done
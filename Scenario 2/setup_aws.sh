#!/bin/bash
# AWS CLI Installation
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
aws --version

# AWS CLI Configuration
aws configure

# Verify AWS setup
aws sts get-caller-identity

# Create EC2 instance example
REGION="eu-north-1"
AMI_ID="ami-0a716d3f3b16d290c"
INSTANCE_TYPE="t3.small"
KEY_NAME="smv-keypair"
IAM_ROLE="smv-data-ai-role"
SG_ID="sg-0bc34354cba40f75d"

aws ec2 run-instances --image-id $AMI_ID --instance-type $INSTANCE_TYPE --key-name $KEY_NAME --security-group-ids $SG_ID --iam-instance-profile Name=$IAM_ROLE --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=smv_data-ai-node}]' --region $REGION

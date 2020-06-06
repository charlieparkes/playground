# setup
from datetime import datetime

from aws_sso import boto3_client
from lpipe import sqs
from utils.aws import auth

with auth(["everest-qa"]):
    client = boto3_client("sqs", region_name="us-east-2")
    queue_urls = client.list_queues(
        QueueNamePrefix="lam-omni-taxonomy-alerts"
    )  # ["QueueUrls"]

print(queue_urls)

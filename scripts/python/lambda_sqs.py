# setup
from datetime import datetime

from aws_sso import boto3_client
from lpipe import sqs
from utils.aws import auth

with auth(["everest-qa"]):
    client = boto3_client("sqs", region_name="us-east-2")
    queues = client.list_queues(QueueNamePrefix="wall-e")
    print(queues)
    queue_url = [q for q in queues["QueueUrls"] if "dlq" not in q][0]
    print(queue_url)
    record = {
        "foo": "bar",
    }
    response = client.send_message_batch(
        QueueUrl=queue_url, Entries=[sqs.build(record)]
    )
    print(response)

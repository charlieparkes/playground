# setup
import boto3
from lpipe import sqs
from utils.aws import auth

with auth(["everest-qa"]):
    client = boto3.client("sqs", region_name="us-east-2")
    queue_urls = client.list_queues(QueueNamePrefix="lam-shepherd")["QueueUrls"]
    queue_url = [q for q in queue_urls if "dlq" not in q][0]

    record = {"path": "REPROCESS_URI", "kwargs": {"uri": "taxonomy-v1/company/399"}}

    client.send_message_batch(QueueUrl=queue_url, Entries=[sqs.build(record)])

# setup
from datetime import datetime
from aws_sso import boto3_client
from lpipe import sqs

client = boto3_client("sqs", region_name="us-east-2")
queue_urls = client.list_queues(QueueNamePrefix="lam-omni-taxonomy-alerts")["QueueUrls"]
queue_url = [q for q in queue_urls if "dlq" not in q][0]


record = {
    "path": "CREATE_ALERT",
    "kwargs": {"timestamp": datetime.utcnow().isoformat(), "uri": "test/1234"},
}

# execute
client.send_message_batch(QueueUrl=queue_url, Entries=[sqs.build(record)])

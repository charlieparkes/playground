import json

import boto3
from botocore.exceptions import ClientError
from lpipe import sqs
from lpipe.utils import batch, get_nested, check_status, hash
from multi_sqs_listener import QueueConfig, EventBus, MultiSQSListener

from utils.aws import auth, check_status

name_prefix="lam-asset-collector"


with auth(["everest-prod"]):
    client = boto3.client("sqs", region_name="us-east-2")
    queue_urls = client.list_queues(QueueNamePrefix=name_prefix)["QueueUrls"]

    _queue = [q for q in queue_urls if "dlq" not in q][0]
    QUEUE = {"name": _queue.split('/')[-1], "url": _queue}
    print(f"QUEUE: {QUEUE}")

    _dlq = [q for q in queue_urls if "dlq" in q][0]
    DLQ = {"name": _dlq.split('/')[-1], "url": _dlq}
    print(f"DLQ: {DLQ}")

    # for b in batch(records, 10):
    #     response = client.send_message_batch(QueueUrl=QUEUE["url"], Entries=[sqs.build(r) for r in b])
    #     check_status(response)


class MyQueueConfig(QueueConfig):
    def __init__(self, queue_name, bus, **kwargs):
        super().__init__(queue_name, bus, **kwargs)


class MyListener(MultiSQSListener):
    def __init__(self, queues_configuration):
        super().__init__(queues_configuration)
        self.client = boto3.client("sqs", region_name="us-east-2")

    def handle_message(self, queue, bus, priority, message):
        # This is where your actual event handler code would sit
        try:
            with open(f"{name_prefix}.dlq",'a') as f:
                f.write(message.body + '\n')
        except Exception as e:
            print(f"Failed to write to disk. {e}")

        record = json.loads(message.body)
        print(record)
        response = self.client.send_message_batch(QueueUrl=QUEUE["url"], Entries=[sqs.build(record)])
        check_status(response)


with auth(["everest-prod"]):
    my_event_bus = EventBus()  # leaving default name & priority
    EventBus.register_buses([my_event_bus])
    my_queue = MyQueueConfig(DLQ["name"], my_event_bus, region_name="us-east-2")  # multiple default values here
    my_listener = MyListener([my_queue])
    my_listener.listen()

import json

import boto3
from botocore.exceptions import ClientError
from lpipe import sqs
from lpipe.utils import get_nested, check_status, hash
from multi_sqs_listener import QueueConfig, EventBus, MultiSQSListener

from utils.aws import auth


class MyQueueConfig(QueueConfig):
    def __init__(self, queue_name, bus, **kwargs):
        super().__init__(queue_name, bus, **kwargs)


class MyListener(MultiSQSListener):
    def __init__(self, queues_configuration):
        super().__init__(queues_configuration)

        self.client = boto3.client("sqs", region_name="us-east-2")
        queue_urls = self.client.list_queues(QueueNamePrefix="lam-new-product-line-discovery")["QueueUrls"]
        self.queue_url = [q for q in queue_urls if "dlq" not in q][0]
        print(f"{self.queue_url}")

    def handle_message(self, queue, bus, priority, message):
        # This is where your actual event handler code would sit
        record = json.loads(message.body)
        print(record)
        self.client.send_message_batch(QueueUrl=self.queue_url, Entries=[sqs.build(record)])


with auth(["everest-prod"]):
    my_event_bus = EventBus()  # leaving default name & priority
    EventBus.register_buses([my_event_bus])
    my_queue = MyQueueConfig("lam-new-product-line-discovery-dlq20200226232329665400000001", my_event_bus, region_name="us-east-2")  # multiple default values here
    my_listener = MyListener([my_queue])
    my_listener.listen()

import json
import logging
from functools import wraps

import boto3
import botocore
from decouple import config

from utils import batch, hash


def build(message_data, message_group_id=None):
    data = json.dumps(message_data, sort_keys=True)
    msg = {"Id": hash(data), "MessageBody": data}
    if message_group_id:
        msg["MessageGroupId"] = str(message_group_id)
    return msg


def mock_sqs(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (
            botocore.exceptions.NoCredentialsError,
            botocore.exceptions.ClientError,
            botocore.exceptions.NoRegionError,
        ):
            if config("MOCK_AWS", default=False):
                log = kwargs["logger"] if "logger" in kwargs else logging.getLogger()
                log.debug(
                    "Mocked SQS: {}()".format(func),
                    function=f"{func}",
                    params={"args": f"{args}", "kwargs": f"{kwargs}"},
                )
                return
            else:
                raise

    return wrapper


@mock_sqs
def batch_put_messages(
    queue_url, messages, batch_size=10, message_group_id=None, **kwargs
):
    """Put messages into a sqs queue, batched by the maximum of 10."""
    assert batch_size <= 10  # send_message_batch will fail otherwise
    client = boto3.client("sqs")
    responses = []
    for b in batch(messages, batch_size):
        response = client.send_message_batch(
            QueueUrl=queue_url,
            Entries=[build(message, message_group_id) for message in b],
        )
        responses.append(response)
    return tuple(responses)


def put_message(queue_url, data, message_group_id=None, **kwargs):
    return batch_put_messages(
        queue_url=queue_url, messages=[data], message_group_id=message_group_id
    )


@mock_sqs
def get_queue_url(queue_name):
    client = boto3.client("sqs")
    response = client.get_queue_url(QueueName=queue_name)
    return response["QueueUrl"]


def queue_exists(q):
    try:
        get_queue_url(q)
        return True
    except:
        return False


def wait_for_queues_exist(queue_names):
    for queue_name in queue_names:
        while not queue_exists(queue_name):
            sleep(1)


@backoff.on_exception(backoff.expo, ClientError, max_time=30)
def create_queue(q):
    response = client.create_queue(QueueName=queue_name)
    check_status(response)
    return response["QueueUrl"]


def create_queues(queue_names):
    queues = {}
    for queue_name in queue_names:
        try:
            queues[queue_name] = create_queue(queue_name)
        except ClientError as e:
            exists = queue_exists(queue_name)
            raise Exception(f"queue_exists({queue_name}) -> {exists}") from e
    return queues


def delete_queue(q):
    delete_queues([q])


def delete_queues(queue_names):
    for queue_name in queue_names:
        url = get_queue_url(queue_name)
        client.delete_queue(QueueUrl=url)


def create_then_destroy(queue_names):
    # check(localstack, "sqs")
    client = boto3.client("sqs")
    try:
        queues = create_queues(queue_names)
        wait_for_queues_exist(queue_names)
        yield queues
    finally:
        delete_queues(queue_names)

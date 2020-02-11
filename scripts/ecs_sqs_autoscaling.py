import json
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError
from lpipe.utils import get_nested, check_status, hash

from utils.aws import auth


service_name = "pypedream-orchestrator"
queue_name = "orchestrator-input-queue.fifo"
# service_name = "pypedream-taxonomy"
# queue_name = "taxonomy-input-queue.fifo"
now = datetime.now(tz=timezone.utc)
region = "us-east-2"

"""
from scripts.ecs_sqs_autoscaling import Queue, Service
service_name = "pypedream-taxonomy"
queue_name = "taxonomy-input-queue.fifo"
target_task_pressure = 5000
queue = Queue(queue_name)
services = Service.load([service_name])
"""


# def _check(response, code=2, keys=["ResponseMetadata", "HTTPStatusCode"]):
#     status = get_nested(response, keys)
#     assert status // 100 == code
#     return status // 100 == code


def _call(_callable, *args, **kwargs):
    try:
        resp = _callable(*args, **kwargs)
        check_status(resp)
        return resp
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "ResourceAlreadyExistsException":
            raise


class Queue:
    def __init__(self, name, now=None):
        self.client = boto3.client("sqs", region_name=region)
        self.name = name
        self.url = get_nested(
            _call(self.client.get_queue_url, QueueName=name), ["QueueUrl"]
        )
        self.now = now if now else datetime.now(tz=timezone.utc)

        attr_map = (
            ("arn", "QueueArn"),
            ("num_msgs", "ApproximateNumberOfMessages"),
            # ("num_msgs_visible", "ApproximateNumberOfMessagesVisible"),
            # ("num_msgs_not_visible", "ApproximateNumberOfMessagesNotVisible"),
            # ("num_msgs_delayed", "ApproximateNumberOfMessagesDelayed"),
        )
        self.attributes = get_nested(
            _call(
                self.client.get_queue_attributes,
                QueueUrl=self.url,
                AttributeNames=[a[1] for a in attr_map],
            ),
            ["Attributes"],
        )
        for a in attr_map:
            setattr(self, a[0], self.attributes[a[1]])

        metrics = {}
        cw_namespace = "AWS/SQS"
        cw_metrics = (
            ("num_sent", "NumberOfMessagesSent", "Sum", "Count"),
            ("num_received", "NumberOfMessagesReceived", "Sum", "Count"),
        )
        cw_dimensions = [
            {"Name": "QueueName", "Value": self.name,},
        ]

        common_timestamps = None

        def _fmt_data(m):
            return [{"Timestamp": x, "Sum": y} for x, y in dict(zip(m["Timestamps"], m["Values"])).items()]

        def _set_intersection(s1, s2):
            return s1 & s2 if s1 else s2

        def _sort_datapoints(datapoints):
            return sorted(datapoints, key=lambda s: s["Timestamp"])

        # Fetch desired cloudwatch metrics
        queries = []
        for m in cw_metrics:
            _metric = {
                'Namespace': cw_namespace,
                'MetricName': m[1],
                'Dimensions': cw_dimensions,
            }
            queries.append({
                'Id': f"sqsautoscaler_{hash(json.dumps(_metric, sort_keys=True))[:8]}",
                'MetricStat': {
                    'Metric': _metric,
                    'Stat': m[2],
                    'Unit': m[3],
                    'Period': 60,
                },
            })

        cloudwatch = boto3.client("cloudwatch", region_name=region)
        response = cloudwatch.get_metric_data(
            MetricDataQueries=queries,
            StartTime=self.now - timedelta(minutes=5),
            EndTime=self.now,
        )
        assert check_status(response)

        _data = {m["Label"]: _sort_datapoints(_fmt_data(m)) for m in response["MetricDataResults"]}

        for m in cw_metrics:
            try:
                assert m[1] in _data
                common_timestamps = _set_intersection(
                    common_timestamps, set([s["Timestamp"] for s in _data[m[1]]])
                )
                metrics[m[0]] = _data[m[1]]
            except AssertionError as e:
                raise Exception(
                    f"Metrics request failed or returned no datapoints ({cw_namespace} {m[1]} {cw_dimensions})"
                ) from e

        # Reduce cloudwatch metric datapoints to common timestamps
        for m in cw_metrics:
            try:
                metric = metrics[m[0]]
                filtered_datapoints = [
                    d for d in metric if d["Timestamp"] in common_timestamps
                ]
                assert filtered_datapoints
                metrics[m[0]] = _sort_datapoints(filtered_datapoints)[-1]
            except AssertionError:
                raise Exception(
                    f"Found no datapoints with common timestamps ({cw_namespace} {m[1]} {cw_dimensions})"
                )

        self.metrics = metrics
        for m in cw_metrics:
            metric_name = m[0]
            metric = metrics[metric_name]
            setattr(self, metric_name, float(metric["Sum"]))

    def attr(name):
        return self.attributes[name]

    def __repr__(self):
        return f"Queue<{self.name}>"


class Service:
    def __init__(self, meta):
        self.client = boto3.client("ecs", region_name=region)
        self.meta = meta
        attr_map = (
            ("name", "serviceName"),
            ("arn", "serviceArn"),
            ("events", "events"),
            ("desired_count", "desiredCount"),
            ("pending_count", "pendingCount"),
            ("running_count", "runningCount"),
            ("deployments", "deployments"),
        )
        for a in attr_map:
            setattr(self, a[0], self.meta[a[1]])

    def __repr__(self):
        return f"Service<{self.name}>"

    @classmethod
    def load(cls, names):
        client = boto3.client("ecs", region_name=region)
        descriptions = _call(client.describe_services, services=[service_name])[
            "services"
        ]
        services = {}
        for d in descriptions:
            s = cls(d)
            services[s.name] = s
        return services


def estimate_pct_of_desired_pressure(
    queue: Queue, service: Service, target_task_pressure: int
):
    """Are there enough tasks for the number of messages we have?"""
    if int(queue.num_msgs) > 0 and int(service.desired_count) == 0:
        return 200
    try:
        return (
            (int(queue.num_msgs) / int(service.desired_count))
            / target_task_pressure
            * 100
        )
    except ZeroDivisionError:
        return 0


def estimate_msg_processing_ratio(queue: Queue):
    """Are we processing messages faster or slower than they're arriving?"""
    if queue.num_sent == 0 and queue.num_received == 0:
        return 0
    try:
        return (queue.num_sent / queue.num_received) * 100
    except ZeroDivisionError:
        return 0


with auth(["everest-prod"]):
    queue = Queue(queue_name, now=now)
    service = Service.load([service_name])[service_name]

target_pressure = 5000
print(f"{service_name} {queue_name}")
# pct_of_desired_pressure = estimate_pct_of_desired_pressure(
#     queue, service, target_pressure
# )
# print(
#     f"Pressure (approx_num_msgs({queue.num_msgs}) / desired_count({service.desired_count})) / target_pressure({target_pressure}) * 100 -> {pct_of_desired_pressure}"
# )
msg_processing_ratio = estimate_msg_processing_ratio(queue)
print(
    f"MessageProcessingRatio num_sent({queue.num_sent}) / num_received({queue.num_received}) -> {msg_processing_ratio}"
)

namespace = "SQSAutoscaling"
base = {
    "Dimensions": [
        # {"Name": "ServiceName", "Value": service_name},
        {"Name": "QueueName", "Value": queue_name},
    ],
    "Timestamp": now,
    "Unit": "None",
    #'StorageResolution': 123
}

with auth(["everest-prod"]):
    client = boto3.client("cloudwatch", region_name="us-east-2")
    metrics = []
    # metrics.append({**base, "MetricName": "PercentOfDesiredPressure", "Value": pct_of_desired_pressure})
    metrics.append({**base, "MetricName": "MessageProcessingRatio", "Value": msg_processing_ratio})
    resp = client.put_metric_data(Namespace=namespace, MetricData=metrics)
    metric_names = ', '.join([m["MetricName"] for m in metrics])
    print(f"Cloudwatch Metrics: {metric_names} ... {check_status(resp)}")

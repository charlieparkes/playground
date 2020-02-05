import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone
from lpipe.utils import get_nested


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


def _check(response, code=2, keys=["ResponseMetadata", "HTTPStatusCode"]):
    status = get_nested(response, keys)
    assert status // 100 == code
    return status // 100 == code


def _call(_callable, *args, **kwargs):
    try:
        resp = _callable(*args, **kwargs)
        _check(resp)
        return resp
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "ResourceAlreadyExistsException":
            raise


class Queue:
    def __init__(self, name):
        self.client = boto3.client("sqs", region_name=region)
        self.name = name
        self.url = get_nested(
            _call(self.client.get_queue_url, QueueName=name), ["QueueUrl"]
        )

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
            ("num_sent", "NumberOfMessagesSent", ["Sum"], "Count"),
            ("num_received", "NumberOfMessagesReceived", ["Sum"], "Count"),
        )
        cw_dimensions = [
            {"Name": "QueueName", "Value": self.name,},
        ]

        common_timestamps = None

        def _set_intersection(s1, s2):
            return s1 & s2 if s1 else s2

        def _sort_datapoints(datapoints):
            return sorted(datapoints, key=lambda s: s["Timestamp"])

        # Fetch desired cloudwatch metrics
        cloudwatch = boto3.resource("cloudwatch", region_name=region)
        for m in cw_metrics:
            metric = cloudwatch.Metric(cw_namespace, m[1])
            stats = metric.get_statistics(
                Dimensions=cw_dimensions,
                Period=60,
                StartTime=now - timedelta(minutes=5),
                EndTime=now,
                Statistics=m[2],
                Unit=m[3],
            )
            if _check(stats) and stats.get("Datapoints", []):
                datapoints = _sort_datapoints(stats["Datapoints"])
                common_timestamps = _set_intersection(
                    common_timestamps, set([s["Timestamp"] for s in datapoints])
                )
                metrics[m[0]] = datapoints
            else:
                raise Exception(
                    f"Metrics request failed or returned no datapoints ({cw_namespace} {m[1]} {cw_dimensions})"
                )

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


def estimate_pressure(queue: Queue, service: Service, target_task_pressure: int):
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


def estimate_load(queue: Queue):
    """Are we processing messages faster or slower than they're arriving?"""
    try:
        return (queue.num_sent / queue.num_received) * 100
    except ZeroDivisionError:
        return 0


queue = Queue(queue_name)
service = Service.load([service_name])[service_name]

target_pressure = 5000
print(f"{service_name} {queue_name}")
print(
    f"Pressure (approx_num_msgs({queue.num_msgs}) / desired_count({service.desired_count})) / target_pressure({target_pressure}) * 100 -> {estimate_pressure(queue, service, target_pressure)}"
)
print(
    f"MessageProcessingRatio num_sent({queue.num_sent}) / num_received({queue.num_received}) -> {estimate_load(queue)}"
)

# p = calculate_pressure(queue, service, target_task_pressure)
# print(f"Pressure: {p}")
#
# l = calculate_load(queue)
# print(f"Load: {l}")


# def pressure(service_name: str, queue_name: str, target_task_pressure: int):
#     sqs = boto3.client("sqs", region_name=region)
#     ecs = boto3.client("ecs", region_name=region)
#
#     url = get_nested(
#         _call(
#             sqs.get_queue_url,
#             QueueName=queue_name
#         ),
#         ["QueueUrl"]
#     )
#     queue_attr = int(get_nested(
#         _call(
#             sqs.get_queue_attributes,
#             QueueUrl=url,
#             AttributeNames=["ApproximateNumberOfMessages"]
#         ),
#         ["Attributes"]
#     ))
#     approx_num_msgs = get_nested(queue_attr, ["ApproximateNumberOfMessages"])
#     approx_num_msgs = get_nested(queue_attr, ["ApproximateNumberOfMessages"])
#     desired_count = int(_call(
#         ecs.describe_services,
#         services=[service_name]
#     )["services"][0]["desiredCount"])
#
#     p = _calculate(approx_num_msgs, desired_count, target_task_pressure)
#
#     print(f"Write pressure to cloudwatch: {p}")
#     now = datetime.now(tz=timezone.utc)


# print("")
# print(f"Simulating effects of autoscaling...")
# print(f"( ( approx_num_msgs({approx_num_msgs}) / desired_count({desired_count}) ) / target_task_pressure({target_task_pressure}) ) * 100")
# if p == float("inf"):
#     pass
# elif p > 100:
#     print("We should scale UP.")
#     new_desired_count = desired_count + 1
#     ideal_p = pressure(approx_num_msgs, new_desired_count, target_task_pressure)
#     while ideal_p > 100:
#         new_desired_count += 1
#         ideal_p = pressure(approx_num_msgs, new_desired_count, target_task_pressure)
# elif p < 100:
#     print("We should scale DOWN.")
#     new_desired_count = desired_count - 1 if desired_count > 0 else desired_count
#     ideal_p = pressure(approx_num_msgs, new_desired_count, target_task_pressure)
#     while ideal_p < 100:
#         new_desired_count -= 1
#         if new_desired_count == 0:
#             break
#         ideal_p = pressure(approx_num_msgs, new_desired_count, target_task_pressure)
#
# print(f"ideal_pressure: {ideal_p}")
# print(f"ideal_desired_count: {new_desired_count}")


# print("")
# print("Tests")
#
# tests = (
#     (0, 0, 100, 0),
#     (0, 1, 100, 0),
#     (1, 0, 100, 200),
#     (1000000, 0, 100, 200),
#     (1000000, 1, 100, 1000000),
#     (1, 1, 100, 1),
#     (0, 0, 0, 100),
#     (100, 0, 0, 100),
#     (100, 1, 0, 100),
#     (100, 9, 10, 111),
#     (100, 10, 10, 100),
#     (100, 11, 10, 90),
#     (999999999, 10, 10, 999999999),
#     (1000001, 1, 10000, 10000),
#     (1000001, 10, 10000, 1000),
#     (1000001, 100, 10000, 100),
# )
#
# for t in tests:
#     p = pressure(t[0], t[1], t[2])
#     try:
#         assert p == t[3]
#         print(f"PASS get_pressure({t[0]}, {t[1]}, {t[2]}) == {t[3]}%")
#     except AssertionError:
#         print(f"FAIL get_pressure({t[0]}, {t[1]}, {t[2]})->{p}% != {t[3]}%")


# client = boto3_client('cloudwatch', region_name='us-east-2')
# client.put_metric_data(MetricData=[
#         {
#             'MetricName': 'string',
#             'Dimensions': [
#                 {
#                     'Name': 'string',
#                     'Value': 'string'
#                 },
#             ],
#             'Timestamp': now,
#             'Value': pressure,
#             'Unit': "None",
#             #'StorageResolution': 123
#         },
#     ])

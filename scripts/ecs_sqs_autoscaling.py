import json
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError
from lpipe.utils import get_nested, check_status, hash

from utils.aws import auth


# service_name = "pypedream-orchestrator"
# queue_name = "orchestrator-input-queue.fifo"

# service_name = "pypedream-taxonomy"
# queue_name = "taxonomy-input-queue.fifo"

# queue_name = "publish-expert-input-queue"
# service_name = "pypedream-publish-expert"

# queue_name = "quality_checker_entity-input-queue.fifo"

# queue_name = "language_checker_entity-input-queue.fifo"
# service_name = "pypedream-language_checker_entity"

service_name = "media-service-render"
queue_name = "media-service-render-input"

now = datetime.now(tz=timezone.utc)
region = "us-east-2"


def _call(_callable, *args, **kwargs):
    try:
        resp = _callable(*args, **kwargs)
        check_status(resp)
        return resp
    except ClientError as e:
        raise


class Queue:
    def __init__(self, name, url=None, now=None):
        self.client = boto3.client("sqs", region_name=region)
        self.name = name
        self._url = url
        self.now = now if now else datetime.now(tz=timezone.utc)

        metrics = {}
        cw_namespace = "AWS/SQS"
        cw_metrics = (
            ("num_sent", "NumberOfMessagesSent", "Sum", "Count"),
            ("num_received", "NumberOfMessagesReceived", "Sum", "Count"),
            ("num_deleted", "NumberOfMessagesDeleted", "Sum", "Count"),
            ("num_visible", "ApproximateNumberOfMessagesVisible", "Sum", "Count"),
            ("num_not_visible", "ApproximateNumberOfMessagesNotVisible", "Sum", "Count"),
            ("num_delayed", "ApproximateNumberOfMessagesDelayed", "Sum", "Count"),
            ("age_oldest_msg", "ApproximateAgeOfOldestMessage", "Average", "Seconds"),
        )
        cw_dimensions = [
            {"Name": "QueueName", "Value": self.name},
        ]
        common_timestamps = None

        def _get_metric_stat(m):
            return queries[m["Id"]]["MetricStat"]["Stat"]

        def _fmt_data(m, metric_stat):
            return [
                {"Timestamp": x, metric_stat: y}
                for x, y in dict(zip(m["Timestamps"], m["Values"])).items()
            ]

        def _set_intersection(s1, s2):
            return s1 & s2 if s1 else s2

        def _sort_datapoints(datapoints):
            return sorted(datapoints, key=lambda s: s["Timestamp"])

        # Fetch desired cloudwatch metrics
        queries = {}
        for m in cw_metrics:
            _metric = {
                "Namespace": cw_namespace,
                "MetricName": m[1],
                "Dimensions": cw_dimensions,
            }
            _id = f"sqsautoscaler_{m[1]}_{hash(json.dumps(_metric, sort_keys=True))[:8]}"
            queries[_id] = {
                "Id": _id,
                "MetricStat": {
                    "Metric": _metric,
                    "Stat": m[2],
                    "Unit": m[3],
                    "Period": 60,
                },
            }

        cloudwatch = boto3.client("cloudwatch", region_name=region)
        response = _call(
            cloudwatch.get_metric_data,
            MetricDataQueries=list(queries.values()),
            StartTime=self.now - timedelta(minutes=5),
            EndTime=self.now
        )

        _data = {
            m["Label"]: _sort_datapoints(_fmt_data(m, _get_metric_stat(m)))
            for m in response["MetricDataResults"]
        }

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
        # for m in cw_metrics:
        #     try:
        #         metric = metrics[m[0]]
        #         filtered_datapoints = [
        #             d for d in metric if d["Timestamp"] in common_timestamps
        #         ]
        #         assert filtered_datapoints
        #         metrics[m[0]] = _sort_datapoints(filtered_datapoints)[-1]
        #     except AssertionError:
        #         raise Exception(
        #             f"Found no datapoints with common timestamps ({cw_namespace} {m[1]} {cw_dimensions})"
        #         )
        #
        # self.metrics = metrics
        # for m in cw_metrics:
        #     metric_name = m[0]
        #     metric = metrics[metric_name]
        #     setattr(self, metric_name, float(metric[m[2]]))


        # Reduce cloudwatch metric datapoints to common timestamps
        for m in cw_metrics:
            try:
                metric = metrics[m[0]]
                filtered_datapoints = [
                    d for d in metric if d["Timestamp"] in common_timestamps
                ]
                assert filtered_datapoints
                # metrics[m[0]] = _sort_datapoints(filtered_datapoints)[-1]
                metrics[m[0]] = _sort_datapoints(filtered_datapoints)
            except AssertionError:
                raise Exception(
                    f"Found no datapoints with common timestamps ({cw_namespace} {m[1]} {cw_dimensions})"
                )

        self.metrics = metrics
        for m in cw_metrics:
            metric_name = m[0]
            metric = metrics[metric_name]
            vals = [float(met[m[2]]) for met in metric]
            val = sum(vals) / len(vals)
            setattr(self, metric_name, val)

    @property
    def url(self):
        if not self._url:
            self._url = get_nested(
                _call(self.client.get_queue_url, QueueName=self.name), ["QueueUrl"]
            )
        return self._url

    @property
    def num_msgs(self):
        return self.num_visible + self.num_not_visible + self.num_delayed

    def __getattr__(self, attr):
        try:
            return self.__getattribute__(name)
        except Exception as e:
            if not self.attributes:
                attr_map = (
                    ("arn", "QueueArn"),
                    # ("num_msgs", "ApproximateNumberOfMessages"),
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

            if attr in self.attributes:
                return self.attributes[attr]
            else:
                raise

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
    def load_services(cls, names):
        client = boto3.client("ecs", region_name=region)
        descriptions = _call(client.describe_services, services=[service_name])[
            "services"
        ]
        services = {}
        for d in descriptions:
            s = cls(d)
            services[s.name] = s
        return services

    @classmethod
    def load(cls, name):
        return cls.load_services([name])[name]


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


def linear_regression(data: list, r: range = None):
    """Calculate the linear regression of a data-set, assuming each data-point is equidistant on the x-axis.

    Args:
        data (list): list of data points
        r (range):

    Returns:
        tuple: return a, b in solution to y = ax + b such that root mean square distance between trend line and original points is minimized
    """
    _r = r if r else range(len(data))
    N = len(_r)
    Sx = Sy = Sxx = Syy = Sxy = 0.0
    for x, y in zip(_r, data):
        Sx = Sx + x
        Sy = Sy + y
        Sxx = Sxx + x*x
        Syy = Syy + y*y
        Sxy = Sxy + x*y
    det = Sxx * N - Sx * Sx
    return (Sxy * N - Sy * Sx) / det, (Sxx * Sy - Sx * Sxy) / det


def estimate_msg_processing_ratio(queue: Queue, svc_name: str, target_pressure: int = 200):
    """
    Scale in/out with target tracking around (sent / received) @ 100.

    We'll never scale up if nothing is being sent to the queue unless there are
    messages in the queue and no tasks running.

    tl;dr Are we processing messages faster or slower than they're arriving?
    """
    if queue.num_sent > 0 and queue.num_received > 0:
        # steady state
        val = (queue.num_sent / queue.num_received) * 100

        # before we start scaling down, check some edge cases
        if val < 100:
            # if queue age is trending up, gently scale up
            age_oldest_msg_series = [m['Average'] for m in queue.metrics["age_oldest_msg"]]
            slope, _ = linear_regression(age_oldest_msg_series)
            if slope > 0:
                print(f"Will override MessageProcessingRatio: {val}")
                return 120

            # never scale down if the queue age is greater than one hour
            if queue.age_oldest_msg / 60 / 60 > 1:
                print(f"Will override MessageProcessingRatio: {val}")
                return 100

        # if keeping up, try to gradually scale down
        svc = Service.load(svc_name)
        if svc.desired_count > 1 and queue.age_oldest_msg < 60 and 90 <= val <= 110:
            print(f"Will override MessageProcessingRatio: {val}")
            return (
                int(queue.num_sent) / int(svc.desired_count) / target_pressure
            ) * 100

        return val

    if queue.num_msgs > 0:
        # the queue is active...
        if queue.num_deleted > 0 and queue.age_oldest_msg / 60 / 60 > 1:
            # ...but we're not scaled out enough
            return 200
        svc = Service.load(svc_name)
        if svc.desired_count == 0:
            # ...but we previously scaled to zero
            return 150

    # default behavior, scale in
    return 0


def estimate_msg_processing_ratio_old(queue: Queue):
    """Are we processing messages faster or slower than they're arriving?"""
    if queue.num_sent == 0 and queue.num_received == 0:
        return 0
    try:
        return (queue.num_sent / queue.num_received) * 100
    except ZeroDivisionError:
        return 400


target_pressure = 5000

with auth(["everest-qa"]):
    queue = Queue(queue_name, now=now)
    # service = Service.load([service_name])[service_name]
    # pct_of_desired_pressure = estimate_pct_of_desired_pressure(
    #     queue, service, target_pressure
    # )
    msg_processing_ratio = estimate_msg_processing_ratio(queue, service_name)

# print(f"{service_name} {queue_name}")
# print(
#     f"Pressure (approx_num_msgs({queue.num_msgs}) / desired_count({service.desired_count})) / target_pressure({target_pressure}) * 100 -> {pct_of_desired_pressure}"
# )
# print(
#     f"MessageProcessingRatio num_msgs({queue.num_msgs}) and num_sent({queue.num_sent}) / num_received({queue.num_received}) -> {msg_processing_ratio}"
# )
print(f"MessageProcessingRatio: {msg_processing_ratio}")

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

# with auth(["everest-qa"]):
#     client = boto3.client("cloudwatch", region_name="us-east-2")
#     metrics = []
#     # metrics.append({**base, "MetricName": "PercentOfDesiredPressure", "Value": pct_of_desired_pressure})
#     metrics.append({**base, "MetricName": "MessageProcessingRatio", "Value": msg_processing_ratio})
#     resp = client.put_metric_data(Namespace=namespace, MetricData=metrics)
#     metric_names = ', '.join([m["MetricName"] for m in metrics])
#     print(f"Push {metric_names} to Cloudwatch ... {'OK' if check_status(resp) == 200 else 'ERR'}")

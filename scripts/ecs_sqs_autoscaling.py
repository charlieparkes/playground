import datetime
from lpipe.utils import get_nested
from aws_sso import boto3_client


def check_status(response, code=2, keys=["ResponseMetadata", "HTTPStatusCode"]):
    status = get_nested(response, keys)
    assert status // 100 == code
    return status


def _call(_callable, *args, **kwargs):
    try:
        resp = _callable(*args, **kwargs)
        check_status(resp)
        return resp
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "ResourceAlreadyExistsException":
            raise


def pressure(approx_num_msgs: int, desired_count: int, target_task_pressure: int):
    if target_task_pressure == 0:
        return 100
    if approx_num_msgs > 0 and desired_count == 0:
        return 200
    try:
        return int(((approx_num_msgs / desired_count) / target_task_pressure) * 100)
    except ZeroDivisionError:
        return 0


def calculate_pressure(service_name: str, queue_name: str, target_task_pressure: int):
    sqs = boto3_client('sqs', region_name='us-east-2')
    ecs = boto3_client('ecs', region_name='us-east-2')

    url = get_nested(
        _call(
            sqs.get_queue_url,
            QueueName=queue_name
        ),
        ["QueueUrl"]
    )
    approx_num_msgs = int(get_nested(
        _call(
            sqs.get_queue_attributes,
            QueueUrl=url,
            AttributeNames=["ApproximateNumberOfMessages"]
        ),
        ["Attributes", "ApproximateNumberOfMessages"]
    ))
    desired_count = int(_call(
        ecs.describe_services,
        services=[service_name]
    )["services"][0]["desiredCount"])

    p = pressure(approx_num_msgs, desired_count, target_task_pressure)

    print(f"Write pressure to cloudwatch: {p}")
    #now = datetime.datetime.now()



#service_name = "pypedream-orchestrator"
#queue_name = "orchestrator-input-queue.fifo"
service_name = "pypedream-taxonomy"
queue_name = "taxonomy-input-queue.fifo"
target_task_pressure = 5000

calculate_pressure(service_name, queue_name, target_task_pressure)


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


print("")
print("Tests")

tests = (
    (0, 0, 100, 0),
    (0, 1, 100, 0),
    (1, 0, 100, 200),
    (1000000, 0, 100, 200),
    (1000000, 1, 100, 1000000),
    (1, 1, 100, 1),
    (0, 0, 0, 100),
    (100, 0, 0, 100),
    (100, 1, 0, 100),
    (100, 9, 10, 111),
    (100, 10, 10, 100),
    (100, 11, 10, 90),
    (999999999, 10, 10, 999999999),
    (1000001, 1, 10000, 10000),
    (1000001, 10, 10000, 1000),
    (1000001, 100, 10000, 100),
)

for t in tests:
    p = pressure(t[0], t[1], t[2])
    try:
        assert p == t[3]
        print(f"PASS get_pressure({t[0]}, {t[1]}, {t[2]}) == {t[3]}%")
    except AssertionError:
        print(f"FAIL get_pressure({t[0]}, {t[1]}, {t[2]})->{p}% != {t[3]}%")


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

import datetime
from lpipe.utils import get_nested
from aws_sso import boto3_client

def check_status(response, code=2, keys=["ResponseMetadata", "HTTPStatusCode"]):
    status = get_nested(response, keys)
    assert status // 100 == code
    return status

target_pressure = 5000
#service_name = "pypedream-orchestrator"
#queue_name = "orchestrator-input-queue.fifo"
service_name = "pypedream-taxonomy"
queue_name = "taxonomy-input-queue.fifo"

client = boto3_client('sqs', region_name='us-east-2')

resp = client.get_queue_url(QueueName=queue_name)
assert check_status(resp)
url = resp["QueueUrl"]
print(f"url: {url}")

now = datetime.datetime.now()

resp = client.get_queue_attributes(QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"])
assert check_status(resp)
approx_num_msgs = resp["Attributes"]["ApproximateNumberOfMessages"]
print(f"approx_num_msgs: {approx_num_msgs}")

client = boto3_client('ecs', region_name='us-east-2')

resp = client.describe_services(services=[service_name])
assert check_status(resp)
try:
    desired_count = resp["services"][0]["desiredCount"]
except:
    print(resp)
    raise
print(f"desired_count: {desired_count}")

class ServiceScaledToZero(Exception):
    pass

def pressure(approx_num_msgs, desired_count, target_pressure):
    if desired_count == 0:
        raise ServiceScaledToZero()
    return ((int(approx_num_msgs) / int(desired_count)) / target_pressure) * 100

print(f"( ( approx_num_msgs({approx_num_msgs}) / desired_count({desired_count}) ) / target_pressure({target_pressure}) ) * 100")
try:
    p = pressure(approx_num_msgs, desired_count, target_pressure)
except ServiceScaledToZero:
    if approx_num_msgs > 0:
        new_desired_count = 1
        p = 100
        print("ServiceScaledToZero but we have messages. We should scale UP.")

print(f"pressure: {p}")

if p > 100:
    print("We should scale UP.")
    new_desired_count = desired_count + 1
    ideal_p = pressure(approx_num_msgs, new_desired_count, target_pressure)
    while ideal_p > 100:
        new_desired_count += 1
        ideal_p = pressure(approx_num_msgs, new_desired_count, target_pressure)

elif p < 100:
    print("We should scale DOWN.")
    new_desired_count = desired_count - 1 if desired_count > 0 else desired_count
    ideal_p = pressure(approx_num_msgs, new_desired_count, target_pressure)
    while ideal_p < 100:
        if new_desired_count - 1 == 0:
            break
        new_desired_count -= 1
        ideal_p = pressure(approx_num_msgs, new_desired_count, target_pressure)

print(f"ideal_pressure: {ideal_p}")
print(f"ideal_desired_count: {new_desired_count}")


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

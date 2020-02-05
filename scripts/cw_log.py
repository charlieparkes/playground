# pulled some cool ideas in from kislyuk/watchtower

# Among the suggestions here, most notable is that we don't need the "describe" permission, and,
# in cases where the log stream or group need to be created, reduce the number of calls against
# the cloudwatch API.

import boto3
import json
from botocore.exceptions import ClientError
from datetime import datetime, timezone

client = boto3.client("logs", region_name="us-east-2")
now = datetime.now(tz=timezone.utc)
log_group_name = "/aws/ec2/jenkins"
log_stream_name = now.date().isoformat()
timestamp = int(now.timestamp() * 1000)


def _create(_callable, *args, **kwargs):
    try:
        _callable(*args, **kwargs)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "ResourceAlreadyExistsException":
            raise


_create(client.create_log_group, logGroupName=log_group_name)
_create(client.put_retention_policy, logGroupName=log_group_name, retentionInDays=30)
_create(
    client.create_log_stream, logGroupName=log_group_name, logStreamName=log_stream_name
)

payload = {
    "timestamp": timestamp,
    "message": json.dumps(
        {
            "user": "{COMITTER_EMAIL}",  # last comitter to a branch
            "target": "{ARTIFACT_NAME}",
            "stage": "publish",
            "tags": ["{GIT_HASH}", "latest"],
            "environment": "qa",
        }
    ),
}
sequence_token = None

for retry in range(5):
    try:
        kwargs = {
            "logGroupName": log_group_name,
            "logStreamName": log_stream_name,
            "logEvents": [payload],
        }
        if sequence_token:
            kwargs["sequenceToken"] = sequence_token
        client.put_log_events(**kwargs)
        print(f"{log_group_name}:{log_stream_name} {payload}")
        break
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in (
            "DataAlreadyAcceptedException",
            "InvalidSequenceTokenException",
        ):
            sequence_token = e.response["Error"]["Message"].rsplit(" ", 1)[-1]
        else:
            raise Exception("Failed to deliver log.") from e
    except Exception as e:
        raise Exception("Failed to deliver log.") from e

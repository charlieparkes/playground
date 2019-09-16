import hashlib
import json
from datetime import datetime, timedelta

import boto3
from elasticsearch_dsl import Search, Q
from everest_elasticsearch_dsl import configure_connections, constants
from aws_sso import boto3_client


def batch(iterable, n=1):
    iter_len = len(iterable)
    for ndx in range(0, iter_len, n):
        yield iterable[ndx : min(ndx + n, iter_len)]


def hash(encoded_data):
    return hashlib.sha1(encoded_data.encode("utf-8")).hexdigest()


def batch_put_records(stream_name, records, batch_size=500):
    """Put records into a kinesis stream, batched by the maximum of 500."""
    client = boto3_client("kinesis", region_name="us-east-2")

    def build(r):
        data = json.dumps(r, sort_keys=True)
        return {"Data": data, "PartitionKey": hash(data)}

    output = []
    for b in batch(records, batch_size):
        result = client.put_records(
            StreamName=stream_name, Records=[build(record) for record in b]
        )
        output.append(result)
    return tuple(output)


def put_record(stream_name, data, **kwargs):
    return batch_put_records(stream_name=stream_name, records=[data])


def build_record(asset_id):
    return { "path": "REPROCESS_ASSET_ID", "kwargs": {"asset_id": asset_id} }


configure_connections("prod")

event = "No OGs were affected."

fields = ["kwargs", "message", "event"]
must = []
must.append(Q("term", cloudwatch_logs__log_group="/aws/lambda/lam-product-tagger-collector"))
must.append(Q("term", event__keyword=event))
must.append(Q("range", timestamp={"gte": "now-14d"}))

#gt_dt = (datetime.now() - timedelta(days=3))

s = (
    Search(using=constants.LOGS, index="application-logs-*")
    .source(fields)
    .query(Q("bool", must=must))
    .update_from_dict({"size": 0})
)

n_logs = s.count()

print(n_logs)
print(s.to_dict())

asset_ids = []
for log in s.scan():
    try:
        assert log.event == event
        asset_id = log.kwargs.asset_id
        if asset_id not in asset_ids:
            asset_ids.append(asset_id)
    except:
        print(log.to_dict())
        raise

client = boto3_client("kinesis", region_name="us-east-2")
stream_name = "kin-st-sas-shepherd"

print(len(asset_ids))
records = [build_record(aid) for aid in asset_ids]
batch_put_records(stream_name, records)

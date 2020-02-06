import json
from datetime import datetime

from elasticsearch_dsl import Search
from everest_elasticsearch_dsl.documents.staging.product_tagger_outcome import \
    ProductTaggerOutcome

from workflow import kinesis

start = datetime.strptime("2019-10-17 00:00:00", "%Y-%m-%d %H:%M:%S")
end = datetime.strptime("2019-10-22 00:00:00", "%Y-%m-%d %H:%M:%S")


audit_logs = list(
    Search(index="audit-logs-*", using="logs")
    .filter("range", timestamp={"gte": start, "lte": end})
    .filter("term", event="decision")
    .sort("timestamp")
    .source(["object_uri", "timestamp", "actor", "output", "queue", "flow"])
    .scan()
)

pto_assets = set(
    [
        (outcome.assetId, outcome.flow, outcome.queue, outcome.actor)
        for outcome in ProductTaggerOutcome.search()
        .filter("range", timestamp={"gte": start, "lte": end})
        .source(["assetId", "timestamp", "actor", "output", "queue", "flow"])
        .scan()
    ]
)

audit_logs_assets = set(
    [(log.object_uri, log.flow, log.queue, log.actor) for log in audit_logs]
)

count = 0
for log in audit_logs:
    if (log.object_uri, log.flow, log.queue, log.actor) in (
        audit_logs_assets - pto_assets
    ):
        count += 1
        kinesis.put_record(
            **{
                "stream_name": "kin-st-sas-shepherd",
                "data": {
                    "path": "ADD_OUTCOME",
                    "kwargs": {
                        "timestamp": log.timestamp,
                        "actor": log.actor,
                        "flow": log.flow,
                        "queue": log.queue,
                        "asset_id": log.object_uri,
                        "updated_fields": json.loads(log.output),
                    },
                },
            }
        )

print(f"count {count}")

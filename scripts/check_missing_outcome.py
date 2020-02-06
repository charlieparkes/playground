import hashlib
import json
import pdb
from datetime import datetime, timedelta

import backoff
import boto3
import elasticsearch
from aws_sso import boto3_client
from elasticsearch_dsl import MultiSearch, Q, Search
from everest_elasticsearch_dsl import configure_connections, constants
from everest_elasticsearch_dsl.documents.staging.product_tagger_outcome import (
    Outcome, ProductTaggerOutcome)

configure_connections("prod")


def execute_es_multisearch(searches, index, using):
    if searches and len(searches) > 0:
        ms = MultiSearch(index=index, using=using)
        for search in searches:
            ms = ms.add(search)
        return ms.execute()
    else:
        return []


def build_record(log):
    try:
        return {
            "path": "ADD_OUTCOME",
            "kwargs": {
                "timestamp": log.timestamp,
                "actor": log.actor,
                "flow": log.flow,
                "queue": log.queue,
                "asset_id": log.object_uri,
                "updated_fields": json.loads(log.output),
            },
        }
    except AttributeError as e:
        raise Exception(log.to_dict()) from e


def prune_and_build_records(logs):
    skipped_records = []
    records = []
    outcome_hashes = {}
    searches = []
    for log in logs:
        updated_fields = json.loads(log.output)
        renamed_fields = [
            ("companyId", "marketingCompanyURI"),
            ("productIds", "productURIs"),
        ]
        for field in renamed_fields:
            if field[0] in updated_fields:
                updated_fields[field[1]] = updated_fields.pop(field[0])
        outcome = Outcome(
            {
                "timestamp": log.timestamp,
                "actor": log.actor,
                "flow": log.flow,
                "queue": log.queue,
                "assetId": log.object_uri,
                "outcome": updated_fields,
            }
        )
        outcome_hash = outcome.hash
        outcome_hashes[log.meta.id] = outcome_hash

        must = []
        must.append(Q("term", actor=log.actor))
        must.append(Q("term", flow=log.flow))
        must.append(Q("term", queue=log.queue))
        must.append(Q("term", assetId=log.object_uri))
        must.append(Q("term", outcomeHash=outcome_hash))
        outcome_s = (
            ProductTaggerOutcome.search()
            .query(Q("bool", must=must))
            .update_from_dict({"size": 0})
        )
        searches.append(outcome_s)

    try:
        responses = execute_es_multisearch(
            searches, using=constants.STAGING, index="product_tagger_outcome"
        )
    except ValueError:
        import pdb

        pdb.set_trace()
        return [], []

    assert len(logs) == len(searches) == len(responses)

    for log, search, response in zip(logs, searches, responses):
        if response.hits.total > 0:
            skipped_records.append(
                {"log": log.meta.id, "outcome": outcome_hashes[log.meta.id]}
            )
        else:
            records.append(build_record(log))
    return records, skipped_records, outcome_hashes


lte = datetime(year=2019, month=6, day=6, tzinfo=datetime.now().tzinfo)
gte = lte - timedelta(days=1)

must = []
# must.append(Q("term", container_name="ui-workflow"))
must.append(Q("exists", field="flow"))
must.append(Q("exists", field="queue"))
must.append(Q("term", event__keyword="DECISION"))
if gte and lte:
    must.append(Q("range", timestamp={"gte": gte, "lte": lte}))
# must.append(Q("term", _id="b37c9225d7805a1e1fc26fe43b83c36c"))

s = (
    Search(using=constants.LOGS, index="audit-logs-*")
    .source(["timestamp", "actor", "flow", "queue", "object_uri", "output"])
    .query(Q("bool", must=must))
    .update_from_dict({"size": 0})
)

print(s.to_dict())

n_logs = s.count()

total = 0
logs = []
records = []
invalid_logs = []
skipped_records = []

for log in s.scan():
    try:
        assert log.timestamp
        assert log.actor
        assert log.flow
        assert log.queue
        assert log.object_uri
        assert log.output
        logs.append(log)
    except AssertionError as e:
        print(f"Skipping log {log.meta.id} because it failed an assertion. {e}")
        print(log.to_dict())
        invalid_logs.append(log.meta.id)

new_records, new_skipped_records, outcome_hashes = prune_and_build_records(logs)
records.extend(new_records)
skipped_records.extend(new_skipped_records)


pdb.set_trace()

import hashlib
import json
from datetime import datetime, timedelta

import backoff
import boto3
import elasticsearch
from aws_sso import boto3_client
from elasticsearch_dsl import MultiSearch, Q, Search
from everest_elasticsearch_dsl import configure_connections, constants
from everest_elasticsearch_dsl.documents.staging.product_tagger_outcome import (
    Outcome, ProductTaggerOutcome)
from tqdm import tqdm

DRY_RUN = False
STREAM_NAME = "kin-st-sas-shepherd"
NOW = datetime.now() - timedelta(days=2)
CUTOFF = NOW - timedelta(days=14)
# NOW = datetime(year=2019, month=6, day=6, tzinfo=datetime.now().tzinfo)
# CUTOFF = NOW - timedelta(days=1)


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


def execute_es_multisearch(searches, index, using):
    if searches and len(searches) > 0:
        ms = MultiSearch(index=index, using=using)
        for search in searches:
            ms = ms.add(search)
        return ms.execute()
    else:
        return []


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

    for log, search, response in zip(logs, searches, responses):
        if response.hits.total > 0:
            skipped_records.append(
                {"log": log.meta.id, "outcome": outcome_hashes[log.meta.id]}
            )
        else:
            records.append(build_record(log))
    return records, skipped_records


@backoff.on_exception(backoff.expo, elasticsearch.exceptions.TransportError)
def backfill_range(gte, lte):
    tqdm.write(f"Backfilling {gte} to {lte}")

    must = []
    # must.append(Q("term", container_name="ui-workflow"))
    must.append(Q("exists", field="flow"))
    must.append(Q("exists", field="queue"))
    must.append(Q("term", event__keyword="DECISION"))
    must.append(Q("range", timestamp={"gte": gte, "lte": lte}))

    s = (
        Search(using=constants.LOGS, index="audit-logs-*")
        .source(["timestamp", "actor", "flow", "queue", "object_uri", "output"])
        .query(Q("bool", must=must))
        .update_from_dict({"size": 0})
    )

    n_logs = s.count()

    if s.count() > 0:
        print(f"Found {n_logs} audit logs will be checked.")
    else:
        return

    # print(s.to_dict())

    total = 0
    logs = []
    records = []
    invalid_logs = []
    skipped_records = []

    for log in tqdm(s.scan()):
        try:
            assert log.timestamp
            assert log.actor
            assert log.flow
            assert log.queue
            assert log.object_uri
            assert log.output
            logs.append(log)
        except AssertionError as e:
            tqdm.write(
                f"Skipping log {log.meta.id} because it failed an assertion. {e}"
            )
            tqdm.write(log.to_dict())
            invalid_logs.append(log.meta.id)

        ### BATCH EARLY ###
        if len(logs) >= 100:
            new_records, new_skipped_records = prune_and_build_records(logs)
            records.extend(new_records)
            skipped_records.extend(new_skipped_records)
            # tqdm.write(f"new_records: {len(new_records)}")
            total += len(new_records)
            logs = []

        if len(records) >= 2000:
            if DRY_RUN:
                tqdm.write(f"batch_put_records: {len(records)}")
            else:
                batch_put_records(STREAM_NAME, records, 100)
            records = []
            pass
        ### END BATCH EARLY ###

    ### FINAL BATCH ###
    new_records, new_skipped_records = prune_and_build_records(logs)
    records.extend(new_records)
    skipped_records.extend(new_skipped_records)
    # tqdm.write(f"new_records: {len(new_records)}")
    total += len(new_records)
    logs = []

    if DRY_RUN:
        tqdm.write(f"batch_put_records: {len(records)}")
    else:
        batch_put_records(STREAM_NAME, records, 100)
    records = []
    ### END FINAL BATCH ###

    tqdm.write(f"backfill: {total}, skipped: {len(skipped_records)}")


def main():
    configure_connections("prod")

    delta = timedelta(hours=1)
    dt = NOW

    while dt > CUTOFF:
        gte = dt - delta
        lte = dt
        backfill_range(gte, lte)
        dt = gte


main()

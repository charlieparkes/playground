import backoff
import urllib3
from aws_sso import boto3_client
from elasticsearch.exceptions import ConnectionTimeout
from elasticsearch_dsl import Q, Search
from everest_elasticsearch_dsl import configure_connections, constants
from everest_elasticsearch_dsl.documents.staging.product_tagger_outcome import (
    Outcome,
    ProductTaggerOutcome,
)
from lpipe import sqs
from lpipe.utils import batch
from peewee import IntegrityError
from tqdm import tqdm

from db import Outcome, db

db.connect()
try:
    db.create_tables([Outcome])
except Exception as e:
    print(e)
    pass


def insert(records):
    try:
        Outcome.insert(records).execute()
        pbar.update(len(records))
    except IntegrityError as e:
        for r in records:
            try:
                Outcome.insert([r])
                pbar.update(1)
            except Exception as e:
                print(f"Failed to insert record {r}: {e.__class__} - {e}")


configure_connections("prod")
s = (
    ProductTaggerOutcome.search(using=constants.STAGING, index="product_tagger_outcome")
    .query()
    .source([])
    .update_from_dict({"size": 0})
)
resp = s.execute()
pbar = tqdm(total=resp.hits.total)
records = []
for hit in s.scan():
    # update_hash(hit)
    records.append({"doc_id": hit.meta.id})
    # o = Outcome(doc_id=hit.meta.id)
    # o.save()
    if len(records) >= 1000:
        insert(records)
        records = []

insert(records)

tqdm.write(f"Finished fetching")
pbar.close()

print("Done")

# https://www.elastic.co/guide/en/elasticsearch/reference/5.4/search-request-search-after.html

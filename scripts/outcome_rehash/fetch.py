import backoff
import urllib3
from aws_sso import boto3_client
from elasticsearch.exceptions import ConnectionTimeout
from elasticsearch_dsl import Search, Q
from everest_elasticsearch_dsl import configure_connections, constants
from everest_elasticsearch_dsl.documents.staging.product_tagger_outcome import (
    Outcome,
    ProductTaggerOutcome,
)
from lpipe import sqs
from lpipe.utils import batch
from tqdm import tqdm

from db import db, Outcome


db.connect()
try:
    db.create_tables([Outcome])
except:
    pass


configure_connections("prod")
s = (
    ProductTaggerOutcome.search(using=constants.STAGING, index="product_tagger_outcome")
    .query()
    .source([])
    .update_from_dict({"size": 0})
)
resp = s.execute()
pbar = tqdm(total=resp.hits.total)
# records = []
for hit in s.scan():
    # update_hash(hit)
    # records.append(hit.meta.id)
    o = Outcome(id=hit.meta.id)
    o.save()
    pbar.update(1)
tqdm.write(f"Finished fetching")
pbar.close()

print("Done")

# https://www.elastic.co/guide/en/elasticsearch/reference/5.4/search-request-search-after.html

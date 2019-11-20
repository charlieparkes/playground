import backoff
import urllib3
from aws_sso import boto3_client
from elasticsearch.exceptions import ConnectionTimeout
from elasticsearch_dsl import Search, Q
from everest_elasticsearch_dsl import configure_connections, constants
from everest_elasticsearch_dsl.documents.staging.product_tagger_outcome import Outcome, ProductTaggerOutcome
from lpipe import sqs
from lpipe.utils import batch
from tqdm import tqdm

@backoff.on_exception(backoff.expo, ConnectionTimeout)
def update_hash(hit):
    # hit = ProductTaggerOutcome.get(h.meta.id)

    o = Outcome({**hit.to_dict(), "meta": hit.meta.to_dict()})
    tqdm.write(f"{o.hash}")
    if hit.outcomeHash != o.hash:
        tqdm.write(f"Updating hash of {hit.meta.id} (old: {hit.outcomeHash} new: {o.hash})")
        o.save()


def record(id):
    return sqs.build({
        "path": "UPDATE_OUTCOME_HASH",
        "kwargs": {
            "id": id
        }
    })


configure_connections("prod")
client = boto3_client('sqs', region_name='us-east-2')
queue_urls = client.list_queues(QueueNamePrefix="lam-shepherd")['QueueUrls']
queue_url = [q for q in queue_urls if "dlq" not in q][0]
s = ProductTaggerOutcome.search(using=constants.STAGING, index="product_tagger_outcome").query().source([]).update_from_dict({"size": 0})
resp = s.execute()
pbar = tqdm(total=resp.hits.total)
records = []
for hit in s.scan():
    # update_hash(hit)
    records.append(hit.meta.id)
    pbar.update(1)
tqdm.write(f"Finished fetching")
pbar.close()


pbar = tqdm(total=len(records))
tqdm.write(f"Writing {len(records)} records to SQS")
for b in batch(records, 10):
    client.send_message_batch(QueueUrl=queue_url,Entries=[record(id) for id in b])
    pbar.update(10)
pbar.close()


print("Done")

# https://www.elastic.co/guide/en/elasticsearch/reference/5.4/search-request-search-after.html

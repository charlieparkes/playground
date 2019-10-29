import backoff
import urllib3
from elasticsearch.exceptions import ConnectionTimeout
from elasticsearch_dsl import Search, Q
from everest_elasticsearch_dsl import configure_connections, constants
from everest_elasticsearch_dsl.documents.staging.product_tagger_outcome import Outcome, ProductTaggerOutcome
from tqdm import tqdm

@backoff.on_exception(backoff.expo, ConnectionTimeout)
def update_hash(hit):
    o = Outcome({**hit.to_dict(), "meta": hit.meta.to_dict()})
    if hit.outcomeHash != o.hash:
        tqdm.write(f"Updating hash of {hit.meta.id} (old: {hit.outcomeHash} new: {o.hash})")
        o.save()

configure_connections("prod")
s = ProductTaggerOutcome.search(using=constants.STAGING, index="product_tagger_outcome").query().update_from_dict({"size": 0})
resp = s.execute()
pbar = tqdm(total=resp.hits.total)
for hit in s.scan():
    update_hash(hit)
    pbar.update(1)

pbar.close()
print("Done")

# https://www.elastic.co/guide/en/elasticsearch/reference/5.4/search-request-search-after.html

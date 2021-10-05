# setup
from aws_sso import boto3_client
from elasticsearch_dsl import Q, Search
from everest_elasticsearch_dsl import configure_connections, constants
from lpipe import sqs

configure_connections("prod")

product_ids = [4022]
should = []
must = []
must.append(Q("term", og_o_join="og"))

should.append(
    Q(
        "nested",
        path="unbrandedProduct",
        query=Q("terms", unbrandedProduct__id=product_ids),
    )
)

should.append(
    Q(
        "nested",
        path="brandedProduct",
        query=Q("terms", brandedProduct__id=product_ids),
    )
)

search = (
    Search(using=constants.ANALYTICS, index="eve")
    .update_from_dict({"size": 0})
    .source(["vendor"])
    .query(
        Q("bool", must=must, should=should, minimum_should_match=(1 if should else 0),)
    )
)

results = []
for doc in search.scan():
    if doc.meta.id not in results:
        results.append(doc.meta.id)

print(results)
print(len(results))


# ######################


client = boto3_client("sqs", region_name="us-east-2")
queue_urls = client.list_queues(QueueNamePrefix="lam-shepherd")["QueueUrls"]
queue_url = [q for q in queue_urls if "dlq" not in q][0]

# message
uris = [
    "taxonomy-v1/product/4022",
]

records = [{"path": "REPROCESS_URI", "kwargs": {"uri": uri}} for uri in uris]

# execute
sqs.batch_put_messages(
    queue_url, [{"path": "REPROCESS_URI", "kwargs": {"uri": uri}} for uri in uris]
)
# client.send_message_batch(QueueUrl=queue_url,Entries=records)

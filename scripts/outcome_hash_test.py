import pdb
from datetime import datetime

from elasticsearch_dsl import MultiSearch, Q, Search
from everest_elasticsearch_dsl import configure_connections, constants
from everest_elasticsearch_dsl.documents.staging.product_tagger_outcome import (
    Outcome,
    ProductTaggerOutcome,
)

# configure_connections("prod")
# s = (
#     ProductTaggerOutcome.search(using=constants.STAGING, index="product_tagger_outcome")
#     .query(Q("term", outcomeHash="d8db81271f02b8d58bfaf97df2e3cb22d96f216c"))
#     .update_from_dict({"size": 0})
# )
# s.aggs.bucket("outcomeHash", "terms", field="outcomeHash")
# s.aggs["outcomeHash"].bucket("assetId", "terms", field="assetId")
#
# response = s.execute()
# aggs = response.aggregations.to_dict()
#
# hashes = {}
#
# for hash_bucket in aggs["outcomeHash"]["buckets"]:
#     asset_counts = {}
#     for asset_bucket in hash_bucket["assetId"]["buckets"]:
#         asset_counts[asset_bucket["key"]] = asset_bucket["doc_count"]
#     hashes[hash_bucket["key"]] = asset_counts
#
# fields = {
#     "timestamp": datetime.utcfromtimestamp(1571159939).isoformat(),
#     "actor": "cmathews@mintel.com",
#     "flow": "product-entry",
#     "queue": "entry",
#     "assetId": "asset:product-tagger/6a18404bdfc54ca8df8be637b1b7e2d8",
# }
#
# fields["outcome"] = {"productURIs": ["taxonomy-v1/product/4816"]}
# p1 = Outcome(fields)
# assert p1.valid
#
# fields["outcome"] = {"productURIs": ["taxonomy-v1/product/4817"]}
# p2 = Outcome(fields)
# assert p2.valid
#
# p3 = Outcome({
#     "timestamp": "2019-10-15T17:29:07.835379",
#     "actor": "silkasara.peter@rrd.com",
#     "flow": "product-entry",
#     "queue": "entry",
#     "assetId": "asset:product-tagger/030057366b5cf5237fb94eb6b56a9571",
#     "outcome": {
#       "isGeneralBranding": False,
#       "marketingCompanyURI": "taxonomy-v1/company/399",
#       "productURIs": [
#         "taxonomy-v1/product/5491"
#       ]
#     },
# })
#
# p4 = Outcome({
#     "timestamp": "2019-10-15T17:27:43.694851",
#     "actor": "sajin.rifahi@rrd.com",
#     "flow": "product-entry",
#     "queue": "entry",
#     "assetId": "asset:product-tagger/73eea384e740e07ea63bb1477223a8a3",
#     "outcome": {
#       "isGeneralBranding": False,
#       "marketingCompanyURI": "taxonomy-v1/company/399",
#       "productURIs": [
#         "taxonomy-v1/product/152"
#       ]
#     },
# })

o3 = p3.doc.outcome
o4 = p4.doc.outcome

mc3 = getattr(o3, "marketingCompanyURI", None)
mc4 = getattr(o4, "marketingCompanyURI", None)
print(f"{mc3} {mc4}")

gb3 = getattr(o3, "isGeneralBranding", None)
gb4 = getattr(o4, "isGeneralBranding", None)
print(f"{gb3} {gb4}")

nur3 = getattr(o3, "notUsableReason", None)
nur4 = getattr(o4, "notUsableReason", None)
print(f"{nur3} {nur4}")

pu3 = getattr(o3, "productURIs", [])
pu4 = getattr(o4, "productURIs", [])
print(f"{pu3} {pu4}")

spu3 = sorted(pu3) if isinstance(pu3, list) else []
spu4 = sorted(pu4) if isinstance(pu4, list) else []
print(f"{spu3} {spu4}")

# product_uris = getattr(o, "productURIs", [])
# return calculate_hash(
#     {
#         "marketingCompanyURI": getattr(o, "marketingCompanyURI", None),
#         "isGeneralBranding": getattr(o, "isGeneralBranding", None),
#         "notUsableReason": getattr(o, "notUsableReason", None),
#         "productURIs": sorted(product_uris)
#         if isinstance(product_uris, list)
#         else [],
#     }
# )


pdb.set_trace()

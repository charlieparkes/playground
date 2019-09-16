import hashlib
import json
from aws_sso import boto3_client
from datetime import datetime
client = boto3_client('kinesis', region_name='us-east-2')
stream_name = 'kin-st-sas-shepherd'
record = {
    'path': 'ADD_OUTCOME',
    'kwargs': {
        'timestamp': datetime.now().isoformat(),
        'actor': 'cmathews@mintel.com',
        'flow': 'product-entry',
        'queue': 'entry',
        'asset_id': 'asset:product-tagger/96aaf4e62933d5c70fbcb0f011d5dece',
        'updated_fields': {"companyId": "taxonomy-v1/company/1220", "isGeneralBranding": False, "notUsableReason": None, "productIds": ["taxonomy-v1/product/5379"]},
    }
}
data = json.dumps(record)
key = hashlib.sha1(data.encode('utf-8')).hexdigest()
print(record)
client.put_record(StreamName=stream_name, Data=data, PartitionKey=key)

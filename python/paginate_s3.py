import boto3

bucket_name = "everest-s3-qa-sherpa-training-sets"
client = boto3.client("s3", region_name="us-east-2")
paginator = client.get_paginator(
    "list_objects"
)  # client.list_objects only goes to 1000
page_iterator = paginator.paginate(Bucket=bucket_name)

objects = [
    k
    for page in [[o for o in page["Contents"]] for page in page_iterator]
    for k in page
]
get_last_modified = lambda obj: int(obj["LastModified"].strftime("%s"))
sorted_objects = [
    obj["LastModified"] for obj in sorted(objects, key=get_last_modified, reverse=True)
]

# ['ResponseMetadata', 'IsTruncated', 'Marker', 'Contents', 'Name', 'Prefix', 'MaxKeys', 'EncodingType']

results = []
for page in page_iterator:
    results.extend[[o["Key"] for o in page["Contents"]]]

[o["Key"] for o in page["Contents"]]
pages = [[o["Key"] for o in page["Contents"]] for page in page_iterator]
objects = [k for page in pages for k in page]


client.list_objects(Bucket=bucket_name)

# unsorted = []
# for file in my_bucket.objects.filter():
#   unsorted.append(file)

# o.key.split("/")[0] for o in bucket.objects.all() if o.key.split("/")[0]

s3 = boto3.resource("s3")
bucket = s3.Bucket(bucket_name)
get_last_modified = lambda obj: int(obj["LastModified"].strftime("%s"))
files = [
    obj.key
    for obj in sorted(bucket.objects.filter(), key=get_last_modified, reverse=True)
][0:9]


s3 = boto3.resource("s3")
bucket = s3.Bucket(bucket_name)
keys = [o.key for o in bucket.objects.all()]
for key in keys:
    o = s3.Object(BUCKET_NAME, key)
    set_name = o.key.split("/")[0]
    body = o.get().get("Body")

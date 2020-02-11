@backoff.on_exception(
    backoff.expo, (ClientError, ConnectionClosedError), max_time=30
)
def create_bucket(b):
    response = s3.create_bucket(Bucket=b)
    check_status(response)
    s3.get_waiter("bucket_exists").wait(Bucket=b)


@backoff.on_exception(
    backoff.expo, (ClientError, ConnectionClosedError), max_time=30
)
def delete_bucket(b):
    bucket = boto3.resource("s3").Bucket(b)
    objects = bucket.objects.all()
    keys = [s3.delete_object(Bucket=b, Key=o.key) for o in objects]
    response = s3.delete_bucket(Bucket=b)
    check_status(response)
    s3.get_waiter("bucket_not_exists").wait(Bucket=b)


def create_then_destroy(bucket_name):
    # check(localstack, "s3")
    s3 = boto3.client("s3")
    try:
        create_bucket(bucket_name)
        yield bucket_name
    finally:
        delete_bucket(bucket_name)

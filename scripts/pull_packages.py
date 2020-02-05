import boto3
import botocore
import hashlib
import io
import json
import os
from aws_sso import boto3_client, boto3_resource
from pathlib import Path


bucket_name = "everest-shared-ue2-backups"
s3 = boto3_resource("s3", region_name="us-east-2")
bucket = s3.Bucket(bucket_name)
keys = [
    k
    for k in [
        o.key
        for o in bucket.objects.filter(
            Prefix="devpi/backups/20190417034801/everest/prod"
        )
    ]
    if ".dev" not in k and ".rc" not in k
]

path = Path(str(Path().absolute()) + "/pkg")
if not path.is_dir():
    path.mkdir(path)

for key in keys:
    try:
        file_name = key.split("/")
        print(f"{file_name[-1]}")

        print("\tMetadata...", end="")
        object = s3.Object(bucket_name, key)
        print("ok.")

        print("\tDownloading...", end="")
        body = object.get().get("Body")
        print("ok.")
    except botocore.exceptions.ClientError as e:
        print("fail.")
        raise

    if file_name and body:
        print("\tWriting to disk...", end="")
        try:
            fpath = Path(str(path) + f"/{file_name[-1]}")
            if not fpath.is_file():
                with io.FileIO(f"{fpath}", "w") as file:
                    for b in body._raw_stream:
                        file.write(b)
                print("ok.")
            else:
                print("skipped.")
        except:
            print("fail.")
            raise

    print("")

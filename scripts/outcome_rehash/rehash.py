import configparser
import contextlib
import getpass
import math
import os
import platform
import random
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from unittest import mock

import aws_sso
import backoff
import urllib3
from botocore.exceptions import ClientError, NoCredentialsError
from elasticsearch.exceptions import ConnectionTimeout
from elasticsearch_dsl import Q, Search
from everest_elasticsearch_dsl import configure_connections, constants
from everest_elasticsearch_dsl.documents.staging.product_tagger_outcome import (
    Outcome, ProductTaggerOutcome)
from lpipe import sqs
from lpipe.utils import batch, set_env
from tqdm import tqdm

from db import Outcome, db

TEMP_CREDENTIALS_DURATION = 8 * 60 * 60  # 8 hours


# def auth(role_name="PROD_Operator", duration=3600 * 8, region_name="us-east-2"):
#     fetcher = credentials.MintelAWSCredentialsFetcher(
#         "product_tagger_outcome_rehash",
#         role_name=role_name,
#         account_hint=role_name,
#         duration=duration,
#     )
#     response = fetcher.fetch_credentials()
#
#     provider = credentials.MintelSSOSharedCredentialProvider()
#     creds = {
#         "aws_access_key_id": response["access_key"],
#         "aws_secret_access_key": response["secret_key"],
#         "aws_session_token": response["token"],
#     }
#     status = boto3.setup_default_session(
#         creds, region_name=region_name
#     )
#     return (status, creds)


@contextlib.contextmanager
def aws_auth(profiles, credentials_file="~/.aws/credentials", expire_threshold=1200):
    """Context manager to auth multiple AWS accounts.

    Dumps temporary credentials into a temporary file and
    sets the AWS_SHARED_CREDENTIALS_FILE environment variable.

    Args:
        profiles (list[str]): List of profile names in the AWS credentials file.
        credentials_file (str, optional): The path to the standard AWS credentials file
            where Mintel IDP login info is stored.
        expire_threshold (int): Cached credentials expiring during Terraform apply
            can cause headaches. Cached credentials with fewer than `expire_threshold`
            seconds left to expiration will be deleted.

    """
    expire_threshold += time.time()
    for cache_credentials_path in (Path.home() / ".aws").glob("*.json"):
        c_stat = cache_credentials_path.stat()
        if c_stat.st_mtime + TEMP_CREDENTIALS_DURATION < expire_threshold:
            cache_credentials_path.unlink()

    config = configparser.RawConfigParser()
    for profile in profiles:
        providers = [
            aws_sso.credentials.MintelSSOSharedCredentialProvider(
                profile_name=profile,
                creds_filename=credentials_file,
                account_hint=profile,
                duration=TEMP_CREDENTIALS_DURATION,
            ),
            aws_sso.credentials.PromptAllProvider(
                account_hint=profile, duration=TEMP_CREDENTIALS_DURATION
            ),
        ]
        for provider in providers:
            creds = provider.load()
            if creds is not None:
                break
        if creds is None:
            raise Exception("credentials for profile %s not found" % (profile,))
        config.add_section(profile)
        config.set(profile, "aws_access_key_id", creds.access_key)
        config.set(profile, "aws_secret_access_key", creds.secret_key)
        config.set(profile, "aws_session_token", creds.token)
    with tempfile.NamedTemporaryFile("w+") as temp_f:
        config.write(temp_f)
        temp_f.flush()
        with set_env(
            {
                "AWS_SHARED_CREDENTIALS_FILE": os.path.abspath(temp_f.name),
                "AWS_PROFILE": profiles[0],
            }
        ):
            yield


def pages(n_pages):
    for i in range(1, n_pages + 1):
        yield i


def record(id):
    return sqs.build({"path": "UPDATE_OUTCOME_HASH", "kwargs": {"id": id}})


def write_records(page_size=1000):
    assert db.connect()
    query = Outcome.select(Outcome.doc_id).where(Outcome.queued == False)
    n = query.count()
    n_pages = math.ceil(n / page_size)
    sent_count = 0

    if n == 0:
        print("Zero unsent outcomes. Nothing to do.")
        return

    pbar = tqdm(total=n)
    tqdm.write(f"Writing {n} records to SQS")

    with aws_auth(["everest-prod"]):
        client = aws_sso.boto3_client("sqs", region_name="us-east-2")
        queue_urls = client.list_queues(QueueNamePrefix="lam-shepherd")["QueueUrls"]
        queue_url = [q for q in queue_urls if "dlq" not in q][0]
        for p in pages(n_pages):
            records = query.paginate(p, page_size)
            for b in batch(records, 10):
                record_batch = [o for o in b if not o.queued]
                if record_batch:
                    client.send_message_batch(
                        QueueUrl=queue_url,
                        Entries=[record(o.doc_id) for o in record_batch],
                    )
                    sent_count += len(record_batch)
                    for r in record_batch:
                        r.update(queued=True).execute()
            pbar.update(len(records))

    pbar.close()
    print(f"Sent {sent_count} messages")
    unsent_count = query.count()
    print(f"CHECK: {unsent_count == 0}")
    print("Done")


if __name__ == "__main__":
    write_records()

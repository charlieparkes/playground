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

TEMP_CREDENTIALS_DURATION = 8 * 60 * 60  # 8 hours


@backoff.on_exception(
    backoff.expo, pytest_localstack.exceptions.TimeoutError, max_tries=3
)
def check(session, service):
    return SERVICE_CHECKS[service](session)


@backoff.on_exception(backoff.expo, (TimeoutError, ClientError), max_time=30)
def backoff_check(func):
    return func()


def check_status(response, code=2, keys=["ResponseMetadata", "HTTPStatusCode"]):
    """Check status of an AWS API response."""
    status = get_nested(response, keys)
    assert status // 100 == code
    return status


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

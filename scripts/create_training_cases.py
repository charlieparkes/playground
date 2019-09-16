# noqa
import itertools
import json
import logging
import multiprocessing as mp
import os
import queue
import warnings
from collections import defaultdict
from time import sleep
from urllib.parse import quote

import boto3
import elasticsearch
from django.conf import settings
from django.core.management.base import BaseCommand
from elasticsearch_dsl import Q, Search
from mintel_logging import LogLevel
from tqdm import tqdm

from api.es.utils import execute_es_multisearch, setup_connections
from api.image_utils import process_og_media
from api.taxonomy import TaxonomyURI
from sherpa.utils import batch

warnings.filterwarnings("ignore", "unclosed")

logger = logging.getLogger()

DUMP_PATH = "/tmp/training_dumps/"

SETS = settings.TRAINING_SETS
CASE_TYPE_WEIGHTS = {"isGeneralBranding": 0.2, "tagged": 0.6, "notUsable": 0.2}
ANALYTICS_SCAN_BUFFER_SIZE = 50


def es_helpers_handler(record):
    """Logging handler, elasticsearch.helpers"""
    return True


def es_handler(record):
    """Logging handler, elasticsearch"""
    tqdm.write(f"{record}: {record.msg}")
    return True


class Command(BaseCommand):
    help = "Build training cases and upload to s3."

    def add_arguments(self, parser):
        parser.add_argument(
            "--overwrite_existing",
            dest="overwrite_existing",
            choices=["true"],
            help="If training case bucket already contains cases, you must set this variable to overwrite them.  Please note that training cases will need to be audited by Ops again, from scratch!",
        )

    def handle(self, *args, **options):  # noqa: C901
        # Override MLL handlers for elasticsearch
        overwrite_existing = options["overwrite_existing"]
        logging.root.setLevel(LogLevel.WARNING)
        logging.root.handlers[0].handlers["elasticsearch.helpers"] = es_helpers_handler
        logging.root.handlers[0].handlers["elasticsearch"] = es_handler

        # Setup connections
        try:
            setup_connections()
        except elasticsearch.exceptions.TransportError:
            post = "Are you connected to the VPN?" if settings.DEBUG else ""
            tqdm.write(f"Failed to connect to elasticsearch. {post}")

        # Get total number of required cases
        totals = [
            list(counts.values()) for counts in [s["counts"] for s in SETS.values()]
        ]
        self.required_num = sum(list(itertools.chain.from_iterable(totals)))

        # Sum the required cases by vendor, so we can search for all of required
        # vendor's cases at once.
        required_by_vendor = defaultdict(int)
        sets_with_vendor = defaultdict(set)
        for set_name, set_config in SETS.items():
            for vendor in set_config["counts"].keys():
                required_by_vendor[vendor] += set_config["counts"][vendor]
                sets_with_vendor[vendor].add(set_name)

        # Create empty dictionary of sets which look like
        # {"set1": {"vendor1": [], "vendor2": []}}
        self.set_cases = {
            set_name: {
                vendor: {"isGeneralBranding": [], "tagged": [], "notUsable": []}
                for vendor, _ in set_config["counts"].items()
            }
            for set_name, set_config in SETS.items()
        }

        # Initialize GUI, progress bars
        required_pbar = tqdm(
            total=self.required_num,
            leave=False,
            bar_format="(Mapped Cases) {bar} | {n_fmt}/{total_fmt}",
            position=0,
        )
        search_pbar = tqdm(
            total=0,
            leave=False,
            bar_format="(# Cases / # OGs) {bar} | {n_fmt}/{total_fmt}",
            position=1,
        )

        checked_objects = []
        new_cases = []
        total_pulled = 0
        total_created = 0
        total_mapped = 0

        # For each vendor + case_type combination, generate cases by querying elasticsearch
        for vendor, num in required_by_vendor.items():
            for case_type, weight in CASE_TYPE_WEIGHTS.items():
                subtotal_desired = int(num * weight)
                buffer_size = (
                    subtotal_desired
                    if subtotal_desired < ANALYTICS_SCAN_BUFFER_SIZE
                    else ANALYTICS_SCAN_BUFFER_SIZE
                )
                search = get_analytics_search(vendor, case_type)
                subset_new_cases = []

                required_pbar_val = total_mapped
                search_pbar_val = total_created

                with mp.Manager() as manager:
                    pulled = mp.Value("i", 0)
                    evaluated = mp.Value("i", 0)
                    created = mp.Value("i", 0)
                    mapped = mp.Value("i", 0)
                    q_docs = manager.Queue()
                    q_msg = manager.Queue()

                    mp.Pool(processes=1)
                    scan = mp.Process(
                        target=scan_analytics,
                        args=(
                            q_msg,
                            q_docs,
                            search,
                            pulled,
                            evaluated,
                            mapped,
                            subtotal_desired,
                            buffer_size,
                        ),
                    )

                    processes = [scan]
                    scan.start()

                    while mapped.value < subtotal_desired:
                        # Check if our scan process is alive.
                        if not scan.is_alive():
                            tqdm.write("scan died prematurely")
                            break

                        # Fetch any messages from the other process(es)
                        q_size = q_msg.qsize()
                        for i in range(q_size):
                            try:
                                msg = q_msg.get(False)
                                tqdm.write(msg)
                            except queue.Empty:
                                break

                        required_pbar_val = update_pbar(
                            required_pbar,
                            required_pbar_val,
                            (total_mapped + mapped.value),
                        )
                        search_pbar_val = update_pbar(
                            search_pbar,
                            search_pbar_val,
                            (total_created + created.value),
                            (total_pulled + pulled.value),
                        )

                        # Fetch docs off the queue.
                        docs = {}
                        total_docs = 0
                        left_to_find = subtotal_desired - mapped.value
                        current_buf_size = (
                            buffer_size if buffer_size < left_to_find else left_to_find
                        )
                        while total_docs < current_buf_size:
                            try:
                                doc = q_docs.get(False)
                                outcome_id = doc["outcomeId"]
                                # If we somehow get two docs with the same outcome ID,
                                # act as though the dropped doc was "evaluated"
                                docs[outcome_id] = doc
                                total_docs += 1
                            except queue.Empty:
                                sleep(0)

                        n_evaluated = total_docs

                        # Create an asset search for each outcome (asset) ID.
                        searches = []
                        for outcome_id in docs.keys():
                            if outcome_id not in checked_objects:
                                searches.append(get_asset_search(outcome_id))

                        # Execute multisearch against asset outcomes index.
                        max_retries = 5
                        retries = 0
                        while retries < max_retries:
                            try:
                                results = execute_es_multisearch(
                                    searches,
                                    index="product_tagger_asset",
                                    using="staging",
                                    setup=False,
                                )
                                break
                            except (
                                elasticsearch.exceptions.TransportError,
                                elasticsearch.helpers.ScanError,
                            ) as e:
                                if retries == (max_retries - 1):
                                    raise
                                retries += 1
                                err = type(e).__name__
                                tqdm.write(f"{err}, retrying... ({retries})")
                                sleep(1)

                        # Count fetched docs as "evaluated"
                        with evaluated.get_lock():
                            evaluated.value += n_evaluated

                        # Combine multisearch results into a single list of hits.
                        hits = []
                        for r in results:
                            for asset_doc in r.hits:
                                hits.append(asset_doc)

                        # Validate asset, normalize, and add it to the case.
                        for asset in hits:
                            outcome_id = asset.meta.id
                            try:
                                assert asset.outcome is not None
                                assert outcome_id not in checked_objects
                                assert outcome_id in docs
                            except AssertionError:
                                doc_id = None
                                if outcome_id in docs:
                                    doc = docs[outcome_id]
                                    doc_id = doc["id"]
                                in_checked_objects = outcome_id in checked_objects
                                test_case = doc
                                test_case["correctOutcome"] = normalize_tagging(
                                    asset.to_dict().get("outcome")
                                )
                                test_case_type = get_case_type(test_case)
                                tqdm.write(
                                    f"Case rejected because its asset failed an assertion. ({outcome_id}, {doc_id}, in_checked_objects: {in_checked_objects}, case_type: {test_case_type})"
                                )
                                continue

                            # If asset is valid, finish the new case, and place it in our output.
                            new_case = docs[outcome_id]
                            new_case["correctOutcome"] = normalize_tagging(
                                asset.to_dict().get("outcome")
                            )
                            checked_objects.append(outcome_id)

                            with created.get_lock():
                                created.value += 1

                            # Try to place it in a training batch based on our requirements.
                            case_id = (
                                "("
                                + new_case["id"]
                                + ", "
                                + new_case["outcomeId"]
                                + ")"
                            )
                            if is_case_type(new_case, case_type):
                                subset_new_cases.append(new_case)
                                with mapped.get_lock():
                                    mapped.value += 1
                            else:
                                tqdm.write(f"Case {case_id} was not placed.")

                            if mapped.value >= subtotal_desired:
                                break

                        required_pbar_val = update_pbar(
                            required_pbar,
                            required_pbar_val,
                            (total_mapped + mapped.value),
                        )
                        search_pbar_val = update_pbar(
                            search_pbar,
                            search_pbar_val,
                            (total_created + created.value),
                            (total_pulled + pulled.value),
                        )

                    # The scan process should have exited on its own,
                    # but if it didn't, ensure we terminate.
                    stop_processes(processes, force_stop=True)

                    total_pulled += pulled.value
                    total_created += created.value
                    total_mapped += mapped.value

                new_cases.extend(subset_new_cases)
                n_cases = len(subset_new_cases)
                tqdm.write(
                    f"Generated {n_cases}/{subtotal_desired} cases for {vendor} {case_type}"
                )
        self.place_cases_in_sets(new_cases)

        required_pbar.close()
        search_pbar.close()

        # Print summary and aggregate complete sets.
        tqdm.write(f"Search complete.", end="\n\n")
        tqdm.write("Summary:")

        n_found = 0
        sets_to_write = {}
        for set_name, vendors in self.set_cases.items():
            for vendor, case_types in vendors.items():
                for case_type, cases in case_types.items():
                    n_cases = len(cases)
                    n_found += n_cases
                    if n_cases > 0:
                        tqdm.write(f" • {set_name} {vendor} {case_type} {n_cases}")

            # Combine all the cases split by vendor/case_type into a single list per set.
            sets_to_write[set_name] = list(
                itertools.chain.from_iterable(
                    list(
                        itertools.chain.from_iterable(
                            [
                                [cases for cases in vendor.values()]
                                for vendor in vendors.values()
                            ]
                        )
                    )
                )
            )

        tqdm.write("")
        tqdm.write(
            f"Found {total_pulled} OGs, created {total_created} cases, and mapped {total_mapped} of those to your desired training sets."
        )
        tqdm.write(f"Mapped {n_found}/{self.required_num} desired cases.")

        if query_sets_exist():
            if overwrite_existing:
                delete_existing_sets()
                write_sets(sets_to_write)
            else:
                tqdm.write(
                    f"Sets already found; please set overwrite_existing to overwrite (but remember that new cases must be re-audited by Ops!)"
                )
        else:
            write_sets(sets_to_write)

    def place_cases_in_sets(self, cases):
        """Given a list of the exact number of cases we need, sort them into sets as desired.

        Args:
            cases (list): Case dicts. See `build_case()` for the format of this dict.
        """
        for case in cases:
            # Try to place it in a training batch based on our requirements.
            placed = False
            for set_name, set_config in SETS.items():
                vendors = set_config["counts"]
                if case["vendor"] in vendors:
                    vendor = case["vendor"]
                    count = vendors[vendor]
                    for case_type, weight in CASE_TYPE_WEIGHTS.items():
                        n_matching_cases = len(
                            self.set_cases[set_name][vendor][case_type]
                        )
                        n_desired_cases = int(count * weight)
                        if n_matching_cases < n_desired_cases:
                            if is_case_type(case, case_type):
                                case["meta"] = {"case_type": case_type}
                                self.set_cases[set_name][vendor][case_type].append(case)
                                placed = True
                                break
                if placed:
                    break
            if not placed:
                raise Exception(
                    "It should be impossible to 'not place' a case at this point."
                )


def scan_analytics(
    q_msg,
    q_docs,
    analytics_search,
    total_pulled,
    total_evaluated,
    total_mapped,
    total_desired,
    buffer_size,
    s=0,
):
    """Fetch analytics documents as necessary while on a separate thread/process.

    Args:
        q_msg (multiprocessing.Queue): Messages for the main thread to write to stdout
        q_docs (multiprocessing.Queue): Baby cases formed from analytics doc fields
        analytics_search (elasticsearch_dsl.Search): Search for specific vendor/case_type
        total_pulled (multiprocessing.Value): Counter of analytics docs pulled
        total_evaluated (multiprocessing.Value): Counter of attempted doc -> asset matchups
        total_mapped (multiprocessing.Value): Counter of valid cases mapped to sets
        total_desired (multiprocessing.Value): Counter of total valid cases required
        buffer_size (int): Number of documents/cases to fetch at once
        s (int): parameter for sleep()
    """
    try:
        for doc in analytics_search.scan():
            with total_pulled.get_lock():
                total_pulled.value += 1

            # Validate the document we just fetched from analytics.
            try:
                assert doc.scratch is not None
                assert doc.vendor is not None
                assert doc.marketingCompany is not None
                raw_doc = doc.to_dict()
                assert "uri" in raw_doc["marketingCompany"]
            except AssertionError:
                q_msg.put(
                    f"scan: Doc rejected because it failed an assertion. {doc.meta.id}"
                )
                continue

            # Push the beginnings of our new case onto a queue.
            q_docs.put(
                {
                    "id": str(doc.meta.id),
                    "vendor": str(doc.vendor),
                    "companyId": str(doc.marketingCompany.uri),
                    "media": process_og_media(doc.to_dict()),
                    "outcomeId": doc.scratch.productTaggerAssetId,
                    "correctOutcome": None,
                }
            )

            if total_mapped.value >= total_desired:
                return True

            # Once we've filled the buffer, wait for it to empty the buffer before loading more.
            n_in_buffer = total_pulled.value - total_evaluated.value
            if n_in_buffer >= buffer_size:
                while n_in_buffer > 0:
                    # We met our requirements with the last buffer.
                    if total_mapped.value >= total_desired:
                        return True
                    sleep(0)
                    n_in_buffer = total_pulled.value - total_evaluated.value
                if total_mapped.value >= total_desired:
                    return True
    except (
        elasticsearch.exceptions.TransportError,
        elasticsearch.helpers.ScanError,
    ) as e:
        err = type(e).__name__
        q_msg.put(f"scan: {err}")
        sleep(0)
    return False


def get_asset_search(asset_id):
    """Given an asset_id, create a Search which fetches the asset.

    Args:
        asset_id (str): ES Staging product_tagger_asset document ID

    Returns:
        elasticsearch_dsl.Search
    """
    must = []
    must.append(Q("term", _id=asset_id))
    must.append(Q("exists", field="outcome"))
    return (
        Search(using="staging", index="product_tagger_asset")
        .update_from_dict({"size": 1})
        .source(["outcome"])
        .query(Q("bool", must=must))
    )


def get_analytics_search(vendor, case_type):
    """Create a search to find OGs with a specific vendor and case_type.

    Args:
        vendor (str): Vendor abbreviation/acronym e.g. MIN, EDS, etc
        case_type (str): See CASE_TYPE_WEIGHTS and is_case_type()

    Returns:
        elasticsearch_dsl.Search
    """
    fields = [
        "vendor",
        "scratch.productTaggerAssetId",
        "media",
        "marketingCompany",
        "ingestionTs",
        # XXX: The following fields are for process_media
        "RIQ",
        "assetUrl",
        "PAT",
        "channelType",
        "pageId",
        "postId",
        "subChannel",
    ]
    must = []
    must.append(Q("term", og_o_join="og"))
    must.append(Q("term", vendor=vendor))
    must.append(Q("term", isPublishedOps=True))

    if case_type == "isGeneralBranding":
        must.append(Q("term", isGeneralBranding=True))
    elif case_type == "notUsable":
        must.append(Q("term", isUsable=False))
    elif case_type == "tagged":
        must.append(Q("term", isGeneralBranding=False))
        must.append(Q("term", isUsable=True))

    must.append(Q("exists", field="scratch.productTaggerAssetId"))
    must.append(Q("exists", field="vendor"))
    must.append(Q("exists", field="media"))
    must.append(Q("exists", field="marketingCompany"))

    search = (
        Search(using="analytics", index="eve")
        .source(fields)
        .query(Q("bool", must=must))
    )
    return search


def get_case_type(case):
    for case_type in CASE_TYPE_WEIGHTS.keys():
        if is_case_type(case, case_type):
            return case_type
    return None


def is_case_type(case, case_type):
    """Determins if case is case_type.

    Args:
        case (dict): A valid case.
        case_type (str): See CASE_TYPE_WEIGHTS and is_case_type()

    Returns:
        bool: is type of case == case_type
    """
    try:
        assert "correctOutcome" in case and case["correctOutcome"]
    except AssertionError:
        raise
    outcome = case["correctOutcome"]
    if case_type == "isGeneralBranding" and "isGeneralBranding" in outcome:
        is_general_branding = outcome.get("isGeneralBranding")
        if isinstance(is_general_branding, bool) and is_general_branding is True:
            return True
    if case_type == "tagged" and "productIds" in outcome:
        product_ids = outcome.get("productIds")
        if isinstance(product_ids, list) and len(product_ids) > 0:
            return True
    if case_type == "notUsable" and "notUsableReason" in outcome:
        reason = outcome.get("notUsableReason")
        if isinstance(reason, str) and len(reason) > 0:
            return True
    return False


def upload_to_s3(path, filename, metadata):
    s3_client = boto3.client("s3")
    s3_client.upload_file(
        path, settings.TRAINING_CASES_S3_BUCKET, filename, {"Metadata": metadata}
    )


def create_s3_object(set_name, key, case, metadata):
    clean_set_name = quote(set_name, safe="")
    clean_key = quote(key, safe="")
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=settings.TRAINING_CASES_S3_BUCKET,
        Key=f"{set_name}/{clean_key}",
        Body=json.dumps(case),
        Metadata=metadata,
        Tagging=f"set_name={clean_set_name}",
    )


def query_sets_exist():
    """See whether sets exist inside the training bucket"""
    tqdm.write("Checking existing cases in s3")
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(settings.TRAINING_CASES_S3_BUCKET)
    x = bucket.objects.all()
    if x:
        return True
    return False


def delete_existing_sets():
    # We don't need to bother deleting the sets on disk, but let's get the ones in s3.
    if not settings.DEBUG:
        tqdm.write("Deleting all existing cases in s3.")
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(settings.TRAINING_CASES_S3_BUCKET)
        keys = [{"Key": object.key} for object in bucket.objects.all()]
        if keys:
            for key_batch in batch(keys, 100):
                bucket.delete_objects(Delete={"Objects": key_batch})


def write_sets(sets_to_write):
    # Write sets to disk as JSON, upload to s3 if we're in prod
    if not os.path.exists(DUMP_PATH):
        os.makedirs(DUMP_PATH)

    for set_name, the_set in sets_to_write.items():
        metadata = {"set_name": set_name, "size": str(len(the_set))}

        # Write set to disk.
        file_path = f"{DUMP_PATH}{set_name}.json"
        with open(file_path, "w") as file:
            json.dump(the_set, file, indent=4, ensure_ascii=False)
            tqdm.write(f"Wrote {set_name} to {file_path}.")

        # Upload set to s3.
        if not settings.DEBUG:
            for case in the_set:
                create_s3_object(set_name, case["id"], case, metadata)
            tqdm.write(f"Wrote {set_name} to S3.")


def normalize_tagging(data):
    """Convert input to the current form if not already so."""
    product_objects = data.get("brandedProducts", []) + data.get(
        "unbrandedProducts", []
    )
    if product_objects:
        product_ids = [product["id"] for product in product_objects]
    else:
        product_ids = data.get("productIds", [])
    product_ids = [clean_taxonomy_id(id_, type="product") for id_ in product_ids]

    if "rejectReason" in data:
        not_usable_reason = data["rejectReason"]
        is_general_branding = "general branding" in not_usable_reason.lower()
        if is_general_branding:
            not_usable_reason = None
    else:
        is_general_branding = data.get("isGeneralBranding", False)
        not_usable_reason = data.get("notUsableReason")

    if "company" in data:
        company_id = clean_taxonomy_id(data["company"]["id"], type="company")
    elif "companyId" in data:
        company_id = clean_taxonomy_id(data["companyId"], type="company")
    else:
        company_id = None

    return {
        "productIds": product_ids,
        "isGeneralBranding": is_general_branding,
        "companyId": company_id,
        "notUsableReason": not_usable_reason,
    }


def clean_taxonomy_id(id, type):
    """Given an id and type, return a correctly formatted taxonomy ID"""
    if TaxonomyURI.pattern.search(str(id)):
        return id
    else:
        return TaxonomyURI(version=1, type=type, id=id).encoded


def update_pbar(pbar, current, new, total=None):
    """Update a tqdm progress bar."""
    delta = new - current
    if total:
        pbar.total = total
    if delta > 0:
        pbar.update(delta)
    return new


def stop_processes(procs, force_stop=False):
    """Stop processes if one is dead or we just want them dead.

    Args:
        procs (list): of multiprocessing.Process objects
        force_stop (bool): If true, terminate all
    """
    if (False in [p.is_alive() for p in procs]) or force_stop:
        for p in procs:
            p.terminate()
        return True
    return False


class NoMoreRecords(Exception):
    pass

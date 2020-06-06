from diagrams import Cluster, Diagram, Edge
from diagrams.aws.compute import EC2, ECS, Lambda
from diagrams.aws.database import RDS, ElastiCache
from diagrams.aws.integration import SQS
from diagrams.aws.network import ELB, Route53
from diagrams.aws.storage import S3

# https://github.com/mingrammer/diagrams
# https://diagrams.mingrammer.com/docs/nodes/aws

# def _sqs_lam(name):
#     q = SQS(name)
#     l = Lambda(name)
#     q >> l
#     return SQS(name), Lambda(name)

with Diagram("MDP Enrichment", show=False):
    with Cluster("es-staging"):
        staging = ECS("master")
        with Cluster("data"):
            staging_data = [ECS("data"), ECS("data"), ECS("data")]
        staging - staging_data

    with Cluster("gateway"):
        gateway = ECS("gateway")
        jwt = ECS("jwt")
        jwt - RDS()
        gateway >> jwt

    with Cluster("pypedream") as pypedream:
        orchestrator = ECS("orchestrator")
        ops_pipeline = SQS("operations-pipeline")
        ops_pipeline >> orchestrator

        with Cluster("scratch"):
            scratch = SQS()
            scratch >> ECS()
            orchestrator >> scratch

        with Cluster("publish"):
            publish = SQS()
            publish >> ECS()
            orchestrator >> publish

    with Cluster("Data Entry"):
        with Cluster("taxonomy"):
            taxonomy = ECS("taxonomy")
            taxonomy - RDS()

        with Cluster("workflow"):
            workflow = ECS("workflow")
            workflow - RDS()

        serf = ECS("serf")
        sherpa_api = ECS("sherpa-api")
        reports_api = ECS("reports-api")
        bucket_api = ECS("bucket-api")
        es_api = ECS("es-api")

    with Cluster("Discovery & Creation"):
        with Cluster("new-product-line-discovery"):
            new_product_line_discovery = SQS()
            new_product_line_discovery_lam = Lambda()
            new_product_line_discovery >> new_product_line_discovery_lam

        with Cluster("case-creator"):
            case_creator = SQS()
            case_creator_lam = Lambda()
            case_creator >> case_creator_lam

    with Cluster("Service Planner & Collection"):
        with Cluster("shepherd"):
            shepherd = SQS()
            shepherd_lam = Lambda()
            shepherd >> shepherd_lam

        with Cluster("freestyle"):
            freestyle = SQS()
            freestyle_lam = Lambda()
            freestyle >> freestyle_lam

        with Cluster("taxonomy-collector"):
            tax_col = SQS()
            tax_col_lam = Lambda()
            tax_col >> tax_col_lam

        with Cluster("asset-collector"):
            asset_col = SQS()
            asset_col_lam = Lambda()
            asset_col >> asset_col_lam

        with Cluster("training-cases"):
            training = SQS()
            training_lam = Lambda()
            training_sets = S3("training-sets")
            training >> training_lam
            training_lam - training_sets

    with Cluster("ingestion-agent"):
        ia = SQS()
        ia_lam = Lambda()
        ia >> ia_lam

    gateway >> serf

    es_api >> staging

    serf >> taxonomy
    serf >> workflow
    serf >> sherpa_api
    serf >> reports_api
    serf >> bucket_api
    serf >> es_api

    case_creator_lam >> workflow

    taxonomy >> shepherd
    workflow >> shepherd
    reports_api >> shepherd

    shepherd_lam >> tax_col
    shepherd_lam >> asset_col
    shepherd_lam >> training
    shepherd_lam >> freestyle

    tax_col_lam >> ia
    asset_col_lam >> ia
    asset_col_lam >> new_product_line_discovery
    freestyle_lam >> case_creator
    training_lam >> case_creator

    new_product_line_discovery_lam >> case_creator

    ia_lam >> ops_pipeline
    ia_lam >> staging

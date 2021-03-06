import json
from datetime import datetime, timedelta, timezone

import click

import boto3
from botocore.exceptions import ClientError
from lpipe.utils import check_status, get_nested, hash
from tabulate import tabulate
from tqdm import tqdm
from utils import batch
from utils.aws import auth

REGION = "us-east-2"


def _call(_callable, *args, **kwargs):
    try:
        resp = _callable(*args, **kwargs)
        check_status(resp)
        return resp
    except ClientError as e:
        raise


class Resource:
    def __init__(self, meta, attr_map=None):
        try:
            self.meta = meta
            if attr_map:
                for attr_name, meta_key in attr_map:
                    value = self.meta.get(meta_key, None)
                    setattr(self, attr_name, value)
            else:
                for k, v in meta.items():
                    setattr(self, k, v)
        except Exception as e:
            print(f"Error in {self.__class__}: {e}")
            raise


class ContainerDefinition(Resource):
    def __init__(self, meta):
        super().__init__(
            meta,
            (
                ("name", "name"),
                ("cpu", "cpu"),
                ("memory", "memory"),
                ("memory_reservation", "memoryReservation"),
            ),
        )

    def __repr__(self):
        return f"ContainerDefinition<{self.name}>"


class TaskDefinition(Resource):
    def __init__(self, meta):
        super().__init__(
            meta,
            (
                ("arn", "taskDefinitionArn"),
                # ('containers', 'containerDefinitions'),
                ("family", "family"),
                ("revision", "revision"),
                ("cpu", "cpu"),
                ("memory", "memory"),
            ),
        )
        self.container_definitions = []
        for cd in self.meta.get("containerDefinitions", []):
            self.container_definitions.append(ContainerDefinition(cd))

    def __repr__(self):
        return f"TaskDefinition<{self.arn}>"

    @property
    def reservations(self):
        def _total(attr):
            return sum(
                [
                    getattr(cd, attr)
                    for cd in self.container_definitions
                    if getattr(cd, attr)
                ]
            )

        if not getattr(self, "_reservations", None):
            self._reservations = {
                "cpu": _total("cpu"),
                "memory": _total("memory"),
                "memory_reservation": _total("memory_reservation"),
            }

        return self._reservations

    @classmethod
    def load(cls, arn):
        client = boto3.client("ecs", region_name=REGION)
        response = _call(client.describe_task_definition, taskDefinition=arn)
        return cls(response["taskDefinition"])


class Service(Resource):
    def __init__(self, meta):
        super().__init__(
            meta,
            (
                ("name", "serviceName"),
                ("arn", "serviceArn"),
                ("events", "events"),
                ("desired_count", "desiredCount"),
                ("pending_count", "pendingCount"),
                ("running_count", "runningCount"),
                ("deployments", "deployments"),
                ("_task_definition_arn", "taskDefinition"),
            ),
        )
        self._task_definition = None

    def __repr__(self):
        return f"Service<{self.name}>"

    @property
    def tabulate(self):
        return [
            self.name,
            self.desired_count,
            self.cpu[0],
            self.memory[0],
            self.memory_reservation[0],
            self.cpu[1],
            self.memory[1],
            self.memory_reservation[1],
        ]

    @property
    def task_definition(self):
        if not self._task_definition:
            self._task_definition = TaskDefinition.load(self._task_definition_arn)
        return self._task_definition

    @property
    def cpu(self):
        if not getattr(self, "_cpu", None):
            self._cpu = self.task_definition.reservations["cpu"]
        return (self._cpu, self._cpu * self.desired_count)

    @property
    def memory(self):
        if not getattr(self, "_memory", None):
            self._memory = self.task_definition.reservations["memory"]
        return (self._memory, self._memory * self.desired_count)

    @property
    def memory_reservation(self):
        if not getattr(self, "_memory_reservation", None):
            self._memory_reservation = self.task_definition.reservations[
                "memory_reservation"
            ]
        return (self._memory_reservation, self._memory_reservation * self.desired_count)

    @classmethod
    def load_all(cls, services=None, cluster="default"):
        services = services if services else Service.list(cluster=cluster)
        print(f"Found {len(services)} services.")
        if not services:
            return {}
        client = boto3.client("ecs", region_name=REGION)
        _services = {}
        print("Describing services...")
        for b in tqdm(batch(services, 10)):
            descriptions = _call(
                client.describe_services, services=b, cluster=cluster, include=["TAGS"]
            )
            for d in descriptions["services"]:
                s = cls(d)
                _services[s.name] = s
        return _services

    @classmethod
    def load(cls, name):
        return cls.load_services([name])[name]

    @classmethod
    def list(cls, cluster="default"):
        def _values(r):
            return (r["serviceArns"], r.get("nextToken", None))

        client = boto3.client("ecs", region_name=REGION)
        service_arns = []
        print("Listing services...")
        arns, next_token = _values(
            _call(client.list_services, cluster=cluster, maxResults=20)
        )
        service_arns.extend(arns)
        while next_token:
            arns, next_token = _values(
                _call(
                    client.list_services,
                    cluster=cluster,
                    maxResults=100,
                    nextToken=next_token,
                )
            )
            service_arns.extend(arns)
        return list(set(service_arns))


@click.command()
@click.option("--sort", default="cpu")
@click.option("--env", default="prod")
@click.option("--cluster", default="default")
def cmd(env, sort, cluster):
    with auth([f"everest-{env}"]):
        services = list(Service.load_all(cluster=cluster).values())
        services = [s for s in services if s.desired_count > 0]
        services.sort(key=lambda x: getattr(x, sort)[1], reverse=True)

        # client = boto3.client('ce', region_name=REGION)

        print("Loading task definitions...")
        for s in tqdm(services):
            s.task_definition

        print("Tabulating...")
        table = [s.tabulate for s in services]
        print(
            tabulate(
                table,
                headers=[
                    "Name",
                    "Desired Count",
                    "CPU",
                    "MEM",
                    "MEMR",
                    "Total CPU",
                    "Total MEM",
                    "Total MEMR",
                ],
            )
        )


if __name__ == "__main__":
    cmd()

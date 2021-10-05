# @pytest.fixture(scope="session")
# def dynamodb_tables():
#     return [
#         {
#             "AttributeDefinitions": [
#                 {"AttributeName": "uri", "AttributeType": "S"},
#                 {"AttributeName": "timestamp", "AttributeType": "S"},
#             ],
#             "TableName": "omni-taxonomy-alerts-table",
#             "KeySchema": [
#                 {"AttributeName": "uri", "KeyType": "HASH"},
#                 {"AttributeName": "timestamp", "KeyType": "RANGE"},
#             ],
#         },
#     ]


# @pytest.fixture(scope="class")
# def dynamodb(localstack, dynamodb_tables):
#     check(localstack, "dynamodb")
def create_then_destroy(dynamodb_tables):
    dbd = boto3.client("dynamodb")

    @backoff.on_exception(backoff.expo, ClientError, max_time=30)
    def create_table(config):
        config.update({"BillingMode": "PAY_PER_REQUEST"})
        response = dbd.create_table(**config)
        return lpipe.utils.check_status(response)

    try:
        for table in dynamodb_tables:
            assert create_table(table)

        for table in dynamodb_tables:
            waiter = dbd.get_waiter("table_exists")
            waiter.wait(
                TableName=table["TableName"],
                WaiterConfig={"Delay": 1, "MaxAttempts": 30,},
            )

        yield [t["TableName"] for t in dynamodb_tables]
    finally:
        for table in dynamodb_tables:
            dbd.delete_table(TableName=table["TableName"])

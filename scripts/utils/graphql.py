import re
import shlex

import requests
from utils.exceptions import InvalidTaxonomyURI, GraphQLError


def query_graphql(raw_query, endpoint):
    """Query a graphql API handle errors."""
    query = " ".join(shlex.split(raw_query, posix=False))
    r = requests.get(endpoint, params={"query": query})
    if r.status_code == 200:
        return r.json()
    elif r.status_code == 400:
        response = r.json()
        assert "errors" in response
        raise GraphQLError("".join([e["message"] for e in response["errors"]]))
    else:
        raise requests.exceptions.RequestException(
            f"HTTP Status: {r.status_code}, Response Body: {r.text}"
        )

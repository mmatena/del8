"""My modified version of the vast.py file for better access from within python."""
import json
import re
import requests
import os
from urllib.parse import quote_plus

from . import constants as C


def _assert_no_unconsumed_text(opts, query_str):
    joined = "".join("".join(x) for x in opts)
    if joined != query_str:
        raise ValueError(
            "Unconsumed text. Did you forget to quote your query? "
            + repr(joined)
            + " != "
            + repr(query_str)
        )


def _parse_opts(query_str):
    if type(query_str) == list:
        query_str = " ".join(query_str)
    query_str = query_str.strip()
    opts = re.findall(
        r"([a-zA-Z0-9_]+)( *[=><!]+| +(?:[lg]te?|nin|neq|eq|not ?eq|not ?in|in) )?( *)(\[[^\]]+\]|[^ ]+)?( *)",
        query_str,
    )
    _assert_no_unconsumed_text(opts, query_str)
    return opts


def _assert_valid_opt(field, op_name, value, op):
    if not field:
        raise ValueError(
            "Field cannot be blank. Did you forget to quote your query? "
            + repr((field, op, value))
        )
    if field not in C.FIELDS:
        raise ValueError(
            "Unrecognized field in query: {}, see list of recognized fields.".format(
                field
            )
        )
    if not op_name:
        raise ValueError(
            "Unknown operator. Did you forget to quote your query? "
            + repr(op).strip("u")
        )
    if not value:
        raise ValueError(
            "Value cannot be blank. Did you forget to quote your query? "
            + repr((field, op, value))
        )


def parse_search_query(query_str, res=None):
    if res is None:
        res = {}
    opts = _parse_opts(query_str)
    for field, op, _, value, _ in opts:
        value = value.strip(",[]")
        v = res.setdefault(field, {})
        op = op.strip()
        op_name = C.OP_NAMES.get(op)

        if field in C.FIELD_ALIASES:
            field = C.FIELD_ALIASES[field]

        if op_name in ["in", "notin"]:
            value = [x.strip() for x in value.split(",") if x.strip()]

        _assert_valid_opt(field, op_name, value, op)

        if value in ["?", "*", "any"]:
            if op_name != "eq":
                raise ValueError("Wildcard only makes sense with equals.")
            if field in v:
                del v[field]
            if field in res:
                del res[field]
            continue

        if field in C.FIELD_MULTIPLIERS:
            value = str(float(value) * C.FIELD_MULTIPLIERS[field])

        v[op_name] = value
        res[field] = v
    return res


def parse_search_order(order_str):
    order = []
    for name in order_str.split(","):
        name = name.strip()
        if not name:
            continue

        direction = "asc"
        if name.strip("-") != name:
            direction = "desc"

        field = name.strip("-")
        if field in C.FIELD_ALIASES:
            field = C.FIELD_ALIASES[field]

        if field not in C.FIELDS:
            raise ValueError(
                "Unrecognized field in order: {}, see list of recognized fields.".format(
                    field
                )
            )

        order.append([field, direction])
    return order


def _get_api_key():
    api_key_file = os.path.expanduser(C.API_KEY_FILE_BASE)
    if not os.path.exists(api_key_file):
        raise ValueError(f"Api key file not found at {api_key_file}")
    with open(api_key_file, "r") as reader:
        return reader.read().strip()


def _apiurl(subpath, query_args=None):
    if query_args is None:
        query_args = {}
    if "api_key" not in query_args:
        query_args["api_key"] = _get_api_key()
    return (
        C.SERVER_URL_DEFAULT
        + subpath
        + "?"
        + "&".join(
            "{x}={y}".format(
                x=x, y=quote_plus(y if isinstance(y, str) else json.dumps(y))
            )
            for x, y in query_args.items()
        )
    )


def search_offers(
    query_str,
    order_str,
    *,
    use_defaults=True,
    offer_type="on-demand",
    disable_bundling=True,
):
    if use_defaults:
        query = {
            "verified": {"eq": True},
            "external": {"eq": False},
            "rentable": {"eq": True},
        }
    else:
        query = {}

    query = parse_search_query(query_str, query)
    query["order"] = parse_search_order(order_str)
    query["type"] = offer_type
    if disable_bundling:
        query["disable_bundling"] = True

    url = _apiurl("/bundles", {"q": query})
    r = requests.get(url)
    r.raise_for_status()
    rows = r.json()["offers"]
    return rows


###############################################################################


def create_instance(
    instance_id, *, disk_gb: int, image: str, onstart_cmd: str = None, label: str = None
):
    url = _apiurl(f"/asks/{instance_id}/")
    r = requests.put(
        url,
        json={
            "client_id": "me",
            "image": image,
            "disk": disk_gb,
            "label": label,
            "onstart": onstart_cmd,
            "runtype": "ssh",
        },
    )
    r.raise_for_status()
    return r.json()


###############################################################################


def get_instances(owner: str = "me"):
    req_url = _apiurl("/instances", {"owner": owner})
    r = requests.get(req_url)
    r.raise_for_status()
    rows = r.json()["instances"]
    return rows


###############################################################################


def destroy_instance(instance_id):
    url = _apiurl(f"/instances/{instance_id}/")
    r = requests.delete(url, json={})
    r.raise_for_status()
    return r.json()

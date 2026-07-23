"""
Tiny CLI client. Talks to whichever node you point it at -- if that node
isn't the leader, it transparently forwards writes, so you can hit ANY
node and it still works.

Usage:
    python client.py --node 0 put name nutanix
    python client.py --node 1 get name
    python client.py --node 2 delete name
    python client.py --node 0 status
"""

import argparse
import json

import requests

from config import CLUSTER


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=int, required=True, help="which node to send the request to")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_put = sub.add_parser("put")
    p_put.add_argument("key")
    p_put.add_argument("value")

    p_get = sub.add_parser("get")
    p_get.add_argument("key")

    p_del = sub.add_parser("delete")
    p_del.add_argument("key")

    sub.add_parser("status")

    args = parser.parse_args()
    base_url = CLUSTER[args.node]

    if args.cmd == "put":
        r = requests.put(f"{base_url}/keys/{args.key}", json={"value": args.value})
    elif args.cmd == "get":
        r = requests.get(f"{base_url}/keys/{args.key}")
    elif args.cmd == "delete":
        r = requests.delete(f"{base_url}/keys/{args.key}")
    elif args.cmd == "status":
        r = requests.get(f"{base_url}/status")

    print(f"HTTP {r.status_code}")
    print(json.dumps(r.json(), indent=2))


if __name__ == "__main__":
    main()

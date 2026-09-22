#!/usr/bin/env python3
"""
12_download_grfr.py

NOT MODIFIED IN THE CONUS-OPTIMIZATION PASS -- of everything in this
pipeline, this script needed it least. Because GRFR moves through Globus
rather than direct HTTP, the actual bulk data transfer is handled by
Globus's own managed, parallel, resumable transfer engine, not by this
script's Python process -- this script only submits a transfer task and
polls it. That sidesteps the in-memory-accumulation and serial-download
problems fixed elsewhere in this pass, by construction. The one real
CONUS-scale caveat is scope, not mechanism, and the script already prints
it: narrow --source-path before a full, un-narrowed transfer at CONUS
extent, which could be enormous.

RED tier #50: GRFR (Global Reach-level Flood Reanalysis, VIC-RAPID),
3-hourly discharge/runoff.

STRUCTURALLY DIFFERENT FROM EVERY OTHER SCRIPT IN THIS PIPELINE: GRFR's
3-hourly data is distributed via Globus, which is endpoint-to-endpoint
transfer, not a simple HTTP download. This means:
  1. You need a DESTINATION Globus endpoint -- i.e. wherever you're running
     this needs to already be a registered Globus endpoint (most HPC
     centers, including likely Frontier, have one set up institutionally --
     check with Xiao's team/OLCF for Frontier's Globus endpoint UUID and
     the path you're allowed to write to), or Globus Connect Personal
     installed and running if this is a personal machine.
  2. Transfers are asynchronous -- this script submits a transfer TASK and
     polls for its completion, rather than streaming bytes directly the
     way every other script here does.

One-time setup (required):
    1. A Globus identity -- most institutions (including national labs)
       already provide one; log in at https://app.globus.org to confirm.
    2. Register a native-app OAuth client for this pipeline at
       https://app.globus.org/settings/developers, and set its Client ID
       either via config.py's GRFR_GLOBUS_CLIENT_ID or --client-id.
       (Using a shared/generic client ID here isn't appropriate -- each
       team should register their own.)
    3. Know your destination endpoint's UUID and the path you're allowed
       to write to (ask your HPC center's support/documentation for this).

Source endpoints (verified directly from reachhydro.org/home/records/grfr):
    discharge: 7844823f-12dc-4569-a115-39fb107221d5
    runoff:    c15df1c2-5cec-4804-bda6-1020e7fc829b

Kept in native format: this triggers a Globus transfer of the files as-is,
no processing.

Requires: globus-sdk
    pip install globus-sdk

Usage:
    # First, authenticate (one-time interactive; caches a refresh token
    # in ~/.globus_grfr_tokens.json for future non-interactive runs):
    python 12_download_grfr.py --login

    # See what's actually in the source directory before transferring:
    python 12_download_grfr.py --inspect --source discharge

    # Submit a transfer:
    python 12_download_grfr.py --outdir ./output --source discharge \
        --destination-endpoint-id <your-endpoint-uuid> \
        --destination-path /path/on/your/system/grfr_discharge
"""

import argparse
import json
import sys
import time
from pathlib import Path

from config import GRFR_GLOBUS_DISCHARGE_ENDPOINT, GRFR_GLOBUS_RUNOFF_ENDPOINT, GRFR_GLOBUS_CLIENT_ID

TOKEN_CACHE_PATH = Path.home() / ".globus_grfr_tokens.json"

SOURCE_ENDPOINTS = {
    "discharge": GRFR_GLOBUS_DISCHARGE_ENDPOINT,
    "runoff": GRFR_GLOBUS_RUNOFF_ENDPOINT,
}


def get_client_id(args):
    client_id = args.client_id or GRFR_GLOBUS_CLIENT_ID
    if not client_id:
        print("ERROR: no Globus native-app Client ID configured. Register one at "
              "https://app.globus.org/settings/developers and pass it via --client-id "
              "or set config.GRFR_GLOBUS_CLIENT_ID.", file=sys.stderr)
        sys.exit(1)
    return client_id


def do_login(client_id: str):
    import globus_sdk

    auth_client = globus_sdk.NativeAppAuthClient(client_id)
    auth_client.oauth2_start_flow(refresh_tokens=True)
    authorize_url = auth_client.oauth2_get_authorize_url()

    print("Please go to this URL and log in:\n")
    print(f"  {authorize_url}\n")
    auth_code = input("Paste the authorization code here: ").strip()

    token_response = auth_client.oauth2_exchange_code_for_tokens(auth_code)
    transfer_tokens = token_response.by_resource_server["transfer.api.globus.org"]

    with open(TOKEN_CACHE_PATH, "w") as f:
        json.dump(transfer_tokens, f)
    TOKEN_CACHE_PATH.chmod(0o600)
    print(f"\nLogin successful. Tokens cached at {TOKEN_CACHE_PATH} for future non-interactive runs.")


def get_transfer_client(client_id: str):
    import globus_sdk

    if not TOKEN_CACHE_PATH.exists():
        print(f"ERROR: no cached Globus tokens found at {TOKEN_CACHE_PATH}. "
              f"Run with --login first.", file=sys.stderr)
        sys.exit(1)

    with open(TOKEN_CACHE_PATH) as f:
        tokens = json.load(f)

    auth_client = globus_sdk.NativeAppAuthClient(client_id)
    authorizer = globus_sdk.RefreshTokenAuthorizer(tokens["refresh_token"], auth_client)
    return globus_sdk.TransferClient(authorizer=authorizer)


def inspect_source(tc, source_endpoint: str, path: str = "/"):
    print(f"Listing {source_endpoint}:{path} ...")
    try:
        listing = tc.operation_ls(source_endpoint, path=path)
    except Exception as e:
        print(f"ERROR listing directory: {e}", file=sys.stderr)
        print("This could mean the endpoint UUID is stale, or your Globus identity "
              "doesn't have access -- verify at https://app.globus.org/file-manager "
              "using the same endpoint UUID before assuming this script is wrong.", file=sys.stderr)
        sys.exit(1)
    for item in listing:
        print(f"  {item['type']:6s} {item['name']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--outdir", default="./output", help="Used only for local bookkeeping; the actual "
                         "data lands via Globus transfer at --destination-path")
    parser.add_argument("--source", choices=["discharge", "runoff"], default="discharge")
    parser.add_argument("--source-path", default="/", help="Path within the source Globus endpoint")
    parser.add_argument("--client-id", default=None)
    parser.add_argument("--destination-endpoint-id", default=None)
    parser.add_argument("--destination-path", default=None)
    parser.add_argument("--login", action="store_true", help="Run the one-time interactive Globus login and exit")
    parser.add_argument("--inspect", action="store_true", help="List the source directory and exit -- run this before a real transfer")
    args = parser.parse_args()

    client_id = get_client_id(args)

    if args.login:
        do_login(client_id)
        return

    tc = get_transfer_client(client_id)
    source_endpoint = SOURCE_ENDPOINTS[args.source]

    if args.inspect:
        inspect_source(tc, source_endpoint, args.source_path)
        return

    if not args.destination_endpoint_id or not args.destination_path:
        print("ERROR: --destination-endpoint-id and --destination-path are required for a real "
              "transfer. Run --inspect first to confirm the source structure, and check with your "
              "HPC center for your system's own Globus endpoint UUID.", file=sys.stderr)
        sys.exit(1)

    import globus_sdk

    print(f"Submitting transfer: {source_endpoint}:{args.source_path} -> "
          f"{args.destination_endpoint_id}:{args.destination_path}")
    transfer_data = globus_sdk.TransferData(
        tc, source_endpoint, args.destination_endpoint_id,
        label=f"GRFR {args.source} transfer",
    )
    transfer_data.add_item(args.source_path, args.destination_path, recursive=True)

    task = tc.submit_transfer(transfer_data)
    task_id = task["task_id"]
    print(f"Transfer submitted, task ID: {task_id}")
    print("Polling for completion (this can take a very long time for the full dataset -- "
          "consider narrowing --source-path to a subset first) ...")

    while not tc.task_wait(task_id, timeout=60, polling_interval=30):
        task_info = tc.get_task(task_id)
        print(f"  status: {task_info['status']}, "
              f"{task_info.get('bytes_transferred', 0) / 1e9:.2f} GB transferred so far")

    final_status = tc.get_task(task_id)
    print(f"Done. Final status: {final_status['status']}")


if __name__ == "__main__":
    main()

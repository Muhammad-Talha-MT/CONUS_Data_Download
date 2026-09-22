#!/usr/bin/env python3
"""
03_download_groundwater.py  (CONUS-optimized)

Download full-record (Feb 1979 - Jan 2023), hourly NWM v3.0 retrospective
groundwater-bucket output (depth, inflow, outflow) for every reach/basin
unit in a region's graph.

NOTE: NWM's groundwater "depth" is depth within the conceptual subsurface
bucket store draining to each catchment -- it is NOT river channel water
depth. It's a useful additional predictor/state, not a substitute for
channel stage.

SAME CONUS FIX AS 02_download_streamflow.py, applied here too (see that
script's docstring for the full reasoning): output is one file per
(year, node-chunk):

    groundwater/{region}_groundwater_{year}_chunk{i:04d}.nc

instead of one monolithic file per year, with each chunk written
immediately after loading (bounded memory, per-chunk resumability) and
bounded-concurrency chunk loads within a year via pipeline_utils.parallel_map.

Requires: xarray, zarr, s3fs, pandas, pyarrow
Usage:
    python 03_download_groundwater.py --outdir ./output
    python 03_download_groundwater.py --outdir ./output --region CONUS --node-chunk-size 5000 --max-workers 4
"""

import argparse
import sys

import pandas as pd
import xarray as xr

from config import DEFAULT_REGION, NWM_START, NWM_END, ZARR_GWOUT, NODE_CHUNK_SIZE_DEFAULT, DOWNLOAD_MAX_WORKERS, make_dirs
from pipeline_utils import chunked, parallel_map


def open_gwout_store():
    import s3fs

    fs = s3fs.S3FileSystem(anon=True)
    store = s3fs.S3Map(root=ZARR_GWOUT, s3=fs, check=False)
    return xr.open_zarr(store, consolidated=True)


def load_node_ids(nodes_path) -> list:
    nodes = pd.read_parquet(nodes_path)
    return sorted(int(c) for c in nodes["comid"].dropna().unique())


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--outdir", default="./output")
    parser.add_argument("--region", default=DEFAULT_REGION, help="Region code matching 01_build_graph.py's --region")
    parser.add_argument("--start", default=NWM_START)
    parser.add_argument("--end", default=NWM_END)
    parser.add_argument(
        "--variables", nargs="+", default=["depth", "inflow", "outflow"],
        help="GWOUT variables to try to extract (auto-filtered to what the store actually has)",
    )
    parser.add_argument("--node-chunk-size", type=int, default=NODE_CHUNK_SIZE_DEFAULT)
    parser.add_argument("--max-workers", type=int, default=DOWNLOAD_MAX_WORKERS,
                         help=f"Concurrent chunk loads within a year (default: {DOWNLOAD_MAX_WORKERS})")
    args = parser.parse_args()

    dirs = make_dirs(args.outdir)
    nodes_path = dirs["graph"] / f"{args.region}_nodes.parquet"
    if not nodes_path.exists():
        print(f"ERROR: {nodes_path} not found. Run 01_build_graph.py first.", file=sys.stderr)
        sys.exit(1)

    feature_ids = load_node_ids(nodes_path)
    print(f"Loaded {len(feature_ids)} feature_id(s) for region {args.region}")
    node_chunks = chunked(feature_ids, args.node_chunk_size)
    print(f"  Will process in {len(node_chunks)} node-chunk(s) of up to {args.node_chunk_size} reaches each, "
          f"{args.max_workers} concurrent")

    print("Opening GWOUT zarr store (lazy) ...")
    ds = open_gwout_store()
    print(f"  Available variables in store: {list(ds.data_vars)}")

    available_vars = [v for v in args.variables if v in ds.data_vars]
    if not available_vars:
        print(f"ERROR: none of {args.variables} found in GWOUT store. "
              f"Inspect list(ds.data_vars) above and pass --variables explicitly.", file=sys.stderr)
        sys.exit(1)
    print(f"  Using variables: {available_vars}")

    years = range(pd.Timestamp(args.start).year, pd.Timestamp(args.end).year + 1)

    for year in years:
        year_start = max(pd.Timestamp(args.start), pd.Timestamp(f"{year}-01-01"))
        year_end = min(pd.Timestamp(args.end), pd.Timestamp(f"{year}-12-31 23:00"))

        pending = []
        for i, chunk in enumerate(node_chunks):
            out_path = dirs["groundwater"] / f"{args.region}_groundwater_{year}_chunk{i:04d}.nc"
            if out_path.exists():
                continue
            pending.append((i, chunk, out_path))

        if not pending:
            print(f"[{year}] all {len(node_chunks)} chunk(s) already exist, skipping")
            continue

        print(f"[{year}] {len(pending)}/{len(node_chunks)} chunk(s) remaining "
              f"({year_start.date()} to {year_end.date()}) ...")

        def _load_and_write(item):
            i, chunk, out_path = item
            sub = ds[available_vars].sel(feature_id=chunk, time=slice(year_start, year_end))
            sub = sub.load()
            sub.to_netcdf(out_path)
            return sub.sizes.get("time", "?")

        successes, failures = parallel_map(
            pending, _load_and_write, max_workers=args.max_workers,
            label=f"[{year}] chunk", progress_every=max(1, len(pending) // 10),
        )

        if successes:
            print(f"[{year}] wrote {len(successes)} chunk file(s) this run")
        if failures:
            print(f"[{year}] {len(failures)} chunk(s) failed -- re-run to retry just those "
                  f"(already-written chunk files are skipped).", file=sys.stderr)

    print("Done.")


if __name__ == "__main__":
    main()

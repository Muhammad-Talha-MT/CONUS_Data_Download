#!/usr/bin/env python3
"""
02_download_streamflow.py  (CONUS-optimized)

Download full-record (Feb 1979 - Jan 2023), hourly NWM v3.0 retrospective
streamflow for every reach (COMID/feature_id) in a region's graph.

WHAT CHANGED IN THIS PASS, AND WHY:

  1. Output is now one file per (year, node-chunk) instead of one giant
     file per year:
         streamflow/{region}_streamflow_{year}_chunk{i:04d}.nc
     The original held every node-chunk's loaded data for a whole year in
     a Python list (`chunk_results`) before concatenating and writing a
     SINGLE file -- fine at HUC8/HUC2 scale (a few chunks), but at CONUS
     scale (~2.7M reaches / 5,000 per chunk = ~540 chunks) that's ~540
     chunks' worth of data held in memory simultaneously before the first
     byte is written, AND a single ~200+ GB NetCDF file even if memory
     wasn't a problem. Writing each chunk immediately after it loads
     bounds memory to one chunk's footprint regardless of total region
     size, and makes resumability per-chunk instead of per-year -- a late
     chunk failure no longer discards everything already loaded that year.
  2. Chunk loads within a year now run with bounded concurrency
     (ThreadPoolExecutor via pipeline_utils.parallel_map, default 4
     workers) instead of a strict serial loop. xarray/zarr S3 reads are
     I/O-bound and release the GIL during the network wait, so this is a
     real wall-clock win, not just a cosmetic change. Kept modest by
     default -- this is a shared public S3 bucket, not a private one.
  3. Prints the store's native feature_id chunk size (from
     ds['streamflow'].encoding) once at startup and warns if
     --node-chunk-size doesn't align with it -- misaligned chunk
     boundaries make .sel(feature_id=[...]) touch more of the underlying
     Zarr store than necessary on every single batch.

A downstream step that wants "the whole region, one year" back as a single
xarray Dataset can still get that cheaply and lazily with:
    xr.open_mfdataset(sorted(glob("streamflow/{region}_streamflow_{year}_chunk*.nc")),
                       combine="nested", concat_dim="feature_id")
without ever needing all chunks in memory in a single Python process.

Requires: xarray, zarr, s3fs, pandas, pyarrow
    pip install xarray zarr s3fs pandas pyarrow

Usage:
    python 02_download_streamflow.py --outdir ./output
    python 02_download_streamflow.py --outdir ./output --region CONUS --node-chunk-size 5000 --max-workers 4
"""

import argparse
import sys

import pandas as pd
import xarray as xr

from config import DEFAULT_REGION, NWM_START, NWM_END, ZARR_CHRTOUT, NODE_CHUNK_SIZE_DEFAULT, DOWNLOAD_MAX_WORKERS, make_dirs
from pipeline_utils import chunked, parallel_map


def open_chrtout_store():
    """Lazily open the CONUS-wide CHRTOUT zarr store (anonymous S3 access)."""
    import s3fs

    fs = s3fs.S3FileSystem(anon=True)
    store = s3fs.S3Map(root=ZARR_CHRTOUT, s3=fs, check=False)
    ds = xr.open_zarr(store, consolidated=True)
    return ds


def load_node_ids(nodes_path) -> list:
    nodes = pd.read_parquet(nodes_path)
    if "comid" not in nodes.columns:
        raise RuntimeError(f"'comid' column not found in {nodes_path}")
    return sorted(int(c) for c in nodes["comid"].dropna().unique())


def report_chunk_alignment(ds, variables, node_chunk_size: int):
    """
    Print the store's native feature_id chunk size and warn if
    --node-chunk-size doesn't align with it -- misalignment means every
    .sel(feature_id=[...]) call touches more underlying Zarr chunks than
    it needs to, which compounds across hundreds of chunk-batches at
    CONUS scale.
    """
    for var in variables:
        if var not in ds.data_vars:
            continue
        native_chunks = ds[var].encoding.get("chunks")
        if not native_chunks:
            print(f"  (Could not determine {var}'s native chunk size from .encoding -- skipping alignment check.)")
            return
        dims = ds[var].dims
        if "feature_id" not in dims:
            continue
        feature_dim_pos = dims.index("feature_id")
        native_feature_chunk = native_chunks[feature_dim_pos]
        print(f"  Store's native feature_id chunk size for '{var}': {native_feature_chunk}")
        if node_chunk_size % native_feature_chunk != 0 and native_feature_chunk % node_chunk_size != 0:
            print(f"  WARNING: --node-chunk-size={node_chunk_size} does not evenly divide or align "
                  f"with the store's native chunk size ({native_feature_chunk}) -- consider setting "
                  f"--node-chunk-size to a multiple of {native_feature_chunk} for fewer, cleaner "
                  f"underlying chunk reads per batch.", file=sys.stderr)
        return


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--outdir", default="./output", help="Output root directory")
    parser.add_argument("--region", default=DEFAULT_REGION, help="Region code matching 01_build_graph.py's --region")
    parser.add_argument("--start", default=NWM_START, help="Start date (default: full record start)")
    parser.add_argument("--end", default=NWM_END, help="End date (default: full record end)")
    parser.add_argument("--variables", nargs="+", default=["streamflow", "velocity"])
    parser.add_argument("--node-chunk-size", type=int, default=NODE_CHUNK_SIZE_DEFAULT,
                         help=f"Reaches per output file / .sel() batch (default: {NODE_CHUNK_SIZE_DEFAULT})")
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

    print("Opening CHRTOUT zarr store (lazy, no data pulled yet) ...")
    ds = open_chrtout_store()

    available_vars = [v for v in args.variables if v in ds.data_vars]
    missing = set(args.variables) - set(available_vars)
    if missing:
        print(f"WARNING: requested variable(s) not found in store: {missing}. "
              f"Available variables: {list(ds.data_vars)}", file=sys.stderr)
    if not available_vars:
        print("ERROR: none of the requested variables exist in this store.", file=sys.stderr)
        sys.exit(1)

    report_chunk_alignment(ds, available_vars, args.node_chunk_size)

    years = range(pd.Timestamp(args.start).year, pd.Timestamp(args.end).year + 1)

    for year in years:
        year_start = max(pd.Timestamp(args.start), pd.Timestamp(f"{year}-01-01"))
        year_end = min(pd.Timestamp(args.end), pd.Timestamp(f"{year}-12-31 23:00"))

        pending = []
        for i, chunk in enumerate(node_chunks):
            out_path = dirs["streamflow"] / f"{args.region}_streamflow_{year}_chunk{i:04d}.nc"
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
            return sub.sizes.get("time", "?"), sub.sizes.get("feature_id", "?")

        successes, failures = parallel_map(
            pending, _load_and_write, max_workers=args.max_workers,
            label=f"[{year}] chunk", progress_every=max(1, len(pending) // 10),
        )

        if successes:
            ntime, nreach_last = successes[-1][1]
            print(f"[{year}] wrote {len(successes)} chunk file(s) this run "
                  f"({ntime} timesteps per chunk)")
        if failures:
            print(f"[{year}] {len(failures)} chunk(s) failed -- re-run the same command to retry "
                  f"just those (already-written chunk files are skipped).", file=sys.stderr)

    print("Done. Streamflow files are resumable per (year, node-chunk) -- re-run to fill in any that failed.")


if __name__ == "__main__":
    main()

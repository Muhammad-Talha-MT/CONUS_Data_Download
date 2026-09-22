#!/usr/bin/env python3
"""
04_download_forcing.py  (CONUS-optimized, NLDAS-2)

Dense, per-reach forcing using NLDAS-2 (NLDAS_FORA0125_H v2.0) -- the
alternate forcing source (ERA5-Land, 04_download_forcing_era5land.py, is
recommended for CONUS-scale runs due to NLDAS-2's one-file-per-hour
distribution; this script remains for basins/eras where NLDAS-2 is
specifically wanted).

SAME CRITICAL FIX AS 04_download_forcing_era5land.py, applied here too
(see that script's docstring for the full reasoning): the original built
one Python table holding every reach x every hour of a year before a
single write -- at CONUS scale, ~23.7 billion rows for one year alone.
Output is now one file per (year, node-chunk):

    forcing/{region}_forcing_{year}_chunk{i:04d}.parquet

Catchment fetching now runs with bounded concurrency (pipeline_utils.
parallel_map) instead of a strict serial loop, same as the ERA5-Land
script.

NLDAS-2's own SCALE NOTE still applies unchanged: the ~385,000-granule
full-record file count is driven by TIME range, not area, and does not
get better or worse from this pass's changes -- ERA5-Land remains the
better choice for a genuinely CONUS-scale run for that reason alone.

One-time setup:
    1. Create a free account: https://urs.earthdata.nasa.gov/
    2. Either run `earthaccess.login()` once interactively, or set
       EARTHDATA_USERNAME / EARTHDATA_PASSWORD, or add a ~/.netrc entry.

Requires: earthaccess, xarray, pynhd, geopandas, shapely, pandas, pyarrow
Usage:
    python 04_download_forcing.py --inspect
    python 04_download_forcing.py --outdir ./output --region CONUS --node-chunk-size 5000
"""

import argparse
import sys

import numpy as np
import pandas as pd
import xarray as xr

from config import (
    DEFAULT_REGION,
    NLDAS2_SHORT_NAME,
    NLDAS2_VERSION,
    NLDAS2_START,
    NLDAS2_END,
    NLDAS2_VARIABLES,
    NODE_CHUNK_SIZE_DEFAULT,
    DOWNLOAD_MAX_WORKERS,
    make_dirs,
)
from pipeline_utils import chunked, parallel_map, retry_with_backoff


def earthdata_login():
    import earthaccess

    auth = earthaccess.login()
    if not auth.authenticated:
        raise RuntimeError(
            "Earthdata authentication failed. Set EARTHDATA_USERNAME/EARTHDATA_PASSWORD, "
            "add a ~/.netrc entry for urs.earthdata.nasa.gov, or run earthaccess.login() "
            "interactively first. Create an account at https://urs.earthdata.nasa.gov/"
        )
    return auth


def search_year_granules(year: int, start: str, end: str):
    import earthaccess

    year_start = max(pd.Timestamp(start), pd.Timestamp(f"{year}-01-01"))
    year_end = min(pd.Timestamp(end), pd.Timestamp(f"{year}-12-31 23:59"))
    return earthaccess.search_data(
        short_name=NLDAS2_SHORT_NAME, version=NLDAS2_VERSION,
        temporal=(year_start.isoformat(), year_end.isoformat()),
    )


def inspect_one_granule():
    import earthaccess

    earthdata_login()
    print(f"Searching for one {NLDAS2_SHORT_NAME} v{NLDAS2_VERSION} granule to inspect ...")
    results = earthaccess.search_data(
        short_name=NLDAS2_SHORT_NAME, version=NLDAS2_VERSION,
        temporal=(NLDAS2_START, "1979-01-02"), count=1,
    )
    if not results:
        print("No granules found -- check credentials and dataset short_name/version.", file=sys.stderr)
        sys.exit(1)
    files = earthaccess.open(results)
    ds = xr.open_dataset(files[0], engine="h5netcdf")
    print("Dims:", dict(ds.dims))
    print("Coords:", list(ds.coords))
    print("Data variables:", list(ds.data_vars))
    for v in NLDAS2_VARIABLES:
        print(f"  expected var '{v}' present: {v in ds.data_vars}")
    print("Global attrs:", dict(ds.attrs))


def fetch_catchments(feature_ids, chunk_size=200, max_workers=DOWNLOAD_MAX_WORKERS,
                      retry_chunk_size=20, max_retry_rounds=3):
    """Same threaded fetch + retry-rounds logic as 04_download_forcing_era5land.py."""
    from pynhd import WaterData
    import geopandas as gpd

    wd_cat = WaterData("catchmentsp")
    chunks = chunked(feature_ids, chunk_size)

    successes, _failures = parallel_map(
        chunks, lambda c: wd_cat.byid("featureid", c), max_workers=max_workers,
        label="catchment chunk", progress_every=max(1, len(chunks) // 10),
    )
    parts = [gdf for _chunk, gdf in successes]
    if not parts:
        raise RuntimeError("No catchment chunks were successfully fetched.")
    catchments = gpd.GeoDataFrame(pd.concat(parts, ignore_index=True)).to_crs("EPSG:4326")

    found_ids = set(catchments["featureid"].astype(int).tolist())
    missing_ids = [fid for fid in feature_ids if fid not in found_ids]
    round_num = 0
    while missing_ids and round_num < max_retry_rounds:
        round_num += 1
        print(f"  Retry round {round_num}/{max_retry_rounds}: {len(missing_ids)} catchment(s) "
              f"missing, retrying in batches of {retry_chunk_size} ...")
        retry_chunks = chunked(missing_ids, retry_chunk_size)
        retry_successes, _ = parallel_map(
            retry_chunks, lambda c: wd_cat.byid("featureid", c), max_workers=max_workers, label="retry batch",
        )
        retry_parts = [gdf for _chunk, gdf in retry_successes]
        if retry_parts:
            recovered = gpd.GeoDataFrame(pd.concat(retry_parts, ignore_index=True)).to_crs("EPSG:4326")
            catchments = gpd.GeoDataFrame(pd.concat([catchments, recovered], ignore_index=True))
            found_ids = set(catchments["featureid"].astype(int).tolist())
            missing_ids = [fid for fid in feature_ids if fid not in found_ids]

    if missing_ids:
        print(f"  WARNING: {len(missing_ids)} catchment(s) still missing -- these reaches will "
              f"have NO forcing data. (First 20: {missing_ids[:20]})", file=sys.stderr)
    else:
        print(f"  All {len(feature_ids)} catchments successfully retrieved.")

    return catchments.drop_duplicates(subset="featureid").reset_index(drop=True)


def build_catchment_to_cell_map(lat, lon, catchments_wgs84):
    minx, miny, maxx, maxy = catchments_wgs84.total_bounds
    buffer = 0.15
    lat_mask = (lat >= miny - buffer) & (lat <= maxy + buffer)
    lon_mask = (lon >= minx - buffer) & (lon <= maxx + buffer)

    lat_sel = lat[lat_mask]
    lon_sel = lon[lon_mask]
    print(f"  Bounding box subset: {len(lat_sel)} x {len(lon_sel)} = {len(lat_sel) * len(lon_sel)} cells")

    centroids = catchments_wgs84.geometry.centroid
    cat_lat = centroids.y.values
    cat_lon = centroids.x.values
    comids = catchments_wgs84["featureid"].values

    lat_idx = np.abs(lat_sel[:, None] - cat_lat[None, :]).argmin(axis=0)
    lon_idx = np.abs(lon_sel[:, None] - cat_lon[None, :]).argmin(axis=0)
    flat_cell_idx = lat_idx * len(lon_sel) + lon_idx

    n_unique_cells = len(set(flat_cell_idx.tolist()))
    print(f"  {len(comids)} catchments mapped to {n_unique_cells} distinct grid cells")

    return lat_mask, lon_mask, flat_cell_idx, comids, len(lat_sel), len(lon_sel)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--outdir", default="./output")
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--start", default=NLDAS2_START)
    parser.add_argument("--end", default=NLDAS2_END)
    parser.add_argument("--variables", nargs="+", default=NLDAS2_VARIABLES)
    parser.add_argument("--catchment-chunk-size", type=int, default=200)
    parser.add_argument("--catchment-workers", type=int, default=DOWNLOAD_MAX_WORKERS)
    parser.add_argument("--node-chunk-size", type=int, default=NODE_CHUNK_SIZE_DEFAULT,
                         help=f"Reaches per OUTPUT file (default: {NODE_CHUNK_SIZE_DEFAULT})")
    parser.add_argument("--inspect", action="store_true",
                         help="Open one granule, print its structure, and exit (no bulk download)")
    args = parser.parse_args()

    if args.inspect:
        inspect_one_granule()
        return

    dirs = make_dirs(args.outdir)
    nodes_path = dirs["graph"] / f"{args.region}_nodes.parquet"
    if not nodes_path.exists():
        print(f"ERROR: {nodes_path} not found. Run 01_build_graph.py first.", file=sys.stderr)
        sys.exit(1)
    feature_ids = sorted(int(c) for c in pd.read_parquet(nodes_path)["comid"].dropna().unique())
    print(f"Loaded {len(feature_ids)} feature_id(s) for region {args.region}")

    print("Authenticating with NASA Earthdata ...")
    earthdata_login()

    print("Fetching NHDPlus catchment polygons ...")
    catchments = fetch_catchments(feature_ids, chunk_size=args.catchment_chunk_size,
                                   max_workers=args.catchment_workers)
    print(f"  Retrieved {len(catchments)} catchment polygons")

    print("Building the cell-to-catchment map from one reference granule ...")
    import earthaccess

    ref_start = pd.Timestamp(args.start)
    ref_end = ref_start + pd.Timedelta(days=7)
    ref_results = earthaccess.search_data(
        short_name=NLDAS2_SHORT_NAME, version=NLDAS2_VERSION,
        temporal=(ref_start.isoformat(), ref_end.isoformat()), count=1,
    )
    if not ref_results:
        print(f"ERROR: no {NLDAS2_SHORT_NAME} v{NLDAS2_VERSION} granule found between "
              f"{ref_start.isoformat()} and {ref_end.isoformat()}.", file=sys.stderr)
        sys.exit(1)
    ref_file = earthaccess.open(ref_results)[0]
    ref_ds = xr.open_dataset(ref_file, engine="h5netcdf")
    lat = ref_ds["lat"].values
    lon = ref_ds["lon"].values

    lat_mask, lon_mask, flat_cell_idx, comids, nlat, nlon = build_catchment_to_cell_map(lat, lon, catchments)
    comid_to_cell = dict(zip(comids.tolist(), flat_cell_idx.tolist()))

    approx_hours_per_year = 8784
    available_vars = [v for v in args.variables if v in ref_ds.data_vars]
    missing = set(args.variables) - set(available_vars)
    if missing:
        print(f"WARNING: variables not found in granule: {missing}. "
              f"Available: {list(ref_ds.data_vars)}.", file=sys.stderr)
    if not available_vars:
        print("ERROR: none of the requested variables exist in this dataset.", file=sys.stderr)
        sys.exit(1)

    est_gb = approx_hours_per_year * nlat * nlon * 8 * len(available_vars) / 1e9
    print(f"  Grid subset: {nlat} x {nlon} = {nlat * nlon} cells -> ~{est_gb:.2f} GB in memory per "
          f"full-year grid load (ALL {len(available_vars)} variable(s) together). This does NOT grow "
          f"with region/reach count. Output-table memory is bounded separately by "
          f"--node-chunk-size={args.node_chunk_size}, independent of this region's total "
          f"{len(feature_ids)} reaches.")

    node_chunks = chunked(feature_ids, args.node_chunk_size)
    print(f"  {len(node_chunks)} output node-chunk(s) of up to {args.node_chunk_size} reaches each")

    years = range(pd.Timestamp(args.start).year, pd.Timestamp(args.end).year + 1)

    for year in years:
        pending_chunks = [
            i for i in range(len(node_chunks))
            if not (dirs["forcing"] / f"{args.region}_forcing_{year}_chunk{i:04d}.parquet").exists()
        ]
        if not pending_chunks:
            print(f"[{year}] all {len(node_chunks)} node-chunk(s) already exist, skipping")
            continue

        print(f"[{year}] searching granules ...")
        granules = search_year_granules(year, args.start, args.end)
        if not granules:
            print(f"[{year}] no granules found, skipping", file=sys.stderr)
            continue
        print(f"[{year}] {len(granules)} hourly granules found")

        def _open_and_load():
            files = earthaccess.open(granules)
            ds = xr.open_mfdataset(
                files, combine="nested", concat_dim="time",
                data_vars=available_vars, coords="minimal", compat="override",
                engine="h5netcdf",
            )
            sub = ds[available_vars].isel(lat=lat_mask, lon=lon_mask)
            sub = sub.sortby("time")
            return sub.load()

        arr = retry_with_backoff(_open_and_load, max_attempts=3, wait_s=30, label=f"[{year}] year load")
        if arr is None:
            print(f"[{year}] FAILED after retries -- re-run later to retry this year.", file=sys.stderr)
            continue

        arr_time = arr["time"].values
        ntime = len(arr_time)
        flat_by_var = {var: arr[var].values.reshape(ntime, nlat * nlon) for var in available_vars}

        written = 0
        for i in pending_chunks:
            out_path = dirs["forcing"] / f"{args.region}_forcing_{year}_chunk{i:04d}.parquet"
            chunk_ids = node_chunks[i]
            chunk_comids_present = [c for c in chunk_ids if c in comid_to_cell]
            if not chunk_comids_present:
                continue
            chunk_cells = [comid_to_cell[c] for c in chunk_comids_present]
            ncomid = len(chunk_comids_present)
            data = {var: flat_by_var[var][:, chunk_cells].ravel() for var in available_vars}
            time_col = np.repeat(arr_time, ncomid)
            comid_col = np.tile(chunk_comids_present, ntime)
            combined = pd.DataFrame({"time": time_col, "comid": comid_col, **data})
            combined.to_parquet(out_path, index=False)
            written += 1

        print(f"[{year}] wrote {written}/{len(pending_chunks)} node-chunk file(s)")

    print("Done.")


if __name__ == "__main__":
    main()

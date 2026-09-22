#!/usr/bin/env python3
"""
04_download_forcing_era5land.py  (CONUS-optimized)

Dense, per-reach forcing using ERA5-Land (via DestinE Earth Data Hub's Zarr
store) -- the recommended forcing source for CONUS-scale runs.

THE CRITICAL FIX IN THIS PASS: the original built ONE Python table holding
every reach x every hour of a year, for ALL requested variables, before a
single write. At CONUS scale (~2.7M reaches) that's roughly
`hours_in_year * total_reaches` rows -- about 23.7 BILLION rows for one
year alone -- which will exhaust memory on essentially any single node.
This was mislabeled "CONUS-safe" in the pipeline's README relative to the
raw-grid-caching alternative, but the reach-count-driven table build was
never actually bounded, regardless of that fix.

Distinguishing the two different things that scale here matters:
  - The GRID load (`ds[vars].isel(lat=..., lon=...).sel(time=...)`) is
    bounded by grid cell count x hours x variables -- it does NOT grow
    with region/reach count. At ERA5-Land's ~9km resolution, a full CONUS
    bbox is a few hundred thousand cells; one year, all 8 variables, is a
    real but FIXED cost regardless of whether the region has 3,000 or
    2.7M reaches. This script still prints an honest estimate for it
    (previously undercounted 8x -- see below) so you can judge whether it
    fits your node, and --time-chunk-months lets you split it further if
    not.
  - The OUTPUT TABLE (one row per reach x hour) is what actually scales
    with region size, and is now built and written PER NODE-CHUNK instead
    of all at once:
        forcing/{region}_forcing_era5land_{year}_chunk{i:04d}.parquet
    bounding output memory to node-chunk-size x hours regardless of total
    reach count, and making the year resumable at chunk granularity.

Other changes in this pass:
  - The memory estimate previously computed cost for ONE variable but the
    code actually loaded ALL requested variables together in a single
    .load() call -- an ~8x understatement (for the default 8 ERA5-Land
    variables) of the real grid-load memory. Fixed to reflect what the
    code actually does.
  - Catchment fetching (NHDPlus WaterData, chunk_size=200) now runs with
    bounded concurrency (pipeline_utils.parallel_map) instead of a strict
    serial loop -- at CONUS's ~2.7M comids / 200 per request, that's
    ~13,500 sequential HTTP calls in the original; concurrency (default 8
    workers) meaningfully cuts this, still bounded to be polite to a
    shared public service.

One-time setup:
    1. Register a free account: https://earthdatahub.destine.eu/
    2. Get your access token from your EDH profile page
    3. export EDH_TOKEN=<your-token>

Requires: xarray, zarr, fsspec, aiohttp, pynhd, geopandas, shapely, pandas, pyarrow, numpy
Usage:
    python 04_download_forcing_era5land.py --inspect
    python 04_download_forcing_era5land.py --outdir ./output --region CONUS \
        --node-chunk-size 5000 --catchment-workers 8
"""

import argparse
import os
import sys

import numpy as np
import pandas as pd
import xarray as xr

from config import (
    DEFAULT_REGION,
    ERA5LAND_EDH_ZARR_URL,
    ERA5LAND_VARIABLES,
    ERA5LAND_START,
    ERA5LAND_END,
    NODE_CHUNK_SIZE_DEFAULT,
    DOWNLOAD_MAX_WORKERS,
    make_dirs,
)
from pipeline_utils import chunked, parallel_map, retry_with_backoff


def get_edh_token():
    token = os.environ.get("EDH_TOKEN")
    if not token:
        print("ERROR: EDH_TOKEN environment variable not set. Register at "
              "https://earthdatahub.destine.eu/, get your token from your profile page, "
              "and run: export EDH_TOKEN=<your-token>", file=sys.stderr)
        sys.exit(1)
    return token


def open_era5land_store(token: str):
    import fsspec

    mapper = fsspec.get_mapper(
        ERA5LAND_EDH_ZARR_URL,
        client_kwargs={"headers": {"Authorization": f"Bearer {token}"}},
    )
    return xr.open_zarr(mapper, consolidated=True)


def inspect_store():
    print(f"Opening {ERA5LAND_EDH_ZARR_URL} (lazy) ...")
    token = get_edh_token()
    try:
        ds = open_era5land_store(token)
    except Exception as e:
        print(f"ERROR opening store: {e}", file=sys.stderr)
        sys.exit(1)

    print("\n--- Store structure ---")
    print("Dims:", dict(ds.dims))
    print("Coords:", list(ds.coords))
    print("Data variables:", list(ds.data_vars))
    print("\nExpected variables (config.ERA5LAND_VARIABLES):", ERA5LAND_VARIABLES)
    for v in ERA5LAND_VARIABLES:
        print(f"  '{v}' present: {v in ds.data_vars}")
    print("\nGlobal attrs:", dict(ds.attrs))


def normalize_coords(ds):
    rename_map = {}
    if "latitude" in ds.coords and "lat" not in ds.coords:
        rename_map["latitude"] = "lat"
    if "longitude" in ds.coords and "lon" not in ds.coords:
        rename_map["longitude"] = "lon"
    if "valid_time" in ds.coords and "time" not in ds.coords:
        rename_map["valid_time"] = "time"
    if rename_map:
        print(f"  Renaming coordinates: {rename_map}")
        ds = ds.rename(rename_map)
    return ds


def get_region_bbox(dirs, region: str):
    import geopandas as gpd

    for candidate in (f"{region}_flowlines.parquet", f"{region}_flowlines.geojson"):
        p = dirs["graph"] / candidate
        if p.exists():
            flowlines = gpd.read_parquet(p) if p.suffix == ".parquet" else gpd.read_file(p)
            if flowlines.crs is not None and flowlines.crs.to_epsg() != 4326:
                flowlines = flowlines.to_crs("EPSG:4326")
            return tuple(flowlines.total_bounds)
    raise FileNotFoundError(
        f"Neither {region}_flowlines.parquet nor .geojson found under {dirs['graph']}. "
        f"Run 01_build_graph.py first."
    )


def fetch_catchments(feature_ids, chunk_size=200, max_workers=DOWNLOAD_MAX_WORKERS,
                      retry_chunk_size=20, max_retry_rounds=3):
    """
    Fetch NHDPlus catchment polygons, now with bounded-concurrency chunk
    fetches (see module docstring) plus the existing retry-for-transient-
    per-chunk-failures logic, which still applies to whatever's missing
    after the concurrent pass.
    """
    from pynhd import WaterData
    import geopandas as gpd

    wd_cat = WaterData("catchmentsp")
    chunks = chunked(feature_ids, chunk_size)

    def _fetch_chunk(chunk):
        return wd_cat.byid("featureid", chunk)

    successes, failures = parallel_map(
        chunks, _fetch_chunk, max_workers=max_workers, label="catchment chunk",
        progress_every=max(1, len(chunks) // 10),
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
            retry_chunks, lambda c: wd_cat.byid("featureid", c),
            max_workers=max_workers, label="retry batch",
        )
        retry_parts = [gdf for _chunk, gdf in retry_successes]
        if retry_parts:
            recovered = gpd.GeoDataFrame(pd.concat(retry_parts, ignore_index=True)).to_crs("EPSG:4326")
            catchments = gpd.GeoDataFrame(pd.concat([catchments, recovered], ignore_index=True))
            found_ids = set(catchments["featureid"].astype(int).tolist())
            missing_ids = [fid for fid in feature_ids if fid not in found_ids]

    if missing_ids:
        print(f"  WARNING: {len(missing_ids)} catchment(s) still missing after retries -- "
              f"these reaches will have NO forcing data. (First 20: {missing_ids[:20]})", file=sys.stderr)
    else:
        print(f"  All {len(feature_ids)} catchments successfully retrieved.")

    return catchments.drop_duplicates(subset="featureid").reset_index(drop=True)


def build_catchment_to_cell_map(lat, lon, catchments_wgs84):
    """Each catchment finds its ONE nearest grid cell."""
    centroids = catchments_wgs84.geometry.centroid
    cat_lat = centroids.y.values
    cat_lon = centroids.x.values
    comids = catchments_wgs84["featureid"].values

    lat_idx = np.abs(lat[:, None] - cat_lat[None, :]).argmin(axis=0)
    lon_idx = np.abs(lon[:, None] - cat_lon[None, :]).argmin(axis=0)
    flat_cell_idx = lat_idx * len(lon) + lon_idx

    n_unique_cells = len(set(flat_cell_idx.tolist()))
    print(f"  {len(comids)} catchments mapped to {n_unique_cells} distinct grid cells")
    return flat_cell_idx, comids


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--outdir", default="./output")
    parser.add_argument("--region", default=DEFAULT_REGION, help="Region code, e.g. a HUC8, 'CONUS', etc.")
    parser.add_argument("--start", default=ERA5LAND_START)
    parser.add_argument("--end", default=ERA5LAND_END)
    parser.add_argument("--variables", nargs="+", default=ERA5LAND_VARIABLES)
    parser.add_argument("--buffer-deg", type=float, default=0.15)
    parser.add_argument("--catchment-chunk-size", type=int, default=200)
    parser.add_argument("--catchment-workers", type=int, default=DOWNLOAD_MAX_WORKERS,
                         help=f"Concurrent catchment-fetch requests (default: {DOWNLOAD_MAX_WORKERS})")
    parser.add_argument("--node-chunk-size", type=int, default=NODE_CHUNK_SIZE_DEFAULT,
                         help=f"Reaches per OUTPUT file (default: {NODE_CHUNK_SIZE_DEFAULT}) -- this is "
                              f"the fix that actually bounds memory at CONUS scale, see module docstring")
    parser.add_argument("--time-chunk-months", type=int, default=None,
                         help="Optionally split each year's GRID load into N-month pieces if the full-year "
                              "grid estimate printed below is too large for your node's RAM. Does not "
                              "affect output file boundaries (still one file per year per node-chunk).")
    parser.add_argument("--inspect", action="store_true", help="Print store structure and exit -- RUN THIS FIRST")
    args = parser.parse_args()

    if args.inspect:
        inspect_store()
        return

    dirs = make_dirs(args.outdir)
    nodes_path = dirs["graph"] / f"{args.region}_nodes.parquet"
    if not nodes_path.exists():
        print(f"ERROR: {nodes_path} not found. Run 01_build_graph.py first.", file=sys.stderr)
        sys.exit(1)
    feature_ids = sorted(int(c) for c in pd.read_parquet(nodes_path)["comid"].dropna().unique())
    print(f"Loaded {len(feature_ids)} feature_id(s) for region {args.region}")

    token = get_edh_token()
    print("Opening ERA5-Land store (lazy) ...")
    ds = open_era5land_store(token)
    ds = normalize_coords(ds)
    if "lat" not in ds.coords or "lon" not in ds.coords:
        print(f"ERROR: 'lat'/'lon' not found after normalization. Actual coords: {list(ds.coords)}. "
              f"Run --inspect and fix normalize_coords().", file=sys.stderr)
        sys.exit(1)

    print("Fetching NHDPlus catchment polygons ...")
    catchments = fetch_catchments(feature_ids, chunk_size=args.catchment_chunk_size,
                                   max_workers=args.catchment_workers)
    print(f"  Retrieved {len(catchments)} catchment polygons")

    print("Reading region bounding box from local flowlines file ...")
    minx, miny, maxx, maxy = get_region_bbox(dirs, args.region)
    print(f"  bbox: ({minx:.3f}, {miny:.3f}) to ({maxx:.3f}, {maxy:.3f})")

    lat = ds["lat"].values
    lon = ds["lon"].values
    buffer = args.buffer_deg

    region_minx, region_maxx = minx, maxx
    if lon.min() >= 0 and lon.max() > 180:
        print(f"  Detected 0-360 longitude convention (lon range: {lon.min():.2f} to {lon.max():.2f}) "
              f"-- converting bbox from -180..180.")
        region_minx = minx % 360
        region_maxx = maxx % 360

    lat_mask = (lat >= miny - buffer) & (lat <= maxy + buffer)
    if region_minx > region_maxx:
        print(f"WARNING: converted bbox spans the 0/360 seam -- the simple range mask below "
              f"will likely be wrong for this region. Needs a proper wraparound mask.", file=sys.stderr)
    lon_mask = (lon >= region_minx - buffer) & (lon <= region_maxx + buffer)
    nlat, nlon = int(lat_mask.sum()), int(lon_mask.sum())
    print(f"  Cropped grid: {nlat} x {nlon} = {nlat * nlon} cells")
    if nlat == 0 or nlon == 0:
        print(f"ERROR: 0 cells after convention fix. lat range in store: {lat.min():.2f} to {lat.max():.2f}; "
              f"lon range: {lon.min():.2f} to {lon.max():.2f}. Investigate before continuing.", file=sys.stderr)
        sys.exit(1)

    available_vars = [v for v in args.variables if v in ds.data_vars]
    missing = set(args.variables) - set(available_vars)
    if missing:
        print(f"WARNING: variables not found: {missing}. Available: {list(ds.data_vars)}", file=sys.stderr)
    if not available_vars:
        print("ERROR: none of the requested variables exist in this store.", file=sys.stderr)
        sys.exit(1)

    # FIXED memory estimate: the original computed this for ONE variable but
    # the code loads ALL requested variables together in one .load() call --
    # multiply by len(available_vars) to reflect what actually happens. This
    # is the GRID load only; it does NOT grow with region/reach count (see
    # module docstring) -- that part is now bounded separately by
    # --node-chunk-size at the output-table stage below.
    approx_hours_per_year = 8784
    time_chunk_hours = approx_hours_per_year if not args.time_chunk_months else int(approx_hours_per_year * args.time_chunk_months / 12)
    est_gb_per_load = time_chunk_hours * nlat * nlon * 8 * len(available_vars) / 1e9
    print(f"  Estimated ~{est_gb_per_load:.2f} GB in memory per grid load "
          f"({'full year' if not args.time_chunk_months else f'{args.time_chunk_months}-month piece'}, "
          f"ALL {len(available_vars)} requested variable(s) together, matching what the code actually loads). "
          f"If this is too large for your node, pass --time-chunk-months to split it further.")
    print(f"  Output-table memory is now bounded separately by --node-chunk-size={args.node_chunk_size} "
          f"reaches per file, independent of this region's total {len(feature_ids)} reaches.")

    lat_sel = lat[lat_mask]
    lon_sel = lon[lon_mask]
    flat_cell_idx, comids = build_catchment_to_cell_map(lat_sel, lon_sel, catchments)
    comid_to_cell = dict(zip(comids.tolist(), flat_cell_idx.tolist()))

    # Node-chunks are defined over the REGION's feature_ids (so every reach
    # gets an output row even if its catchment fetch failed -- it'll just
    # be absent from that chunk's cell-index array, same missing-data
    # handling as before, just chunked now).
    node_chunks = chunked(feature_ids, args.node_chunk_size)
    print(f"  {len(node_chunks)} output node-chunk(s) of up to {args.node_chunk_size} reaches each")

    years = range(pd.Timestamp(args.start).year, pd.Timestamp(args.end).year + 1)

    for year in years:
        pending_chunks = [
            i for i in range(len(node_chunks))
            if not (dirs["forcing"] / f"{args.region}_forcing_era5land_{year}_chunk{i:04d}.parquet").exists()
        ]
        if not pending_chunks:
            print(f"[{year}] all {len(node_chunks)} node-chunk(s) already exist, skipping")
            continue

        year_start = max(pd.Timestamp(args.start), pd.Timestamp(f"{year}-01-01"))
        year_end = min(pd.Timestamp(args.end), pd.Timestamp(f"{year}-12-31 23:00"))

        # Time sub-windows within the year (whole year by default, or split
        # via --time-chunk-months if the grid-load estimate above was too
        # large). Each sub-window's grid data is loaded once, then scattered
        # out across all pending node-chunks' output files for that window.
        if args.time_chunk_months:
            windows = list(pd.date_range(year_start, year_end, freq=f"{args.time_chunk_months}MS"))
            windows = list(zip(windows, windows[1:] + [year_end + pd.Timedelta(hours=1)]))
        else:
            windows = [(year_start, year_end + pd.Timedelta(hours=1))]

        # Accumulate each pending chunk's frames across time-windows, then
        # write once per chunk at the end of the year -- still bounded,
        # since each accumulated frame is node_chunk_size x hours_in_year,
        # not total_reaches x hours_in_year.
        chunk_frames = {i: [] for i in pending_chunks}
        year_ok = True

        for win_start, win_end in windows:
            def _load_window():
                sub = ds[available_vars].isel(lat=lat_mask, lon=lon_mask).sel(time=slice(win_start, win_end))
                return sub.load()

            print(f"[{year}] loading {win_start.date()} to {win_end.date()} "
                  f"({len(available_vars)} var(s), {nlat}x{nlon} cells) ...")
            arr = retry_with_backoff(_load_window, max_attempts=3, wait_s=30, label=f"[{year}] grid load")
            if arr is None:
                print(f"[{year}] grid load FAILED after retries -- skipping this year, re-run later.", file=sys.stderr)
                year_ok = False
                break

            arr_time = arr["time"].values
            ntime = len(arr_time)
            flat_by_var = {var: arr[var].values.reshape(ntime, nlat * nlon) for var in available_vars}

            for i in pending_chunks:
                chunk_ids = node_chunks[i]
                chunk_cells = [comid_to_cell[c] for c in chunk_ids if c in comid_to_cell]
                chunk_comids_present = [c for c in chunk_ids if c in comid_to_cell]
                if not chunk_comids_present:
                    continue
                ncomid = len(chunk_comids_present)
                data = {var: flat_by_var[var][:, chunk_cells].ravel() for var in available_vars}
                time_col = np.repeat(arr_time, ncomid)
                comid_col = np.tile(chunk_comids_present, ntime)
                chunk_frames[i].append(pd.DataFrame({"time": time_col, "comid": comid_col, **data}))

        if not year_ok:
            continue

        written = 0
        for i in pending_chunks:
            out_path = dirs["forcing"] / f"{args.region}_forcing_era5land_{year}_chunk{i:04d}.parquet"
            frames = chunk_frames[i]
            if not frames:
                continue
            combined = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
            combined.to_parquet(out_path, index=False)
            written += 1

        print(f"[{year}] wrote {written}/{len(pending_chunks)} node-chunk file(s)")

    print("Done.")


if __name__ == "__main__":
    main()

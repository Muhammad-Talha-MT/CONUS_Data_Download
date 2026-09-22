#!/usr/bin/env python3
"""
04_download_forcing.py  (NLDAS-2, rewritten to use Google Earth Engine)

Dense, per-reach forcing using NLDAS-2 (NLDAS_FORA0125_H v2.0), now pulled
through Google Earth Engine instead of NASA GES DISC/earthaccess.

WHY THIS CHANGED: the previous version downloaded one file PER HOUR
(~385,000 files for the full 1979-present record) and did the
catchment-to-nearest-cell reduction locally, after transfer. Earth Engine
publishes the same product (NASA/NLDAS/FORA0125_H002) and lets that
reduction happen SERVER-SIDE instead: `image.reduceRegions(catchments,
reducer, scale)` computes the zonal mean directly against each
catchment's real polygon, for every hour in a date range, and only the
resulting small table (reach x hour x variable) is ever transferred --
the underlying hourly grids never leave Google's infrastructure. This is
a genuine architectural improvement, not just a faster way to fetch the
same bytes: it also replaces the old nearest-grid-cell approximation with
a real area-weighted zonal mean over each catchment's actual shape.

*** NOT YET BENCHMARKED AT SCALE. *** This has been written against Earth
Engine's documented API and the collection's confirmed availability, but
-- same discipline as every other new source in this pipeline -- run
--inspect first, then a small HUC8 test, before trusting it at CONUS
scale. In particular: mapping reduceRegions over many hourly images in one
export task is a real, well-known way to hit Earth Engine's server-side
"user memory limit exceeded" error; --time-chunk-days exists specifically
to let you shrink each export task if that happens, and the default here
(31 days) is a conservative starting point, not a tuned value.

One-time setup:
    1. Register/enable Earth Engine for a Google Cloud project you control:
       https://code.earthengine.google.com/
    2. pip install earthengine-api google-cloud-storage
    3. Run `earthengine authenticate` once (or let this script call
       ee.Authenticate() interactively the first time it runs).
    4. export GEE_PROJECT=<your-gcp-project-id>
    5. export GEE_EXPORT_GCS_BUCKET=<a GCS bucket you can write to> --
       Earth Engine table exports land here as an intermediate step;
       this script downloads each export then deletes the GCS copy.

Output is unchanged from the previous version, so nothing downstream in
this pipeline needs to change:
    forcing/{region}_forcing_{year}_chunk{i:04d}.parquet
    columns: time, comid, Rainf, Tair, Qair, Wind_E, Wind_N, PSurf, SWdown, LWdown

Requires: earthengine-api, google-cloud-storage, pynhd, geopandas, shapely, pandas, pyarrow
Usage:
    python 04_download_forcing.py --inspect
    python 04_download_forcing.py --outdir ./test_output --region 06010105
    python 04_download_forcing.py --outdir ./output --region CONUS --node-chunk-size 5000 --time-chunk-days 31
"""

import argparse
import os
import sys
import time

import pandas as pd

from config import (
    DEFAULT_REGION,
    NLDAS2_GEE_COLLECTION,
    NLDAS2_GEE_BAND_MAP,
    NLDAS2_GEE_SCALE_M,
    NLDAS2_START,
    NLDAS2_END,
    NLDAS2_VARIABLES,
    NODE_CHUNK_SIZE_DEFAULT,
    DOWNLOAD_MAX_WORKERS,
    make_dirs,
)
from pipeline_utils import chunked, parallel_map


def get_gee_project():
    project = os.environ.get("GEE_PROJECT")
    if not project:
        print("ERROR: GEE_PROJECT environment variable not set. Register/enable Earth Engine "
              "for a Google Cloud project at https://code.earthengine.google.com/ and run: "
              "export GEE_PROJECT=<your-gcp-project-id>", file=sys.stderr)
        sys.exit(1)
    return project


def get_gcs_bucket():
    bucket = os.environ.get("GEE_EXPORT_GCS_BUCKET")
    if not bucket:
        print("ERROR: GEE_EXPORT_GCS_BUCKET environment variable not set. Earth Engine table "
              "exports need a GCS bucket to land in first. export GEE_EXPORT_GCS_BUCKET=<bucket>", file=sys.stderr)
        sys.exit(1)
    return bucket


def ee_init():
    import ee

    project = get_gee_project()
    try:
        ee.Initialize(project=project)
    except Exception:
        ee.Authenticate()
        ee.Initialize(project=project)
    return ee


def inspect_collection():
    ee = ee_init()
    ic = ee.ImageCollection(NLDAS2_GEE_COLLECTION)
    first = ic.first()
    print(f"Collection: {NLDAS2_GEE_COLLECTION}")
    print("Band names in first image:", first.bandNames().getInfo())
    print(f"\nRequested variables (config.NLDAS2_VARIABLES) -> Earth Engine band mapping:")
    band_names = set(first.bandNames().getInfo())
    for var, band in NLDAS2_GEE_BAND_MAP.items():
        present = band in band_names
        print(f"  {var:>8} -> {band:<22} present: {present}")
    print(f"\nNative pixel size (config.NLDAS2_GEE_SCALE_M): {NLDAS2_GEE_SCALE_M} m -- "
          f"confirm this against the collection's own catalog page if in doubt.")
    print("\nIf any band is missing or the mapping looks wrong, check this collection's band "
          "descriptions on its Earth Engine catalog page before trusting NLDAS2_GEE_BAND_MAP "
          "in config.py.")


def fetch_catchments(feature_ids, chunk_size=200, max_workers=DOWNLOAD_MAX_WORKERS,
                      retry_chunk_size=20, max_retry_rounds=3):
    """Same threaded fetch + retry-rounds logic used by 04_download_forcing_era5land.py."""
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


def catchments_to_ee_featurecollection(ee, catchments_chunk_gdf):
    """
    Build an ee.FeatureCollection from a node-chunk's catchment polygons,
    simplified slightly to keep the upload payload and server-side
    computation reasonable -- full-resolution NHDPlus catchment boundaries
    are far more detail than a ~12km grid's zonal mean needs.
    """
    features = []
    for _, row in catchments_chunk_gdf.iterrows():
        geom = row.geometry.simplify(0.0005, preserve_topology=True)
        ee_geom = ee.Geometry(geom.__geo_interface__)
        features.append(ee.Feature(ee_geom, {"comid": int(row["featureid"])}))
    return ee.FeatureCollection(features)


def month_windows(start: pd.Timestamp, end: pd.Timestamp, chunk_days: int):
    """Split [start, end] into consecutive windows of at most chunk_days days."""
    windows = []
    cur = start
    while cur <= end:
        nxt = min(cur + pd.Timedelta(days=chunk_days), end + pd.Timedelta(seconds=1))
        windows.append((cur, nxt))
        cur = nxt
    return windows


def export_window(ee, fc, bands_ee, window_start, window_end, description, bucket, prefix):
    ic = (
        ee.ImageCollection(NLDAS2_GEE_COLLECTION)
        .filterDate(window_start.isoformat(), window_end.isoformat())
        .select(bands_ee)
    )

    def _reduce_one_image(image):
        stats = image.reduceRegions(collection=fc, reducer=ee.Reducer.mean(), scale=NLDAS2_GEE_SCALE_M)
        t = image.date().format("YYYY-MM-dd'T'HH:mm:ss")
        return stats.map(lambda f: f.set("time", t))

    table = ic.map(_reduce_one_image).flatten()
    task = ee.batch.Export.table.toCloudStorage(
        collection=table, description=description, bucket=bucket,
        fileNamePrefix=prefix, fileFormat="CSV",
    )
    task.start()
    return task


def wait_for_tasks(tasks, poll_s=20, label="export"):
    """Poll a batch of Earth Engine tasks to completion. Returns (done, failed) task lists."""
    pending = list(tasks)
    done, failed = [], []
    while pending:
        time.sleep(poll_s)
        still_pending = []
        for task in pending:
            state = task.status()["state"]
            if state == "COMPLETED":
                done.append(task)
            elif state in ("FAILED", "CANCELLED"):
                failed.append(task)
                print(f"  {label} task FAILED: {task.status().get('error_message', 'unknown error')}", file=sys.stderr)
            else:
                still_pending.append(task)
        pending = still_pending
        if pending:
            print(f"  {label}: {len(done)} done, {len(failed)} failed, {len(pending)} still running ...")
    return done, failed


def download_and_merge_gcs_csvs(bucket_name, prefix, band_to_var):
    """
    Earth Engine table exports can shard into multiple CSV files
    (prefix-00000-of-00002.csv, etc.) for large tables. Download every
    blob matching the prefix, concatenate, rename EE band columns back to
    the pipeline's canonical variable names, and drop EE's extra columns
    (.geo, system:index) that aren't part of our output schema.
    """
    from google.cloud import storage

    client = storage.Client()
    blobs = list(client.list_blobs(bucket_name, prefix=prefix))
    if not blobs:
        raise RuntimeError(f"No export files found in gs://{bucket_name}/{prefix}*")

    frames = []
    for blob in blobs:
        local_tmp = f"/tmp/{blob.name.split('/')[-1]}"
        blob.download_to_filename(local_tmp)
        frames.append(pd.read_csv(local_tmp))
        os.remove(local_tmp)
        blob.delete()  # clean up the GCS intermediate copy

    combined = pd.concat(frames, ignore_index=True)
    var_to_band = {v: k for k, v in band_to_var.items()}
    rename_map = {ee_band: var for ee_band, var in var_to_band.items() if ee_band in combined.columns}
    combined = combined.rename(columns=rename_map)
    drop_cols = [c for c in (".geo", "system:index") if c in combined.columns]
    combined = combined.drop(columns=drop_cols)
    return combined


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
                         help=f"Reaches per OUTPUT file AND per Earth Engine FeatureCollection "
                              f"(default: {NODE_CHUNK_SIZE_DEFAULT})")
    parser.add_argument("--time-chunk-days", type=int, default=31,
                         help="Days per Earth Engine export task (default: 31 -- conservative "
                              "starting point; lower this if you hit a 'user memory limit "
                              "exceeded' error from Earth Engine)")
    parser.add_argument("--max-concurrent-tasks", type=int, default=8,
                         help="Cap on simultaneously running Earth Engine export tasks (default: 8)")
    parser.add_argument("--inspect", action="store_true", help="RUN THIS FIRST")
    args = parser.parse_args()

    if args.inspect:
        inspect_collection()
        return

    dirs = make_dirs(args.outdir)
    nodes_path = dirs["graph"] / f"{args.region}_nodes.parquet"
    if not nodes_path.exists():
        print(f"ERROR: {nodes_path} not found. Run 01_build_graph.py first.", file=sys.stderr)
        sys.exit(1)
    feature_ids = sorted(int(c) for c in pd.read_parquet(nodes_path)["comid"].dropna().unique())
    print(f"Loaded {len(feature_ids)} feature_id(s) for region {args.region}")

    bucket = get_gcs_bucket()
    ee = ee_init()

    available_vars = [v for v in args.variables if v in NLDAS2_GEE_BAND_MAP]
    missing = set(args.variables) - set(available_vars)
    if missing:
        print(f"WARNING: variables not in NLDAS2_GEE_BAND_MAP: {missing}", file=sys.stderr)
    if not available_vars:
        print("ERROR: none of the requested variables are mapped to an Earth Engine band.", file=sys.stderr)
        sys.exit(1)
    bands_ee = [NLDAS2_GEE_BAND_MAP[v] for v in available_vars]
    band_to_var = {NLDAS2_GEE_BAND_MAP[v]: v for v in available_vars}
    print(f"  Using variables: {available_vars} -> Earth Engine bands: {bands_ee}")

    print("Fetching NHDPlus catchment polygons ...")
    catchments = fetch_catchments(feature_ids, chunk_size=args.catchment_chunk_size,
                                   max_workers=args.catchment_workers)
    print(f"  Retrieved {len(catchments)} catchment polygons")

    node_chunks = chunked(feature_ids, args.node_chunk_size)
    print(f"  {len(node_chunks)} node-chunk(s) of up to {args.node_chunk_size} reaches each")

    years = range(pd.Timestamp(args.start).year, pd.Timestamp(args.end).year + 1)
    run_tag = f"nldas2_{args.region}"

    for year in years:
        year_start = max(pd.Timestamp(args.start), pd.Timestamp(f"{year}-01-01"))
        year_end = min(pd.Timestamp(args.end), pd.Timestamp(f"{year}-12-31 23:00"))

        for i, chunk_ids in enumerate(node_chunks):
            out_path = dirs["forcing"] / f"{args.region}_forcing_{year}_chunk{i:04d}.parquet"
            if out_path.exists():
                print(f"[{year}][chunk {i}] already exists, skipping")
                continue

            chunk_catchments = catchments[catchments["featureid"].isin(chunk_ids)]
            if chunk_catchments.empty:
                print(f"[{year}][chunk {i}] no catchments found for this chunk, skipping", file=sys.stderr)
                continue
            fc = catchments_to_ee_featurecollection(ee, chunk_catchments)

            windows = month_windows(year_start, year_end, args.time_chunk_days)
            print(f"[{year}][chunk {i}] {len(chunk_catchments)} catchments, "
                  f"{len(windows)} time-window export task(s) ...")

            tasks, task_meta = [], {}
            for w_idx, (w_start, w_end) in enumerate(windows):
                prefix = f"{run_tag}/{year}/chunk{i:04d}/window{w_idx:03d}"
                description = f"{run_tag}_{year}_c{i:04d}_w{w_idx:03d}"
                task = export_window(ee, fc, bands_ee, w_start, w_end, description, bucket, prefix)
                tasks.append(task)
                task_meta[task] = prefix

                # Throttle: don't let more than --max-concurrent-tasks run at once
                while sum(1 for t in tasks if t.status()["state"] in ("READY", "RUNNING")) >= args.max_concurrent_tasks:
                    time.sleep(15)

            done, failed = wait_for_tasks(tasks, label=f"[{year}][chunk {i}]")
            if failed:
                print(f"[{year}][chunk {i}] {len(failed)}/{len(windows)} window(s) failed -- "
                      f"re-run this command to retry the whole chunk (Earth Engine tasks aren't "
                      f"individually resumable, so a partial chunk failure means redoing the "
                      f"chunk's windows next run).", file=sys.stderr)
                continue

            window_frames = []
            for task in done:
                prefix = task_meta[task]
                window_frames.append(download_and_merge_gcs_csvs(bucket, prefix, band_to_var))

            combined = pd.concat(window_frames, ignore_index=True)
            combined["time"] = pd.to_datetime(combined["time"])
            keep_cols = ["time", "comid"] + available_vars
            combined = combined[[c for c in keep_cols if c in combined.columns]]
            combined.to_parquet(out_path, index=False)
            print(f"[{year}][chunk {i}] wrote {out_path} ({len(combined)} rows)")

    print("Done.")


if __name__ == "__main__":
    main()
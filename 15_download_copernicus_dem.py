#!/usr/bin/env python3
"""
15_download_copernicus_dem.py  (CONUS-optimized)

YELLOW tier #43: Copernicus DEM GLO-30 (30m global elevation).

WHAT CHANGED: tile downloads now run with bounded concurrency
(pipeline_utils.parallel_map, default 8 workers) instead of one at a
time. At CONUS scale this is several hundred independent 1x1-degree S3
GetObject calls -- trivially parallelizable, no shared state between
tiles, and this was purely serial in the original for no real reason
(the source is an anonymous, unthrottled public bucket).

Requires: boto3, geopandas
Usage:
    python 15_download_copernicus_dem.py --outdir ./output --region CONUS --download-workers 8
"""

import argparse
import math
import sys

from config import DEFAULT_REGION, COPERNICUS_DEM_S3_BUCKET, DOWNLOAD_MAX_WORKERS, make_dirs
from pipeline_utils import parallel_map


def get_region_bbox(dirs, region: str):
    import geopandas as gpd

    for candidate in (f"{region}_flowlines.parquet", f"{region}_flowlines.geojson"):
        p = dirs["graph"] / candidate
        if p.exists():
            flowlines = gpd.read_parquet(p) if p.suffix == ".parquet" else gpd.read_file(p)
            if flowlines.crs is not None and flowlines.crs.to_epsg() != 4326:
                flowlines = flowlines.to_crs("EPSG:4326")
            return tuple(flowlines.total_bounds)
    raise FileNotFoundError(f"No flowlines file found under {dirs['graph']} for region {region}. Run 01_build_graph.py first.")


def compute_tile_names(minx, miny, maxx, maxy) -> list:
    lat_start = math.floor(miny)
    lat_end = math.ceil(maxy)
    lon_start = math.floor(minx)
    lon_end = math.ceil(maxx)

    tiles = []
    for lat in range(lat_start, lat_end):
        for lon in range(lon_start, lon_end):
            lat_prefix = f"N{lat:02d}" if lat >= 0 else f"S{-lat:02d}"
            lon_prefix = f"E{lon:03d}" if lon >= 0 else f"W{-lon:03d}"
            tiles.append(f"Copernicus_DSM_COG_30_{lat_prefix}_00_{lon_prefix}_00_DEM")
    return tiles


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--outdir", default="./output")
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--download-workers", type=int, default=DOWNLOAD_MAX_WORKERS)
    args = parser.parse_args()

    dirs = make_dirs(args.outdir)
    minx, miny, maxx, maxy = get_region_bbox(dirs, args.region)
    tiles = compute_tile_names(minx, miny, maxx, maxy)
    print(f"Region bbox: ({minx:.3f}, {miny:.3f}) to ({maxx:.3f}, {maxy:.3f})")
    print(f"{len(tiles)} 1-degree tile(s) needed, {args.download_workers} concurrent")

    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    to_fetch = []
    for tile in tiles:
        out_path = dirs["copernicus_dem"] / f"{tile}.tif"
        if not out_path.exists():
            to_fetch.append((tile, out_path))
    already = len(tiles) - len(to_fetch)
    if already:
        print(f"{already} tile(s) already downloaded, skipping")
    if not to_fetch:
        print("Nothing left to download.")
        return

    def _download_one(item):
        tile, out_path = item
        key = f"{tile}/{tile}.tif"
        s3.download_file(COPERNICUS_DEM_S3_BUCKET, key, str(out_path))
        return out_path

    successes, failures = parallel_map(
        to_fetch, _download_one, max_workers=args.download_workers,
        label="tile download", progress_every=max(1, len(to_fetch) // 20),
    )
    if failures:
        print(f"NOTE: {len(failures)} tile(s) failed -- likely ocean-only cells that aren't "
              f"published, not necessarily a real problem. Re-run to retry.", file=sys.stderr)
    print(f"Done. {len(successes)} tile(s) downloaded this run.")


if __name__ == "__main__":
    main()

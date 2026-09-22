#!/usr/bin/env python3
"""
09_download_3dep.py  (CONUS-optimized)

RED tier #44: USGS 3DEP (3D Elevation Program) terrain data.

WHAT CHANGED: per-HUC8 tile fetches now run with bounded concurrency
(pipeline_utils.parallel_map, default 4 workers) instead of a strict
serial loop -- at CONUS scale this is ~2,264 independent tile fetches.

Writes one GeoTIFF per HUC8 tile within the region (resumable).

Requires: py3dep, region.py, geopandas
Usage:
    python 09_download_3dep.py --outdir ./output --region CONUS --resolution 30 --fetch-workers 4
"""

import argparse
import sys

from config import DEFAULT_REGION, USGS_3DEP_RESOLUTION_M, make_dirs
from pipeline_utils import parallel_map
import region as region_lib


def fetch_one_tile(huc8: str, resolution: int, out_path):
    import py3dep

    boundary = region_lib.fetch_boundary_for_code(huc8)
    dem = py3dep.get_dem(boundary.geometry.iloc[0], resolution=resolution, geo_crs=boundary.crs)
    dem.rio.to_raster(out_path)
    return out_path


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--outdir", default="./output")
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--resolution", type=int, default=USGS_3DEP_RESOLUTION_M, choices=[10, 30, 60])
    parser.add_argument("--fetch-workers", type=int, default=4,
                         help="Concurrent HUC8 tile fetches (default: 4, kept modest -- shared public web service)")
    args = parser.parse_args()

    dirs = make_dirs(args.outdir)

    print(f"Resolving region {args.region} into HUC8 tiles ...")
    huc8_units = region_lib.list_huc8_units(args.region)
    print(f"  {len(huc8_units)} HUC8 tile(s)")

    pending = []
    for huc8 in huc8_units:
        out_path = dirs["usgs_3dep"] / f"{huc8}_3dep_{args.resolution}m.tif"
        if not out_path.exists():
            pending.append((huc8, out_path))
    already = len(huc8_units) - len(pending)
    if already:
        print(f"{already} HUC8 tile(s) already downloaded, skipping")
    if not pending:
        print("Nothing left to download.")
        return

    print(f"Fetching {len(pending)} tile(s), {args.fetch_workers} concurrent ...")

    successes, failures = parallel_map(
        pending, lambda item: fetch_one_tile(item[0], args.resolution, item[1]),
        max_workers=args.fetch_workers, label="3DEP tile fetch",
        progress_every=max(1, len(pending) // 20),
    )
    if failures:
        print(f"NOTE: {len(failures)} tile(s) failed. Re-run to retry (already-downloaded "
              f"tiles are skipped).", file=sys.stderr)
    print(f"Done. {len(successes)} tile(s) downloaded this run.")


if __name__ == "__main__":
    main()

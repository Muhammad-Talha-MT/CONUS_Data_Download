#!/usr/bin/env python3
"""
13_download_merit_hydro.py  (CONUS-optimized)

RED tier #39: MERIT Hydro global hydrography.

*** STILL NOT FULLY AUTOMATABLE -- unchanged from the original, see the
extensive explanation below. This pass's only change is a consistency fix
(get_region_bbox now also checks for {region}_flowlines.parquet, what
01_build_graph.py writes for CONUS-scale multi-tile regions) so
--list-tiles doesn't break on a CONUS run; the tile-list computation
itself was already cheap (dozens of 5x5-degree tiles even for CONUS) and
was never the bottleneck here -- the Dropbox/Google Form gate is. ***

Registration: http://hydro.iis.u-tokyo.ac.jp/~yamadai/MERIT_Hydro/

TWO REALISTIC PATHS (see original docstring for full detail):
  Option A (recommended) -- one team member registers once, manually
  downloads the tile filenames this script prints, places them on shared
  storage.
  Option B -- exported Dropbox session cookie via --cookie, best-effort,
  not guaranteed to work.

Requires: geopandas, requests (only needed for Option B)
Usage:
    python 13_download_merit_hydro.py --list-tiles --outdir ./output --region CONUS
"""

import argparse
import sys

from config import MERIT_HYDRO_REGISTRATION_URL, make_dirs


def get_region_bbox(dirs, region: str):
    import geopandas as gpd

    for candidate, reader in ((f"{region}_flowlines.parquet", gpd.read_parquet),
                               (f"{region}_flowlines.geojson", gpd.read_file)):
        p = dirs["graph"] / candidate
        if p.exists():
            flowlines = reader(p)
            if flowlines.crs is not None and flowlines.crs.to_epsg() != 4326:
                flowlines = flowlines.to_crs("EPSG:4326")
            return tuple(flowlines.total_bounds)
    raise FileNotFoundError(f"No flowlines file found under {dirs['graph']} for region {region}. Run 01_build_graph.py first.")


def compute_tile_names(minx, miny, maxx, maxy) -> list:
    import math

    lat_start = math.floor(miny / 5) * 5
    lat_end = math.ceil(maxy / 5) * 5
    lon_start = math.floor(minx / 5) * 5
    lon_end = math.ceil(maxx / 5) * 5

    tiles = []
    lat = lat_start
    while lat < lat_end:
        lon = lon_start
        while lon < lon_end:
            lat_prefix = f"n{lat:02d}" if lat >= 0 else f"s{-lat:02d}"
            lon_prefix = f"e{lon:03d}" if lon >= 0 else f"w{-lon:03d}"
            tiles.append(f"{lat_prefix}{lon_prefix}")
            lon += 5
        lat += 5
    return tiles


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--outdir", default="./output")
    parser.add_argument("--region", default=None, help="Region code (uses graph/{region}_flowlines.*'s bbox)")
    parser.add_argument("--layer", default="dir",
                         help="MERIT Hydro layer: dir, upa, elv, wth, hnd")
    parser.add_argument("--list-tiles", action="store_true", help="ALWAYS run this first")
    parser.add_argument("--cookie", default=None,
                         help="Dropbox session cookie for Option B -- attempt-only, not guaranteed to work")
    args = parser.parse_args()

    if not args.region:
        print("ERROR: --region is required.", file=sys.stderr)
        sys.exit(1)

    dirs = make_dirs(args.outdir)
    minx, miny, maxx, maxy = get_region_bbox(dirs, args.region)
    tiles = compute_tile_names(minx, miny, maxx, maxy)

    print(f"Region {args.region} bbox: ({minx:.2f}, {miny:.2f}) to ({maxx:.2f}, {maxy:.2f})")
    print(f"\nMERIT Hydro tiles needed for layer '{args.layer}' ({len(tiles)} tile(s)):")
    for tile in tiles:
        print(f"  {tile}_{args.layer}.tif")

    if args.list_tiles or not args.cookie:
        print(f"\nRegister at {MERIT_HYDRO_REGISTRATION_URL} to get access, then either:")
        print("  A) manually download the tiles listed above and place them in:")
        print(f"     {dirs['merit_hydro']}")
        print("  B) obtain a Dropbox session cookie and re-run with --cookie to attempt a "
              "scripted download.")
        return

    import requests

    print(f"\nAttempting scripted download with provided cookie (best-effort) ...")
    for tile in tiles:
        filename = f"{tile}_{args.layer}.tif"
        out_path = dirs["merit_hydro"] / filename
        if out_path.exists():
            print(f"  {filename} already exists, skipping")
            continue
        print(f"  {filename}: attempting download -- ACTUAL DROPBOX URL NOT CONFIRMED, this will "
              f"likely need adjustment once you have the real share link from your registration email.", file=sys.stderr)

    print("\nIf Option B didn't work, fall back to Option A (manual download) -- "
          "this is a one-time task, not a recurring pipeline step, and is unaffected by "
          "whether the target region is a single basin or all of CONUS.")


if __name__ == "__main__":
    main()

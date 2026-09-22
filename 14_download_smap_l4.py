#!/usr/bin/env python3
"""
14_download_smap_l4.py  (CONUS-optimized)

YELLOW tier #11: SMAP L4 Global 3-hourly 9km Soil Moisture.

NOTE ON SCALE: this script's granules are global grids, not regionally
cropped -- a CONUS bbox filter costs about the same in download volume as
a HUC8 one, since full-globe files are pulled either way. The real cost
driver is TIME range, not region size, so this was never really a
"CONUS-scale" bottleneck the way the per-reach scripts are.

WHAT CHANGED: earthaccess.download() already parallelizes internally via
its own `threads` parameter -- the original left this at its default.
Exposed here as --download-threads so it can be tuned explicitly rather
than left implicit.

Requires: earthaccess
Usage:
    python 14_download_smap_l4.py --inspect
    python 14_download_smap_l4.py --outdir ./output --region CONUS --start 2020-01-01 --end 2020-12-31 --download-threads 8
"""

import argparse
import sys

import pandas as pd

from config import DEFAULT_REGION, SMAP_L4_SHORT_NAME, SMAP_L4_VERSION, SMAP_L4_START, make_dirs


def earthdata_login():
    import earthaccess

    auth = earthaccess.login()
    if not auth.authenticated:
        raise RuntimeError(
            "Earthdata authentication failed. Same setup as 04_download_forcing.py: "
            "set EARTHDATA_USERNAME/EARTHDATA_PASSWORD, add a ~/.netrc entry for "
            "urs.earthdata.nasa.gov, or run earthaccess.login() interactively."
        )
    return auth


def inspect_dataset():
    import earthaccess

    earthdata_login()
    print(f"Searching for one {SMAP_L4_SHORT_NAME} v{SMAP_L4_VERSION} granule ...")
    results = earthaccess.search_data(
        short_name=SMAP_L4_SHORT_NAME, version=SMAP_L4_VERSION,
        temporal=(SMAP_L4_START, "2015-04-08"), count=1,
    )
    if not results:
        print("ERROR: no granules found. The short_name/version in config.py may be wrong -- "
              "search NASA Earthdata Search (https://search.earthdata.nasa.gov/) for "
              "'SMAP L4 soil moisture' to find the correct current short_name.", file=sys.stderr)
        sys.exit(1)
    print(f"Found {len(results)} granule(s). Example:")
    print(results[0])


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


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--outdir", default="./output")
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--start", default=SMAP_L4_START)
    parser.add_argument("--end", required=True)
    parser.add_argument("--download-threads", type=int, default=8,
                         help="Passed through to earthaccess.download()'s own internal thread pool")
    parser.add_argument("--inspect", action="store_true", help="RUN THIS FIRST")
    args = parser.parse_args()

    if args.inspect:
        inspect_dataset()
        return

    dirs = make_dirs(args.outdir)
    earthdata_login()
    import earthaccess

    minx, miny, maxx, maxy = get_region_bbox(dirs, args.region)
    print(f"Region bbox: ({minx:.3f}, {miny:.3f}) to ({maxx:.3f}, {maxy:.3f})")

    years = range(pd.Timestamp(args.start).year, pd.Timestamp(args.end).year + 1)

    for year in years:
        year_dir = dirs["smap_l4"] / str(year)
        if year_dir.exists() and any(year_dir.iterdir()):
            print(f"[{year}] already has files, skipping: {year_dir}")
            continue
        year_dir.mkdir(exist_ok=True)

        year_start = max(pd.Timestamp(args.start), pd.Timestamp(f"{year}-01-01"))
        year_end = min(pd.Timestamp(args.end), pd.Timestamp(f"{year}-12-31"))

        print(f"[{year}] searching granules ...")
        try:
            results = earthaccess.search_data(
                short_name=SMAP_L4_SHORT_NAME, version=SMAP_L4_VERSION,
                temporal=(year_start.isoformat(), year_end.isoformat()),
                bounding_box=(minx, miny, maxx, maxy),
            )
        except Exception as e:
            print(f"[{year}] search FAILED: {e}", file=sys.stderr)
            continue

        if not results:
            print(f"[{year}] no granules found for this region/period", file=sys.stderr)
            continue
        print(f"[{year}] {len(results)} granule(s) found, downloading ({args.download_threads} threads) ...")

        try:
            earthaccess.download(results, str(year_dir), threads=args.download_threads)
        except TypeError:
            # Older earthaccess versions may not accept `threads` -- fall back gracefully.
            earthaccess.download(results, str(year_dir))
        except Exception as e:
            print(f"[{year}] download FAILED: {e}", file=sys.stderr)
            continue

        print(f"[{year}] wrote {len(list(year_dir.iterdir()))} file(s) to {year_dir}")

    print("Done.")


if __name__ == "__main__":
    main()

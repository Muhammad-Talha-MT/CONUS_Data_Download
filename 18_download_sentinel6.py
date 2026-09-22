#!/usr/bin/env python3
"""
18_download_sentinel6.py  (CONUS-optimized)

YELLOW tier #18: Sentinel-6 (Michael Freilich) radar altimetry.

Same earthaccess `threads` exposure as 14_download_smap_l4.py. Also
carries forward the original's own caveat: this is primarily an OCEAN
altimetry mission, so coverage over small inland basins may be sparse --
worth confirming Sentinel-3 SRAL isn't the better fit before relying on
this for river/lake stage.

Requires: earthaccess
Usage:
    python 18_download_sentinel6.py --inspect
    python 18_download_sentinel6.py --outdir ./output --region CONUS --start 2021-01-01 --end 2021-12-31
"""

import argparse
import sys

import pandas as pd

from config import DEFAULT_REGION, SENTINEL6_SHORT_NAME, SENTINEL6_START, make_dirs


def earthdata_login():
    import earthaccess

    auth = earthaccess.login()
    if not auth.authenticated:
        raise RuntimeError("Earthdata authentication failed -- same setup as 04_download_forcing.py.")
    return auth


def inspect_dataset():
    import earthaccess

    earthdata_login()
    print(f"Searching for one {SENTINEL6_SHORT_NAME} granule ...")
    results = earthaccess.search_data(
        short_name=SENTINEL6_SHORT_NAME, temporal=(SENTINEL6_START, "2020-12-01"), count=1,
    )
    if not results:
        print(f"ERROR: no granules found for short_name '{SENTINEL6_SHORT_NAME}'. Search "
              f"https://search.earthdata.nasa.gov/ for 'Sentinel-6' to find the correct "
              f"current short_name for the product variant you actually want.", file=sys.stderr)
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
    parser.add_argument("--short-name", default=SENTINEL6_SHORT_NAME)
    parser.add_argument("--start", default=SENTINEL6_START)
    parser.add_argument("--end", required=True)
    parser.add_argument("--download-threads", type=int, default=8)
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
    print("NOTE: Sentinel-6 is primarily an OCEAN altimetry mission -- coverage over small "
          "inland basins may be sparse or empty. Confirm this is the right product vs. "
          "Sentinel-3 SRAL before relying on it.", file=sys.stderr)

    years = range(pd.Timestamp(args.start).year, pd.Timestamp(args.end).year + 1)

    for year in years:
        year_dir = dirs["sentinel6"] / str(year)
        if year_dir.exists() and any(year_dir.iterdir()):
            print(f"[{year}] already has files, skipping: {year_dir}")
            continue
        year_dir.mkdir(exist_ok=True)

        year_start = max(pd.Timestamp(args.start), pd.Timestamp(f"{year}-01-01"))
        year_end = min(pd.Timestamp(args.end), pd.Timestamp(f"{year}-12-31"))

        print(f"[{year}] searching granules ...")
        try:
            results = earthaccess.search_data(
                short_name=args.short_name,
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
            earthaccess.download(results, str(year_dir))
        except Exception as e:
            print(f"[{year}] download FAILED: {e}", file=sys.stderr)
            continue

        print(f"[{year}] wrote {len(list(year_dir.iterdir()))} file(s) to {year_dir}")

    print("Done.")


if __name__ == "__main__":
    main()

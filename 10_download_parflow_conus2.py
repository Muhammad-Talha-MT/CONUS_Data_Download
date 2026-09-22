#!/usr/bin/env python3
"""
10_download_parflow_conus2.py  (CONUS-optimized)

RED tier #46: ParFlow-CONUS2 / HydroFrame groundwater simulation.

*** Function signature not independently re-verified -- run --inspect
first, unchanged from the original. ***

WHAT CHANGED: the (huc8, year) fetch loop now runs with bounded
concurrency (pipeline_utils.parallel_map, default 4 workers) instead of a
strict nested serial loop -- each (huc8, year) unit is independent.

One-time setup (required, free): see original docstring / SETUP.md for
HydroFrame API account + PIN registration.

Writes one NetCDF per HUC8 tile per year (resumable):
    parflow_conus2/{huc8}_parflow_{year}.nc

Requires: hf_hydrodata, region.py
Usage:
    python 10_download_parflow_conus2.py --inspect
    python 10_download_parflow_conus2.py --outdir ./output --region CONUS --start 2003-01-01 --end 2003-12-31 --fetch-workers 4
"""

import argparse
import sys

import pandas as pd

from config import DEFAULT_REGION, PARFLOW_DATASET, make_dirs
from pipeline_utils import parallel_map
import region as region_lib


def inspect_package():
    import hf_hydrodata

    print("hf_hydrodata module contents (functions/classes):")
    for name in sorted(dir(hf_hydrodata)):
        if not name.startswith("_"):
            print(f"  {name}")
    print(f"\nConfigured dataset name in config.py: '{PARFLOW_DATASET}' -- confirm this is a real "
          f"dataset name in hf_hydrodata's own catalog before running a real download.")


def fetch_one(huc8: str, year: int, variable: str, dataset: str, start: str, end: str, out_dir):
    import hf_hydrodata

    boundary = region_lib.fetch_boundary_for_code(huc8)
    minx, miny, maxx, maxy = boundary.total_bounds if hasattr(boundary, "total_bounds") else boundary.geometry.total_bounds

    year_start = max(pd.Timestamp(start), pd.Timestamp(f"{year}-01-01"))
    year_end = min(pd.Timestamp(end), pd.Timestamp(f"{year}-12-31"))

    ds = hf_hydrodata.get_gridded_data(
        dataset=dataset, variable=variable,
        date_start=year_start.strftime("%Y-%m-%d"), date_end=year_end.strftime("%Y-%m-%d"),
        latitude_range=(miny, maxy), longitude_range=(minx, maxx),
    )

    out_path = out_dir / f"{huc8}_parflow_{year}.nc"
    try:
        ds.to_netcdf(out_path)
    except AttributeError:
        import pandas as _pd
        if isinstance(ds, _pd.DataFrame):
            out_path = out_path.with_suffix(".parquet")
            ds.to_parquet(out_path, index=False)
        else:
            raise
    return out_path


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--outdir", default="./output")
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--dataset", default=PARFLOW_DATASET)
    parser.add_argument("--variable", default="water_table_depth")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--fetch-workers", type=int, default=4,
                         help="Concurrent (HUC8, year) fetches (default: 4)")
    parser.add_argument("--inspect", action="store_true", help="Print package structure and exit -- RUN THIS FIRST")
    args = parser.parse_args()

    if args.inspect:
        inspect_package()
        return

    dirs = make_dirs(args.outdir)

    print(f"Resolving region {args.region} into HUC8 tiles ...")
    huc8_units = region_lib.list_huc8_units(args.region)
    print(f"  {len(huc8_units)} HUC8 tile(s)")

    years = list(range(pd.Timestamp(args.start).year, pd.Timestamp(args.end).year + 1))

    pending = []
    for huc8 in huc8_units:
        for year in years:
            out_path_nc = dirs["parflow_conus2"] / f"{huc8}_parflow_{year}.nc"
            out_path_pq = out_path_nc.with_suffix(".parquet")
            if not out_path_nc.exists() and not out_path_pq.exists():
                pending.append((huc8, year))
    already = len(huc8_units) * len(years) - len(pending)
    if already:
        print(f"{already} (HUC8, year) unit(s) already downloaded, skipping")
    if not pending:
        print("Nothing left to download.")
        return

    print(f"Fetching {len(pending)} (HUC8, year) unit(s), {args.fetch_workers} concurrent ...")

    def _fetch(item):
        huc8, year = item
        return fetch_one(huc8, year, args.variable, args.dataset, args.start, args.end, dirs["parflow_conus2"])

    successes, failures = parallel_map(
        pending, _fetch, max_workers=args.fetch_workers, label="(HUC8, year) fetch",
        progress_every=max(1, len(pending) // 20),
    )
    if failures:
        print(f"NOTE: {len(failures)} unit(s) failed -- run --inspect and check hf_hydrodata's "
              f"real API if this function name/signature doesn't match. Re-run to retry.", file=sys.stderr)
    print(f"Done. {len(successes)} unit(s) written this run.")


if __name__ == "__main__":
    main()

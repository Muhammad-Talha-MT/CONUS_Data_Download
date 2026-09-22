#!/usr/bin/env python3
"""
08_download_nhdplus_hr.py  (CONUS-optimized)

RED tier #41: NHDPlus High Resolution.

WHAT CHANGED: per-HU4 fetches now run with bounded concurrency
(pipeline_utils.parallel_map, default 4 workers) instead of a strict
serial loop -- each HU4's fetch is independent, and at CONUS scale this
covers all ~222 HU4 units nationally rather than a handful within one
basin.

Writes GeoJSON per HU4 unit (resumable -- skips HU4s already downloaded):
    nhdplus_hr/{region}_hr_{layer}_{hu4}.geojson

Requires: pynhd, geopandas
Usage:
    python 08_download_nhdplus_hr.py --outdir ./output --region CONUS --fetch-workers 4
"""

import argparse
import sys

from config import DEFAULT_REGION, NHDPLUS_HR_LAYER, make_dirs
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


def list_hu4_units_for_region(dirs, region: str) -> list:
    from pynhd import WaterData
    from shapely.geometry import box

    minx, miny, maxx, maxy = get_region_bbox(dirs, region)
    bbox_geom = box(minx, miny, maxx, maxy)
    wd4 = WaterData("wbd04")
    units = wd4.bygeom(bbox_geom, geo_crs="EPSG:4326")
    if "huc4" not in units.columns:
        raise RuntimeError(f"wbd04 query returned no 'huc4' column; columns were {list(units.columns)}")
    return sorted(units["huc4"].dropna().unique().tolist())


def fetch_one_hu4(layer: str, hu4: str):
    from pynhd import NHDPlusHR

    nhd_hr = NHDPlusHR(layer)
    if hasattr(nhd_hr, "byids"):
        return nhd_hr.byids("huc4", [hu4])
    if hasattr(nhd_hr, "bygeom"):
        from pynhd import WaterData
        boundary = WaterData("wbd04").byid("huc4", hu4)
        return nhd_hr.bygeom(boundary.geometry.iloc[0], geo_crs=boundary.crs)
    raise AttributeError(
        "NHDPlusHR object has neither 'byids' nor 'bygeom' -- installed pynhd's API differs "
        "from what this script expects. Check with: python3 -c \"from pynhd import NHDPlusHR; help(NHDPlusHR)\""
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--outdir", default="./output")
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--layer", default=NHDPLUS_HR_LAYER)
    parser.add_argument("--fetch-workers", type=int, default=4,
                         help="Concurrent HU4 fetches (default: 4, kept modest -- shared public web service)")
    args = parser.parse_args()

    dirs = make_dirs(args.outdir)

    print(f"Determining HU4 units covering region {args.region} ...")
    hu4_units = list_hu4_units_for_region(dirs, args.region)
    print(f"  {len(hu4_units)} HU4 unit(s): {hu4_units}")

    pending = []
    for hu4 in hu4_units:
        out_path = dirs["nhdplus_hr"] / f"{args.region}_hr_{args.layer}_{hu4}.geojson"
        if not out_path.exists():
            pending.append((hu4, out_path))
    already = len(hu4_units) - len(pending)
    if already:
        print(f"{already} HU4 unit(s) already downloaded, skipping")
    if not pending:
        print("Nothing left to download.")
        return

    print(f"Fetching {len(pending)} HU4 unit(s), {args.fetch_workers} concurrent ...")

    def _fetch_and_write(item):
        hu4, out_path = item
        gdf = fetch_one_hu4(args.layer, hu4)
        gdf.to_file(out_path, driver="GeoJSON")
        return len(gdf)

    successes, failures = parallel_map(
        pending, _fetch_and_write, max_workers=args.fetch_workers, label="HU4 fetch",
    )
    if failures:
        print(f"NOTE: {len(failures)} HU4 unit(s) failed -- this is the part of the pipeline not "
              f"independently re-verified end-to-end; check pynhd's NHDPlusHR API if these keep "
              f"failing. Re-run to retry.", file=sys.stderr)
    print(f"Done. {len(successes)} HU4 unit(s) written this run.")


if __name__ == "__main__":
    main()

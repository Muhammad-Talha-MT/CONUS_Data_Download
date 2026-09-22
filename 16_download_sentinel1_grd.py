#!/usr/bin/env python3
"""
16_download_sentinel1_grd.py  (CONUS-optimized)

YELLOW tier #12: Sentinel-1 GRD (SAR, inundation/soil moisture proxy).

WHAT CHANGED IN THIS PASS:
  1. search_cdse_catalog() (cdse_common.py) now paginates fully instead of
     silently truncating at 1,000 results -- see that file's docstring.
     At CONUS scale, a multi-year Sentinel-1 search will very likely
     exceed 1,000 matching scenes; this used to return an incomplete list
     with no warning.
  2. File downloads now run with bounded concurrency (pipeline_utils.
     parallel_map, default 6 workers -- kept a bit lower than the general
     default since these are large multi-GB .SAFE archives, not small
     metadata requests) instead of one at a time. At CONUS scale this
     could be thousands of archives; serial downloads of that many
     multi-GB files is a multi-day-to-week bottleneck on its own.

Requires: boto3, requests, geopandas
Usage:
    python 16_download_sentinel1_grd.py --inspect --region 06010105 --start 2023-01-01 --end 2023-01-31
    python 16_download_sentinel1_grd.py --outdir ./output --region CONUS --start 2023-01-01 --end 2023-12-31 --download-workers 6
"""

import argparse
import sys

from config import DEFAULT_REGION, DOWNLOAD_MAX_WORKERS, make_dirs
from cdse_common import get_cdse_s3_client, search_cdse_catalog
from pipeline_utils import parallel_map

COLLECTION_NAME = "SENTINEL-1"


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
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--product-type", default="GRD", help="GRD, SLC, OCN, etc.")
    parser.add_argument("--download-workers", type=int, default=6,
                         help="Concurrent .SAFE archive downloads (default: 6, kept modest -- these "
                              "are large multi-GB files)")
    parser.add_argument("--inspect", action="store_true", help="RUN THIS FIRST")
    args = parser.parse_args()

    dirs = make_dirs(args.outdir)
    bbox = get_region_bbox(dirs, args.region)
    print(f"Region bbox: {bbox}")

    print(f"Searching CDSE catalog for {COLLECTION_NAME} products, {args.start} to {args.end} "
          f"(fully paginated -- see cdse_common.py) ...")
    try:
        max_products = 100 if args.inspect else None
        products = search_cdse_catalog(COLLECTION_NAME, bbox, args.start, args.end, max_products=max_products)
    except Exception as e:
        print(f"ERROR searching CDSE catalog: {e}", file=sys.stderr)
        sys.exit(1)

    products = [p for p in products if args.product_type in p.get("Name", "")]
    print(f"{len(products)} matching product(s) found")

    if args.inspect:
        for p in products[:20]:
            print(f"  {p.get('Name')}  ({p.get('ContentLength', '?')} bytes)  id={p.get('Id')}")
        if len(products) > 20:
            print(f"  ... and {len(products) - 20} more")
        return

    if not products:
        print("Nothing to download. Run --inspect to check the search parameters.", file=sys.stderr)
        return

    s3 = get_cdse_s3_client()
    to_download = []
    for product in products:
        name = product.get("Name", "unnamed_product")
        out_path = dirs["sentinel1_grd"] / name
        if out_path.exists():
            continue
        s3_path = product.get("S3Path")
        if not s3_path:
            print(f"  {name}: no S3Path in catalog response, skipping -- inspect the raw "
                  f"product dict to find the right field.", file=sys.stderr)
            continue
        to_download.append((name, s3_path, out_path))

    already = len(products) - len(to_download)
    if already:
        print(f"{already} product(s) already downloaded, skipping")
    if not to_download:
        print("Nothing left to download.")
        return

    print(f"Downloading {len(to_download)} product(s), {args.download_workers} concurrent ...")

    def _download_one(item):
        name, s3_path, out_path = item
        key = s3_path.lstrip("/")
        if key.startswith("eodata/"):
            key = key[len("eodata/"):]
        s3.download_file("eodata", key, str(out_path))
        return out_path

    successes, failures = parallel_map(
        to_download, _download_one, max_workers=args.download_workers,
        label="product download", progress_every=max(1, len(to_download) // 20),
    )
    print(f"Done. {len(successes)} downloaded, {len(failures)} failed "
          f"(re-run to retry failed ones -- already-downloaded files are skipped).")


if __name__ == "__main__":
    main()

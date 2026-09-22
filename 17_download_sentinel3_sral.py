#!/usr/bin/env python3
"""
17_download_sentinel3_sral.py  (CONUS-optimized)

YELLOW tier #17: Sentinel-3 SRAL (radar altimetry, river/lake stage).

Same fixes as 16_download_sentinel1_grd.py -- paginated CDSE search (via
the fixed cdse_common.py) and bounded-concurrency downloads. See that
script's docstring for the full reasoning.

Requires: boto3, requests, geopandas
Usage:
    python 17_download_sentinel3_sral.py --inspect --region 06010105 --start 2023-01-01 --end 2023-01-31
    python 17_download_sentinel3_sral.py --outdir ./output --region CONUS --start 2023-01-01 --end 2023-12-31
"""

import argparse
import sys

from config import DEFAULT_REGION, make_dirs
from cdse_common import get_cdse_s3_client, search_cdse_catalog
from pipeline_utils import parallel_map

COLLECTION_NAME = "SENTINEL-3"


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
    parser.add_argument("--product-type", default="SR_2_LAN",
                         help="SRAL product type filter -- SR_2_LAN is land/inland-water altimetry")
    parser.add_argument("--download-workers", type=int, default=6)
    parser.add_argument("--inspect", action="store_true", help="RUN THIS FIRST")
    args = parser.parse_args()

    dirs = make_dirs(args.outdir)
    bbox = get_region_bbox(dirs, args.region)
    print(f"Region bbox: {bbox}")

    print(f"Searching CDSE catalog for {COLLECTION_NAME} products, {args.start} to {args.end} "
          f"(fully paginated) ...")
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
        print("Nothing to download. Run --inspect to check the search parameters "
              "(SRAL products are lower-volume than Sentinel-1, so an empty result "
              "for a small basin/short window is plausible, not necessarily a bug).", file=sys.stderr)
        return

    s3 = get_cdse_s3_client()
    to_download = []
    for product in products:
        name = product.get("Name", "unnamed_product")
        out_path = dirs["sentinel3_sral"] / name
        if out_path.exists():
            continue
        s3_path = product.get("S3Path")
        if not s3_path:
            print(f"  {name}: no S3Path in catalog response, skipping", file=sys.stderr)
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
          f"(re-run to retry failed ones).")


if __name__ == "__main__":
    main()

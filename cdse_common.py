"""
cdse_common.py  (CONUS-optimized)

Shared helper for accessing the Copernicus Data Space Ecosystem's (CDSE)
S3-compatible "eodata" object storage -- used by 16_download_sentinel1_grd.py
and 17_download_sentinel3_sral.py.

FIX IN THIS PASS: search_cdse_catalog() previously used a flat "$top": 1000
with no pagination at all -- any query matching more than 1,000 products
was silently truncated, with no error or warning. At HUC8 scale this never
mattered (a small basin rarely has 1,000+ Sentinel scenes in a query
window); at CONUS scale, a multi-year Sentinel-1 search almost certainly
will exceed 1,000 matches, and the original would have quietly returned an
incomplete result with nothing to indicate that. Now follows
'@odata.nextLink' via pipeline_utils.paginated_odata_get() until exhausted.

One-time setup (required, free):
    1. Register at https://dataspace.copernicus.eu/
    2. Generate S3 access/secret keys from your CDSE account's
       "S3 Credentials" section (separate from your login password)
    3. export CDSE_S3_ACCESS_KEY=<your-access-key>
       export CDSE_S3_SECRET_KEY=<your-secret-key>
"""

import os
import sys

from config import CDSE_S3_ENDPOINT, CDSE_S3_BUCKET, CDSE_REGISTRATION_URL, CDSE_PAGE_SIZE
from pipeline_utils import paginated_odata_get


def get_cdse_s3_client():
    import boto3

    access_key = os.environ.get("CDSE_S3_ACCESS_KEY")
    secret_key = os.environ.get("CDSE_S3_SECRET_KEY")
    if not access_key or not secret_key:
        print("ERROR: CDSE_S3_ACCESS_KEY / CDSE_S3_SECRET_KEY not set. Register at "
              f"{CDSE_REGISTRATION_URL} and generate S3 credentials from your account "
              "settings (separate from your login password).", file=sys.stderr)
        sys.exit(1)

    return boto3.client(
        "s3",
        endpoint_url=CDSE_S3_ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="default",
    )


def search_cdse_catalog(collection: str, bbox: tuple, start: str, end: str,
                         extra_filters: dict = None, max_products: int = None):
    """
    Search CDSE's OData/STAC catalog for products matching a
    collection/bbox/time window, now paginating fully via @odata.nextLink
    instead of silently capping at 1,000 results.

    `max_products` is an optional safety cap for a deliberately narrow
    inspection (e.g. --inspect); leave it None for a real download run so
    nothing is silently left out at CONUS scale.
    """
    import requests

    minx, miny, maxx, maxy = bbox
    aoi = f"POLYGON(({minx} {miny},{maxx} {miny},{maxx} {maxy},{minx} {maxy},{minx} {miny}))"
    filter_str = (
        f"Collection/Name eq '{collection}' and "
        f"OData.CSC.Intersects(area=geography'SRID=4326;{aoi}') and "
        f"ContentDate/Start gt {start}T00:00:00.000Z and "
        f"ContentDate/Start lt {end}T23:59:59.999Z"
    )
    url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
    params = {"$filter": filter_str}

    max_pages = None
    if max_products:
        max_pages = max(1, -(-max_products // CDSE_PAGE_SIZE))  # ceil division

    products = paginated_odata_get(requests, url, params, page_size=CDSE_PAGE_SIZE, max_pages=max_pages)
    if max_products:
        products = products[:max_products]
    return products

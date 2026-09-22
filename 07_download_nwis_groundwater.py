#!/usr/bin/env python3
"""
07_download_nwis_groundwater.py  (CONUS-optimized)

RED tier #20: USGS NWIS groundwater observations (depth-to-water-level).

Same threaded-batch-fetch fix as 06_download_nwis_iv.py -- site-batch
fetches within a year now run with bounded concurrency instead of a
strict serial loop. See that script's docstring for the reasoning.

Writes one Parquet file per year (resumable):
    nwis_gwlevels/{region}_nwis_gwlevels_{year}.parquet

Requires: requests, pandas, pyarrow, geopandas, shapely
Usage:
    python 07_download_nwis_groundwater.py --outdir ./output --region CONUS --max-sites 5000
"""

import argparse
import sys

import pandas as pd
import requests

from config import DEFAULT_REGION, NWIS_GWLEVELS_URL, NWIS_GWLEVELS_PARAMETER_CD, NWIS_GWLEVELS_START, make_dirs
from pipeline_utils import chunked, parallel_map, retry_with_backoff

NWIS_SITE_URL = "https://waterservices.usgs.gov/nwis/site/"


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


def get_region_boundary(dirs, region: str):
    import geopandas as gpd

    boundary_path = dirs["basin"] / f"{region}_boundary.geojson"
    if boundary_path.exists():
        return gpd.read_file(boundary_path).to_crs("EPSG:4326").geometry.iloc[0]
    return None


def discover_wells(dirs, region: str) -> list:
    import geopandas as gpd
    from shapely.geometry import Point

    minx, miny, maxx, maxy = get_region_bbox(dirs, region)
    print(f"  bbox: ({minx:.3f}, {miny:.3f}) to ({maxx:.3f}, {maxy:.3f})")

    params = {"format": "rdb", "bBox": f"{minx},{miny},{maxx},{maxy}", "siteType": "GW",
              "hasDataTypeCd": "gw", "siteStatus": "all"}
    resp = requests.get(NWIS_SITE_URL, params=params, timeout=60)
    resp.raise_for_status()
    lines = [l for l in resp.text.splitlines() if not l.startswith("#")]
    if len(lines) < 3:
        print("  No groundwater sites found with level data in this bounding box.", file=sys.stderr)
        return []

    header = lines[0].split("\t")
    rows = [dict(zip(header, l.split("\t"))) for l in lines[2:] if l.strip()]
    df = pd.DataFrame(rows)
    print(f"  {len(df)} groundwater site(s) in bbox")

    boundary = get_region_boundary(dirs, region)
    if boundary is not None and "dec_lat_va" in df.columns and "dec_long_va" in df.columns:
        coords = df[["dec_lat_va", "dec_long_va"]].apply(pd.to_numeric, errors="coerce")
        valid = coords.dropna()
        df = df.loc[valid.index]
        points = gpd.GeoSeries([Point(xy) for xy in zip(valid["dec_long_va"], valid["dec_lat_va"])], crs="EPSG:4326")
        df = df[points.within(boundary).values]
        print(f"  {len(df)} site(s) after exact boundary filter")

    return sorted(df["site_no"].astype(str).str.zfill(8).unique().tolist())


def fetch_gwlevels_batch(sites: list, year: int, start: str, end: str, param_cd: str) -> pd.DataFrame:
    year_start = max(pd.Timestamp(start), pd.Timestamp(f"{year}-01-01"))
    year_end = min(pd.Timestamp(end), pd.Timestamp(f"{year}-12-31"))

    def _fetch():
        params = {
            "format": "rdb", "sites": ",".join(sites),
            "startDT": year_start.strftime("%Y-%m-%d"), "endDT": year_end.strftime("%Y-%m-%d"),
            "parameterCd": param_cd, "siteStatus": "all",
        }
        resp = requests.get(NWIS_GWLEVELS_URL, params=params, timeout=120)
        if resp.status_code == 404:
            return pd.DataFrame(columns=["site_no", "time", "value", "qualifiers"])
        resp.raise_for_status()
        lines = [l for l in resp.text.splitlines() if not l.startswith("#")]
        if len(lines) < 3:
            return pd.DataFrame(columns=["site_no", "time", "value", "qualifiers"])
        header = lines[0].split("\t")
        rows = [dict(zip(header, l.split("\t"))) for l in lines[2:] if l.strip()]
        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(columns=["site_no", "time", "value", "qualifiers"])
        date_col = "lev_dt" if "lev_dt" in df.columns else None
        value_col = "lev_va" if "lev_va" in df.columns else None
        qual_col = "lev_status_cd" if "lev_status_cd" in df.columns else None
        if not date_col or not value_col:
            print(f"[{year}] WARNING: expected columns not found (got {list(df.columns)}), "
                  f"returning raw unparsed rows.", file=sys.stderr)
            return df
        out = pd.DataFrame({
            "site_no": df["site_no"],
            "time": pd.to_datetime(df[date_col], errors="coerce"),
            "value": pd.to_numeric(df[value_col], errors="coerce"),
            "qualifiers": df[qual_col] if qual_col else "",
        })
        return out.dropna(subset=["time", "value"])

    result = retry_with_backoff(_fetch, max_attempts=3, wait_s=30, label=f"[{year}] gwlevels batch")
    if result is None:
        raise RuntimeError(f"[{year}] batch failed after retries")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--outdir", default="./output")
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--sites", nargs="+", default=None)
    parser.add_argument("--start", default=NWIS_GWLEVELS_START)
    parser.add_argument("--end", default=None)
    parser.add_argument("--parameter-cd", default=NWIS_GWLEVELS_PARAMETER_CD)
    parser.add_argument("--site-batch-size", type=int, default=50)
    parser.add_argument("--max-sites", type=int, default=None)
    parser.add_argument("--max-workers", type=int, default=4)
    args = parser.parse_args()

    end = args.end or pd.Timestamp.now().strftime("%Y-%m-%d")
    dirs = make_dirs(args.outdir)

    if args.sites:
        sites = sorted(str(s).zfill(8) for s in args.sites)
    else:
        print(f"Auto-discovering groundwater wells in region {args.region} ...")
        sites = discover_wells(dirs, args.region)
        if not sites:
            print("ERROR: no wells found. Pass --sites explicitly if you know which wells you want.", file=sys.stderr)
            sys.exit(1)
        if args.max_sites and len(sites) > args.max_sites:
            print(f"  Capping {len(sites)} sites down to --max-sites={args.max_sites}")
            sites = sites[:args.max_sites]
    print(f"Sites: {len(sites)}")

    site_batches = chunked(sites, args.site_batch_size)
    years = range(pd.Timestamp(args.start).year, pd.Timestamp(end).year + 1)

    for year in years:
        out_path = dirs["nwis_gwlevels"] / f"{args.region}_nwis_gwlevels_{year}.parquet"
        if out_path.exists():
            print(f"[{year}] already exists, skipping: {out_path}")
            continue

        print(f"[{year}] fetching {len(sites)} well(s) across {len(site_batches)} batch(es), "
              f"{args.max_workers} concurrent ...")
        successes, failures = parallel_map(
            site_batches, lambda b: fetch_gwlevels_batch(b, year, args.start, end, args.parameter_cd),
            max_workers=args.max_workers, label=f"[{year}] gwlevels batch",
            progress_every=max(1, len(site_batches) // 10),
        )
        year_frames = [df for _batch, df in successes if not df.empty]

        if not year_frames:
            print(f"[{year}] no data retrieved, skipping file write", file=sys.stderr)
            continue

        combined = pd.concat(year_frames, ignore_index=True)
        combined.to_parquet(out_path, index=False)
        print(f"[{year}] wrote {out_path} ({len(combined)} rows, {combined['site_no'].nunique()} wells, "
              f"{len(failures)} batch(es) failed)")

    print("Done.")


if __name__ == "__main__":
    main()

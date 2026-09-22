#!/usr/bin/env python3
"""
06_download_nwis_iv.py  (CONUS-optimized)

Fetch USGS NWIS Instantaneous Values (IV) for gauges within a region.

WHAT CHANGED: site-batch fetches within a year now run with bounded
concurrency (pipeline_utils.parallel_map, default 4 workers -- kept
modest, this is a shared public USGS service) instead of a strict serial
loop. At CONUS scale, --max-sites can still be tens of thousands of
gauges even capped, meaning hundreds of batches per year; serial fetching
of those was a real, avoidable wall-clock cost.

RESOLUTION: NWIS IV data is natively 5-15 minute; resampled to hourly
means by default (--no-resample keeps native frequency).

Writes one Parquet file per year (resumable):
    nwis_iv/{region}_nwis_iv_{year}.parquet

Requires: requests, pandas, pyarrow, geopandas, shapely
Usage:
    python 06_download_nwis_iv.py --outdir ./output --region CONUS --max-sites 5000 --max-workers 4
"""

import argparse
import sys

import pandas as pd
import requests

from config import DEFAULT_REGION, NWIS_IV_PARAMETER_CD, NWIS_IV_START, DOWNLOAD_MAX_WORKERS, make_dirs
from pipeline_utils import chunked, parallel_map, retry_with_backoff

NWIS_SITE_URL = "https://waterservices.usgs.gov/nwis/site/"
NWIS_IV_URL = "https://waterservices.usgs.gov/nwis/iv/"


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
        boundary = gpd.read_file(boundary_path).to_crs("EPSG:4326")
        return boundary.geometry.iloc[0]
    return None


def discover_sites(dirs, region: str) -> list:
    import geopandas as gpd
    from shapely.geometry import Point

    minx, miny, maxx, maxy = get_region_bbox(dirs, region)
    print(f"  bbox: ({minx:.3f}, {miny:.3f}) to ({maxx:.3f}, {maxy:.3f})")

    params = {
        "format": "rdb", "bBox": f"{minx},{miny},{maxx},{maxy}",
        "hasDataTypeCd": "iv", "siteType": "ST", "siteStatus": "all",
    }
    resp = requests.get(NWIS_SITE_URL, params=params, timeout=60)
    resp.raise_for_status()
    lines = [l for l in resp.text.splitlines() if not l.startswith("#")]
    if len(lines) < 3:
        print("  No sites found with instantaneous-value data in this bounding box.", file=sys.stderr)
        return []

    header = lines[0].split("\t")
    rows = [dict(zip(header, l.split("\t"))) for l in lines[2:] if l.strip()]
    df = pd.DataFrame(rows)
    print(f"  {len(df)} site(s) in bbox with IV data")

    boundary = get_region_boundary(dirs, region)
    if boundary is not None and "dec_lat_va" in df.columns and "dec_long_va" in df.columns:
        coords = df[["dec_lat_va", "dec_long_va"]].apply(pd.to_numeric, errors="coerce")
        valid = coords.dropna()
        df = df.loc[valid.index]
        points = gpd.GeoSeries([Point(xy) for xy in zip(valid["dec_long_va"], valid["dec_lat_va"])], crs="EPSG:4326")
        df = df[points.within(boundary).values]
        print(f"  {len(df)} site(s) after exact boundary filter")

    return sorted(df["site_no"].astype(str).str.zfill(8).unique().tolist())


def fetch_iv_batch(sites: list, year: int, start: str, end: str, param_cd: str) -> pd.DataFrame:
    year_start = max(pd.Timestamp(start), pd.Timestamp(f"{year}-01-01"))
    year_end = min(pd.Timestamp(end), pd.Timestamp(f"{year}-12-31 23:59"))

    def _fetch():
        params = {
            "format": "json", "sites": ",".join(sites),
            "startDT": year_start.strftime("%Y-%m-%d"), "endDT": year_end.strftime("%Y-%m-%d"),
            "parameterCd": param_cd, "siteStatus": "all",
        }
        resp = requests.get(NWIS_IV_URL, params=params, timeout=120)
        if resp.status_code == 404:
            return pd.DataFrame(columns=["site_no", "time", "value", "qualifiers"])
        resp.raise_for_status()
        data = resp.json()
        series = data.get("value", {}).get("timeSeries", [])
        frames = []
        for s in series:
            site_no = s["sourceInfo"]["siteCode"][0]["value"]
            values = s["values"][0]["value"] if s["values"] else []
            if not values:
                continue
            df = pd.DataFrame(values)
            df["site_no"] = site_no
            df["time"] = pd.to_datetime(df["dateTime"], utc=True, errors="coerce")
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            frames.append(df[["site_no", "time", "value", "qualifiers"]])
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["site_no", "time", "value", "qualifiers"])

    result = retry_with_backoff(_fetch, max_attempts=3, wait_s=30, label=f"[{year}] IV batch")
    if result is None:
        raise RuntimeError(f"[{year}] batch failed after retries")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--outdir", default="./output")
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--sites", nargs="+", default=None)
    parser.add_argument("--start", default=NWIS_IV_START)
    parser.add_argument("--end", default=None, help="Default: today")
    parser.add_argument("--parameter-cd", default=NWIS_IV_PARAMETER_CD)
    parser.add_argument("--no-resample", action="store_true")
    parser.add_argument("--site-batch-size", type=int, default=20)
    parser.add_argument("--max-sites", type=int, default=None)
    parser.add_argument("--max-workers", type=int, default=4,
                         help="Concurrent site-batch fetches per year (default: 4, kept modest -- "
                              "shared public USGS service)")
    args = parser.parse_args()

    end = args.end or pd.Timestamp.now().strftime("%Y-%m-%d")
    dirs = make_dirs(args.outdir)

    if args.sites:
        sites = sorted(str(s).zfill(8) for s in args.sites)
        print(f"Using {len(sites)} explicitly provided site(s).")
    else:
        print(f"Auto-discovering sites with IV data in region {args.region} ...")
        sites = discover_sites(dirs, args.region)
        if args.max_sites and len(sites) > args.max_sites:
            print(f"  Capping {len(sites)} discovered sites down to --max-sites={args.max_sites}")
            sites = sites[:args.max_sites]
        if not sites:
            print("ERROR: no sites found. Pass --sites explicitly if you know which gauges you want.", file=sys.stderr)
            sys.exit(1)
    print(f"Sites: {len(sites)}")

    site_batches = chunked(sites, args.site_batch_size)
    years = range(pd.Timestamp(args.start).year, pd.Timestamp(end).year + 1)

    for year in years:
        out_path = dirs["nwis_iv"] / f"{args.region}_nwis_iv_{year}.parquet"
        if out_path.exists():
            print(f"[{year}] already exists, skipping: {out_path}")
            continue

        print(f"[{year}] fetching {len(sites)} site(s) across {len(site_batches)} batch(es), "
              f"{args.max_workers} concurrent ...")
        successes, failures = parallel_map(
            site_batches, lambda b: fetch_iv_batch(b, year, args.start, end, args.parameter_cd),
            max_workers=args.max_workers, label=f"[{year}] IV batch",
            progress_every=max(1, len(site_batches) // 10),
        )
        year_frames = [df for _batch, df in successes]

        if not year_frames:
            print(f"[{year}] no data retrieved, skipping file write", file=sys.stderr)
            continue

        combined = pd.concat(year_frames, ignore_index=True)
        combined = combined.dropna(subset=["time", "value"])

        if not args.no_resample and not combined.empty:
            combined = (
                combined.set_index("time").groupby("site_no")["value"].resample("1h").mean().reset_index()
            )

        combined.to_parquet(out_path, index=False)
        n_sites = combined["site_no"].nunique() if not combined.empty else 0
        print(f"[{year}] wrote {out_path} ({len(combined)} rows, {n_sites}/{len(sites)} sites, "
              f"{len(failures)} batch(es) failed)")

    print("Done.")


if __name__ == "__main__":
    main()

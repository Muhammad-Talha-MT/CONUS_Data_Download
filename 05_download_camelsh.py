#!/usr/bin/env python3
"""
05_download_camelsh.py  (CONUS-optimized)

Fetch CAMELSH hourly streamflow/water level and basin attributes, filtered
to gauges within a target region.

NOTE ON SCALE: this script's dominant cost (downloading the ~57GB Zenodo
archive once) is genuinely region-size-independent, as the original
already documented -- it was never a CONUS-scale bottleneck. The only
change in this pass is a consistency fix: load_region_extent() now looks
for {region}_flowlines.parquet first (what 01_build_graph.py now writes
for multi-tile HUC2/4/6/CONUS regions) before falling back to .geojson --
without this, a CONUS run of this script would fail outright since the
original only ever looked for the .geojson file.

Everything else (Zenodo file listing, gauge filtering, archive
download/extraction) is unchanged from the original.

Requires: requests, py7zr, pandas, pyarrow, geopandas, shapely
Usage:
    python 05_download_camelsh.py --outdir ./output --region CONUS
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
import requests

from config import (
    DEFAULT_REGION,
    CAMELSH_ZENODO_API,
    CAMELSH_HOURLY_ARCHIVE_NAME,
    CAMELSH_INFO_CSV_NAME,
    make_dirs,
)


def get_zenodo_file_list():
    resp = requests.get(CAMELSH_ZENODO_API, timeout=60)
    resp.raise_for_status()
    record = resp.json()
    return {f["key"]: f["links"]["self"] for f in record.get("files", [])}


def download_file(url, dest_path, description):
    if dest_path.exists():
        print(f"  {description} already downloaded: {dest_path}")
        return
    print(f"  Downloading {description} ...")
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = 100 * downloaded / total
                    print(f"\r    {downloaded/1e9:.2f} / {total/1e9:.2f} GB ({pct:.1f}%)", end="")
        print()
    print(f"  Saved to {dest_path}")


def load_target_sites_from_list(site_list_path: str) -> list:
    p = Path(site_list_path)
    if not p.exists():
        raise FileNotFoundError(f"--site-list file not found: {p}")
    df = pd.read_csv(p, dtype=str)
    col = "site_no" if "site_no" in df.columns else df.columns[0]
    sites = df[col].dropna().astype(str).str.zfill(8).tolist()
    print(f"Loaded {len(sites)} site numbers from {p}")
    return sites


def fetch_gauge_coords_from_usgs(site_numbers: list, chunk_size: int = 100) -> pd.DataFrame:
    import requests

    site_numbers = [str(s).zfill(8) for s in site_numbers]
    chunks = [site_numbers[i:i + chunk_size] for i in range(0, len(site_numbers), chunk_size)]
    frames = []
    for i, chunk in enumerate(chunks, start=1):
        params = {"format": "rdb", "sites": ",".join(chunk), "siteOutput": "expanded", "siteStatus": "all"}
        resp = requests.get("https://waterservices.usgs.gov/nwis/site/", params=params, timeout=60)
        resp.raise_for_status()
        lines = [l for l in resp.text.splitlines() if not l.startswith("#")]
        if len(lines) < 3:
            continue
        header = lines[0].split("\t")
        rows = [dict(zip(header, l.split("\t"))) for l in lines[2:] if l.strip()]
        frames.append(pd.DataFrame(rows))
        if len(chunks) > 5 and i % 5 == 0:
            print(f"    USGS site lookup: {i}/{len(chunks)} batches done")

    if not frames:
        return pd.DataFrame(columns=["site_no", "dec_lat_va", "dec_long_va"])
    coords = pd.concat(frames, ignore_index=True)
    return coords[["site_no", "dec_lat_va", "dec_long_va"]]


def find_latlon_columns(df: pd.DataFrame):
    lat_col = next((c for c in df.columns if c.lower() in ("lat", "latitude", "gauge_lat", "dec_lat_va")), None)
    lon_col = next((c for c in df.columns if c.lower() in ("lon", "long", "longitude", "gauge_lon", "dec_long_va")), None)
    return lat_col, lon_col


def load_region_extent(dirs, region: str):
    """
    Returns (kind, extent): "polygon" (exact, single-basin) or "bbox"
    (approximate, tiled-region case). CHANGED: now checks for
    {region}_flowlines.parquet (what 01_build_graph.py writes for
    multi-tile regions) before falling back to .geojson.
    """
    import geopandas as gpd

    boundary_path = dirs["basin"] / f"{region}_boundary.geojson"
    if boundary_path.exists():
        boundary = gpd.read_file(boundary_path).to_crs("EPSG:4326")
        print(f"Using exact region boundary from {boundary_path}")
        return "polygon", boundary.geometry.iloc[0]

    for candidate, reader in ((f"{region}_flowlines.parquet", gpd.read_parquet),
                               (f"{region}_flowlines.geojson", gpd.read_file)):
        flowlines_path = dirs["graph"] / candidate
        if flowlines_path.exists():
            flowlines = reader(flowlines_path).to_crs("EPSG:4326")
            bbox = tuple(flowlines.total_bounds)
            print(f"No exact boundary found (tiled/multi-basin region) -- using flowlines' "
                  f"bounding box from {flowlines_path} as an approximate filter: {bbox}")
            return "bbox", bbox

    raise FileNotFoundError(
        f"Neither {boundary_path} nor a flowlines file (.parquet/.geojson) found for region "
        f"{region} under {dirs['graph']}. Run 01_build_graph.py first."
    )


def filter_sites_spatially(info: pd.DataFrame, dirs, region: str) -> pd.DataFrame:
    import geopandas as gpd
    from shapely.geometry import Point

    lat_col, lon_col = find_latlon_columns(info)
    if not lat_col or not lon_col:
        staid_col = next((c for c in info.columns if c.lower() in ("staid", "site_no", "gauge_id")), None)
        if not staid_col:
            raise RuntimeError(
                f"No lat/lon columns AND no recognizable site-ID column in info.csv "
                f"(columns were {list(info.columns)}). Use --filter-mode site-list instead."
            )
        print(f"  No lat/lon in info.csv -- fetching coordinates from USGS NWIS for "
              f"{len(info)} sites (column '{staid_col}') ...")
        usgs_coords = fetch_gauge_coords_from_usgs(info[staid_col].tolist())
        usgs_coords["site_no"] = usgs_coords["site_no"].astype(str).str.zfill(8)
        info = info.copy()
        info["_staid_norm"] = info[staid_col].astype(str).str.zfill(8)
        info = info.merge(usgs_coords, left_on="_staid_norm", right_on="site_no", how="left")
        lat_col, lon_col = "dec_lat_va", "dec_long_va"
        n_unmatched = info[lat_col].isna().sum()
        if n_unmatched:
            print(f"  WARNING: {n_unmatched} / {len(info)} sites had no coordinates returned by "
                  f"USGS NWIS -- these will be excluded from the spatial filter.")

    kind, extent = load_region_extent(dirs, region)
    coords = info[[lat_col, lon_col]].apply(pd.to_numeric, errors="coerce")
    valid_coords = coords.dropna()
    info = info.loc[valid_coords.index]
    coords = valid_coords

    if kind == "bbox":
        minx, miny, maxx, maxy = extent
        mask = (
            (coords[lon_col] >= minx) & (coords[lon_col] <= maxx) &
            (coords[lat_col] >= miny) & (coords[lat_col] <= maxy)
        )
        matched = info[mask]
    else:
        points = gpd.GeoSeries([Point(xy) for xy in zip(coords[lon_col], coords[lat_col])], crs="EPSG:4326")
        matched = info[points.within(extent).values]

    print(f"  {len(matched)} / {len(info)} CAMELSH gauges fall within the region ({kind} filter)")
    return matched


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--outdir", default="./output")
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--filter-mode", choices=["spatial", "site-list"], default="spatial")
    parser.add_argument("--site-list", default=None)
    args = parser.parse_args()

    dirs = make_dirs(args.outdir)
    camelsh_dir = dirs["root"] / "camelsh"
    camelsh_dir.mkdir(parents=True, exist_ok=True)

    print("Querying Zenodo record for CAMELSH file list ...")
    files = get_zenodo_file_list()
    print(f"  Found {len(files)} files in the Zenodo record")

    if CAMELSH_INFO_CSV_NAME not in files:
        print(f"ERROR: '{CAMELSH_INFO_CSV_NAME}' not found among Zenodo files: {list(files)}", file=sys.stderr)
        sys.exit(1)

    info_path = camelsh_dir / CAMELSH_INFO_CSV_NAME
    download_file(files[CAMELSH_INFO_CSV_NAME], info_path, "info.csv (gauge index)")
    info = pd.read_csv(info_path, dtype=str)
    print(f"  info.csv columns: {list(info.columns)}")

    if args.filter_mode == "site-list":
        if not args.site_list:
            print("ERROR: --filter-mode site-list requires --site-list <path>.", file=sys.stderr)
            sys.exit(1)
        target_sites = load_target_sites_from_list(args.site_list)
        id_col = next((c for c in info.columns if "gauge" in c.lower() or "site" in c.lower() or "id" in c.lower()), None)
        if not id_col:
            print("ERROR: could not identify a gauge-ID column in info.csv.", file=sys.stderr)
            sys.exit(1)
        info["_site_norm"] = info[id_col].astype(str).str.zfill(8)
        matched = info[info["_site_norm"].isin(target_sites)].drop(columns=["_site_norm"])
        print(f"  {len(matched)} / {len(target_sites)} target sites found in CAMELSH info.csv")
    else:
        matched = filter_sites_spatially(info, dirs, args.region)

    matched_path = camelsh_dir / f"{args.region}_camelsh_matched_sites.csv"
    matched.to_csv(matched_path, index=False)
    print(f"  Wrote matched-site table: {matched_path}")

    id_col_for_extract = next(
        (c for c in matched.columns if "gauge" in c.lower() or "site" in c.lower() or "id" in c.lower()), None,
    )
    if not id_col_for_extract:
        print("ERROR: cannot determine which info.csv column holds the gauge ID.", file=sys.stderr)
        sys.exit(1)
    target_sites = matched[id_col_for_extract].astype(str).tolist()

    if CAMELSH_HOURLY_ARCHIVE_NAME not in files:
        print(f"ERROR: '{CAMELSH_HOURLY_ARCHIVE_NAME}' not found among Zenodo files: {list(files)}", file=sys.stderr)
        sys.exit(1)

    archive_path = camelsh_dir / CAMELSH_HOURLY_ARCHIVE_NAME
    print(f"NOTE: this ~57GB archive covers ALL CAMELSH gauges and downloads once regardless of "
          f"region size (HUC8 or CONUS) -- not the bottleneck that scales with region.")
    download_file(files[CAMELSH_HOURLY_ARCHIVE_NAME], archive_path, CAMELSH_HOURLY_ARCHIVE_NAME)

    print("Extracting matched sites' NetCDF files ...")
    import py7zr

    extract_dir = camelsh_dir / "hourly_extracted"
    extract_dir.mkdir(exist_ok=True)
    with py7zr.SevenZipFile(archive_path, mode="r") as archive:
        all_names = archive.getnames()
        wanted = [n for n in all_names if any(site in n for site in target_sites)]
        print(f"  {len(wanted)} / {len(all_names)} archived files match a target site")
        if wanted:
            archive.extract(path=extract_dir, targets=wanted)
            print(f"  Extracted to {extract_dir}")
        else:
            print("  No matching files found -- check site-number formatting against "
                  "the archive's file-naming convention.", file=sys.stderr)

    print(f"Done. Per-gauge hourly NetCDFs are in {extract_dir}, matched-site table in {matched_path}.")


if __name__ == "__main__":
    main()

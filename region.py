"""
region.py

*** IMPORTANT: this file was NOT among the scripts you uploaded. ***

01_build_graph.py, 08, 09, 10, and 13 all `import region as region_lib` and
call functions on it (fetch_boundary_for_code, fetch_flowlines_for_huc8,
fetch_flowlines_tiled, huc_level, is_conus, list_huc8_units), and the
README/docstrings describe real, validated behavior for it (e.g. "68 raw
HUC8 tiles returned by the spatial query, correctly filtered down to 32
genuine ones" for the Tennessee River Basin). That real, validated module
was not provided to me, so I cannot optimize it -- I can only write a new
implementation of the interface every other script expects, based on what
those call sites and docstrings say it does.

**Treat this file as unverified new code, not a drop-in replacement for
whatever the team's actual region.py does.** In particular, I don't know
what the "raw tiles -> genuine tiles" filtering fix mentioned in the
README actually checks for, so `list_huc8_units()`'s filtering below is my
own best-effort guess (drop tiles whose HUC8 centroid doesn't actually
fall in the queried region), not a reproduction of the validated logic.
**If the real region.py exists somewhere, use that instead of this file.**

What this version adds relative to a "just make it work" implementation:
  - `fetch_flowlines_tiled()` fetches HUC8 tiles concurrently (bounded
    thread pool, via pipeline_utils.parallel_map) instead of serially --
    at CONUS scale (~2,264 HUC8 tiles) a serial fetch against a shared
    public web service is a genuinely long wall-clock bottleneck on its
    own, independent of every other fix in this pass.
  - Per-tile caching to Parquet (not GeoJSON) under `tile_cache_dir`, so a
    resumed CONUS run skips tiles already fetched AND reads them back
    quickly (GeoJSON is a slow, bulky format to repeatedly re-parse at
    this scale -- see 01_build_graph.py's docstring for the same reasoning
    applied to its own output).
"""

from __future__ import annotations

import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

from pipeline_utils import parallel_map

# HUC level -> the WaterData Watershed Boundary Dataset layer name
_WBD_LAYER_BY_LEVEL = {2: "wbd02", 4: "wbd04", 6: "wbd06", 8: "wbd08", 10: "wbd10", 12: "wbd12"}
_HUC_COL_BY_LEVEL = {2: "huc2", 4: "huc4", 6: "huc6", 8: "huc8", 10: "huc10", 12: "huc12"}


def huc_level(code: str) -> int:
    """Number of digits in a HUC code, e.g. '060101050104' -> 12."""
    return len(code)


def is_conus(region: str) -> bool:
    return str(region).strip().upper() == "CONUS"


def fetch_boundary_for_code(huc_code: str) -> gpd.GeoDataFrame:
    """Fetch a single HUC's boundary polygon (any level 2-12) from WBD."""
    from pynhd import WaterData

    level = huc_level(huc_code)
    layer = _WBD_LAYER_BY_LEVEL.get(level)
    col = _HUC_COL_BY_LEVEL.get(level)
    if layer is None:
        raise ValueError(f"Unsupported HUC code length for '{huc_code}' (got {level} digits)")
    wd = WaterData(layer)
    boundary = wd.byid(col, huc_code)
    if boundary is None or len(boundary) == 0:
        raise RuntimeError(f"No boundary returned for {col}={huc_code} from {layer}")
    return boundary


def fetch_flowlines_for_huc8(huc8: str) -> gpd.GeoDataFrame:
    """
    Fetch raw NHDPlus V2 flowlines within one HUC8's boundary -- the
    original, single-basin path, unchanged in spirit from the pre-tiling
    version of this pipeline.
    """
    from pynhd import WaterData

    boundary = fetch_boundary_for_code(huc8)
    wd_fl = WaterData("nhdflowline_network")
    flowlines = wd_fl.bygeom(boundary.geometry.iloc[0], geo_crs=boundary.crs)
    return flowlines


def list_huc8_units(region: str) -> list:
    """
    Resolve a region code (HUC2/4/6/8/10/12, or 'CONUS') into the list of
    HUC8 codes it contains. A single HUC8/10/12 code resolves to itself
    (10/12-digit codes fetch their PARENT HUC8 -- flowline tiling always
    happens at HUC8 granularity in this pipeline).
    """
    from pynhd import WaterData

    if is_conus(region):
        wd2 = WaterData("wbd02")
        # CONUS_HUC2_CODES in config.py exists as a documented fallback,
        # but querying wbd02 directly (rather than trusting a hardcoded
        # list that this project's own config.py flags as "not
        # independently re-verified") avoids depending on that same
        # unverified assumption here too.
        huc2_units = wd2.bygeom(
            gpd.GeoSeries.from_wkt(
                ["POLYGON((-125 24, -66 24, -66 50, -125 50, -125 24))"]
            ).unary_union, geo_crs="EPSG:4326",
        )
        huc2_codes = sorted(huc2_units["huc2"].dropna().unique().tolist())
        huc8s: list = []
        for huc2 in huc2_codes:
            huc8s.extend(_huc8s_within(huc2, level=2))
        return sorted(set(huc8s))

    level = huc_level(region)
    if level == 8:
        return [region]
    if level in (10, 12):
        return [region[:8]]
    if level in (2, 4, 6):
        return _huc8s_within(region, level=level)

    raise ValueError(f"Unrecognized region code: {region!r}")


def _huc8s_within(code: str, level: int) -> list:
    from pynhd import WaterData

    parent_boundary = fetch_boundary_for_code(code)
    wd8 = WaterData("wbd08")
    raw = wd8.bygeom(parent_boundary.geometry.iloc[0], geo_crs=parent_boundary.crs)
    if "huc8" not in raw.columns:
        raise RuntimeError(f"wbd08 query for {code} returned no 'huc8' column")

    # Filtering fix (best-effort reconstruction -- see module docstring):
    # a bbox/geometry intersection query over a HUC's own boundary can
    # return neighboring HUC8s that only marginally clip the query
    # geometry (e.g. share a short border segment) without being
    # meaningfully "in" the basin. Keep only tiles whose own centroid
    # falls inside the parent boundary, which is what actually brought
    # Tennessee's 68 raw hits down to a smaller genuine set in spirit,
    # even if not identically to whatever check the original used.
    centroids = raw.geometry.centroid
    within_mask = centroids.within(parent_boundary.geometry.iloc[0])
    filtered = raw.loc[within_mask]
    huc8_list = sorted(filtered["huc8"].dropna().unique().tolist())

    if len(huc8_list) < len(raw):
        print(f"  {code}: {len(raw)} raw HUC8 tile(s) from wbd08 -> "
              f"{len(huc8_list)} after centroid-in-boundary filtering")
    return huc8_list


def fetch_flowlines_tiled(region: str, tile_cache_dir: Path, max_workers: int = 4) -> gpd.GeoDataFrame:
    """
    Fetch flowlines for a multi-HUC8 region (HUC2/4/6 or CONUS) by tiling
    into HUC8 units, fetching each concurrently (bounded thread pool), and
    caching each tile to Parquet so a resumed run skips tiles already
    fetched. Concatenates every tile's flowlines into one GeoDataFrame for
    topology preparation.

    CHANGED vs. a naive serial version: tile fetches now run through
    pipeline_utils.parallel_map with `max_workers` (default 4, matching
    01_build_graph.py's --tile-workers) instead of one at a time -- at
    CONUS scale (~2,264 HUC8 tiles nationally) this is a meaningful
    wall-clock difference on its own. Kept modest by default since this
    hits a shared public web service (NHDPlus WaterData), same caution
    the original --tile-workers default and docstring already called for.
    """
    tile_cache_dir = Path(tile_cache_dir)
    tile_cache_dir.mkdir(parents=True, exist_ok=True)

    huc8_units = list_huc8_units(region)
    print(f"  {len(huc8_units)} HUC8 tile(s) to fetch for region {region}")

    to_fetch = []
    cached_parts = []
    for huc8 in huc8_units:
        cache_path = tile_cache_dir / f"{huc8}.parquet"
        if cache_path.exists():
            cached_parts.append(gpd.read_parquet(cache_path))
        else:
            to_fetch.append(huc8)

    if cached_parts:
        print(f"  {len(cached_parts)} tile(s) already cached, skipping re-fetch")

    def _fetch_one(huc8: str) -> gpd.GeoDataFrame:
        flowlines = fetch_flowlines_for_huc8(huc8)
        flowlines.to_parquet(tile_cache_dir / f"{huc8}.parquet")
        return flowlines

    fetched_parts = []
    if to_fetch:
        successes, failures = parallel_map(
            to_fetch, _fetch_one, max_workers=max_workers, label="HUC8 tile", progress_every=5,
        )
        fetched_parts = [gdf for _huc8, gdf in successes]
        if failures:
            failed_codes = [huc8 for huc8, _e in failures]
            print(f"  WARNING: {len(failures)} tile(s) failed and are NOT included in this "
                  f"run's flowlines: {failed_codes}. Re-run the same command to retry just "
                  f"these (already-cached tiles are skipped, so this is cheap).", file=sys.stderr)

    all_parts = cached_parts + fetched_parts
    if not all_parts:
        raise RuntimeError(f"No flowline tiles were successfully fetched for region {region}")

    combined = gpd.GeoDataFrame(pd.concat(all_parts, ignore_index=True))
    # A reach near a HUC8 boundary can be returned by more than one tile's
    # query -- de-duplicate by comid before topology prep, or prepare_nhdplus
    # sees (and complains about, or silently mishandles) duplicate nodes.
    if "comid" in combined.columns:
        before = len(combined)
        combined = combined.drop_duplicates(subset="comid").reset_index(drop=True)
        if len(combined) < before:
            print(f"  Dropped {before - len(combined)} duplicate reach(es) fetched by >1 tile "
                  f"(expected near HUC8 boundaries)")
    return combined

#!/usr/bin/env python3
"""
01_build_graph.py  (CONUS-optimized)

Build the NHDPlus V2 river-network graph for a target region, writing:

    basin/{region}_boundary.geojson       (skipped for CONUS -- see below)
    graph/{region}_flowlines.geojson      (single-basin regions only)
    graph/{region}_flowlines.parquet      (multi-tile regions: HUC2/4/6/CONUS)
    graph/{region}_nodes.parquet
    graph/{region}_edges.parquet

WHAT CHANGED IN THIS PASS, AND WHY (see the CONUS-scale assessment this
was written from for the full reasoning):

  1. Topology (prepare_nhdplus) is now run PER HUC2, not once over the
     entire concatenated region. The original ran a single call over all
     flowlines in the region -- fine at HUC2 scale (~59,000 flowlines),
     but a single-threaded, whole-region-at-once call over CONUS's ~2.7M
     flowlines is both a serious memory footprint and a bottleneck with
     no way to resume partway through. HUC2 boundaries are real drainage
     divides, so topology resolves correctly within each HUC2 on its own
     (the rare cross-HUC2 mainstem case is exactly what the existing
     "points_outside_region" outlet-detection logic already handles, now
     just applied at the HUC2 boundary instead of the whole-CONUS one).
     Each HUC2's prepared output is checkpointed to
     graph/_topology_checkpoints/{huc2}_{nodes,edges}.parquet, so a
     resumed CONUS run skips HUC2s already processed -- turning one long
     unresumable job into 18 independently resumable ones.
  2. Multi-tile regions (HUC2/4/6/CONUS) now write flowlines as GeoParquet
     instead of GeoJSON. GeoJSON is a slow-to-write, slow-to-reparse, and
     bulky text format at millions-of-features scale; every OTHER script
     in this pipeline reads {region}_flowlines back in just to get a
     bounding box or geometry, so this is read repeatedly, not just
     written once. Single-basin (HUC8/10/12) runs keep writing GeoJSON --
     it's a few thousand features, GeoJSON's simplicity/portability there
     outweighs the format overhead.
  3. region.py's tile fetch is now concurrent (see region.py's own
     docstring) -- --tile-workers already existed as a flag, it's now
     actually load-bearing across the whole region.fetch_flowlines_tiled
     path rather than just being wired through.

--region accepts the same values as before:
    - an 8/10/12-digit HUC code (one basin): fetched directly.
    - a 2/4/6-digit HUC code (a larger region, e.g. "06"): tiled into its
      contained HUC8 units, topology run per-HUC2.
    - "CONUS": tiled across all CONUS HUC2 units, topology run per-HUC2.
      Still genuinely slow -- this is a long-walltime batch job, not a
      login-node run -- but now resumable at the HUC2 granularity if it's
      interrupted partway through.

Nodes are indexed by NHDPlus COMID, identical to the NWM feature_id used
in the streamflow/groundwater/forcing steps.

Requires: pynhd (HyRiver stack), geopandas, pyarrow
    pip install pynhd geopandas pyarrow

Usage:
    python 01_build_graph.py --outdir ./output                    # default region
    python 01_build_graph.py --outdir ./output --region 06010105  # single HUC8
    python 01_build_graph.py --outdir ./output --region 06        # HUC2 (Tennessee)
    python 01_build_graph.py --outdir ./output --region CONUS --tile-workers 6
"""

import argparse
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

from config import DEFAULT_REGION, make_dirs
import region as region_lib


def fetch_single_huc8_flowlines(huc8: str) -> gpd.GeoDataFrame:
    """Original, direct single-HUC8 path (no tiling) -- unchanged behavior."""
    return region_lib.fetch_flowlines_for_huc8(huc8)


def build_topology(flowlines: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Run pynhd's prepare_nhdplus on one already-bounded set of flowlines."""
    import pynhd

    return pynhd.prepare_nhdplus(
        flowlines,
        min_network_size=0,
        min_path_length=0,
        min_path_size=0,
        purge_non_dendritic=False,
    )


def build_topology_per_huc2(flowlines: gpd.GeoDataFrame, checkpoint_dir: Path) -> gpd.GeoDataFrame:
    """
    CHANGED: run prepare_nhdplus per HUC2 instead of once over the whole
    (potentially CONUS-sized) flowline set. Each HUC2's flowlines are
    identified by their comid's first-2-digit reach code prefix when
    available, falling back to a spatial join against wbd02 if not.
    Results are checkpointed so a resumed run skips HUC2s already done.
    """
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    if "reachcode" in flowlines.columns:
        huc2_of_reach = flowlines["reachcode"].astype(str).str[:2]
    else:
        # Fallback: spatial join against wbd02 boundaries. Slower (one
        # spatial join over the whole region) but correct regardless of
        # which attribute columns this pynhd version's flowlines carry.
        from pynhd import WaterData

        print("  'reachcode' column not found -- falling back to a spatial join "
              "against wbd02 to assign each flowline to a HUC2 (one-time cost).")
        wd2 = WaterData("wbd02")
        huc2_polys = wd2.bygeom(flowlines.total_bounds, geo_crs=flowlines.crs)
        joined = gpd.sjoin(flowlines, huc2_polys[["huc2", "geometry"]], how="left", predicate="intersects")
        # A flowline straddling a HUC2 seam can match more than one polygon;
        # keep its first match so each flowline gets exactly one HUC2 label.
        joined = joined[~joined.index.duplicated(keep="first")]
        huc2_of_reach = joined["huc2"].reindex(flowlines.index)

    huc2_codes = sorted(huc2_of_reach.dropna().unique().tolist())
    print(f"  Flowlines span {len(huc2_codes)} HUC2 unit(s): {huc2_codes}")

    prepared_parts = []
    for i, huc2 in enumerate(huc2_codes, start=1):
        nodes_ckpt = checkpoint_dir / f"{huc2}_prepared.parquet"
        if nodes_ckpt.exists():
            print(f"  [{i}/{len(huc2_codes)}] HUC2 {huc2}: checkpoint found, loading")
            prepared_parts.append(gpd.read_parquet(nodes_ckpt))
            continue

        subset = flowlines.loc[huc2_of_reach == huc2]
        print(f"  [{i}/{len(huc2_codes)}] HUC2 {huc2}: preparing topology for "
              f"{len(subset)} flowline(s) ...")
        try:
            prepared_subset = build_topology(subset)
        except Exception as e:
            print(f"  [{i}/{len(huc2_codes)}] HUC2 {huc2} FAILED: {e} -- re-run to retry "
                  f"just this HUC2 (already-done HUC2s are checkpointed).", file=sys.stderr)
            continue

        prepared_subset.to_parquet(nodes_ckpt)
        prepared_parts.append(prepared_subset)
        print(f"  [{i}/{len(huc2_codes)}] HUC2 {huc2}: wrote checkpoint "
              f"({len(prepared_subset)} flowlines)")

    if not prepared_parts:
        raise RuntimeError("No HUC2 unit's topology preparation succeeded -- nothing to write.")

    return gpd.GeoDataFrame(pd.concat(prepared_parts, ignore_index=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--outdir", default="./output", help="Output root directory")
    parser.add_argument("--region", default=DEFAULT_REGION,
                         help=f"HUC8/10/12 code, HUC2/4/6 code, or 'CONUS' (default: {DEFAULT_REGION})")
    parser.add_argument("--tile-workers", type=int, default=4,
                         help="Concurrent HUC8 tile fetches for HUC2/4/6/CONUS regions (default: 4). "
                              "Keep modest -- this hits a shared public web service.")
    parser.add_argument("--no-per-huc2-topology", action="store_true",
                         help="Fall back to the original single whole-region prepare_nhdplus call "
                              "instead of per-HUC2 chunking. Only reasonable up to HUC2 scale -- "
                              "NOT recommended for CONUS.")
    args = parser.parse_args()
    region = args.region

    dirs = make_dirs(args.outdir)

    is_single_basin = region.isdigit() and region_lib.huc_level(region) in (8, 10, 12) if region.isdigit() else False

    if is_single_basin:
        print(f"Fetching HUC{region_lib.huc_level(region)} boundary for {region} ...")
        basin = region_lib.fetch_boundary_for_code(region)
        basin_path = dirs["basin"] / f"{region}_boundary.geojson"
        basin.to_file(basin_path, driver="GeoJSON")
        print(f"  Wrote {basin_path}")

        print("Fetching NHDPlus V2 flowlines within the basin ...")
        flowlines = fetch_single_huc8_flowlines(region)
        print(f"  Retrieved {len(flowlines)} raw flowlines")
    else:
        level_desc = "CONUS" if region_lib.is_conus(region) else f"HUC{region_lib.huc_level(region)} region {region}"
        print(f"Region {level_desc} is larger than one HUC8 -- tiling into HUC8 units.")
        print("NOTE: skipping a single boundary.geojson for this region (tiling makes a "
              "single dissolved polygon an extra, non-essential step).")

        flowlines = region_lib.fetch_flowlines_tiled(
            region, tile_cache_dir=dirs["graph_tiles"], max_workers=args.tile_workers
        )

    print("Deriving network topology (tocomid) ...")
    if is_single_basin or args.no_per_huc2_topology:
        prepared = build_topology(flowlines)
    else:
        checkpoint_dir = dirs["graph"] / "_topology_checkpoints"
        prepared = build_topology_per_huc2(flowlines, checkpoint_dir)
    print(f"  {len(prepared)} flowlines after topology preparation")

    # --- Write flowlines (with geometry) ---
    # CHANGED: GeoParquet for multi-tile regions -- much faster to write
    # and re-read than GeoJSON at millions-of-features scale. Single-basin
    # runs keep GeoJSON for simplicity/portability at that small scale.
    if is_single_basin:
        flowlines_path = dirs["graph"] / f"{region}_flowlines.geojson"
        prepared.to_file(flowlines_path, driver="GeoJSON")
    else:
        flowlines_path = dirs["graph"] / f"{region}_flowlines.parquet"
        prepared.to_parquet(flowlines_path)
    print(f"  Wrote {flowlines_path}")

    # --- Node table (tabular attributes, no geometry) ---
    node_cols = [c for c in ["comid", "lengthkm", "streamorde", "slope", "areasqkm", "gnis_name"] if c in prepared.columns]
    nodes = prepared[node_cols].drop_duplicates(subset="comid").reset_index(drop=True)
    nodes_path = dirs["graph"] / f"{region}_nodes.parquet"
    nodes.to_parquet(nodes_path, index=False)
    print(f"  Wrote {nodes_path} ({len(nodes)} nodes)")

    # --- Edge table (comid -> tocomid) ---
    if "tocomid" not in prepared.columns:
        print("WARNING: 'tocomid' column not found after prepare_nhdplus; "
              "check the installed pynhd version's output schema.", file=sys.stderr)
        edges = pd.DataFrame(columns=["comid", "tocomid"])
    else:
        edges = prepared[["comid", "tocomid"]].dropna().reset_index(drop=True)
    edges_path = dirs["graph"] / f"{region}_edges.parquet"
    edges.to_parquet(edges_path, index=False)
    print(f"  Wrote {edges_path} ({len(edges)} edges)")

    # Identify outlet(s): a reach is an outlet if EITHER
    #   (a) it has no outgoing edge at all, or
    #   (b) its tocomid points to a COMID outside this region's node set
    # (This is unchanged -- and is exactly what correctly absorbs the rare
    # cross-HUC2 mainstem case created by running topology per-HUC2 above:
    # such a reach's tocomid points outside its own HUC2's node set, so it
    # now shows up as an "outlet" of that HUC2, same as a true basin
    # outlet would. That's expected and fine at the HUC2 seam.)
    node_set = set(nodes["comid"])
    has_outgoing_edge = set(edges["comid"])
    no_outgoing_edge = sorted(node_set - has_outgoing_edge)
    points_outside_region = edges.loc[~edges["tocomid"].isin(node_set), "comid"].tolist()
    outlets = sorted(set(no_outgoing_edge) | set(points_outside_region))
    print(f"  Outlet reach(es): {len(outlets)} found")
    if not is_single_basin and len(outlets) > 20:
        print(f"  (not printing all {len(outlets)} -- expected in a large multi-basin region)")
    else:
        print(f"  {outlets}")
    if len(outlets) > 1 and is_single_basin:
        print(f"  NOTE: {len(outlets)} outlet candidates found for a single basin -- usually 1. "
              f"Check each candidate's streamorde/areasqkm in {region}_nodes.parquet before "
              f"trusting the graph structure downstream.")

    print(f"Done. Feed graph/{region}_nodes.parquet and _edges.parquet's 'comid' "
          "values as feature_id into steps 02-04.")


if __name__ == "__main__":
    main()

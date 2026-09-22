# CONUS-scale optimization pass — what changed and why

This is a full pass over every script in the pipeline, fixing the issues
found in a scale review (summarized below by severity). Read this before
running anything at `--region CONUS` for the first time.

## New shared files

- **`pipeline_utils.py`** — bounded thread-pool helper (`parallel_map`),
  OData pagination fix (`paginated_odata_get`), and a shared
  retry-with-backoff (`retry_with_backoff`), used across most scripts
  below instead of duplicating the same patterns six different ways.
- **`region.py`** — **reconstructed, not the original.** This module was
  referenced by `01`, `08`, `09`, `10`, `13` but was not part of what was
  uploaded to me. I've written a working implementation of the interface
  those scripts expect, with a prominent warning at the top of the file.
  **If your team's actual `region.py` exists, use that instead** — mine
  is unverified against the real WBD service and doesn't reproduce the
  specific "68 raw tiles → 32 genuine" filtering fix the README
  describes, only my own best-effort version of it.

## Fixes that address something that would actually break at CONUS scale

- **`02_download_streamflow.py`, `03_download_groundwater.py`** — output
  is now one file per `(year, node-chunk)` instead of one monolithic file
  per year. The original held every chunk's data in memory before a
  single write and produced a single-file-per-year output that would run
  into hundreds of GB at CONUS scale. Chunk loads within a year now run
  with bounded concurrency (default 4 workers).
- **`04_download_forcing_era5land.py`, `04_download_forcing.py`** — the
  actual CONUS-breaking bug: the original built one table of
  `hours_in_year × total_reaches` rows before writing — ~23.7 billion rows
  for one year at CONUS scale. Output is now built and written per
  node-chunk. The grid load itself (bounded by cell count, not reach
  count) is unchanged in spirit but its memory estimate is now correct
  (previously undercounted ~8x by only accounting for one variable while
  the code loaded all of them together). `--time-chunk-months` lets you
  split the grid load further if needed. Catchment fetching is now
  threaded.
- **`cdse_common.py`** — `search_cdse_catalog()` now paginates fully via
  `@odata.nextLink` instead of a flat `$top=1000` that silently truncated
  any query matching more than 1,000 products, with no warning. This is
  used by `16` and `17`.
- **`01_build_graph.py`** — `prepare_nhdplus` now runs per-HUC2 instead of
  once over the entire concatenated region, with per-HUC2 checkpointing
  (resumable). Multi-tile regions now write flowlines as GeoParquet
  instead of GeoJSON (every other script reads this file back in, so this
  is a repeated cost, not a one-time one).

## Fixes that address real but survivable slowness (serial → bounded concurrency)

- **`05`–`10`, `13`–`18`** consistency fix: any script that reads back
  `{region}_flowlines.geojson` now also checks for `.parquet` first, since
  `01_build_graph.py` writes parquet for CONUS-scale multi-tile regions.
  Without this, those scripts would fail outright on a CONUS run.
- **`06_download_nwis_iv.py`, `07_download_nwis_groundwater.py`** —
  site-batch fetches within a year are now threaded (default 4 workers).
- **`08_download_nhdplus_hr.py`** — per-HU4 fetches now threaded (default 4).
- **`09_download_3dep.py`** — per-HUC8 tile fetches now threaded (default 4).
- **`10_download_parflow_conus2.py`** — the (HUC8, year) loop now threaded
  (default 4).
- **`11_download_globgm.py`, `15_download_copernicus_dem.py`,
  `16_download_sentinel1_grd.py`, `17_download_sentinel3_sral.py`** —
  per-file downloads now threaded (4–8 workers depending on file size).
- **`14_download_smap_l4.py`, `18_download_sentinel6.py`** —
  `earthaccess.download()`'s own internal `threads` parameter is now
  exposed and set explicitly instead of left at its default.
- **`region.py`'s `fetch_flowlines_tiled`** — HUC8 tile fetches for a
  HUC2/4/6/CONUS region now run concurrently (default 4 workers) instead
  of serially.

## Deliberately left unchanged

- **`12_download_grfr.py`** — Globus's own transfer engine already
  handles CONUS-scale movement; this script only submits/polls a task.
  Nothing to fix here.
- **`13_download_merit_hydro.py`** — gated behind a manual Google
  Form/Dropbox step regardless of region size; only the flowlines-file
  consistency fix above applies.
- **`05_download_camelsh.py`** — the ~57GB Zenodo archive download is a
  one-time, region-size-independent cost; only the flowlines-file
  consistency fix above applies.

## What I could not verify from this environment

Same caveat as the original pipeline: several of these fixes (chunk
alignment against the CHRTOUT/GWOUT Zarr stores' real native chunk sizes,
the exact behavior of `earthaccess.download(threads=...)` across
versions, CDSE's OData `@odata.nextLink` field name) were written against
documented/expected behavior, not confirmed against a live run — the
same "run `--inspect` first, always" discipline the original pipeline
already established still applies here.

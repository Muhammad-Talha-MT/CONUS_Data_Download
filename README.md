# CONUS Hydrologic Graph Dataset — Download Pipeline

A complete, resumable pipeline for building a graph-structured, reach-level
hydrologic dataset: river network topology, dense physics-model streamflow/
groundwater, dense meteorological forcing, sparse real-gauge fine-tuning
targets, and the broader RED/ORANGE/YELLOW/WHITE-tier data catalog behind
the team's foundation-model plans.

**This one file replaces the previous README.md + SETUP.md split.**
Everything a new team member needs — what this is, how to install it, how
to run it, what order to run it in, and where things stand — is here, in
the order you'll actually use it.

Built and validated at HUC8 (French Broad River, `06010105`) on Bridges-2
as a GNN-baseline proof of concept. CONUS-scale itself has not been run
end-to-end — read "What's validated vs. extrapolated" (§7) before assuming
anything runs unmodified at full national scale.

---

## Table of contents

1. [What this repo does](#1-what-this-repo-does)
2. [Repo contents](#2-repo-contents)
3. [Data priority order (Red / Orange / Yellow / White)](#3-data-priority-order-red--orange--yellow--white)
4. [Install & credentials](#4-install--credentials)
5. [Step-by-step: validate on a small basin first](#5-step-by-step-validate-on-a-small-basin-first)
6. [Step-by-step: scale up to CONUS](#6-step-by-step-scale-up-to-conus)
7. [What's validated vs. extrapolated](#7-whats-validated-vs-extrapolated)
8. [What changed to make this CONUS-safe](#8-what-changed-to-make-this-conus-safe)
9. [Running this at OLCF (Frontier)](#9-running-this-at-olcf-frontier)
10. [Known, unresolved limitations](#10-known-unresolved-limitations)
11. [Troubleshooting & getting help](#11-troubleshooting--getting-help)

---

## 1. What this repo does

Fetches and organizes the data behind two kinds of work the team is doing:

- **Basin/HUC2-scale GNN baseline models** (already proven — see the
  French Broad HUC8 proof of concept).
- **CONUS-scale multimodal foundation models** for streamflow prediction —
  hourly, mixed-temporal-resolution, and eventually multi-task — per
  Vinh's data-modality plan (spatiotemporal grid/satellite data, time
  series, and graph data).

The pipeline is organized as **18 numbered download scripts** (`01`
through `18`), a shared `config.py` of constants, a shared `region.py` for
resolving HUC codes into fetchable tiles, and a shared `pipeline_utils.py`
of common helpers (bounded-concurrency downloads, pagination, retry).
Every script is **independently resumable** — re-running the same command
picks up wherever it left off, at whatever granularity (year, tile,
node-chunk) that script writes at.

`01_build_graph.py` is the one step every other script depends on: it
builds the river-network graph and writes the node/edge tables that give
every other script its list of reaches (COMIDs) or region extent to fetch
against.

## 2. Repo contents

```
config.py              -- shared constants (paths, dataset IDs, credentials env vars, tuning knobs)
region.py               -- resolves a HUC code (or "CONUS") into fetchable HUC8 tiles
pipeline_utils.py        -- shared bounded thread-pool downloader, OData pagination, retry-with-backoff
cdse_common.py           -- shared Copernicus Data Space Ecosystem (CDSE) S3 + catalog-search helper

01_build_graph.py                 -- river network topology (any HUC8/HUC2/HUC4/HUC6/CONUS)      [core]
02_download_streamflow.py         -- NWM v3.0 streamflow, dense, every reach                       [Red]
03_download_groundwater.py        -- NWM v3.0 groundwater, dense, every reach                      [Red]
04_download_forcing_era5land.py   -- ERA5-Land forcing, dense, every reach (RECOMMENDED)            [Red]
04_download_forcing.py            -- NLDAS-2 forcing, dense, every reach (alternate, slower)        [Orange]
05_download_camelsh.py            -- CAMELSH sparse fine-tuning gauges                              [Yellow]
06_download_nwis_iv.py            -- NWIS surface-water fine-tuning gauges (RECOMMENDED)             [Red]
07_download_nwis_groundwater.py   -- NWIS groundwater-level observations                            [Red]
08_download_nhdplus_hr.py         -- NHDPlus High Resolution hydrography                             [Red]
09_download_3dep.py               -- USGS 3DEP terrain (DEM)                                         [Red]
10_download_parflow_conus2.py     -- ParFlow-CONUS2 / HydroFrame groundwater simulation              [Red]
11_download_globgm.py             -- GLOBGM v1.0 global groundwater model                            [Red]
12_download_grfr.py               -- GRFR (VIC-RAPID) reach-level flood reanalysis, via Globus       [Red]
13_download_merit_hydro.py        -- MERIT Hydro hydrography (tile list only -- see section 3)        [Red]
14_download_smap_l4.py            -- SMAP L4 soil moisture                                          [Yellow]
15_download_copernicus_dem.py     -- Copernicus DEM GLO-30 terrain                                  [Yellow]
16_download_sentinel1_grd.py      -- Sentinel-1 GRD SAR (inundation/soil moisture proxy)             [Yellow]
17_download_sentinel3_sral.py     -- Sentinel-3 SRAL radar altimetry (river/lake stage)              [Yellow]
18_download_sentinel6.py          -- Sentinel-6 radar altimetry (primarily ocean)                    [Yellow]

CONUS_OPTIMIZATION_CHANGES.md     -- file-by-file changelog of the CONUS-scale fixes in this version
```

Tags in brackets are each script's tier per §3.

## 3. Data priority order (Red / Orange / Yellow / White)

Vinh's team maintains a master catalog (`Data_1.xlsx`, 50 datasets) with a
priority ranking encoded as **row color** in that spreadsheet: Red →
Orange → Yellow → White, in that order of importance. This repo currently
implements exactly the union of Red + Orange + Yellow — **17 of the 50
catalog datasets** — which is every dataset Vinh flagged as important.
White (the remaining 33) has no download script yet.

| Tier | Count | Scripted in this repo | Datasets |
|---|---|---|---|
| **Red** | 10 | **10 / 10** | ERA5-Land, NWM Retrospective v3.0, NWIS surface-water, NWIS groundwater, MERIT Hydro*, NHDPlus HR, USGS 3DEP, ParFlow-CONUS2/HydroFrame, GLOBGM v1.0, GRFR |
| **Orange** | 1 | **1 / 1** | NLDAS-2 forcing |
| **Yellow** | 6 | **6 / 6** | SMAP L4, Sentinel-1 GRD, Sentinel-3 SRAL, Sentinel-6, CAMELSH, Copernicus DEM GLO-30 |
| **White** | 33 | **0 / 33** | Everything else in the catalog — AORC, CONUS404, NLDAS-3, GLDAS, Daymet, GEFS, HRRR, NISAR, ALOS-2 PALSAR, SWOT KaRIn, GRACE/GRACE-FO, GRDC, NGWMN, IGRAC, FLUXNET, AmeriFlux, NEON, Caravan, Caravan MultiMet, HYSETS, CAMELS-US/GB/DE/BR/CL/AUS/CH/DK/FR/IND/SE, LamaH-CE, LamaH-Ice, GSIM, HydroATLAS, HAND, SoilGrids, GloFAS, LISFLOOD |

\* MERIT Hydro is **partial**: `13_download_merit_hydro.py` correctly
computes the exact tiles you need, but the actual file transfer requires a
one-time manual step (Google Form registration + password-protected
Dropbox) that cannot be scripted — see that script's own docstring.

**Run order:** go through scripts `01` → `18` first (Red, then Orange,
then Yellow, in that priority order — the script numbering already
reflects this). Once all 17 are done for your target region, White-tier
datasets are the next thing to build scripts for; none of them exist here
yet.

### Temporal-resolution note (from Vinh's modeling plan)

Not every dataset here has hourly resolution, which matters for which
model you're training:

- **Hourly model:** NWM streamflow/groundwater (Red), ERA5-Land / NLDAS-2
  forcing (Red/Orange), NWIS IV (Red) are hourly or resamplable to hourly.
  Exclude the satellite products (SMAP L4 is 3-hourly; Sentinel-1/3/6 have
  multi-day revisit).
- **3-hour model:** the above, plus SMAP L4.
- **Daily model:** the above, plus most of what remains in Yellow, and
  more of White becomes usable (Daymet, GRACE, etc. are daily-or-coarser).
- **Mixed-resolution model:** everything scripted here can be included,
  at each dataset's native cadence.

## 4. Install & credentials

```bash
pip install pynhd geopandas pandas pyarrow xarray zarr s3fs scipy shapely pyproj \
            earthaccess fsspec aiohttp requests py7zr numpy \
            py3dep hf_hydrodata globus-sdk boto3 beautifulsoup4
```

Six data providers require a free account; one (GLOBGM) needs nothing.
Set all of these up **before** running anything at scale, so you're not
discovering a missing credential mid-run:

| Provider | Used by | Setup |
|---|---|---|
| **NASA Earthdata** | NLDAS-2 (`04`), SMAP L4 (`14`), Sentinel-6 (`18`) | Register: https://urs.earthdata.nasa.gov/ — then **authorize "NASA GESDISC DATA ARCHIVE"** under your account's Applications settings (easy to miss, causes silent `403`s if skipped). Set via `~/.netrc`, `EARTHDATA_USERNAME`/`EARTHDATA_PASSWORD`, or `earthaccess.login()` interactively once. |
| **DestinE Earth Data Hub (EDH)** | ERA5-Land (`04_..._era5land.py`) | Register: https://earthdatahub.destine.eu/ — get a token, `export EDH_TOKEN=<token>` |
| **HydroFrame** | ParFlow-CONUS2 (`10`) | Create an account + register your API PIN — see "Creating a HydroFrame API Account" in the hf_hydrodata docs: https://hf-hydrodata.readthedocs.io/ |
| **Globus** | GRFR (`12`) | Most institutions (incl. national labs) already provide a Globus identity — confirm at https://app.globus.org. Register your own native-app OAuth client at https://app.globus.org/settings/developers (don't reuse someone else's) and set it via `config.GRFR_GLOBUS_CLIENT_ID` or `--client-id`. Run `python 12_download_grfr.py --login` once. |
| **Copernicus Data Space Ecosystem (CDSE)** | Sentinel-1 (`16`), Sentinel-3 (`17`) | Register: https://dataspace.copernicus.eu/ — generate **S3 credentials** (separate from your login password), `export CDSE_S3_ACCESS_KEY=...` and `export CDSE_S3_SECRET_KEY=...` |
| **MERIT Hydro** | `13` | **Cannot be fully automated** — see §3. Register at http://hydro.iis.u-tokyo.ac.jp/~yamadai/MERIT_Hydro/, get the emailed password, plan for a one-time manual download. |
| *(none needed)* | NWM, NHDPlus V2 + HR, USGS 3DEP, NWIS (surface-water + groundwater), CAMELSH, Copernicus DEM GLO-30, GLOBGM | Fully public. |

## 5. Step-by-step: validate on a small basin first

**Always do this before touching `--region CONUS`.** Use French Broad
(HUC8 `06010105`) — small, fast, and already the basin the GNN baseline
was proven on:

```bash
# 1. Build the graph first -- everything else depends on it
python 01_build_graph.py --outdir ./test_output --region 06010105

# 2. Core Red-tier pipeline
python 02_download_streamflow.py --outdir ./test_output --region 06010105
python 03_download_groundwater.py --outdir ./test_output --region 06010105
python 04_download_forcing_era5land.py --inspect
python 04_download_forcing_era5land.py --outdir ./test_output --region 06010105
python 06_download_nwis_iv.py --outdir ./test_output --region 06010105
python 07_download_nwis_groundwater.py --outdir ./test_output --region 06010105
python 08_download_nhdplus_hr.py --outdir ./test_output --region 06010105
python 09_download_3dep.py --outdir ./test_output --region 06010105
python 10_download_parflow_conus2.py --inspect
python 11_download_globgm.py --inspect
python 12_download_grfr.py --login
python 12_download_grfr.py --inspect --source discharge
python 13_download_merit_hydro.py --list-tiles --outdir ./test_output --region 06010105

# 3. Orange/Yellow tier
python 04_download_forcing.py --inspect
python 04_download_forcing.py --outdir ./test_output --region 06010105
python 05_download_camelsh.py --outdir ./test_output --region 06010105
python 14_download_smap_l4.py --inspect
python 15_download_copernicus_dem.py --outdir ./test_output --region 06010105
python 16_download_sentinel1_grd.py --inspect --region 06010105 --start 2023-01-01 --end 2023-01-31
python 17_download_sentinel3_sral.py --inspect --region 06010105 --start 2023-01-01 --end 2023-01-31
python 18_download_sentinel6.py --inspect
```

**Every script with an `--inspect` mode should be run that way first,
always**, before a real download — this project has repeatedly found
assumed API shapes/filenames turn out wrong in practice once actually
tested (NLDAS-2's temporal window, ERA5-Land's longitude convention, and
CAMELSH's real Zenodo archive filename all had to be corrected this way).

Confirm each script produces real, sane output — non-empty files,
sensible row/tile counts, no silent zero-coverage results — before
scaling any of them up.

## 6. Step-by-step: scale up to CONUS

Don't run everything at once. Work through datasets roughly in this order
— highest confidence first, so real problems surface on the easier
sources before you're deep into the harder ones:

1. **`01_build_graph.py --region CONUS`** — this is the one every other
   script depends on; get it fully right before anything else.
   ```bash
   python 01_build_graph.py --outdir ./output --region CONUS --tile-workers 6
   ```
2. **`02`, `03`** — NWM streamflow/groundwater, already validated at HUC2 scale:
   ```bash
   python 02_download_streamflow.py --outdir ./output --region CONUS --node-chunk-size 5000 --max-workers 4
   python 03_download_groundwater.py --outdir ./output --region CONUS --node-chunk-size 5000 --max-workers 4
   ```
3. **`04_download_forcing_era5land.py`** — validated core logic, CONUS-safe output chunking:
   ```bash
   python 04_download_forcing_era5land.py --outdir ./output --region CONUS --node-chunk-size 5000
   ```
4. **`06`, `07`** — NWIS surface-water/groundwater:
   ```bash
   python 06_download_nwis_iv.py --outdir ./output --region CONUS --max-sites 5000
   python 07_download_nwis_groundwater.py --outdir ./output --region CONUS --max-sites 5000
   ```
5. **`15_download_copernicus_dem.py`** — simplest new source, no credentials:
   ```bash
   python 15_download_copernicus_dem.py --outdir ./output --region CONUS --download-workers 8
   ```
6. **`08`, `09`** — same HyRiver stack, moderate confidence.
7. **`14`, `18`** — same earthaccess pattern as NLDAS-2.
8. **`10`, `16`, `17`, `11`, `12`** — newest access patterns, least tested.
9. **`13`** — plan for a manual step regardless of order.

Every step is **resumable at fine granularity** — see §8 for exactly what
"resumable" means per script now (year+node-chunk for `02`–`04`, per-HUC2
for `01`, per-file for the rest). An interrupted run just needs the same
command re-run.

## 7. What's validated vs. extrapolated

**Validated by real runs:**
- `01_build_graph.py`'s tiling mechanism, at HUC2 scale (Tennessee River
  Basin, 68 raw HUC8 tiles from the spatial query filtered down to 32
  genuine ones).
- `02`/`03` at ~59,000-reach (HUC2) scale.
- `04_download_forcing_era5land.py`'s core logic (catchment-to-cell
  mapping, longitude handling, retry logic) at HUC8 scale.
- `06_download_nwis_iv.py`'s auto-discovery and fetch logic at HUC8 scale
  (15-gauge French Broad test).
- The GNN baseline model itself, trained on the French Broad HUC8 graph
  on Bridges-2 — performed well, including on ungauged basins, and showed
  meaningful river-connectivity structure between nodes.

**Not yet run at full CONUS scale — go in expecting to debug:**
- `01_build_graph.py --region CONUS` tiles across all ~2,264 HUC8 units
  nationally — expect a genuinely long, monitored batch job.
- Total storage footprint at CONUS scale — extrapolate roughly (~20x
  French Broad for HUC2 Tennessee, another ~15–20x for CONUS) but treat
  as order-of-magnitude, not firm.
- The catchment-to-cell spatial matching in the forcing scripts, run
  against ~2.7M reaches' catchment polygons at once.
- The CONUS-scale memory/output fixes described in §8 — architecturally
  sound, but not yet confirmed against a live multi-day CONUS run.

## 8. What changed to make this CONUS-safe

The pipeline was rewritten in a CONUS-optimization pass after a scale
review found several scripts would OOM or silently truncate results well
before finishing a full national run. Full file-by-file detail is in
`CONUS_OPTIMIZATION_CHANGES.md`; the summary:

- **`02`/`03`/`04` (both forcing scripts):** the original built one giant
  in-memory table (and, for `02`/`03`, one monolithic output file) per
  year across *all* reaches in the region — at CONUS scale
  (~2.7M reaches) this was on the order of tens of billions of rows held
  in memory before a single write. Output is now written **per
  (year, node-chunk)** instead, bounding memory to one chunk's footprint
  and making resumability chunk-level instead of year-level.
- **`01_build_graph.py`:** topology preparation (`prepare_nhdplus`) now
  runs **per HUC2** with checkpointing, instead of one unresumable call
  over the entire concatenated region. Multi-tile regions now write
  flowlines as GeoParquet instead of GeoJSON (faster to write and
  re-read at millions-of-features scale).
- **`cdse_common.py`:** CDSE catalog search now paginates fully via
  `@odata.nextLink` instead of a flat `$top=1000` that silently truncated
  any query matching more than 1,000 products (Sentinel-1/3 scripts).
- **`06`–`10`, `15`–`18`:** independent per-item loops (site batches,
  HU4/HUC8 tiles, product files) now run with bounded thread-pool
  concurrency (`pipeline_utils.parallel_map`, typically 4–8 workers)
  instead of strictly serially.
- **`region.py` is a reconstruction, not the team's original.** It wasn't
  available when this pass was done, so its tiling/filtering logic was
  rebuilt from the interface the other scripts expect — **if the team's
  actual `region.py` exists, use that instead** and treat this one as a
  stopgap.

## 9. Running this at OLCF (Frontier)

This pipeline was built and tested on PSC's Bridges-2, not Frontier, and
one Frontier-specific fact matters more than any tuning knob in this
repo: **Frontier's compute (and login) nodes have no direct internet
access.** Every script here needs outbound internet (S3, NASA Earthdata,
Zenodo, CDSE, USGS, Globus), so none of them can run as a Frontier batch
job.

**Run the download steps on OLCF's Data Transfer Nodes (DTNs) instead:**
`dtn.ccs.ornl.gov`. DTNs are purpose-built for exactly this — external
data transfer, plus a Slurm batch queue (1–4 nodes, up to 24h, one job
per user) — and they mount the same Orion filesystem Frontier compute
nodes read from. This also fits how the pipeline is actually built: its
thread pools (4–8 workers per script) were sized for a single node's
politeness to shared public services, not for Frontier's ~9,400-node
scale, so a DTN node is a better match than the GPU compute partition
would be even if that partition had internet access.

Two OLCF-specific things this pipeline doesn't yet account for:
- **Orion is short-term storage — no backup, purged after 90 days.** A
  full CONUS download can plausibly take weeks; plan to move finished
  output to HPSS archive or a longer-term area before that window closes.
- **Lustre and file count.** The chunked-output design here (thousands of
  files across `02`–`04` at CONUS scale) is a reasonable middle ground,
  but check OLCF's Lustre striping guidance for many-small-file workloads
  before a full run.

The right split is: **DTNs for the download pipeline in this repo,
Frontier's GPU allocation for actual model training** on the data once
it's landed on Orion — that's also the split OLCF's own docs point
toward, not a workaround specific to this project.

Job scheduler specifics (queue/partition names, walltime limits, QOS)
should be confirmed against Frontier's own current documentation or
`help@olcf.ornl.gov` rather than assumed from Bridges-2's constraints
(e.g., Bridges-2's 48-hour GPU-shared cap doesn't necessarily apply here).

## 10. Known, unresolved limitations

- **No H (channel depth) or S (channel storage) target exists in this
  pipeline.** A three-variable (Q, H, S) formulation isn't achievable
  with these sources without a hand-built Manning's-equation
  approximation from NWM's channel geometry parameters. This pipeline
  fully supports a Q-only (streamflow) approach.
- **CAMELSH coverage is basin-dependent and not reliably rich** — its
  hourly curation showed near-zero coverage for French Broad specifically
  (17 of 18 matched gauges had 0 data-availability hours), while the same
  gauges have real, decades-long records directly in NWIS.
  `06_download_nwis_iv.py` is the more dependable sparse-gauge source
  found so far; this may or may not hold for every basin.
- **The nearest-cell forcing aggregation is a real approximation, not
  exact area-weighting.** At ERA5-Land's ~9km / NLDAS-2's ~12km
  resolution vs. typical NHDPlus catchments of a few km², multiple small
  catchments legitimately share one coarse cell's value. Deliberate,
  documented design choice — worth knowing if exact sub-pixel weighting
  ever becomes a requirement.
- **33 White-tier datasets have no script yet** (§3) — next up once
  Red/Orange/Yellow are confirmed working at CONUS scale.
- **`region.py` is unverified** (§8) — confirm against the team's real
  module if one exists.

## 11. Troubleshooting & getting help

- **Always run `--inspect` first** on any script that has it — this has
  caught every real API/filename mismatch found in this project so far.
- **A dataset returning fewer rows/tiles than expected** is usually a
  filter or convention mismatch (longitude 0–360 vs. −180–180, a
  short-lived HTTP error, a stale endpoint UUID) — check that script's
  own printed warnings first; most already explain the likely cause.
- **Credential errors** are almost always a missing or unauthorized
  account (see §4) — in particular, the NASA Earthdata "authorize
  GESDISC" step is the single most common thing to miss.
- Reach out to Talha directly for anything not covered above, or open an
  issue on this repo.

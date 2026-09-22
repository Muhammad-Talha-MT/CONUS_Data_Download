# CONUS Hydrologic Graph Dataset — Download Pipeline

A complete, resumable pipeline for building a graph-structured, reach-level
hydrologic dataset at CONUS scale: river network topology, dense
physics-model streamflow/groundwater, dense meteorological forcing, and
sparse real-gauge fine-tuning targets. Built and validated at HUC8 (French
Broad River, 06010105) and HUC2 (Tennessee River Basin, "06") scale before
this handoff; **CONUS-scale itself has not been run end-to-end** — see
"What's validated vs. extrapolated" below before assuming this just works
unmodified at full national scale.

## What this maps to in the team's dataset catalog (`Data.xlsx`)

| Catalog # | Dataset | Role here |
|---|---|---|
| 41 | NHDPlus (medium-res V2, via live web service — not the High-Resolution product listed) | River network topology (`01_build_graph.py`) |
| 10 | NWM Retrospective v3.0 | Dense streamflow + groundwater, every reach (`02`, `03`) |
| 1 | ERA5-Land | **Recommended** dense forcing (`04_download_forcing_era5land.py`) |
| 4 | NLDAS-2 forcing | Alternate dense forcing (`04_download_forcing.py`) |
| 27 | CAMELSH | Optional sparse fine-tuning gauges (`05_download_camelsh.py`) — see caveat below |
| 19 | USGS NWIS surface-water observations | **Recommended** sparse fine-tuning gauges (`06_download_nwis_iv.py`) |

The other ~45 entries in the catalog (SWOT, GRACE, SAR products, soil
moisture, groundwater simulation models, etc.) are **not** covered by this
pipeline — they represent other candidate research directions the team has
catalogued, not something this codebase attempts.

## Why ERA5-Land over NLDAS-2, and NWIS over CAMELSH

Both decisions came from real problems found during basin-scale testing,
not a priori preference:

- **ERA5-Land over NLDAS-2**: NLDAS-2 is distributed one file per hour
  (~385,000 individual downloads for the full record) via NASA GES DISC —
  confirmed to be the dominant bottleneck in this whole pipeline, taking
  many hours per year even for a single small basin. ERA5-Land, via
  DestinE's Zarr-backed store, allows one lazy dataset open plus a direct
  time/space slice — a full basin's multi-year forcing fetched in what
  NLDAS-2 needed for a single year. Both are independent of the NWM v3.0
  streamflow target (v3.0 runs on AORC, neither of these), so that
  property is preserved either way.
- **NWIS over CAMELSH**: CAMELSH's hourly curation showed near-zero
  coverage for French Broad specifically — 17 of 18 CAMELSH-matched
  gauges had 0 data-availability hours. Checking those same gauges
  directly against USGS NWIS found 12 of 15 have 25–41 years of
  continuous real sub-daily data. CAMELSH's curation process had excluded
  data that genuinely exists. This may or may not hold for every basin —
  `05_download_camelsh.py` is kept in this pipeline as an option, but
  `06_download_nwis_iv.py` is the one actually validated to work well.

## Directory layout

```
config.py, region.py              -- shared settings and region-resolution logic
01_build_graph.py                 -- river network topology (any HUC8/HUC2/HUC4/HUC6/CONUS)
02_download_streamflow.py         -- NWM v3.0 streamflow, dense, every reach
03_download_groundwater.py        -- NWM v3.0 groundwater, dense, every reach (optional/auxiliary)
04_download_forcing_era5land.py   -- ERA5-Land forcing, dense, every reach (RECOMMENDED)
04_download_forcing.py            -- NLDAS-2 forcing, dense, every reach (alternate, slower)
05_download_camelsh.py            -- CAMELSH sparse gauges (optional, see caveat above)
06_download_nwis_iv.py            -- NWIS sparse gauges (RECOMMENDED)

basin_scale_alternates/           -- NOT for CONUS -- see note below
    04a_download_raw_forcing.py           (NLDAS-2, raw-cache split design)
    04a_download_raw_forcing_era5land.py  (ERA5-Land, raw-cache split design)
    04b_aggregate_forcing.py              (aggregation step for either raw cache)
```

### Why `basin_scale_alternates/` exists and why NOT to use it at CONUS scale

Earlier in this project, forcing download and reach-aggregation were split
into two stages (`04a` downloads+crops raw grid, `04b` aggregates it) so
that fixing a bug in the aggregation logic didn't require re-fetching from
the network. This is a real efficiency win **at single-basin scale**,
where the raw cropped grid is a few hundred MB. It becomes actively harmful
at CONUS scale: the raw grid for the whole country, kept on disk across the
full record, runs into multiple TB — exactly the storage blowup flagged as
a concern early in this project. **Use the top-level `04_download_forcing*.py`
scripts for CONUS** — they aggregate during download and never persist the
raw grid at all.

## Prerequisites

```bash
pip install pynhd geopandas pandas pyarrow xarray zarr s3fs scipy shapely pyproj \
            earthaccess fsspec aiohttp requests py7zr numpy
```

**Credentials needed** (free accounts, set up once):
1. **NASA Earthdata** (only if using NLDAS-2): https://urs.earthdata.nasa.gov/,
   then authorize "NASA GESDISC DATA ARCHIVE" under your account's Applications
   settings — a required extra step, easy to miss, causes silent `403` failures
   if skipped. Either `earthaccess.login()` interactively once, set
   `EARTHDATA_USERNAME`/`EARTHDATA_PASSWORD`, or add a `~/.netrc` entry for
   `urs.earthdata.nasa.gov`.
2. **DestinE Earth Data Hub** (only if using ERA5-Land): https://earthdatahub.destine.eu/,
   get a token from your profile, `export EDH_TOKEN=<token>`.

Neither NWM (AWS, anonymous S3), NHDPlus (public web service), NWIS, nor
CAMELSH (Zenodo) require credentials.

## Run order

```bash
# 1. Graph — this is the one step everything else depends on
python 01_build_graph.py --outdir ./output --region CONUS --tile-workers 4

# 2-3. Dense streamflow/groundwater (independent of each other, can run in parallel)
python 02_download_streamflow.py --outdir ./output --region CONUS --node-chunk-size 5000
python 03_download_groundwater.py --outdir ./output --region CONUS --node-chunk-size 5000

# 4. Dense forcing -- ALWAYS run --inspect first
python 04_download_forcing_era5land.py --inspect
python 04_download_forcing_era5land.py --outdir ./output --region CONUS

# 5. Sparse fine-tuning gauges
python 06_download_nwis_iv.py --outdir ./output --region CONUS --max-sites 5000
# (or 05_download_camelsh.py, or both)
```

Every step is **resumable** — each writes one file per year (or per HUC8
tile for the graph step) and skips anything already on disk, so an
interrupted run just needs the same command re-run.

## What's validated vs. extrapolated — read this before trusting a CONUS run blind

**Validated by real runs during this project:**
- `01_build_graph.py`'s tiling mechanism, at HUC2 scale (Tennessee River
  Basin, 68 raw HUC8 tiles returned by the spatial query, correctly
  filtered down to 32 genuine ones — the filtering fix that makes this
  reliable is already in `region.py`, not something CONUS discovers fresh)
- `02`/`03` at ~59,000-reach (HUC2) scale
- `04_download_forcing_era5land.py`'s core logic (catchment-to-cell mapping,
  longitude handling, retry logic) at HUC8 scale; the *CONUS-safe,
  aggregate-during-download* structure of this specific script is new for
  this handoff and has the same logic, but hasn't itself been run end-to-end
  at full national scale
- `06_download_nwis_iv.py`'s auto-discovery and fetch logic at HUC8 scale
  (15-gauge French Broad test)

**Not yet run at full CONUS scale — go in expecting to debug, not expecting
a silent success:**
- `01_build_graph.py --region CONUS` will tile across all ~2,264 HUC8 units
  nationally (18 HUC2 regions × the tiling mechanism already proven at
  HUC2 scale) — expect this to take a genuinely long time and be worth
  running as a monitored job, not a fire-and-forget one
- Total storage footprint at CONUS scale — extrapolate roughly from the
  HUC2 Tennessee numbers (~20x French Broad) by another ~15-20x for CONUS,
  but treat this as a rough order-of-magnitude estimate, not a firm number
- The catchment-to-cell spatial matching in the forcing scripts, run
  against ~2.7M reaches' catchment polygons at once — architecturally the
  same operation validated at 2,891 and 58,833 reaches, but not tested at
  this scale

## Known, currently-unresolved limitations (carried over from basin-scale work)

- **No H (channel depth) or S (channel storage) target exists in this
  pipeline.** GraphRiverCast's three-variable (Q, H, S) formulation isn't
  achievable with these sources without a hand-built Manning's-equation
  approximation from NWM's channel geometry parameters — that gap was
  identified early in this project and was never closed. This pipeline
  fully supports a Q-only (Sun et al. 2022-style) approach.
- **CAMELSH coverage is basin-dependent and not reliably rich** — don't
  assume it will give useful data for every region the way it failed to
  for French Broad. `06_download_nwis_iv.py` is the more dependable source
  found so far, but that finding came from one basin, not a systematic
  CONUS-wide check.
- **The nearest-cell aggregation is a real approximation, not exact
  area-weighting.** At ERA5-Land's ~9km / NLDAS-2's ~12km resolution vs.
  typical NHDPlus catchments of a few km², multiple small catchments
  legitimately share one coarse cell's value. This is a deliberate,
  documented design choice, not a bug — but worth knowing if exact
  sub-pixel area weighting ever becomes a requirement.

## RED tier: status of all 10 datasets

Per the team's priority spreadsheet (`Data_1.xlsx`), here's where every
RED-tier dataset actually stands in this codebase:

| # | Dataset | Script | Registration needed | Status |
|---|---|---|---|---|
| 1 | ERA5-Land | `04_download_forcing_era5land.py` | DestinE EDH token | ✅ Working, validated at HUC2 scale |
| 10 | NWM Retrospective v3.0 | `02`, `03` | None | ✅ Working, validated at HUC2 scale |
| 19 | NWIS surface-water | `06_download_nwis_iv.py` | None | ✅ Working, validated at HUC8 scale |
| 20 | NWIS groundwater | `07_download_nwis_groundwater.py` | None | ✅ New — same pattern as #19, not yet run end-to-end |
| 39 | MERIT Hydro | `13_download_merit_hydro.py` | **Google Form + emailed password, Dropbox-gated** | ⚠️ **Not fully automatable** — see script docstring |
| 41 | NHDPlus High Resolution | `08_download_nhdplus_hr.py` | None | ⚠️ New, `pynhd.NHDPlusHR`'s exact API not independently re-verified |
| 44 | USGS 3DEP | `09_download_3dep.py` | None | ⚠️ New, `py3dep` API not independently re-verified |
| 46 | ParFlow-CONUS2 / HydroFrame | `10_download_parflow_conus2.py` | HydroFrame API PIN | ⚠️ New, `hf_hydrodata` function signature not independently re-verified |
| 47 | GLOBGM v1.0 | `11_download_globgm.py` | None | ⚠️ New, exact filenames not independently verified — run `--inspect` first |
| 50 | GRFR (VIC-RAPID) | `12_download_grfr.py` | Globus identity + native-app client ID | ⚠️ New, structurally different (Globus transfer, not HTTP download) |

**Every script marked "⚠️ New" has an `--inspect` mode (or `--list-tiles`
for MERIT Hydro) — run that first, always, before a real download.** This
project has repeatedly found that assumed API shapes/filenames turn out
wrong in practice (NLDAS-2's temporal window, ERA5-Land's longitude
convention, CAMELSH's actual Zenodo record and archive filename all had
to be corrected after a real run revealed the assumption was off) — nothing
here should be trusted blind.

### On MERIT Hydro specifically

This is the one dataset in the RED tier that **cannot** be scripted the
same way as everything else. It's gated behind a Google Form registration
with a password emailed afterward, served from a password-protected
Dropbox folder — Dropbox's password gate is a browser form, not a
standard HTTP auth header, so it doesn't fit this pipeline's "one-time
token, then fully automated" pattern. `13_download_merit_hydro.py`
computes exactly which tiles you need and gives two realistic paths: a
one-time manual download by a team member, or a best-effort scripted
attempt using a manually-exported browser session cookie (untested,
not guaranteed to work with Dropbox's specific gate implementation).

### Kept in native format, on purpose

Every new RED-tier script downloads raw, unprocessed data — no
catchment-matching, no reach-aggregation, no reprojection beyond what
each source's own API does by default. Per the team's direction, that
preprocessing work is deliberately deferred to a later stage, once the
raw data itself is confirmed to be downloading correctly.

## Testing plan: small basin on Bridges-2 before Frontier

Before handing this to Xiao's team, validate on French Broad (HUC8
`06010105`, already used throughout this project's earlier work) or
another small basin:

```bash
python 01_build_graph.py --outdir ./test_output --region 06010105
python 07_download_nwis_groundwater.py --outdir ./test_output --region 06010105
python 08_download_nhdplus_hr.py --outdir ./test_output --region 06010105
python 09_download_3dep.py --outdir ./test_output --region 06010105
python 11_download_globgm.py --inspect   # check real filenames first
python 10_download_parflow_conus2.py --inspect   # check real API first, needs HydroFrame PIN
python 12_download_grfr.py --inspect --source discharge   # needs Globus login first
python 13_download_merit_hydro.py --list-tiles --outdir ./test_output --region 06010105
```

Confirm each produces real, sane output at this small scale before
scaling any of them to CONUS — the same discipline that caught every real
bug found so far in this project.



This pipeline was built and tested on PSC's Bridges-2, not Frontier — a
few things will need adjusting, and this README intentionally doesn't
guess at Frontier-specific values it can't verify:
- **Job scheduler specifics** (partition/queue names, walltime limits,
  QOS) — Bridges-2's constraints (e.g., a hard 48-hour cap on its
  GPU-shared partition, GPU-only compute due to this project's specific
  allocation) are almost certainly different on Frontier; check Frontier's
  own documentation/allocation rather than assuming these scripts' example
  SLURM directives (not included here, since they'd be Bridges-2-specific
  and actively misleading on a different system) carry over.
- **Data-transfer node equivalent** — Bridges-2 has `data.bridges2.psc.edu`
  for exactly this kind of large external download, meaningfully faster
  than running on a login node. Check whether Frontier has an equivalent
  DTN and use it for these download steps if so.
- **Storage paths** — every script's `--outdir` is a plain path; point it
  at whatever Frontier's equivalent of large-scale project storage is.

# SETUP.md — Complete Onboarding Guide

Everything Xiao's team needs to get this pipeline running, start to finish.
Read this once, top to bottom, before running anything at CONUS scale.

## 1. Install dependencies

```bash
pip install pynhd geopandas pandas pyarrow xarray zarr s3fs scipy shapely pyproj \
            earthaccess fsspec aiohttp requests py7zr numpy \
            py3dep hf_hydrodata globus-sdk boto3 beautifulsoup4
```

## 2. Set up every credential you'll need — do this once, before running anything

This pipeline touches 6 different data providers. Five need a free account;
one (GLOBGM) needs nothing at all. Do all of this up front so you're not
discovering a missing credential mid-run.

| Provider | Used by | Setup |
|---|---|---|
| **NASA Earthdata** | NLDAS-2, SMAP L4, Sentinel-6 | Register: https://urs.earthdata.nasa.gov/ — then **authorize "NASA GESDISC DATA ARCHIVE"** under your account's Applications settings (easy to miss, causes silent failures if skipped). Set credentials via `~/.netrc`, or `EARTHDATA_USERNAME`/`EARTHDATA_PASSWORD`, or run `earthaccess.login()` interactively once. |
| **DestinE Earth Data Hub (EDH)** | ERA5-Land | Register: https://earthdatahub.destine.eu/ — get a token from your profile, `export EDH_TOKEN=<token>` |
| **HydroFrame** | ParFlow-CONUS2 | Create an account + register your API PIN — see "Creating a HydroFrame API Account" in the hf_hydrodata docs: https://hf-hydrodata.readthedocs.io/ |
| **Globus** | GRFR | Most institutions (including national labs) already provide a Globus identity — confirm at https://app.globus.org. Then **register your own native-app OAuth client** at https://app.globus.org/settings/developers (don't reuse someone else's client ID) and set it via `config.GRFR_GLOBUS_CLIENT_ID` or `--client-id`. Run `python 12_download_grfr.py --login` once — this opens a one-time interactive login and caches a reusable token. |
| **Copernicus Data Space Ecosystem (CDSE)** | Sentinel-1, Sentinel-3 | Register: https://dataspace.copernicus.eu/ — then generate **S3 credentials** (separate from your login password) from your account settings, `export CDSE_S3_ACCESS_KEY=...` and `export CDSE_S3_SECRET_KEY=...` |
| **MERIT Hydro** | MERIT Hydro | **Cannot be fully automated** — see `13_download_merit_hydro.py`'s docstring. Register at http://hydro.iis.u-tokyo.ac.jp/~yamadai/MERIT_Hydro/, get the emailed password, and plan for a one-time manual download (this script tells you exactly which tiles you need). |
| *(none needed)* | NWM, NHDPlus (V2 and HR), USGS 3DEP, NWIS (surface-water and groundwater), Copernicus DEM GLO-30, GLOBGM | Fully public, no account required. |

## 3. Validate on a small basin FIRST — always

Every script in this pipeline has an `--inspect` (or `--list-tiles` for
MERIT Hydro) mode. **Run it before any real download, every time**, for
every new source. This project has repeatedly found that assumed API
shapes, filenames, or date ranges turn out wrong in practice once actually
tested — that's not a hypothetical caveat, it's the pattern every real bug
in this whole project followed.

Use French Broad (HUC8 `06010105`) as the test basin — already used
throughout this project, small enough to validate quickly:

```bash
# Build the graph first -- everything else depends on it
python 01_build_graph.py --outdir ./test_output --region 06010105

# Core pipeline (already validated at HUC2 scale in earlier work)
python 02_download_streamflow.py --outdir ./test_output --region 06010105
python 03_download_groundwater.py --outdir ./test_output --region 06010105
python 04_download_forcing_era5land.py --inspect
python 04_download_forcing_era5land.py --outdir ./test_output --region 06010105
python 06_download_nwis_iv.py --outdir ./test_output --region 06010105

# RED tier additions -- inspect first, every one
python 07_download_nwis_groundwater.py --outdir ./test_output --region 06010105
python 08_download_nhdplus_hr.py --outdir ./test_output --region 06010105
python 09_download_3dep.py --outdir ./test_output --region 06010105
python 10_download_parflow_conus2.py --inspect
python 11_download_globgm.py --inspect
python 12_download_grfr.py --login
python 12_download_grfr.py --inspect --source discharge
python 13_download_merit_hydro.py --list-tiles --outdir ./test_output --region 06010105

# YELLOW tier additions -- inspect first, every one
python 14_download_smap_l4.py --inspect
python 15_download_copernicus_dem.py --outdir ./test_output --region 06010105
python 16_download_sentinel1_grd.py --inspect --region 06010105 --start 2023-01-01 --end 2023-01-31
python 17_download_sentinel3_sral.py --inspect --region 06010105 --start 2023-01-01 --end 2023-01-31
python 18_download_sentinel6.py --inspect
```

**Confirm each one produces real, sane output** — non-empty files, sensible
row/tile counts, no silent zero-coverage results — before scaling any of
them to `--region CONUS`.

## 4. Scale to CONUS, one dataset at a time

Don't run everything at once. Given the honest confidence levels in
`README.md`'s status table, work through them roughly in this order —
highest confidence first, so any real problems surface on the easier
sources before you're deep into the harder ones:

1. `01_build_graph.py --region CONUS` (this is the one every other script
   depends on — get this fully right before anything else)
2. `02`, `03` (NWM streamflow/groundwater — validated at HUC2 scale already)
3. `04_download_forcing_era5land.py` (validated core logic, new CONUS-safe wrapper)
4. `06_download_nwis_iv.py`, `07_download_nwis_groundwater.py`
5. `15_download_copernicus_dem.py` (simplest new source — no credentials, no API guessing)
6. `08`, `09` (same HyRiver stack, moderate confidence)
7. `14_download_smap_l4.py`, `18_download_sentinel6.py` (same earthaccess pattern as NLDAS-2)
8. `10_download_parflow_conus2.py`, `16_download_sentinel1_grd.py`, `17_download_sentinel3_sral.py`, `11_download_globgm.py`, `12_download_grfr.py` (newest access patterns, least tested)
9. `13_download_merit_hydro.py` (plan for a manual step regardless)

## 5. Where to run things

- **Login nodes**: fine for quick tests, `--inspect` calls, and the graph
  step for a single small basin. Not for sustained downloads — this
  project repeatedly hit memory limits and general unfairness-to-other-users
  problems running heavy work on login nodes.
- **Data transfer node** (if your system has one, e.g. Bridges-2's
  `data.bridges2.psc.edu`): the right place for the actual bulk downloads.
  Check whether Frontier has an equivalent.
- **Batch jobs**: for anything that needs to run longer than an interactive
  session allows, or that you want to survive a dropped connection.

## 6. What "kept in native format" means here

Per project direction, none of the RED/YELLOW-tier scripts do any
aggregation, catchment-matching, or reach-level reprocessing — they
download raw data as distributed (NetCDF, GeoTIFF, HDF5, .SAFE archives)
and stop there. That preprocessing work is intentionally deferred to a
later stage once the raw downloads are confirmed working.

## 7. If something doesn't match what this doc says

Given how many of these sources' exact API/filename details couldn't be
independently verified while building this (no internet access in the
environment that wrote it), some mismatch between what's documented here
and what you actually encounter is likely for the newer scripts. The
`--inspect` modes exist specifically to surface that early and cheaply —
lean on them rather than debugging a full download gone wrong.

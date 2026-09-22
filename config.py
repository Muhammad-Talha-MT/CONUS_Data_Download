"""
config.py

Shared constants for the NWM/NHDPlus pipeline. Originally written for HUC8
06010105 (French Broad River) at full record; now generalized (see
region.py) to also run against a whole HUC2 (e.g. the Tennessee River
Basin, "06") or "CONUS". Scripts still default to the French Broad HUC8
unless you pass --region explicitly.
"""

from pathlib import Path

# ---------------------------------------------------------------------------
# CONUS-scale tuning (NEW -- shared across the download scripts via
# pipeline_utils.py so these live in one place instead of being separately
# hardcoded per script)
# ---------------------------------------------------------------------------
# Bounded thread-pool size for the many "loop over independent items, each
# doing one network call" patterns (per-tile/per-HU4/per-product downloads,
# batched site or catchment lookups). Kept modest by default -- every
# source here is a shared public service, not a private bucket, and a large
# burst of concurrent requests risks rate-limiting/blocking rather than
# helping. Override per-invocation with each script's --max-workers flag
# where present.
DOWNLOAD_MAX_WORKERS = 8

# Reaches per output file for the dense, per-reach time series scripts
# (02/03/04) -- this is the SAME axis --node-chunk-size already controlled
# for the in-memory batch size, but is now ALSO the output-file boundary:
# one file per (year, node-chunk) instead of one giant file per year. This
# is what actually makes CONUS scale (~2.7M reaches) survivable -- it
# bounds both memory and per-unit resumability to this chunk size
# regardless of total reach count in the region.
NODE_CHUNK_SIZE_DEFAULT = 5000

# Page size for CDSE's OData product search (see pipeline_utils.
# paginated_odata_get). 1000 matches CDSE's own typical max page size;
# pagination continues via @odata.nextLink beyond this.
CDSE_PAGE_SIZE = 1000

# ---------------------------------------------------------------------------
# Target region
# ---------------------------------------------------------------------------
# Any of: an 8/10/12-digit HUC code (single basin, no tiling needed), a
# 2/4/6-digit HUC code (region.py tiles this into its contained HUC8 units),
# or the literal string "CONUS" (tiles across all CONUS HUC2 units).
# Examples: "06010105" (French Broad HUC8), "06" (Tennessee River Basin,
# HUC2), "CONUS".
HUC8 = "06010105"          # kept as the historical default / variable name
DEFAULT_REGION = HUC8      # scripts' --region flag defaults to this
BASIN_LABEL = "french_broad"  # used only in filenames, for readability

# Two-digit HUC2 codes covering the continental US (used when --region CONUS
# is given, to tile the fetch region-by-region rather than as one request).
# NOTE: written from general USGS HUC2 numbering knowledge, not indepedently
# re-verified against a live WBD query while building this -- cross-check
# against the wbd02 layer's actual feature list before a real CONUS run.
CONUS_HUC2_CODES = [f"{i:02d}" for i in range(1, 19)]  # 01-18

# ---------------------------------------------------------------------------
# NWM v3.0 Retrospective, full CONUS record
# Confirmed from NOAA's public archive: Feb 1979 - Jan 2023, hourly.
# ---------------------------------------------------------------------------
NWM_START = "1979-02-01"
NWM_END = "2023-01-31"

NWM_BUCKET = "noaa-nwm-retrospective-3-0-pds"
NWM_ZARR_ROOT = f"s3://{NWM_BUCKET}/CONUS/zarr"

# Model-output (point/reach-indexed) zarr stores, keyed by feature_id == COMID
ZARR_CHRTOUT = f"{NWM_ZARR_ROOT}/chrtout.zarr"   # streamflow, velocity
ZARR_GWOUT = f"{NWM_ZARR_ROOT}/gwout.zarr"       # groundwater bucket: depth, inflow, outflow

# NOTE: NWM's own native forcing (AORC) is intentionally NOT used for the
# dense per-reach forcing anymore -- decided (see project discussion) in
# favor of NLDAS-2, so that forcing is independent of the NWM v3.0
# streamflow pretraining target (v3.0 is driven by AORC, not NLDAS-2), and
# consistent with the forcing used in CAMELSH for the gauge-level
# fine-tuning data. The old ZARR_FORCING dict / LCC grid constants have
# been removed; see NLDAS2_* below.

# ---------------------------------------------------------------------------
# NLDAS-2 (dense, per-reach forcing) -- NASA GES DISC, via earthaccess
# ---------------------------------------------------------------------------
# NLDAS_FORA0125_H v2.0: hourly, 1/8-degree (~12km) CONUS grid, native lat/lon
# (WGS84), Jan 1979-present. Requires a free NASA Earthdata Login account:
# https://urs.earthdata.nasa.gov/  (then either run `earthaccess.login()`
# interactively once, or set EARTHDATA_USERNAME / EARTHDATA_PASSWORD, or a
# ~/.netrc entry for urs.earthdata.nasa.gov -- see 04_download_forcing.py).
NLDAS2_SHORT_NAME = "NLDAS_FORA0125_H"
NLDAS2_VERSION = "2.0"
NLDAS2_START = "1979-01-01"   # NLDAS-2 begins Jan 1979 (NWM streamflow begins Feb 1979)
NLDAS2_END = "2023-01-31"     # capped to match NWM v3.0 CHRTOUT's end of record

# The variable names inside each NLDAS_FORA0125_H granule, as documented by
# GES DISC. Confirm against a downloaded granule's ds.data_vars the first
# time you run this -- the script prints them either way.
NLDAS2_VARIABLES = [
    "Rainf",   # precipitation hourly total (kg/m^2, i.e. mm)
    "Tair",    # 2m air temperature (K)
    "Qair",    # 2m specific humidity (kg/kg)
    "Wind_E",  # 10m eastward wind (m/s)
    "Wind_N",  # 10m northward wind (m/s)
    "PSurf",   # surface pressure (Pa)
    "SWdown",  # downward shortwave radiation (W/m^2)
    "LWdown",  # downward longwave radiation (W/m^2)
]

# ---------------------------------------------------------------------------
# CAMELSH (gauge-level hourly streamflow + basin attributes, for fine-tuning)
# Tran et al. 2025, Scientific Data. https://doi.org/10.1038/s41597-025-05612-6
# ---------------------------------------------------------------------------
CAMELSH_ZENODO_RECORD = "15066778"  # verified: the record actually hosting Hourly.7z/info.csv (attributes,
                                     # shapefiles, time series for observed basins). 15413207, used
                                     # previously, is a separate landing/description entry with 0
                                     # attached files -- confirmed on a real run ("Found 0 files in
                                     # the Zenodo record") and independently verified against the
                                     # published Scientific Data paper, which cites 15066778 as the
                                     # current version. If Zenodo's structure changes again, check
                                     # https://zenodo.org/records/15066778 directly for the live list.
CAMELSH_ZENODO_API = f"https://zenodo.org/api/records/{CAMELSH_ZENODO_RECORD}"
CAMELSH_HOURLY_ARCHIVE_NAME = "timeseries.7z"  # confirmed against the real file listing of record
                                                # 15066778: ['timeseries.7z', 'attributes.7z',
                                                # 'info.csv', 'shapefiles.7z']. Previously assumed
                                                # "Hourly.7z" based on the dataset's own description
                                                # text, which didn't match the actual filename.
CAMELSH_INFO_CSV_NAME = "info.csv"

# ---------------------------------------------------------------------------
# ERA5-Land (alternative forcing source, Zarr-based -- faster than NLDAS-2's
# one-file-per-hour distribution, per real-world benchmarks found while
# researching this switch)
#
# NOT independently verified end-to-end from this environment (no internet
# access while writing this) -- run 04a_download_raw_forcing_era5land.py
# --inspect FIRST and confirm the printed structure before a real run.
#
# Source: DestinE Earth Data Hub (EDH), a documented, third-party-confirmed
# Zarr mirror of ERA5-Land specifically (not the coarser 31km parent ERA5
# product -- that distinction matters, several "ERA5 on cloud" sources are
# the wrong, coarser dataset). Requires a free EDH account/token, separate
# from your NASA Earthdata and CDS credentials.
# Registration: https://earthdatahub.destine.eu/
# ---------------------------------------------------------------------------
ERA5LAND_EDH_ZARR_URL = "https://data.earthdatahub.destine.eu/era5/reanalysis-era5-land-no-antartica-v0.zarr"

# ERA5-Land's standard short variable names (ECMWF/CDS convention -- these
# are the commonly documented names; confirm against --inspect output,
# names can differ slightly by store/version):
#   t2m    - 2m temperature
#   d2m    - 2m dewpoint temperature (humidity proxy)
#   tp     - total precipitation
#   sp     - surface pressure
#   u10    - 10m U wind component
#   v10    - 10m V wind component
#   ssrd   - surface solar (shortwave) radiation downwards
#   strd   - surface thermal (longwave) radiation downwards
ERA5LAND_VARIABLES = ["t2m", "d2m", "tp", "sp", "u10", "v10", "ssrd", "strd"]

ERA5LAND_START = "1980-01-01"  # ERA5-Land coverage begins 1950, but keep aligned to CAMELSH's 1980 start by default
ERA5LAND_END = NWM_END  # align to NWM v3.0's record end (Jan 2023) by default

# ---------------------------------------------------------------------------
# USGS NWIS Instantaneous Values (IV) -- the fine-tuning gauge source used
# in place of CAMELSH for French Broad, since CAMELSH's hourly curation
# showed near-zero coverage for this basin's gauges (confirmed: 17 of 18
# CAMELSH-matched French Broad gauges had 0 data-availability hours) while
# these same gauges have real, decades-long sub-daily records directly in
# NWIS (confirmed: 12 of 15 checked gauges have 25-41 years of continuous
# coverage running to the present). NWIS calls this data type "iv" in its
# modern service and "uv" (unit values) in older site-catalog metadata --
# same underlying data, just two different labels used in different parts
# of the API.
# ---------------------------------------------------------------------------
NWIS_IV_PARAMETER_CD = "00060"  # discharge, cubic feet per second
NWIS_IV_START = "1980-01-01"
# No fixed end date -- IV data is real-time and current; scripts default to
# "today" unless --end is passed explicitly.

# ---------------------------------------------------------------------------
# NWIS Groundwater Levels -- RED tier #20. Same service as NWIS-IV above,
# different parameter code and endpoint (groundwater levels aren't an
# "Instantaneous Values" product -- they're discrete depth-to-water
# measurements at irregular intervals, served by NWIS's dedicated
# groundwater-levels service).
# ---------------------------------------------------------------------------
NWIS_GWLEVELS_URL = "https://waterservices.usgs.gov/nwis/gwlevels/"
NWIS_GWLEVELS_PARAMETER_CD = "72019"  # depth to water level, below land surface, feet
NWIS_GWLEVELS_START = "1980-01-01"

# ---------------------------------------------------------------------------
# NHDPlus High Resolution -- RED tier #41. Verified: accessible via pynhd's
# NHDPlusHR class (same HyRiver stack already used throughout this
# pipeline for medium-resolution NHDPlus V2), no registration needed.
# Distinct from the NHDPlus V2 (medium-res) topology this project's earlier
# HUC8/HUC2 work was built on -- see the graph-building script's docstring
# for how this relates to 01_build_graph.py.
# ---------------------------------------------------------------------------
NHDPLUS_HR_LAYER = "flowline"  # other available layers: catchment, nhdwaterbody, nhdarea, etc.

# ---------------------------------------------------------------------------
# USGS 3DEP -- RED tier #44. Verified: accessible via the HyRiver stack's
# py3dep package (National Map's 3DEP web service), no registration needed.
# ---------------------------------------------------------------------------
USGS_3DEP_RESOLUTION_M = 30  # 3DEP is available at 10m, 30m, 60m; 30m is a
                             # reasonable default balancing detail vs. size

# ---------------------------------------------------------------------------
# ParFlow-CONUS2 / HydroFrame -- RED tier #46. Verified: accessible via the
# hf_hydrodata package. Requires a free HydroFrame API account + PIN --
# same one-time-registration pattern as NASA Earthdata / DestinE EDH used
# elsewhere in this pipeline. Register at: https://hydrogen.princeton.edu/hydrodata
# (account creation) then follow "Creating a HydroFrame API Account" in the
# hf_hydrodata docs to register your PIN.
# ---------------------------------------------------------------------------
PARFLOW_DATASET = "conus2_baseline_mod"  # the CONUS2 baseline simulation dataset name;
                                          # confirm against hf_hydrodata's own dataset catalog,
                                          # this was not independently re-verified against a live run

# ---------------------------------------------------------------------------
# GLOBGM v1.0 -- RED tier #47. Verified: output data is hosted as a plain,
# public HTTP directory (Utrecht University's Yoda research data platform)
# -- no registration needed. Exact per-file naming within this directory
# was NOT independently verified (no internet access while writing this) --
# the download script has an --inspect mode to list the real directory
# contents before assuming any specific filenames.
# ---------------------------------------------------------------------------
GLOBGM_BASE_URL = "https://geo.data.uu.nl/research-globgm/output/version_1.0/"

# ---------------------------------------------------------------------------
# GRFR (Global Reach-level Flood Reanalysis, VIC-RAPID) -- RED tier #50.
# Verified: 3-hourly discharge/runoff distributed via Globus (endpoint UUIDs
# below, confirmed directly from reachhydro.org/home/records/grfr). Globus
# requires a one-time interactive login (free Globus ID -- most institutions,
# including national labs, already have Globus identities that work here)
# via the Globus SDK's native app authentication flow -- after that first
# login, tokens are cached and subsequent runs are non-interactive, the same
# pattern as earthaccess/EDH_TOKEN elsewhere in this pipeline.
# ---------------------------------------------------------------------------
GRFR_GLOBUS_DISCHARGE_ENDPOINT = "7844823f-12dc-4569-a115-39fb107221d5"
GRFR_GLOBUS_RUNOFF_ENDPOINT = "c15df1c2-5cec-4804-bda6-1020e7fc829b"
# Globus "Client ID" for this pipeline's native-app auth flow. Using Globus's
# own public "Globus CLI" client ID here is not appropriate for a shared
# pipeline -- Xiao's team should register their own native app client ID at
# https://app.globus.org/settings/developers before running the GRFR script;
# see that script's docstring.
GRFR_GLOBUS_CLIENT_ID = None  # MUST be set by whoever runs 12_download_grfr.py

# ---------------------------------------------------------------------------
# MERIT Hydro -- RED tier #39 -- HONEST FLAG, NOT FULLY AUTOMATABLE.
# Distribution is gated behind a Google Form registration (license
# agreement), with the password emailed afterward, and the data itself
# served from a password-protected Dropbox folder. Dropbox's password gate
# is a browser-interactive flow, not a simple HTTP Basic Auth header -- it
# cannot be scripted the way every other credentialed source in this
# pipeline (Earthdata, EDH, HydroFrame, Globus) was, which all support a
# one-time login producing a reusable token/PIN. See
# 13_download_merit_hydro.py's docstring for the realistic options.
# ---------------------------------------------------------------------------
MERIT_HYDRO_REGISTRATION_URL = "http://hydro.iis.u-tokyo.ac.jp/~yamadai/MERIT_Hydro/"

# =============================================================================
# YELLOW TIER (per Data_1.xlsx: #11 SMAP L4, #12 Sentinel-1 GRD, #17
# Sentinel-3 SRAL, #18 Sentinel-6, #43 Copernicus DEM GLO-30 -- #27 CAMELSH
# is also yellow but already covered by 05_download_camelsh.py)
# =============================================================================

# ---------------------------------------------------------------------------
# SMAP L4 -- YELLOW tier #11. Verified: accessible via earthaccess, the
# SAME NASA Earthdata credentials already required for NLDAS-2 -- no new
# registration needed if you've already set up 04_download_forcing.py's
# credentials. Short name confirmed from NSIDC's own dataset page URL slug
# ("spl4smgp"); NOT independently re-verified via a live search from this
# environment -- run --inspect first.
# ---------------------------------------------------------------------------
SMAP_L4_SHORT_NAME = "SPL4SMGP"
SMAP_L4_VERSION = "8"
SMAP_L4_START = "2015-04-01"  # mission start

# ---------------------------------------------------------------------------
# Copernicus DEM GLO-30 -- YELLOW tier #43. Verified: public, ANONYMOUS AWS
# S3 bucket, no registration needed at all -- the simplest source in this
# entire pipeline. 1-degree x 1-degree tiles.
# ---------------------------------------------------------------------------
COPERNICUS_DEM_S3_BUCKET = "copernicus-dem-30m"
# Tile folder naming, confirmed: Copernicus_DSM_COG_30_<N|S><lat2>_00_<E|W><lon3>_00_DEM/
# containing a file of the same name + "_DEM.tif"

# ---------------------------------------------------------------------------
# Sentinel-1 GRD / Sentinel-3 SRAL -- YELLOW tier #12, #17. Verified:
# distributed via the Copernicus Data Space Ecosystem's (CDSE) S3-compatible
# "eodata" object storage. Requires a free CDSE account
# (https://dataspace.copernicus.eu/) plus generating S3-style access/secret
# keys from your CDSE account settings -- a one-time step, same "free
# account -> reusable credentials" pattern as every other credentialed
# source in this pipeline. NOT boto3-against-real-AWS -- it's an
# S3-compatible endpoint, so boto3 works once pointed at CDSE's endpoint URL.
# ---------------------------------------------------------------------------
CDSE_S3_ENDPOINT = "https://eodata.dataspace.copernicus.eu"
CDSE_S3_BUCKET = "eodata"
CDSE_REGISTRATION_URL = "https://dataspace.copernicus.eu/"

# ---------------------------------------------------------------------------
# Sentinel-6 (Michael Freilich) -- YELLOW tier #18. Verified: distributed
# through NASA PO.DAAC via the NASA Earthdata Cloud -- accessible via
# earthaccess, SAME credentials as NLDAS-2/SMAP L4 above.
# ---------------------------------------------------------------------------
SENTINEL6_SHORT_NAME = "JASON_CS_S6A_L2_ALT_LR_STD_OST_NTC_F"  # confirm against --inspect;
                                                                 # several product variants exist
                                                                 # (LR/HR, NTC/STC/NRT) -- this is
                                                                 # the standard non-time-critical
                                                                 # low-resolution ocean topography one
SENTINEL6_START = "2020-11-21"  # mission launch


# ---------------------------------------------------------------------------
# Output layout (mirrors the pilot's directory structure)
# ---------------------------------------------------------------------------
def make_dirs(outdir: str) -> dict:
    root = Path(outdir)
    dirs = {
        "root": root,
        "basin": root / "basin",
        "graph": root / "graph",
        "streamflow": root / "streamflow",
        "groundwater": root / "groundwater",
        "forcing": root / "forcing",
        # Raw, bbox-cropped NLDAS-2 grid (one NetCDF per year), kept as a
        # persistent local cache by 04a_download_raw_forcing.py so that
        # 04b_aggregate_forcing.py can be re-run cheaply -- e.g. after a bug
        # fix to the aggregation logic -- without re-fetching from NASA.
        # Only sensible at basin scale (a single HUC8's bbox is a few dozen
        # NLDAS cells, a few hundred MB across the full record); NOT
        # recommended at HUC2/CONUS scale, where the raw grid itself is the
        # multi-TB object Vinh's original email was cautious about.
        "forcing_raw": root / "forcing_raw",
        # Separate cache for the ERA5-Land alternative, kept distinct from
        # NLDAS-2's cache so switching sources doesn't overwrite or mix with
        # progress already made under the other one.
        "forcing_raw_era5land": root / "forcing_raw_era5land",
        # Per-HUC8 tile cache, used by region.py when --region is a HUC2/HUC4/
        # HUC6/CONUS code that needs to be tiled into many HUC8 fetches.
        # Lets a large-region graph build resume without re-fetching tiles
        # that already succeeded.
        "graph_tiles": root / "graph" / "_tiles",
        # USGS NWIS Instantaneous Values -- the sparse real-gauge fine-tuning
        # target, used instead of CAMELSH for basins where CAMELSH's curated
        # coverage is too sparse to be useful (see note above).
        "nwis_iv": root / "nwis_iv",
        "nwis_gwlevels": root / "nwis_gwlevels",
        "nhdplus_hr": root / "nhdplus_hr",
        "usgs_3dep": root / "usgs_3dep",
        "parflow_conus2": root / "parflow_conus2",
        "globgm": root / "globgm",
        "grfr": root / "grfr",
        "merit_hydro": root / "merit_hydro",
        "smap_l4": root / "smap_l4",
        "copernicus_dem": root / "copernicus_dem",
        "sentinel1_grd": root / "sentinel1_grd",
        "sentinel3_sral": root / "sentinel3_sral",
        "sentinel6": root / "sentinel6",
    }
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return dirs

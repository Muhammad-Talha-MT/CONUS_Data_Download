#!/usr/bin/env python3
"""
11_download_globgm.py  (CONUS-optimized)

RED tier #47: GLOBGM v1.0 global-scale groundwater model.

GLOBGM is a global product with a small, fixed file count (not tiled by
region), so this was never really a "CONUS scale" bottleneck the way the
per-reach/per-HUC8 scripts are -- but the file-download loop is now
threaded (pipeline_utils.parallel_map, default 4 workers -- kept modest,
these can be large files and this is a smaller public research-data
server, not a CDN) for consistency and in case --pattern ever matches a
larger set of files than the default 'wtd' search does today.

Requires: requests, beautifulsoup4
Usage:
    python 11_download_globgm.py --inspect
    python 11_download_globgm.py --outdir ./output --pattern "wtd"
"""

import argparse
import sys

import requests

from config import GLOBGM_BASE_URL, make_dirs
from pipeline_utils import parallel_map


def list_directory(url: str) -> list:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "html.parser")
        links = [a.get("href") for a in soup.find_all("a") if a.get("href")]
    except ImportError:
        print("  (beautifulsoup4 not installed -- falling back to a crude regex parse. "
              "pip install beautifulsoup4 for a more reliable listing.)", file=sys.stderr)
        import re
        links = re.findall(r'href="([^"]+)"', resp.text)

    links = [l for l in links if l not in ("../", "./") and not l.startswith("?") and not l.startswith("/")]
    return links


def inspect_directory():
    print(f"Listing {GLOBGM_BASE_URL} ...")
    links = list_directory(GLOBGM_BASE_URL)
    print(f"\n{len(links)} entries found:")
    for link in links:
        print(f"  {link}")
    print("\nIf any of these are subfolders (end with '/'), you'll likely need to "
          "recurse into them -- adjust --pattern or extend the script once you see "
          "the real structure.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--outdir", default="./output")
    parser.add_argument("--pattern", default="wtd",
                         help="Only download files whose name contains this substring (default: 'wtd')")
    parser.add_argument("--download-workers", type=int, default=4)
    parser.add_argument("--inspect", action="store_true", help="RUN THIS FIRST")
    args = parser.parse_args()

    if args.inspect:
        inspect_directory()
        return

    dirs = make_dirs(args.outdir)

    print(f"Listing {GLOBGM_BASE_URL} ...")
    links = list_directory(GLOBGM_BASE_URL)
    matching = [l for l in links if args.pattern.lower() in l.lower() and not l.endswith("/")]
    print(f"  {len(matching)} / {len(links)} entries match pattern '{args.pattern}'")

    if not matching:
        print("No matching files -- run --inspect to see the real listing and adjust --pattern.", file=sys.stderr)
        sys.exit(1)

    to_fetch = []
    for filename in matching:
        out_path = dirs["globgm"] / filename
        if not out_path.exists():
            to_fetch.append((filename, out_path))
    already = len(matching) - len(to_fetch)
    if already:
        print(f"{already} file(s) already downloaded, skipping")
    if not to_fetch:
        print("Nothing left to download.")
        return

    def _download_one(item):
        filename, out_path = item
        file_url = GLOBGM_BASE_URL + filename
        with requests.get(file_url, stream=True, timeout=300) as r:
            r.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
        return out_path.stat().st_size

    successes, failures = parallel_map(
        to_fetch, _download_one, max_workers=args.download_workers, label="file download",
    )
    print(f"Done. {len(successes)} file(s) downloaded this run, {len(failures)} failed.")


if __name__ == "__main__":
    main()

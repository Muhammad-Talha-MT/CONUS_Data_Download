"""
pipeline_utils.py

NEW in this CONUS-optimization pass. Shared helpers used across several
scripts so the same fixes don't get duplicated (and drift out of sync)
in six different files:

  1. parallel_map()       -- bounded-concurrency thread pool for the many
                              "loop over N independent items, each doing
                              one network call" patterns that were plain
                              serial `for` loops throughout this pipeline
                              (per-tile downloads, per-HU4 fetches, etc).
                              I/O-bound work releases the GIL, so a modest
                              thread pool (default 8 workers) gives a real
                              wall-clock win here without needing
                              multiprocessing.
  2. paginated_odata_get() -- CDSE's OData product search
                              (cdse_common.search_cdse_catalog) was capped
                              at "$top": 1000 with no pagination at all.
                              At HUC8 scale this never mattered (a basin
                              rarely has 1,000+ Sentinel scenes). At CONUS
                              scale it WILL silently truncate results with
                              no warning. This follows @odata.nextLink
                              until exhausted or a safety cap is hit.
  3. chunked()             -- the `[seq[i:i+n] for i in range(0, len(seq), n)]`
                              pattern repeated in nearly every script,
                              factored out once.

Nothing here changes behavior at small scale -- these are drop-in
replacements for the equivalent serial code, just bounded and resumable
where the original wasn't.
"""

from __future__ import annotations

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Iterable, Sequence, TypeVar

T = TypeVar("T")
R = TypeVar("R")


def chunked(seq: Sequence[T], size: int) -> list[list[T]]:
    """Split seq into consecutive chunks of at most `size` items."""
    return [seq[i:i + size] for i in range(0, len(seq), size)]


def parallel_map(
    items: Sequence[T],
    fn: Callable[[T], R],
    max_workers: int = 8,
    label: str = "item",
    progress_every: int = 1,
) -> tuple[list[tuple[T, R]], list[tuple[T, Exception]]]:
    """
    Run fn(item) for every item in `items`, up to `max_workers` at once.

    This is deliberately a bounded thread pool, not "fire off everything
    at once" -- every source in this pipeline is a shared public service
    (NHDPlus WaterData, USGS NWIS, anonymous S3 buckets, CDSE) and a large
    unbounded burst of concurrent requests is a good way to get
    rate-limited or blocked, which would be a worse outcome than the
    original serial loop. 8 is a reasonable default; pass a smaller value
    for services known to be stricter (NHDPlus's WaterData in particular).

    Returns (successes, failures) as lists of (item, result_or_exception)
    pairs. Never raises on an individual item's failure -- the caller
    decides what a partial failure means (same "print and continue" spirit
    as the rest of this pipeline's resumable design).
    """
    successes: list[tuple[T, R]] = []
    failures: list[tuple[T, Exception]] = []
    total = len(items)
    if total == 0:
        return successes, failures

    done = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_item = {pool.submit(fn, item): item for item in items}
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            done += 1
            try:
                result = future.result()
                successes.append((item, result))
            except Exception as e:  # noqa: BLE001 -- intentionally broad, see docstring
                failures.append((item, e))
                print(f"    [{done}/{total}] {label} FAILED: {item!r}: {e}", file=sys.stderr)
            if progress_every and done % progress_every == 0:
                print(f"    [{done}/{total}] {label}(s) processed "
                      f"({len(successes)} ok, {len(failures)} failed)")

    return successes, failures


def paginated_odata_get(
    session,
    url: str,
    params: dict,
    page_size: int = 1000,
    max_pages: int | None = None,
    timeout: int = 60,
) -> list[dict]:
    """
    GET an OData collection (CDSE's Products endpoint) and follow
    '@odata.nextLink' until exhausted, a page is empty, or max_pages is
    hit. Fixes a real bug: the original search_cdse_catalog() used a flat
    "$top": 1000 with no continuation at all, which silently truncates any
    query returning more than 1,000 products -- exactly what a CONUS-wide,
    multi-year Sentinel-1 search will do, with no error or warning printed.

    `session` is a requests.Session (or the `requests` module itself,
    which exposes the same .get() interface for a one-off call).
    """
    all_values: list[dict] = []
    request_params = dict(params)
    request_params["$top"] = page_size
    next_url = url
    page = 0

    while next_url:
        page += 1
        if page == 1:
            resp = session.get(next_url, params=request_params, timeout=timeout)
        else:
            # nextLink is already a complete URL with its own query string
            resp = session.get(next_url, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        values = data.get("value", [])
        all_values.extend(values)

        next_url = data.get("@odata.nextLink")
        if max_pages and page >= max_pages:
            if next_url:
                print(f"    paginated_odata_get: stopping at --max-pages={max_pages} "
                      f"but more results remain (nextLink present) -- raise --max-pages "
                      f"or narrow the query if you need the rest.", file=sys.stderr)
            break
        if not values:
            break

    return all_values


def retry_with_backoff(fn: Callable[[], R], max_attempts: int = 3, wait_s: int = 30,
                        label: str = "operation") -> R | None:
    """
    Shared version of the retry-with-backoff pattern already used ad hoc in
    02/03/04/06/07 -- factored out so future fixes to the retry behavior
    land in one place. Returns None (rather than raising) after exhausting
    attempts, matching this pipeline's existing "print and skip, resumable
    next run" convention.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            print(f"  {label}: attempt {attempt}/{max_attempts} FAILED: {e}", file=sys.stderr)
            if attempt < max_attempts:
                print(f"  {label}: waiting {wait_s}s before retrying ...", file=sys.stderr)
                time.sleep(wait_s)
    return None

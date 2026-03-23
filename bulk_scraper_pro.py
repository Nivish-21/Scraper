#!/usr/bin/env python3
"""
Bulk Image Scraper — Production Version
========================================
Built for large datasets (10,000 - 200,000+ rows).

Improvements over bulk_scraper.py:
  - Parallel workers  : multiple browser instances running simultaneously
  - Checkpoint/resume : crash at row 47,832 -> restart exactly from row 47,832
  - Retry + backoff   : 429/503/timeout -> wait and retry automatically
  - Browser restart   : restarts Chromium every N rows to prevent memory leak
  - Duplicate skip    : skips rows whose output file already exists
  - Incremental log   : CSV written row-by-row, never loads full log into memory

Usage:
  python bulk_scraper_pro.py <file>
  python bulk_scraper_pro.py <file> --workers 5
  python bulk_scraper_pro.py <file> --workers 5 --delay 0.5
  python bulk_scraper_pro.py <file> --rows 500
  python bulk_scraper_pro.py <file> --resume
"""

import os
import re
import ssl
import sys
import csv
import time
import random
import argparse
import threading
import urllib.request
from queue import Queue, Empty
from datetime import datetime
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

import pandas as pd
from playwright.sync_api import sync_playwright

try:
    from playwright_stealth import stealth_sync
    STEALTH_AVAILABLE = True
except ImportError:
    STEALTH_AVAILABLE = False

# ── Platform fixes ────────────────────────────────────────────────────────────
# macOS/Windows SSL fix — bypasses certificate verification errors
ssl._create_default_https_context = ssl._create_unverified_context

# Windows fix — Playwright's sync API uses greenlet which conflicts with
# Python's default ProactorEventLoop on Windows. Switch to SelectorEventLoop.
import sys
if sys.platform == "win32":
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ── Dataset config (same as bulk_scraper.py) ───────────────────────────────────
DATASET_CONFIG = {
    "data.csv":                        {"url_col": "URL",         "name_col": "TranslatedRecipeName", "id_col": None},
    "food_recipes.csv":                {"url_col": "url",         "name_col": "recipe_title",          "id_col": None},
    "IndianFoodDatasetXLSFinal.xlsx":  {"url_col": "URL",         "name_col": "name",                  "id_col": "foodID"},
    "Indonesian_Food_Recipes.csv":     {"url_col": "URL",         "name_col": "Title",                 "id_col": None},
    "INDIAN FOOD RECIPE.csv":          {"url_col": "RECIPE LINK", "name_col": "RECIPE TITLE",          "id_col": None},
}

# ── Tuning constants ───────────────────────────────────────────────────────────
BROWSER_RESTART_EVERY = 500   # restart Chromium after this many rows per worker
MAX_RETRIES           = 3     # max attempts per row before marking as failed
RETRY_BACKOFF_BASE    = 5     # seconds — doubles on each retry: 5, 10, 20
RATE_LIMIT_WAIT       = 60    # seconds to pause when a 429 is received
COOKPAD_EXCLUDE       = ('guest_user', '/avatar.', '/comments/', '/steps/', '/users/')


# ── Thread-safe shared state ───────────────────────────────────────────────────

class Stats:
    """Live progress counter shared across all worker threads."""
    def __init__(self, total):
        self._lock    = threading.Lock()
        self.total    = total
        self.ok       = 0
        self.failed   = 0
        self.skipped  = 0
        self.start_ts = time.time()

    def record(self, status):
        with self._lock:
            if status == "ok":     self.ok      += 1
            elif status == "skip": self.skipped += 1
            else:                  self.failed  += 1

    def eta(self):
        done = self.ok + self.failed + self.skipped
        if done == 0:
            return "--:--:--"
        elapsed   = time.time() - self.start_ts
        rate      = done / elapsed
        remaining = self.total - done
        secs      = remaining / rate if rate > 0 else 0
        h, m, s   = int(secs // 3600), int((secs % 3600) // 60), int(secs % 60)
        return f"{h:02d}:{m:02d}:{s:02d}"

    def line(self):
        done = self.ok + self.failed + self.skipped
        pct  = (done / self.total * 100) if self.total else 0
        return (f"  Progress: {done}/{self.total} ({pct:.1f}%) | "
                f"OK={self.ok}  FAIL={self.failed}  SKIP={self.skipped}  ETA={self.eta()}")


class LogWriter:
    """
    Thread-safe incremental CSV log.
    Writes one row at a time and flushes immediately.
    Never loads the full log into memory — safe for 200k rows.
    """
    def __init__(self, path, resume=False):
        self.path  = path
        self._lock = threading.Lock()
        mode = 'a' if (resume and os.path.exists(path)) else 'w'
        self._fh   = open(path, mode, newline='', encoding='utf-8')
        self._w    = csv.writer(self._fh)
        if mode == 'w':
            self._w.writerow(["id", "name", "page_url", "image_url", "saved_path", "status", "timestamp", "worker"])
        self._fh.flush()

    def write(self, id_, name, page_url, image_url, saved_path, status, worker_id):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._lock:
            self._w.writerow([id_, name, page_url, image_url, saved_path, status, ts, worker_id])
            self._fh.flush()

    def close(self):
        self._fh.close()


class CheckpointManager:
    """
    Tracks completed row IDs in a plain text file (one ID per line).
    On --resume, loads all completed IDs at startup and skips them in the queue.
    Thread-safe — multiple workers mark rows done simultaneously.
    """
    def __init__(self, path, resume=False):
        self.path      = path
        self._lock     = threading.Lock()
        self._done_ids = set()

        if resume and os.path.exists(path):
            with open(path, 'r') as f:
                for line in f:
                    sid = line.strip()
                    if sid:
                        self._done_ids.add(sid)
            print(f"[*] Checkpoint loaded — {len(self._done_ids)} rows already completed, skipping them")
        elif not resume and os.path.exists(path):
            os.remove(path)  # fresh run — clear old checkpoint

        self._fh = open(path, 'a')

    def is_done(self, row_id):
        return str(row_id) in self._done_ids

    def mark_done(self, row_id):
        with self._lock:
            sid = str(row_id)
            self._done_ids.add(sid)
            self._fh.write(sid + '\n')
            self._fh.flush()

    def close(self):
        self._fh.close()


# ── Stateless helper functions (identical logic to bulk_scraper.py) ────────────

def safe_filename(food_id, name):
    clean = re.sub(r'[^\w\s\-]', '', str(name)).strip()
    clean = re.sub(r'\s+', '_', clean)
    return f"{food_id}_{clean}"


def upgrade_to_hires(src, base_origin):
    if src.startswith('//'):
        src = 'https:' + src
    elif src.startswith('/'):
        src = base_origin + src
    parsed = urlparse(src)
    if '/_next/image' in parsed.path:
        qs = parse_qs(parsed.query)
        qs['w'] = ['1920']
        qs['q'] = ['90']
        return urlunparse(parsed._replace(query=urlencode({k: v[0] for k, v in qs.items()})))
    if 'cpcdn.com' in src:
        src = re.sub(r'/\d+x\d+cq\d+/', '/1200x1700cq90/', src)
        src = re.sub(r'\.(jpg|jpeg|png|webp|gif)$', '', src)
    if 'ndtvimg.com' in src or 'ndtv.com/cooks' in src:
        src = re.sub(r'_\d+x\d+_', '_1200x900_', src)
    return src


def get_image_extension(image_url):
    parsed = urlparse(image_url)
    if '/_next/image' in parsed.path:
        inner = parse_qs(parsed.query).get('url', [''])[0]
        path  = urlparse(inner).path
    else:
        path = parsed.path
    ext = os.path.splitext(path)[-1].lower()
    return ext if ext in ['.jpg', '.jpeg', '.png', '.webp', '.gif'] else '.jpg'


def select_best_image(images, og_image, base_origin):
    """
    Apply the same 4-tier selection priority as bulk_scraper.py:
      1. /_next/image in <img> src  -> Archana's Kitchen full-res
      2. cpcdn.com + recipes/ path  -> Cookpad actual recipe photo
      3. og:image (not generated)   -> NDTV and others
      4. First meaningful <img>     -> generic fallback
    """
    for img in images:
        img['original_src'] = img['src']
        img['src'] = upgrade_to_hires(img['src'], base_origin)

    meaningful = [i for i in images if i.get('width', 0) > 100 and i.get('height', 0) > 100]
    pool = meaningful if meaningful else images

    # Priority 1 — Next.js
    next_js = [i for i in pool if '/_next/image' in i['src']]
    if next_js:
        return next_js[0]['src'], next_js[0].get('original_src', next_js[0]['src'])

    # Priority 2 — Cookpad recipe image
    cpcdn = [
        i for i in pool
        if 'cpcdn.com' in (i['src'] or '')
        and 'recipes/' in (i['src'] or '')
        and not any(x in (i['src'] or '') for x in COOKPAD_EXCLUDE)
    ]
    if cpcdn:
        return cpcdn[0]['src'], cpcdn[0].get('original_src', cpcdn[0]['src'])

    # Priority 3 — og:image (real photo only)
    if og_image and 'og-image.' not in og_image:
        return upgrade_to_hires(og_image, base_origin), og_image

    # Priority 4 — first meaningful image
    if pool:
        return pool[0]['src'], pool[0].get('original_src', pool[0]['src'])

    return None, None


# ── Browser management ─────────────────────────────────────────────────────────

def make_browser_context(playwright_instance):
    """Launch a fresh Chromium browser with full anti-bot fingerprint."""
    browser = playwright_instance.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins,site-per-process",
        ]
    )
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1440, "height": 900},
        locale="en-IN",
        timezone_id="Asia/Kolkata",
        extra_http_headers={
            "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language":           "en-IN,en;q=0.9",
            "Accept-Encoding":           "gzip, deflate, br",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest":            "document",
            "Sec-Fetch-Mode":            "navigate",
            "Sec-Fetch-Site":            "none",
            "Sec-Fetch-User":            "?1",
            "Sec-CH-UA":                 '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "Sec-CH-UA-Mobile":          "?0",
            "Sec-CH-UA-Platform":        '"macOS"',
        }
    )
    context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver',           { get: () => undefined });
        Object.defineProperty(navigator, 'plugins',             { get: () => [1, 2, 3, 4, 5] });
        Object.defineProperty(navigator, 'languages',           { get: () => ['en-IN', 'en'] });
        Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
        Object.defineProperty(screen,    'colorDepth',          { get: () => 24 });
        window.chrome = { runtime: {} };
    """)
    page = context.new_page()
    if STEALTH_AVAILABLE:
        stealth_sync(page)
    return browser, context, page


def normalise_url(url):
    """
    Normalise old-format Archana's Kitchen URLs to the current format.
    Old: http://www.archanaskitchen.com/recipe-name
    New: https://www.archanaskitchen.com/recipe/recipe-name
    This avoids a double redirect (http->https + path prefix) that causes
    networkidle to fire before the final page renders.
    """
    parsed = urlparse(url)
    if 'archanaskitchen.com' in parsed.netloc:
        # Upgrade http to https
        scheme = 'https'
        # Add /recipe/ prefix if missing
        path = parsed.path
        if not path.startswith('/recipe/') and path.startswith('/') and len(path) > 1:
            path = '/recipe' + path
        return parsed._replace(scheme=scheme, path=path).geturl()
    return url


def scrape_image_url(page, url):
    """Navigate to URL and return (upgraded_image_url, original_image_url)."""
    # Normalise URL before visiting to avoid multi-hop redirects
    url = normalise_url(url)
    base_origin = urlparse(url).scheme + "://" + urlparse(url).netloc
    try:
        page.goto(url, wait_until="networkidle", timeout=30000)
    except Exception:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
    # Wait longer if page redirected to a different path (content may still be rendering)
    final_url = page.url
    if urlparse(final_url).path != urlparse(url).path:
        page.wait_for_timeout(4000)  # extra wait for redirect targets
    else:
        page.wait_for_timeout(2500)
    base_origin = urlparse(page.url).scheme + "://" + urlparse(page.url).netloc

    images = page.evaluate("""() => {
        function allSrcsFromSrcset(s) {
            if (!s) return [];
            return s.split(',').map(e => {
                const t = e.trim().split(/\\s+/);
                return { url: t[0], w: t[1] ? parseInt(t[1]) : 0 };
            }).filter(e => e.url);
        }
        const results = [];
        document.querySelectorAll('img').forEach(img => {
            const srcset  = img.getAttribute('srcset') || img.getAttribute('data-srcset');
            const dataSrc = img.getAttribute('data-src');
            const src     = img.getAttribute('src');
            let chosen = null;
            if (srcset) {
                const c = allSrcsFromSrcset(srcset).sort((a,b) => b.w - a.w);
                chosen = c[0]?.url || null;
            } else if (dataSrc && !dataSrc.startsWith('data:')) {
                chosen = dataSrc;
            } else if (src && !src.startsWith('data:')) {
                chosen = src;
            }
            if (chosen) results.push({ src: chosen, width: img.naturalWidth, height: img.naturalHeight });
        });
        return results;
    }""")

    og_image = page.evaluate(
        "() => { const m = document.querySelector('meta[property=\"og:image\"]'); return m ? m.content : null; }"
    )

    if not images and not og_image:
        return None, None

    return select_best_image(images, og_image, base_origin)


def download_image(context, image_url, output_path, referer, original_url=None):
    """Download with browser context, fallback to original URL, fallback to urllib."""
    urls_to_try = [image_url]
    if original_url and original_url != image_url:
        urls_to_try.append(original_url)

    last_err = None
    for url in urls_to_try:
        try:
            resp = context.request.get(url, headers={
                "Referer": referer,
                "Accept":  "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            })
            if not resp.ok:
                raise Exception(f"HTTP {resp.status}")
            with open(output_path, 'wb') as f:
                f.write(resp.body())
            return
        except Exception as e:
            last_err = e

    # urllib last resort
    try:
        req = urllib.request.Request(urls_to_try[-1], headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
            "Referer":    referer,
            "Accept":     "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        })
        with urllib.request.urlopen(req, timeout=15) as r:
            with open(output_path, 'wb') as f:
                f.write(r.read())
    except Exception as e:
        raise Exception(f"All download attempts failed. Browser: {last_err} | urllib: {e}")


# ── Worker ─────────────────────────────────────────────────────────────────────

def worker(worker_id, task_queue, output_dir, delay, log_writer, checkpoint, stats, print_lock, existing_files):
    """
    One worker thread. Owns its own Playwright instance and browser.
    Pulls tasks from task_queue until the queue is empty.
    Restarts its browser every BROWSER_RESTART_EVERY rows to prevent memory leak.
    existing_files is a pre-built set of filenames in output_dir — checked in O(1)
    instead of scanning the folder on every row.
    """
    def log(msg):
        with print_lock:
            print(f"  [W{worker_id}] {msg}")

    rows_this_browser = 0

    with sync_playwright() as p:
        browser, context, page = make_browser_context(p)
        log("Browser started")

        while True:
            try:
                task = task_queue.get(timeout=5)
            except Empty:
                break

            food_id, name, url = task['id'], task['name'], task['url']

            # Skip if already done (checkpoint)
            if checkpoint.is_done(food_id):
                stats.record("skip")
                task_queue.task_done()
                continue

            # Skip if output file already exists — O(1) set lookup.
            # existing_files was built once at startup from os.listdir().
            # Avoids scanning the folder on every row which would be O(n^2).
            stem = safe_filename(food_id, name)
            matched = next((f for f in existing_files if f.startswith(stem)), None)
            if matched:
                log(f"SKIP (exists): {matched}")
                checkpoint.mark_done(food_id)
                stats.record("skip")
                task_queue.task_done()
                continue

            # Retry loop with exponential backoff
            success    = False
            last_error = ""
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    image_url, original_url = scrape_image_url(page, url)

                    if not image_url:
                        log(f"No image found: {name}")
                        log_writer.write(food_id, name, url, "", "", "no_image", worker_id)
                        checkpoint.mark_done(food_id)
                        stats.record("failed")
                        success = True  # not retryable
                        break

                    ext       = get_image_extension(image_url)
                    filename  = safe_filename(food_id, name) + ext
                    save_path = os.path.join(output_dir, filename)

                    download_image(context, image_url, save_path, referer=url, original_url=original_url)

                    size_kb = os.path.getsize(save_path) / 1024
                    log(f"OK ({size_kb:.0f}KB): {filename}")
                    log_writer.write(food_id, name, url, image_url, save_path, "ok", worker_id)
                    checkpoint.mark_done(food_id)
                    stats.record("ok")
                    success = True
                    break

                except Exception as e:
                    last_error = str(e)
                    is_rate_limited = "429" in last_error or "Too Many" in last_error
                    is_last_attempt = attempt == MAX_RETRIES

                    if is_rate_limited:
                        log(f"Rate limited (429) — waiting {RATE_LIMIT_WAIT}s before retry")
                        time.sleep(RATE_LIMIT_WAIT)
                    elif not is_last_attempt:
                        wait = RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
                        # Add jitter so multiple workers don't hammer the server in sync
                        wait += random.uniform(0, 2)
                        log(f"Attempt {attempt} failed — retrying in {wait:.1f}s | {last_error[:60]}")
                        time.sleep(wait)

            if not success:
                log(f"FAILED after {MAX_RETRIES} attempts: {name} | {last_error[:80]}")
                log_writer.write(food_id, name, url, "", "", f"error: {last_error[:200]}", worker_id)
                checkpoint.mark_done(food_id)
                stats.record("failed")

            # Polite delay between requests
            time.sleep(delay + random.uniform(0, 0.5))

            rows_this_browser += 1
            task_queue.task_done()

            # Restart browser to release accumulated memory
            if rows_this_browser >= BROWSER_RESTART_EVERY:
                log(f"Restarting browser after {rows_this_browser} rows (memory management)...")
                try:
                    browser.close()
                except Exception:
                    pass
                browser, context, page = make_browser_context(p)
                rows_this_browser = 0
                log("Browser restarted")

        try:
            browser.close()
        except Exception:
            pass
        log("Done")


# ── Progress printer ───────────────────────────────────────────────────────────

def progress_printer(stats, stop_event, print_lock, interval=15):
    """Prints a progress line every `interval` seconds until stop_event is set."""
    while not stop_event.is_set():
        time.sleep(interval)
        with print_lock:
            print(f"\n{stats.line()}\n")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Production bulk image scraper — parallel, resumable, retry-capable.")
    parser.add_argument("file",          help="Path to the CSV or Excel file")
    parser.add_argument("--rows",        type=int,   default=None,  help="Limit rows to process (for testing)")
    parser.add_argument("--workers",     type=int,   default=3,     help="Number of parallel browser workers (default: 3)")
    parser.add_argument("--delay",       type=float, default=1.0,   help="Base delay between requests per worker in seconds (default: 1)")
    parser.add_argument("--resume",      action="store_true",       help="Resume from last checkpoint instead of starting fresh")
    parser.add_argument("--output-dir",  default=None,              help="Output folder (default: named after input file)")
    parser.add_argument("--url-col",     default=None,              help="URL column name override")
    parser.add_argument("--name-col",    default=None,              help="Name column name override")
    parser.add_argument("--id-col",      default=None,              help="ID column name override")
    args = parser.parse_args()

    # ── Resolve columns ────────────────────────────────────────────────────────
    basename = os.path.basename(args.file)
    cfg      = DATASET_CONFIG.get(basename, {})

    url_col  = args.url_col  or cfg.get("url_col")  or "URL"
    name_col = args.name_col or cfg.get("name_col") or "name"
    id_col   = args.id_col   or cfg.get("id_col")   or "foodID"

    if basename in DATASET_CONFIG:
        print(f"[*] Known dataset — config auto-loaded for '{basename}'")
    else:
        print(f"[!] Unknown dataset — url={url_col}, name={name_col}, id={id_col}")

    # ── Read file ──────────────────────────────────────────────────────────────
    print(f"[*] Reading {args.file}...")
    ext = os.path.splitext(args.file)[-1].lower()
    if ext == '.csv':
        df = pd.read_csv(args.file)
    elif ext in ['.xlsx', '.xls']:
        df = pd.read_excel(args.file)
    else:
        print(f"[X] Unsupported file type '{ext}'")
        sys.exit(1)

    print(f"[*] Columns: {list(df.columns)}")

    for col in [url_col, name_col]:
        if col not in df.columns:
            print(f"[X] Column '{col}' not found. Available: {list(df.columns)}")
            sys.exit(1)

    if id_col not in df.columns:
        print(f"[!] ID column '{id_col}' not found — using row number")
        df[id_col] = df.index + 1

    df = df[df[url_col].notna()].reset_index(drop=True)

    total_in_file = len(df)
    if args.rows:
        df = df.head(args.rows)
        print(f"[*] {total_in_file} rows in file. Processing first {len(df)}.")
    else:
        print(f"[*] {total_in_file} rows found. Processing all.")

    # ── Output folder ──────────────────────────────────────────────────────────
    input_basename = os.path.splitext(basename)[0]
    output_dir     = args.output_dir or input_basename
    os.makedirs(output_dir, exist_ok=True)
    print(f"[*] Output folder : {output_dir}/")

    # ── Shared infrastructure ──────────────────────────────────────────────────
    checkpoint_path = os.path.join(output_dir, "_checkpoint.txt")
    log_path        = os.path.join(output_dir, f"{input_basename}_log.csv")

    checkpoint = CheckpointManager(checkpoint_path, resume=args.resume)
    log_writer  = LogWriter(log_path, resume=args.resume)
    stats       = Stats(total=len(df))
    print_lock  = threading.Lock()

    # Build a set of existing filenames once at startup — O(n) one time.
    # Workers check against this set in O(1) instead of scanning the folder
    # on every row, which would be O(n) per row = O(n^2) total for 100k rows.
    existing_files = set(os.listdir(output_dir))
    print(f"[*] Existing files: {len(existing_files)} found in output folder")

    stealth_str = "ON" if STEALTH_AVAILABLE else "OFF (pip install playwright-stealth)"
    print(f"[*] Stealth mode  : {stealth_str}")
    print(f"[*] Workers       : {args.workers}")
    print(f"[*] Delay         : {args.delay}s (+0-0.5s jitter)")
    print(f"[*] Browser restart every {BROWSER_RESTART_EVERY} rows per worker")
    print(f"[*] Max retries   : {MAX_RETRIES} (backoff: {RETRY_BACKOFF_BASE}s, {RETRY_BACKOFF_BASE*2}s, {RETRY_BACKOFF_BASE*4}s)")
    print(f"[*] Resume mode   : {'ON' if args.resume else 'OFF'}\n")

    # ── Fill task queue ────────────────────────────────────────────────────────
    task_queue = Queue()
    for _, row in df.iterrows():
        task_queue.put({
            "id":   row[id_col],
            "name": str(row[name_col]).strip(),
            "url":  str(row[url_col]).strip(),
        })

    # ── Start progress printer ─────────────────────────────────────────────────
    stop_progress = threading.Event()
    progress_thread = threading.Thread(
        target=progress_printer,
        args=(stats, stop_progress, print_lock, 15),
        daemon=True
    )
    progress_thread.start()

    # ── Start workers ──────────────────────────────────────────────────────────
    start_time = time.time()
    threads = []
    for wid in range(1, args.workers + 1):
        t = threading.Thread(
            target=worker,
            args=(wid, task_queue, output_dir, args.delay, log_writer, checkpoint, stats, print_lock, existing_files),
            daemon=True
        )
        t.start()
        threads.append(t)
        time.sleep(2)  # stagger worker startup so they don't all hit the site simultaneously

    # ── Wait for all workers to finish ────────────────────────────────────────
    for t in threads:
        t.join()

    stop_progress.set()
    checkpoint.close()
    log_writer.close()

    # ── Final summary ──────────────────────────────────────────────────────────
    elapsed = time.time() - start_time
    h, m, s = int(elapsed // 3600), int((elapsed % 3600) // 60), int(elapsed % 60)

    print(f"\n{'─'*55}")
    print(f"  Done in {h:02d}:{m:02d}:{s:02d}")
    print(f"  OK      : {stats.ok}")
    print(f"  Failed  : {stats.failed}")
    print(f"  Skipped : {stats.skipped}")
    print(f"  Output  : {os.path.abspath(output_dir)}/")
    print(f"  Log     : {log_path}")
    print(f"  Checkpoint: {checkpoint_path}")
    print(f"{'─'*55}")

    if stats.failed > 0:
        print(f"\n  To retry failed rows: re-run with --resume")
        print(f"  (Successful rows will be skipped automatically)")


if __name__ == "__main__":
    main()

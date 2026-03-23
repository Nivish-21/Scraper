# Recipe Image Scraper

A production-grade web scraper that downloads the highest-quality recipe images from multiple food datasets. Built with Python and Playwright (headless Chromium), it handles bot detection, lazy-loaded images, CDN resolution upgrades, parallel execution, crash recovery, and automatic retry — across three different website architectures.

---

## Scripts

| Script | Purpose |
|---|---|
| `bulk_scraper.py` | Standard scraper — single browser, sequential. For small datasets and test runs. |
| `bulk_scraper_pro.py` | Production scraper — parallel browsers, checkpoint/resume, retry with backoff. For 10k–200k rows. |
| `retry_failed.py` | Post-run repair — removes failed rows from checkpoint, exports `_invalid.xlsx` report of all failed URLs. |
| `diagnose.py` | Debug tool — visits one URL and prints every image found, og:image, and all network image requests. |

---

## Supported Datasets

| File | Site | Rows |
|---|---|---|
| `data.csv` | archanaskitchen.com | ~1,000 |
| `food_recipes.csv` | archanaskitchen.com | ~5,000 |
| `IndianFoodDatasetXLSFinal.xlsx` | archanaskitchen.com | ~1,500 |
| `Indonesian_Food_Recipes.csv` | cookpad.com | ~15,000 |
| `INDIAN FOOD RECIPE.csv` | food.ndtv.com | ~2,000 |

---

## Installation

### macOS
```bash
pip install playwright pandas openpyxl
python -m playwright install chromium
pip install playwright-stealth          # optional — recommended
```

### Ubuntu / Linux
```bash
pip install playwright pandas openpyxl
python -m playwright install chromium
python -m playwright install-deps chromium   # required on Linux
pip install playwright-stealth               # optional — recommended
```

### Windows
```bash
pip install playwright pandas openpyxl
python -m playwright install chromium
pip install playwright-stealth          # optional — recommended
```

> `playwright-stealth` activates automatically if installed. No extra flags needed.  
> The terminal will confirm: `[*] Stealth mode : ON`

---

## Usage

### Standard scraper — small/medium datasets

```bash
python bulk_scraper.py data.csv
python bulk_scraper.py data.csv --rows 10        # test run
python bulk_scraper.py data.csv --delay 2        # slower, less likely to get blocked
```

### Production scraper — large datasets

```bash
# Run
python bulk_scraper_pro.py Indonesian_Food_Recipes.csv --workers 3

# Test run first
python bulk_scraper_pro.py Indonesian_Food_Recipes.csv --workers 3 --rows 20

# Resume after crash or interruption
python bulk_scraper_pro.py Indonesian_Food_Recipes.csv --workers 3 --resume
```

**Worker count guide by RAM:**

| Mac RAM | `--workers` |
|---|---|
| 8 GB | 3 |
| 16 GB | 5 |
| 32 GB | 10 |

### Retry failed rows

```bash
# After a run, retry all no_image and error rows
python retry_failed.py Indonesian_Food_Recipes

# Then resume
python bulk_scraper_pro.py Indonesian_Food_Recipes.csv --workers 3 --resume
```

`retry_failed.py` also accepts the dataset filename directly:
```bash
python retry_failed.py Indonesian_Food_Recipes.csv
```

### Diagnose a failing URL

```bash
python diagnose.py "https://www.archanaskitchen.com/masala-karela-recipe"
```

---

## Output

Running on `IndianFoodDatasetXLSFinal.xlsx` produces:

```
IndianFoodDatasetXLSFinal/
    1_Masala_Karela.jpg
    2_Spicy_Tomato_Rice.jpg
    3_Ragi_Semiya_Upma.jpg
    IndianFoodDatasetXLSFinal_log.xlsx    ← bulk_scraper.py
    IndianFoodDatasetXLSFinal_log.csv     ← bulk_scraper_pro.py
    _checkpoint.txt                        ← resume tracker
    _invalid.xlsx                          ← created by retry_failed.py
```

**Log columns:** `id`, `name`, `page_url`, `image_url`, `saved_path`, `status`, `timestamp`, `worker`

**Status values:** `ok` / `no_image` / `error: <message>`

**`_invalid.xlsx`** has two sheets:
- `Failed Rows` — full details of every failed row
- `URLs to Check` — clean list of id, name, page_url, status for manual review

---

## All Options

### `bulk_scraper_pro.py`

| Flag | Default | Description |
|---|---|---|
| `--workers N` | 3 | Parallel browser instances |
| `--rows N` | all | Limit rows (for testing) |
| `--delay N` | 1.0 | Seconds between requests per worker |
| `--resume` | off | Continue from last checkpoint |
| `--output-dir` | named after file | Override output folder |
| `--url-col` | auto | Override URL column name |
| `--name-col` | auto | Override name column name |
| `--id-col` | auto | Override ID column name |

### `bulk_scraper.py`

| Flag | Default | Description |
|---|---|---|
| `--rows N` | all | Limit rows |
| `--delay N` | 1.0 | Seconds between requests |
| `--output-dir` | named after file | Override output folder |

---

## How It Works

### Scraping pipeline (every row)

**1. URL normalisation**  
Old-format Archana's Kitchen URLs (`http://www.archanaskitchen.com/recipe-name`) are rewritten to the current format (`https://www.archanaskitchen.com/recipe/recipe-name`) before visiting. This eliminates a double redirect that caused images to load too late.

**2. Page load**  
A real headless Chromium browser navigates to the URL and waits for all network activity to settle (`networkidle`). Falls back to `domcontentloaded` + extended wait for heavy pages. The browser uses a spoofed fingerprint to avoid bot detection.

**3. Image extraction**  
JavaScript runs inside the live browser and inspects every `<img>` tag. Attributes are checked in priority order: `srcset` (largest width picked) → `data-src` (lazy-load) → `src`. The `og:image` meta tag is read separately.

**4. CDN resolution upgrade**

| Site | Detection | Upgrade applied |
|---|---|---|
| Archana's Kitchen | `/_next/image` in URL | `w=1920&q=90` query params |
| Cookpad | `cpcdn.com` in hostname | `/1200x1700cq90/` + extension stripped |
| NDTV | `ndtvimg.com` in hostname | `_1200x900_` in filename |

**5. Image selection — 4-tier priority**

| # | Condition | Site | Why |
|---|---|---|---|
| 1 | `/_next/image` in `<img>` src | Archana's Kitchen | srcset has full-res URL; og:image points to old low-res CDN |
| 2 | `cpcdn.com` + `recipes/` in src (excluding avatars/steps) | Cookpad | og:image is sometimes a generated text card, not a food photo |
| 3 | `og:image` (not a generated card) | NDTV + others | Reliable for sites where og:image is the actual recipe photo |
| 4 | First `<img>` > 100×100px | Fallback | Generic fallback for unknown sites |

**6. Download fallback chain**  
(1) Upgraded URL via browser context → (2) original URL via browser context → (3) original URL via urllib

---

### Production features (`bulk_scraper_pro.py` only)

**Parallel workers**  
Each worker is a Python thread owning its own Playwright browser. Workers pull tasks from a shared `queue.Queue` — natural work-stealing load balancing. Workers start 2 seconds apart to avoid request bursts.

**Checkpoint / resume**  
Every completed row ID is written to `_checkpoint.txt` immediately. On `--resume`, the file is loaded into a Python `set` for O(1) lookups — 99,000 already-done rows are skipped in milliseconds, not minutes.

**Retry with exponential backoff**  
Up to 3 attempts per row. Wait times: 5s, 10s, 20s. A 429 (rate-limit) response triggers a 60-second pause. ±0.5s jitter prevents multiple workers from retrying in sync.

**Browser restart every 500 rows**  
Each worker restarts its Chromium browser after 500 pages to release accumulated memory (DOM nodes, JS heap, cached assets). RAM management only — no effect on traffic patterns.

**O(1) duplicate check**  
`os.listdir(output_dir)` is called once at startup and stored as a `set`. Per-row check is a single hash lookup — O(1) instead of O(n²) for 100k rows.

**Incremental CSV log**  
One row written and flushed per result. No data lost on crash. Never loads the full log into memory.

---

### `retry_failed.py` — O(1) data structures

All operations use hash-based data structures to stay efficient at scale:

```python
# Checkpoint loaded into a set — O(1) lookup
checkpoint_ids = set(...)           # {"1", "2", "1495"}
"1128" in checkpoint_ids            # O(1) hash lookup, not O(n) scan

# Log loaded into a dict — O(1) access by ID
all_rows = {"1": row1, "2": row2}   # keyed by ID
all_rows["1128"]                    # O(1) direct access

# Separating rows — O(n) total, O(1) per check
for row_id, row in all_rows.items():
    if row_id in failed_ids:        # O(1) set lookup inside O(n) loop
        failed_rows.append(row)

# Checkpoint update — set difference O(n), not O(n^2)
new_checkpoint_ids = checkpoint_ids - failed_ids
```

---

## Adding a New Dataset

Add one entry to `DATASET_CONFIG` in both `bulk_scraper.py` and `bulk_scraper_pro.py`:

```python
DATASET_CONFIG = {
    ...
    "new_file.csv": {"url_col": "link", "name_col": "title", "id_col": None},
}
```

Set `id_col` to `None` if the file has no ID column — row number is used automatically.

---

## Adding a New Site

1. Run `diagnose.py` on a sample URL to see the CDN URL pattern
2. Add a CDN upgrade rule to `upgrade_to_hires()` in both scraper files
3. If `og:image` is unreliable, add a site-specific selection condition to `select_best_image()` above the `og:image` fallback
4. Add the file to `DATASET_CONFIG`

---

## Troubleshooting

| Problem | Fix |
|---|---|
| 403 errors on all rows | `pip install playwright-stealth` |
| Frequent 429 errors | Increase `--delay` to 2 or 3 |
| `no_image` rows in log | Run `retry_failed.py` then `--resume` |
| Images < 10KB | Likely a CAPTCHA page — run `retry_failed.py` then `--resume` after a delay |
| Script crashed | Re-run with `--resume` |
| RAM pressure turns yellow | Reduce `--workers` by 1 |
| Column not found | Pass `--url-col` and `--name-col` with correct names |
| Wrong image downloaded | Run `diagnose.py` on the URL to inspect what the browser sees |

---

## Tech Stack

- **Python 3.8+**
- **Playwright** — headless Chromium browser automation
- **playwright-stealth** — bot detection bypass
- **pandas** — dataset reading and Excel report writing
- **openpyxl** — Excel engine for multi-sheet output
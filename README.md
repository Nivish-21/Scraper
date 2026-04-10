# 🍽️ Recipe Image Scraper

A production-grade bulk image scraper built for large food recipe datasets (10,000–200,000+ rows). Give it a CSV or Excel file, walk away, and come back to a folder of downloaded dish images.

Built with Python and Playwright. Handles parallel workers, automatic retries, crash recovery, and intelligent image selection — all from a single command.

---

## What It Does

Given a dataset of recipes with page URLs or direct image URLs, this tool:

- Visits each recipe webpage and finds the best quality dish photo
- Skips logos, avatars, social sharing cards, and step-by-step thumbnails using a scoring engine
- Downloads images in parallel across multiple browser workers
- Recovers automatically from failures — retries blocked rows, resumes after crashes
- Works across dozens of recipe sites without manual configuration per site

---

## Supported Dataset Shapes

| Dataset shape | Behaviour |
|---|---|
| Recipe page URLs only | Visits each page, extracts and scores all images, downloads best |
| Direct image URLs only | Downloads directly — fastest mode, no browser needed |
| Both URL columns | Tries direct image first, falls back to page visit if blocked |

The scraper auto-detects which columns to use. No manual column mapping needed for new datasets.

---

## Quick Start

### 1. Clone the repo

```bash
git clone https://github.com/your-username/recipe-image-scraper.git
cd recipe-image-scraper
```

### 2. Install dependencies

```bash
pip install playwright pandas openpyxl
playwright install chromium
```

Optional but recommended — reduces bot detection on heavily protected sites:

```bash
pip install playwright-stealth
```

### 3. Run

```bash
python run_image_pipeline.py your_dataset.csv
```

That's it. The pipeline runs the scraper, retries failures automatically, and prints a final summary.

---

## Usage

### Basic

```bash
python run_image_pipeline.py data.csv
```

### With options

```bash
python run_image_pipeline.py data.csv --workers 6 --delay 0.5
```

### Test on a small sample first

```bash
python run_image_pipeline.py data.csv --rows 30
```

### All options

```
python run_image_pipeline.py <file> [options]

Arguments:
  file                    Path to CSV or Excel dataset

Options:
  --workers N             Parallel browser workers (default: 4)
  --delay N               Seconds between requests per worker (default: 1.0)
  --rows N                Limit rows to process — useful for testing
  --output-dir PATH       Override the output folder name
  --url-col NAME          Override the page URL column name
  --name-col NAME         Override the recipe name column name
  --id-col NAME           Override the ID column name
  --image-col NAME        Override the direct image URL column name
  --max-retry-rounds N    Automatic retry attempts after first pass (default: 2)
```

---

## Output

After a run, you'll find:

```
your_dataset/
├── 1_Butter_Chicken.jpg
├── 2_Dal_Makhani.jpg
├── 3_Palak_Paneer.webp
├── ...
├── your_dataset_log.csv      ← result for every row (status, URL, path, timestamp)
├── _checkpoint.txt           ← completed row IDs for resume support
└── _invalid.xlsx             ← failed rows exported for manual review (if any)
```

### Log CSV columns

| Column | Description |
|---|---|
| `id` | Row ID from the dataset |
| `name` | Recipe name |
| `page_url` | Recipe page visited |
| `image_url` | Image URL downloaded |
| `saved_path` | Local file path |
| `status` | `ok`, `no_image`, or `error: ...` |
| `timestamp` | When the row was processed |
| `worker` | Which worker thread handled it |

---

## Resuming After a Crash

If a run is interrupted, restart with `--resume` and it picks up exactly where it stopped:

```bash
python scraper.py data.csv --resume
```

Completed rows are skipped instantly. There is no re-downloading of already-successful rows.

---

## Retrying Failed Rows Manually

If you want to inspect and retry failures yourself rather than using the pipeline:

```bash
# See what failed and prepare them for retry
python retry_failed.py data.csv

# Then re-run with resume
python scraper.py data.csv --workers 4 --resume
```

This removes failed row IDs from the checkpoint, cleans the log, and exports `_invalid.xlsx` with all failed rows and their page URLs for manual inspection.

---

## Debugging a Specific URL

If a particular recipe site keeps failing, use the diagnostic tool:

```bash
python diagnose.py https://www.example.com/recipe/chicken-curry
```

This visits the URL, collects every image candidate from the page, and prints the full ranked list with scores and sources. Use this to understand why the wrong image is being selected or why a page is failing before modifying any code.

Sample output:
```
[*] Best candidate: https://cdn.example.com/recipes/chicken-curry-1200x900.jpg
    source=dom_image score=114

[*] Top candidates:
  [1] score=114 source=dom_image     https://cdn.example.com/recipes/chicken-curry-1200x900.jpg
  [2] score= 92 source=meta_image    https://cdn.example.com/og/chicken-curry.jpg
  [3] score= 45 source=dom_image     https://cdn.example.com/avatars/author.jpg
```

---

## How the Image Scoring Works

The scraper collects every image from a page and scores each one rather than blindly picking the first. Scoring factors include:

- **Where it was found** — DOM images score higher than meta tags; meta tags higher than network intercepts
- **Dimensions** — images over 1 megapixel get a large bonus; thumbnails under 15,000 pixels get penalised
- **URL signals** — paths containing "recipe", "hero", "food", "1200" score up; "avatar", "logo", "icon", "banner" score down sharply
- **Social card detection** — "og-image", "share", "twitter", "facebook" in the URL drop the score by 35 points
- **Name match** — if the recipe name "Butter Chicken" appears in the image's alt text, filename, or surrounding attributes, the score increases
- **Site-specific bonuses** — Cookpad CDN recipe paths and Archana's Kitchen Next.js image format both get explicit score boosts

After selection, the downloaded file's actual bytes are inspected to confirm it's a real image — not an error page in disguise.

---

## Known Limitations

**Direct image 403 with no page URL fallback**
If a dataset has only direct image URLs and those URLs return `403 Forbidden`, the scraper cannot recover. This happens when the image host requires a valid session cookie or referer header from a prior page visit. The only fix is to enrich the dataset with recipe page URLs.

**Cloudflare / aggressive anti-bot protection**
Sites using Cloudflare's JS challenge or similar services will block even stealth browser requests. These are documented hard limits. Installing `playwright-stealth` helps on moderately protected sites but is not a bypass for enterprise-grade bot detection.

**Pages that never fully render**
Some recipe sites load content through infinite scroll or require user interaction. The scraper will still capture whatever images are present after the initial page load.

---

## Project Structure

```
recipe-image-scraper/
├── run_image_pipeline.py   ← Single entry point — run this
├── scraper.py              ← Parallel bulk scraping engine
├── image_discovery.py      ← Image extraction, scoring, and download logic
├── retry_failed.py         ← Failed-row cleanup and retry preparation
├── diagnose.py             ← Single-URL debugging tool
└── README.md
```

---

## Requirements

- Python 3.9+
- `playwright` — browser automation
- `pandas` — dataset reading (CSV and Excel)
- `openpyxl` — Excel file support
- `playwright-stealth` *(optional)* — reduces bot detection fingerprint

---

## Performance

With default settings (4 workers, 1s delay):

| Dataset size | Approximate time |
|---|---|
| 1,000 rows | ~15–30 minutes |
| 10,000 rows | ~2–5 hours |
| 50,000 rows | ~10–25 hours |

Times vary significantly by site speed, network conditions, and how many rows fall into fast direct-download mode vs. full page crawl mode. Direct-image-only datasets are 5–10x faster than page-crawl datasets.

Increase `--workers` to speed up. Decrease `--delay` cautiously — too low and you risk rate limiting or IP blocks from target sites.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup instructions, code structure notes, and how to add support for a new recipe site.

---

## Licence

MIT

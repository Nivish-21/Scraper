# Recipe Image Scraper

Bulk image downloader for food recipe datasets. Give it a CSV or Excel file with recipe URLs, it downloads the best dish photo for every row — in parallel, with automatic retries and crash recovery.

Built with Python and Playwright.

---

## What it does

- Visits recipe pages and finds the main dish photo (skips logos, avatars, icons)
- Works with direct image URLs too — no browser needed, much faster
- Runs multiple workers in parallel
- Resumes automatically if interrupted
- Retries failed rows without re-downloading successful ones

---

## Setup

```bash
pip install -r requirements.txt
playwright install chromium
```

Optional but recommended (helps with bot detection):
```bash
pip install playwright-stealth
```

---

## Usage

```bash
python run_image_pipeline.py your_dataset.csv
```

Test on a small sample first:
```bash
python run_image_pipeline.py your_dataset.csv --rows 30
```

More workers = faster (be careful with aggressive sites):
```bash
python run_image_pipeline.py your_dataset.csv --workers 6
```

All options:
```
--workers N            Parallel browser workers (default: 4)
--delay N              Seconds between requests per worker (default: 1.0)
--rows N               Limit rows — good for testing
--output-dir PATH      Custom output folder
--url-col NAME         Override page URL column name
--name-col NAME        Override recipe name column name
--id-col NAME          Override ID column name
--image-col NAME       Override direct image URL column name
--max-retry-rounds N   Retry rounds after first pass (default: 2)
```

---

## Output

```
your_dataset/
├── 1_Butter_Chicken.jpg
├── 2_Dal_Makhani.webp
├── ...
├── your_dataset_log.csv   ← status, URLs, file paths for every row
├── _checkpoint.txt        ← tracks completed rows for resume
└── _invalid.xlsx          ← failed rows exported for review
```

---

## Debugging a specific URL

```bash
python diagnose.py https://www.example.com/recipe/some-dish
```

Prints every image found on the page with scores — useful when a site keeps failing or selects the wrong image.

---

## Dataset format

The scraper auto-detects columns. It looks for a page URL column, a recipe name column, and optionally a direct image URL column. If auto-detection is wrong, pass column names manually:

```bash
python run_image_pipeline.py data.csv --url-col "RecipeLink" --name-col "DishName" --image-col "PhotoURL"
```

Supported file types: `.csv`, `.xlsx`, `.xls`

---

## Known limits

- If a dataset has only direct image URLs and the host returns `403 Forbidden`, there is no fallback — you need a recipe page URL column in the dataset too
- Sites behind Cloudflare JS challenge will block requests even in stealth mode

---

## Files

```
run_image_pipeline.py   ← entry point, run this
scraper.py              ← parallel worker engine
image_discovery.py      ← image scoring and extraction logic
retry_failed.py         ← cleans up failed rows for retry
diagnose.py             ← single-URL debug tool
```

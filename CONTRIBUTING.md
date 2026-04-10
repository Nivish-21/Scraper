# Contributing

Thanks for looking at this project. This document covers how to set up the development environment, how the codebase is structured, and how to add support for a new recipe site or dataset.

---

## Development Setup

```bash
git clone https://github.com/your-username/recipe-image-scraper.git
cd recipe-image-scraper

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install playwright pandas openpyxl playwright-stealth
playwright install chromium
```

Run a quick sanity check against a known URL:

```bash
python diagnose.py https://food.ndtv.com/recipe-3-ingredient-onion-pickle-955910
```

You should see a ranked list of image candidates with scores. If that works, the setup is correct.

---

## Codebase Overview

```
run_image_pipeline.py   ← Orchestrator. Runs scraper → retry → resume loop.
scraper.py              ← Parallel worker engine. Owns threading, checkpointing, logging.
image_discovery.py      ← Image extraction and scoring. Used by both scraper and diagnose.
retry_failed.py         ← Reads log CSV, cleans checkpoint, exports _invalid.xlsx.
diagnose.py             ← Single-URL debug tool. No side effects.
```

### Dependency direction

```
run_image_pipeline.py
  └── scraper.py
        └── image_discovery.py
  └── retry_failed.py

diagnose.py
  └── image_discovery.py
```

`image_discovery.py` has no imports from the other project files. It is the shared foundation. `scraper.py` imports from it. `diagnose.py` imports from it independently. Never import `scraper.py` into `image_discovery.py` — that would create a circular dependency.

---

## Adding Support for a Known Dataset

If you have a new CSV or Excel file that the auto-detection gets wrong, add an entry to `DATASET_CONFIG` in `scraper.py`:

```python
DATASET_CONFIG = {
    # existing entries...
    "my_new_dataset.csv": {
        "url_col":   "RecipePageLink",   # column with the recipe page URL
        "name_col":  "DishName",         # column with the recipe name
        "id_col":    "RecipeID",         # column with a unique row ID (or None)
        "image_col": "PhotoURL",         # column with a direct image URL (or None)
    },
}
```

The key is the exact filename (basename only, not the full path). If `id_col` is `None`, row numbers are used as IDs. If `image_col` is `None`, the scraper goes straight to page crawl mode.

---

## Adding Support for a New Recipe Site

If a site consistently produces wrong images or fails silently, the fix lives in one of two places depending on what kind of fix is needed.

### Fix 1 — URL normalisation (redirect chains)

If the site's URLs redirect before the page loads (like old Archana's Kitchen links), add a normalisation rule to `normalise_url()` in `scraper.py` and `normalise_page_url()` in `image_discovery.py`:

```python
def normalise_url(url):
    parsed = urlparse(url)
    if 'example-recipe-site.com' in parsed.netloc:
        # Force HTTPS
        # Add a required path prefix
        path = parsed.path
        if not path.startswith('/recipes/'):
            path = '/recipes' + path
        return parsed._replace(scheme='https', path=path).geturl()
    return url
```

Both functions need the same logic because `scraper.py` and `image_discovery.py` normalise URLs independently.

### Fix 2 — CDN URL upgrade (low-res to high-res)

If the site serves a thumbnail URL that can be rewritten to a full-size URL by changing part of the path or query string, add a rewrite rule to `upgrade_to_hires()` in both `scraper.py` and `image_discovery.py`:

```python
def upgrade_to_hires(src, base_origin):
    # ... existing rules ...
    if 'cdn.example-site.com' in src:
        # Replace /thumb/300x200/ with /full/1200x800/
        src = re.sub(r'/thumb/\d+x\d+/', '/full/1200x800/', src)
    return src
```

### Fix 3 — Scoring adjustment for a specific CDN pattern

If the site's image URLs contain words that trigger the negative URL hint penalties (like "thumbnail" in the path when the image is actually full-size), add a positive override to `_score_candidate()` in `image_discovery.py`:

```python
# After the existing NEGATIVE_URL_HINTS block:
if 'example-site-cdn.com' in url and '/photos/' in url:
    score += 25  # override the false negative penalty for this CDN
```

### Fix 4 — Cookpad-style exclusion rules

If a site serves recipe images and author/step images from the same CDN and they're hard to distinguish by score alone, add an exclusion tuple (similar to `COOKPAD_EXCLUDE` at the top of `image_discovery.py`):

```python
EXAMPLE_SITE_EXCLUDE = ('/user-avatars/', '/step-images/', '/comments/')
```

Then reference it in `_score_candidate()` with a large negative score when matched.

---

## Testing Your Changes

Always test with `diagnose.py` before running a full batch:

```bash
python diagnose.py "https://www.the-site-youre-fixing.com/some-recipe"
```

Check that:
1. The best candidate shown is the actual dish hero image
2. Its score is clearly higher than the second candidate (by at least 15–20 points)
3. No logos, avatars, or social cards appear in the top 3

Then run a small batch (30 rows) against a real dataset to confirm:

```bash
python run_image_pipeline.py your_dataset.csv --rows 30
```

Review the downloaded images and the log CSV. Check that `status=ok` rows have the correct image in `image_url`, and that any failures have a useful error message.

---

## Coding Standards

- **No new dependencies** without a strong reason. The current stack (Playwright, pandas, openpyxl) covers all needs.
- **Thread safety in scraper.py** — any shared state accessed by multiple workers must go through a `threading.Lock()`. The existing `Stats`, `LogWriter`, and `CheckpointManager` classes show the pattern.
- **No navigation in image_discovery.py methods that aren't `diagnose()`** — scoring, extraction, and download methods must not call `page.goto()`. Only `diagnose()` navigates.
- **Fail clearly** — if a row can't be processed for a structural reason (no URL, blocked host, unsupported content type), log an informative error string and move on. Don't let worker threads hang silently.
- **Keep `diagnose.py` side-effect free** — it must never write files, modify the log, or touch the checkpoint. It is a read-only inspection tool.

---

## Reporting Issues

When opening an issue, include:

1. The output of `python diagnose.py <failing-url>`
2. The error message from the log CSV (the full `status` field value)
3. Whether the dataset has page URLs, direct image URLs, or both
4. The Python version and OS

This information makes the root cause almost always immediately apparent.

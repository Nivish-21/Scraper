#!/usr/bin/env python3
"""
Diagnostic script — visits a URL and prints everything it finds.
Tries playwright-stealth first, falls back to standard Playwright.

Usage:
  python diagnose.py <url>
"""

import sys
from playwright.sync_api import sync_playwright

url = sys.argv[1] if len(sys.argv) > 1 else "https://food.ndtv.com/recipe-3-ingredient-onion-pickle-955910"

# Try to import stealth — optional dependency
try:
    from playwright_stealth import stealth_sync
    STEALTH_AVAILABLE = True
    print("[*] playwright-stealth found — stealth mode ON")
except ImportError:
    STEALTH_AVAILABLE = False
    print("[!] playwright-stealth not installed — running without stealth")
    print("    Install with: pip install playwright-stealth")

print(f"\n{'='*60}")
print(f"Diagnosing: {url}")
print(f"{'='*60}\n")

intercepted_images = []

with sync_playwright() as p:
    browser = p.chromium.launch(
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
            "Accept":                  "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language":         "en-IN,en;q=0.9",
            "Accept-Encoding":         "gzip, deflate, br",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest":          "document",
            "Sec-Fetch-Mode":          "navigate",
            "Sec-Fetch-Site":          "none",
            "Sec-Fetch-User":          "?1",
            "Sec-CH-UA":               '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "Sec-CH-UA-Mobile":        "?0",
            "Sec-CH-UA-Platform":      '"macOS"',
        }
    )

    # Manual fingerprint patches (always applied)
    context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver',  { get: () => undefined });
        Object.defineProperty(navigator, 'plugins',    { get: () => [1, 2, 3, 4, 5] });
        Object.defineProperty(navigator, 'languages',  { get: () => ['en-IN', 'en'] });
        Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
        Object.defineProperty(screen, 'colorDepth',   { get: () => 24 });
        window.chrome = { runtime: {} };
    """)

    page = context.new_page()

    # Apply stealth on top if available — patches ~20 additional fingerprint vectors
    if STEALTH_AVAILABLE:
        stealth_sync(page)

    def on_request(request):
        if any(x in request.url for x in ['.jpg', '.jpeg', '.png', '.webp', '.gif', 'cpcdn', 'image', 'ndtvimg']):
            intercepted_images.append(request.url)

    page.on("request", on_request)

    print("[*] Loading page...")
    try:
        response = page.goto(url, wait_until="networkidle", timeout=30000)
        print(f"[*] HTTP status   : {response.status}")
    except Exception as e:
        print(f"[!] networkidle timed out — trying domcontentloaded...")
        try:
            response = page.goto(url, wait_until="domcontentloaded", timeout=30000)
            print(f"[*] HTTP status   : {response.status}")
        except Exception as e2:
            print(f"[X] Failed to load page: {e2}")
            browser.close()
            sys.exit(1)

    print(f"[*] Final URL     : {page.url}")
    page.wait_for_timeout(3000)
    print(f"[*] Page title    : {page.title()[:80]}")

    images = page.evaluate("""() => {
        return Array.from(document.querySelectorAll('img')).map(img => ({
            src:     img.getAttribute('src'),
            dataSrc: img.getAttribute('data-src'),
            srcset:  img.getAttribute('srcset'),
            width:   img.naturalWidth,
            height:  img.naturalHeight,
            alt:     img.alt
        }));
    }""")

    print(f"\n[*] Total <img> tags found: {len(images)}")
    for idx, im in enumerate(images[:15]):  # cap at 15 for readability
        print(f"\n  [{idx}] src     : {im['src']}")
        print(f"       data-src : {im['dataSrc']}")
        srcset_preview = im['srcset'][:80] + '...' if im['srcset'] and len(im['srcset']) > 80 else im['srcset']
        print(f"       srcset   : {srcset_preview}")
        print(f"       size     : {im['width']}x{im['height']}")
        print(f"       alt      : {im['alt']}")
    if len(images) > 15:
        print(f"\n  ... and {len(images) - 15} more")

    og = page.evaluate(
        "() => { const m = document.querySelector('meta[property=\"og:image\"]'); return m ? m.content : null; }"
    )
    print(f"\n[*] og:image meta : {og}")

    print(f"\n[*] Network image requests intercepted:")
    if intercepted_images:
        for img_url in intercepted_images[:20]:
            print(f"  {img_url}")
    else:
        print("  (none)")

    browser.close()

print(f"\n{'='*60}")
print("Diagnosis complete.")
print(f"{'='*60}\n")

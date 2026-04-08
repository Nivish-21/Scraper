#!/usr/bin/env python3
"""
Diagnostic script for the shared image discovery engine.

Visits one URL, gathers diagnose-style image evidence, and prints the ranked
image candidates that the production scraper will try.
"""

import sys

from playwright.sync_api import sync_playwright

from image_discovery import ImageDiscoveryEngine

try:
    from playwright_stealth import stealth_sync
    STEALTH_AVAILABLE = True
except ImportError:
    STEALTH_AVAILABLE = False


def make_browser_context(playwright_instance):
    browser = playwright_instance.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins,site-per-process",
        ],
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
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-IN,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Sec-CH-UA": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"macOS"',
        },
    )
    context.add_init_script(
        """
        Object.defineProperty(navigator, 'webdriver',  { get: () => undefined });
        Object.defineProperty(navigator, 'plugins',    { get: () => [1, 2, 3, 4, 5] });
        Object.defineProperty(navigator, 'languages',  { get: () => ['en-IN', 'en'] });
        Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
        Object.defineProperty(screen, 'colorDepth',   { get: () => 24 });
        window.chrome = { runtime: {} };
        """
    )
    page = context.new_page()
    if STEALTH_AVAILABLE:
        stealth_sync(page)
    return browser, context, page


def main():
    url = sys.argv[1] if len(sys.argv) > 1 else "https://food.ndtv.com/recipe-3-ingredient-onion-pickle-955910"

    if STEALTH_AVAILABLE:
        print("[*] playwright-stealth found — stealth mode ON")
    else:
        print("[!] playwright-stealth not installed — running without stealth")
        print("    Install with: pip install playwright-stealth")

    print(f"\n{'=' * 60}")
    print(f"Diagnosing: {url}")
    print(f"{'=' * 60}\n")

    with sync_playwright() as p:
        browser, context, page = make_browser_context(p)
        discovery = ImageDiscoveryEngine(page, context)

        try:
            result = discovery.diagnose(url)
        finally:
            browser.close()

    print(f"[*] Final URL     : {result['final_url']}")
    print(f"[*] Page title    : {result['title'][:100]}")
    print(f"[*] DOM images    : {result['dom_image_count']}")
    print(f"[*] Network hits  : {len(result['network_images'])}")

    best = result.get("best_candidate")
    if best:
        print(f"\n[*] Best candidate: {best['url']}")
        print(f"    source={best['source']} score={best['score']}")
    else:
        print("\n[!] No image candidate found")

    print("\n[*] Top candidates:")
    for idx, candidate in enumerate(result["candidates"][:10], start=1):
        print(f"  [{idx}] score={candidate['score']:>3} source={candidate['source']:<13} {candidate['url']}")

    if result["network_images"]:
        print("\n[*] Sample network image requests:")
        for candidate in result["network_images"][:15]:
            print(f"  {candidate}")

    print(f"\n{'=' * 60}")
    print("Diagnosis complete.")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()

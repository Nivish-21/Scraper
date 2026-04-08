#!/usr/bin/env python3
"""
Generic image discovery engine for recipe-like web pages and mixed datasets.

The goal is to avoid site-by-site refactors by combining:
  - direct image URLs from the dataset
  - diagnose-style page inspection
  - heuristic ranking across DOM, meta, JSON-LD, and network requests
"""

import json
import os
import re
import ssl
import struct
import urllib.request
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse


# Keep the same macOS urllib workaround used by the production scraper.
ssl._create_default_https_context = ssl._create_unverified_context


NEGATIVE_URL_HINTS = (
    "avatar", "logo", "icon", "sprite", "placeholder", "thumb", "thumbnail",
    "profile", "user", "comment", "step", "rating", "banner", "badge",
    "ads", "advert", "pixel", "emoji", "favicon", "og-image", "share",
)

POSITIVE_URL_HINTS = (
    "recipe", "recipes", "hero", "food", "dish", "meal", "upload", "original",
    "full", "large", "1200", "1600", "1920",
)

SOCIAL_CARD_HINTS = (
    "social", "share", "sharing", "twitter", "facebook", "opengraph", "og-image",
)

STOPWORDS = {
    "the", "and", "with", "recipe", "recipes", "style", "for", "from", "your",
    "you", "our", "this", "that", "into", "using", "made", "make", "food",
}

IMAGE_META_KEYS = (
    "og:image",
    "og:image:url",
    "twitter:image",
    "twitter:image:src",
    "image",
)

COOKPAD_EXCLUDE = ("guest_user", "/avatar.", "/comments/", "/steps/", "/users/")


def normalise_page_url(url):
    """
    Normalise old-format Archana's Kitchen URLs to current format to reduce
    redirect chains before the page fully renders.
    """
    parsed = urlparse(url)
    if "archanaskitchen.com" in parsed.netloc:
        scheme = "https"
        path = parsed.path
        if not path.startswith("/recipe/") and path.startswith("/") and len(path) > 1:
            path = "/recipe" + path
        return parsed._replace(scheme=scheme, path=path).geturl()
    return url


def upgrade_to_hires(src, base_origin):
    if not src:
        return src
    if src.startswith("//"):
        src = "https:" + src
    elif src.startswith("/"):
        src = urljoin(base_origin, src)

    parsed = urlparse(src)
    if "/_next/image" in parsed.path:
        qs = parse_qs(parsed.query)
        qs["w"] = ["1920"]
        qs["q"] = ["90"]
        return urlunparse(parsed._replace(query=urlencode({k: v[0] for k, v in qs.items()})))
    if "cpcdn.com" in src:
        src = re.sub(r"/\d+x\d+cq\d+/", "/1200x1700cq90/", src)
        src = re.sub(r"\.(jpg|jpeg|png|webp|gif)$", "", src, flags=re.IGNORECASE)
    if "ndtvimg.com" in src or "ndtv.com/cooks" in src:
        src = re.sub(r"_\d+x\d+_", "_1200x900_", src)
    return src


def get_image_extension(image_url):
    parsed = urlparse(image_url)
    if "/_next/image" in parsed.path:
        inner = parse_qs(parsed.query).get("url", [""])[0]
        path = urlparse(inner).path
    else:
        path = parsed.path
    ext = os.path.splitext(path)[-1].lower()
    return ext if ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"] else ".jpg"


def _normalise_asset_url(src, base_url):
    if not src or not isinstance(src, str):
        return None
    src = src.strip()
    if not src or src.startswith("data:") or src.startswith("javascript:"):
        return None
    if src.startswith("//"):
        return "https:" + src
    return urljoin(base_url, src)


def _extract_image_urls_from_json(value):
    urls = []
    if isinstance(value, str):
        if value.startswith("http://") or value.startswith("https://") or value.startswith("//") or value.startswith("/"):
            urls.append(value)
    elif isinstance(value, list):
        for item in value:
            urls.extend(_extract_image_urls_from_json(item))
    elif isinstance(value, dict):
        for key, item in value.items():
            key_lower = str(key).lower()
            if key_lower in {"image", "imageurl", "contenturl", "thumbnailurl", "url"}:
                urls.extend(_extract_image_urls_from_json(item))
    return urls


def _extract_urls_from_style(style_value):
    if not style_value:
        return []
    return re.findall(r'url\((?:["\']?)(.*?)(?:["\']?)\)', style_value)


def _looks_like_image_bytes(body):
    if len(body) < 64:
        return False
    signatures = (
        body.startswith(b"\xff\xd8\xff"),     # JPEG
        body.startswith(b"\x89PNG\r\n\x1a\n"), # PNG
        body.startswith(b"GIF87a") or body.startswith(b"GIF89a"),
        body.startswith(b"RIFF") and body[8:12] == b"WEBP",
    )
    return any(signatures)


def _image_dimensions_from_bytes(body):
    try:
        if body.startswith(b"\x89PNG\r\n\x1a\n") and len(body) >= 24:
            return struct.unpack(">II", body[16:24])
        if (body.startswith(b"GIF87a") or body.startswith(b"GIF89a")) and len(body) >= 10:
            return struct.unpack("<HH", body[6:10])
        if body.startswith(b"RIFF") and body[8:12] == b"WEBP" and len(body) >= 30:
            if body[12:16] == b"VP8X":
                width_minus_one = int.from_bytes(body[24:27], "little")
                height_minus_one = int.from_bytes(body[27:30], "little")
                return width_minus_one + 1, height_minus_one + 1
        if body.startswith(b"\xff\xd8"):
            i = 2
            while i + 9 < len(body):
                if body[i] != 0xFF:
                    i += 1
                    continue
                marker = body[i + 1]
                if marker in {0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF}:
                    height = int.from_bytes(body[i + 5:i + 7], "big")
                    width = int.from_bytes(body[i + 7:i + 9], "big")
                    return width, height
                if i + 4 > len(body):
                    break
                segment_length = int.from_bytes(body[i + 2:i + 4], "big")
                if segment_length <= 0:
                    break
                i += 2 + segment_length
    except Exception:
        return None
    return None


def _tokenize(text):
    return {token for token in re.findall(r"[a-z0-9]+", (text or "").lower()) if len(token) >= 4}


def _tokenize_text(value):
    if not value:
        return set()
    text = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return {part for part in text.split() if len(part) > 2 and part not in STOPWORDS}


def _decode_next_image_inner_url(url):
    parsed = urlparse(url or "")
    if "/_next/image" in parsed.path:
        return parse_qs(parsed.query).get("url", [""])[0]
    return url or ""


def _score_candidate(candidate, page_host, context_tokens=None):
    url = (candidate.get("url") or "").lower()
    source = candidate.get("source", "")
    score = 0

    source_weights = {
        "dataset_image": 70,
        "jsonld_image": 86,
        "meta_image": 80,
        "dom_image": 92,
        "network_image": 76,
        "background_image": 68,
    }
    score += source_weights.get(source, 60)

    width = int(candidate.get("width") or 0)
    height = int(candidate.get("height") or 0)
    area = width * height
    if area >= 1_000_000:
        score += 22
    elif area >= 300_000:
        score += 12
    elif area >= 40_000:
        score += 5
    elif area and area < 15_000:
        score -= 18

    if width and height:
        ratio = max(width, height) / max(1, min(width, height))
        if width < 180 or height < 180:
            score -= 12
        if 0.9 <= ratio <= 1.1 and max(width, height) < 500:
            score -= 10
    elif source == "dataset_image":
        score -= 12

    if any(token in url for token in POSITIVE_URL_HINTS):
        score += 8
    if any(token in url for token in NEGATIVE_URL_HINTS):
        if width > 0 and height > 0:
            if width < 300 or height < 300:
                score -= 15
        else:
            score -= 5
    if any(token in url for token in SOCIAL_CARD_HINTS):
        score -= 35
    if any(token in url for token in ("twitter", "facebook", "instagram", "pinterest", "linkedin")):
        score -= 18

    if "cpcdn.com" in url and "recipes/" in url and not any(x in url for x in COOKPAD_EXCLUDE):
        score += 20
    if "/_next/image" in url:
        score += 15

    candidate_host = urlparse(candidate.get("url") or "").netloc.lower()
    if candidate_host and page_host and (candidate_host == page_host or candidate_host.endswith("." + page_host)):
        score += 6

    alt = (candidate.get("alt") or "").lower()
    if any(token in alt for token in ("logo", "avatar", "profile", "icon", "share")):
        score -= 20

    if context_tokens:
        inner_url = _decode_next_image_inner_url(candidate.get("url", ""))
        text_blob = " ".join(
            str(candidate.get(key, ""))
            for key in ("alt", "title", "aria", "class_name", "id", "url", "source")
        ) + " " + inner_url
        candidate_tokens = _tokenize_text(text_blob)
        shared = context_tokens & candidate_tokens
        if shared:
            score += min(18, len(shared) * 4)
            if len(shared) >= 2:
                score += 6
        elif source in {"meta_image", "background_image"}:
            score -= 10
        elif source == "network_image":
            score -= 5

    return score


def _post_download_penalty(candidate, diagnosis, dimensions, file_size):
    width, height = dimensions if dimensions else (0, 0)
    penalty = 0

    if file_size < 8 * 1024:
        penalty += 35
    elif file_size < 20 * 1024:
        penalty += 15

    if width and height:
        ratio = max(width, height) / max(1, min(width, height))
        if width < 220 or height < 220:
            penalty += 18
        if ratio >= 2.6:
            penalty += 28
        elif ratio >= 1.9:
            penalty += 12
        if 0.9 <= ratio <= 1.1 and max(width, height) < 450:
            penalty += 16

    url = (candidate.get("url") or "").lower()
    if any(token in url for token in SOCIAL_CARD_HINTS):
        penalty += 28

    candidate_tokens = _tokenize_text(
        " ".join(
            str(candidate.get(key, ""))
            for key in ("alt", "title", "aria", "class_name", "id", "url")
        )
    )
    context_tokens = _tokenize_text(diagnosis.get("expected_title", "")) | _tokenize_text(diagnosis.get("title", ""))
    shared = candidate_tokens & context_tokens
    if context_tokens and not shared and candidate.get("source") in {"meta_image", "background_image"}:
        penalty += 12

    return penalty


def _quality_bonus(dimensions, file_size):
    width, height = dimensions if dimensions else (0, 0)
    score = 0
    area = width * height

    if area >= 2_000_000:
        score += 36
    elif area >= 1_000_000:
        score += 24
    elif area >= 500_000:
        score += 14
    elif area >= 150_000:
        score += 6

    if file_size >= 500 * 1024:
        score += 20
    elif file_size >= 250 * 1024:
        score += 12
    elif file_size >= 100 * 1024:
        score += 6

    return score


class ImageDiscoveryEngine:
    def __init__(self, page, context):
        self.page = page
        self.context = context

    def extract_snapshot(self, page_url=None):
        intercepted = []
        snapshot = {"title": "", "images": [], "metaImages": [], "jsonLd": [], "backgroundImages": []}
        base_url = self.page.url or ""

        def on_request(request):
            resource_type = getattr(request, "resource_type", "")
            url = request.url
            if resource_type == "image" or any(
                token in url.lower() for token in [".jpg", ".jpeg", ".png", ".webp", ".gif", "/image", "img", "cpcdn", "ndtvimg"]
            ):
                intercepted.append(url)

        if page_url:
            page_url = normalise_page_url(page_url)
            self.page.on("request", on_request)
            try:
                self.page.goto(page_url, wait_until="domcontentloaded", timeout=25000)
                try:
                    self.page.wait_for_function(
                        """() => {
                            const og = document.querySelector('meta[property="og:image"]');
                            if (og && og.content) return true;
                            const imgs = [...document.querySelectorAll('img')];
                            return imgs.some(i => (i.naturalWidth || 0) > 200 && (i.naturalHeight || 0) > 200);
                        }""",
                        timeout=6000,
                    )
                except Exception:
                    pass
            finally:
                try:
                    self.page.remove_listener("request", on_request)
                except Exception:
                    pass
            base_url = self.page.url or page_url

        snapshot = self.page.evaluate(
            """() => {
                function pickLargest(srcset) {
                    if (!srcset) return null;
                    const parsed = srcset.split(',')
                        .map(entry => entry.trim().split(/\\s+/))
                        .filter(parts => parts[0])
                        .map(parts => ({ url: parts[0], w: parseInt(parts[1]) || 0 }))
                        .sort((a, b) => b.w - a.w);
                    return parsed.length ? parsed[0].url : null;
                }

                const images = Array.from(document.querySelectorAll('img')).map(img => ({
                    src: img.getAttribute('src'),
                    currentSrc: img.currentSrc || null,
                    dataSrc: img.getAttribute('data-src'),
                    srcsetBest: pickLargest(img.getAttribute('srcset') || img.getAttribute('data-srcset')),
                    width: img.naturalWidth || 0,
                    height: img.naturalHeight || 0,
                    alt: img.alt || '',
                    className: img.className || '',
                    id: img.id || ''
                }));

                const metaImages = Array.from(document.querySelectorAll('meta')).map(meta => ({
                    key: meta.getAttribute('property') || meta.getAttribute('name') || meta.getAttribute('itemprop') || '',
                    value: meta.getAttribute('content') || ''
                })).filter(item => item.key && item.value);

                const jsonLd = Array.from(document.querySelectorAll('script[type="application/ld+json"]'))
                    .map(node => node.textContent || '')
                    .filter(Boolean);

                const backgroundImages = Array.from(document.querySelectorAll('[style]')).map(node => ({
                    style: node.getAttribute('style') || '',
                    title: node.getAttribute('title') || '',
                    aria: node.getAttribute('aria-label') || ''
                }));

                return {
                    title: document.title || '',
                    images,
                    metaImages,
                    jsonLd,
                    backgroundImages,
                };
            }"""
        )
        return {"snapshot": snapshot, "intercepted": intercepted, "base_url": base_url}

    def rank_from_snapshot(self, snapshot, base_url, page_url=None, direct_image_urls=None, expected_title=None, intercepted=None):
        intercepted = intercepted or []
        if not base_url and direct_image_urls:
            base_url = direct_image_urls[0]

        page_host = urlparse(base_url).netloc.lower()
        context_tokens = _tokenize_text(expected_title) | _tokenize_text(snapshot.get("title", ""))

        candidates = []

        for url in direct_image_urls or []:
            normalised = _normalise_asset_url(url, base_url)
            if normalised:
                candidates.append({
                    "url": upgrade_to_hires(normalised, base_url),
                    "original_url": normalised,
                    "source": "dataset_image",
                    "width": 0,
                    "height": 0,
                    "alt": "",
                    "title": "",
                    "aria": "",
                    "class_name": "",
                    "id": "",
                })

        for image in snapshot.get("images", []):
            raw = image.get("srcsetBest") or image.get("currentSrc") or image.get("dataSrc") or image.get("src")
            normalised = _normalise_asset_url(raw, base_url)
            if normalised:
                candidates.append({
                    "url": upgrade_to_hires(normalised, base_url),
                    "original_url": normalised,
                    "source": "dom_image",
                    "width": image.get("width", 0),
                    "height": image.get("height", 0),
                    "alt": image.get("alt", ""),
                    "title": "",
                    "aria": "",
                    "class_name": image.get("className", ""),
                    "id": image.get("id", ""),
                })

        for meta in snapshot.get("metaImages", []):
            key = meta.get("key", "").lower()
            if key in IMAGE_META_KEYS:
                normalised = _normalise_asset_url(meta.get("value"), base_url)
                if normalised:
                    candidates.append({
                        "url": upgrade_to_hires(normalised, base_url),
                        "original_url": normalised,
                    "source": "meta_image",
                    "width": 0,
                    "height": 0,
                    "alt": key,
                    "title": "",
                    "aria": "",
                    "class_name": "",
                    "id": "",
                })

        for script_text in snapshot.get("jsonLd", []):
            try:
                payload = json.loads(script_text)
            except Exception:
                continue
            for raw in _extract_image_urls_from_json(payload):
                normalised = _normalise_asset_url(raw, base_url)
                if normalised:
                    candidates.append({
                        "url": upgrade_to_hires(normalised, base_url),
                        "original_url": normalised,
                    "source": "jsonld_image",
                    "width": 0,
                    "height": 0,
                    "alt": "jsonld",
                    "title": "",
                    "aria": "",
                    "class_name": "",
                    "id": "",
                })

        for raw in intercepted:
            normalised = _normalise_asset_url(raw, base_url)
            if normalised:
                candidates.append({
                    "url": upgrade_to_hires(normalised, base_url),
                    "original_url": normalised,
                    "source": "network_image",
                    "width": 0,
                    "height": 0,
                    "alt": "network",
                    "title": "",
                    "aria": "",
                    "class_name": "",
                    "id": "",
                })

        for item in snapshot.get("backgroundImages", []):
            for raw in _extract_urls_from_style(item.get("style", "")):
                normalised = _normalise_asset_url(raw, base_url)
                if normalised:
                    candidates.append({
                        "url": upgrade_to_hires(normalised, base_url),
                        "original_url": normalised,
                        "source": "background_image",
                        "width": 0,
                        "height": 0,
                        "alt": f"{item.get('title', '')} {item.get('aria', '')}".strip(),
                        "title": item.get("title", ""),
                        "aria": item.get("aria", ""),
                        "class_name": "",
                        "id": "",
                    })

        deduped = {}
        for candidate in candidates:
            candidate["score"] = _score_candidate(candidate, page_host, context_tokens=context_tokens)
            key = candidate["url"]
            if not key:
                continue
            if key not in deduped or candidate["score"] > deduped[key]["score"]:
                deduped[key] = candidate

        ranked = sorted(deduped.values(), key=lambda item: item["score"], reverse=True)
        best = ranked[0] if ranked else None

        return {
            "page_url": page_url,
            "final_url": base_url,
            "title": snapshot.get("title", ""),
            "expected_title": expected_title or "",
            "candidates": ranked,
            "best_candidate": best,
            "network_images": intercepted[:40],
            "dom_image_count": len(snapshot.get("images", [])),
        }

    def diagnose(self, page_url=None, direct_image_urls=None, expected_title=None):
        extracted = self.extract_snapshot(page_url=page_url)
        return self.rank_from_snapshot(
            extracted["snapshot"],
            extracted["base_url"],
            page_url=page_url,
            direct_image_urls=direct_image_urls,
            expected_title=expected_title,
            intercepted=extracted["intercepted"],
        )

    def download_candidate(self, candidate, output_path, referer):
        urls_to_try = []
        for url in [candidate.get("url"), candidate.get("original_url")]:
            if url and url not in urls_to_try:
                urls_to_try.append(url)

        last_err = None
        for url in urls_to_try:
            try:
                resp = self.context.request.get(
                    url,
                    headers={
                        "Referer": referer,
                        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                    },
                )
                if not resp.ok:
                    raise Exception(f"HTTP {resp.status}")
                body = resp.body()
                content_type = (resp.headers.get("content-type") or "").lower()
                if "html" in content_type:
                    raise Exception(f"Unexpected content-type {content_type}")
                if len(body) < 1024 and "image/" not in content_type:
                    raise Exception("Response too small to be a valid image")
                if "image/" in content_type and not _looks_like_image_bytes(body):
                    raise Exception(f"Body did not look like a real image for content-type {content_type}")
                with open(output_path, "wb") as fh:
                    fh.write(body)
                return {"downloaded_from": url, "body": body, "content_type": content_type}
            except Exception as exc:
                last_err = exc

        try:
            req = urllib.request.Request(
                urls_to_try[-1],
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
                    ),
                    "Referer": referer,
                    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                },
            )
            with urllib.request.urlopen(req, timeout=15) as response:
                body = response.read()
            if len(body) < 1024:
                raise Exception("urllib fallback received a very small response")
            if not _looks_like_image_bytes(body):
                raise Exception("urllib fallback response did not look like an image")
            with open(output_path, "wb") as fh:
                fh.write(body)
            return {"downloaded_from": urls_to_try[-1], "body": body, "content_type": ""}
        except Exception as exc:
            raise Exception(f"All download attempts failed. Browser: {last_err} | urllib: {exc}")

    def try_ranked_candidates(self, diagnosis, output_path, referer, max_candidates=12):
        attempts = []
        candidates = diagnosis.get("candidates", [])[:max_candidates]
        last_error = None
        best_success = None

        for index, candidate in enumerate(candidates, start=1):
            temp_path = f"{output_path}.candidate_{index}"
            try:
                download = self.download_candidate(candidate, temp_path, referer)
                body = download["body"]
                dimensions = _image_dimensions_from_bytes(body)
                penalty = _post_download_penalty(candidate, diagnosis, dimensions, len(body))
                final_score = candidate.get("score", 0) - penalty + _quality_bonus(dimensions, len(body))
                if final_score < 45:
                    raise Exception(
                        f"Rejected likely wrong image (score={candidate.get('score', 0)}, "
                        f"penalty={penalty}, final={final_score}, dimensions={dimensions}, bytes={len(body)})"
                    )
                current = {
                    "candidate": candidate,
                    "downloaded_from": download["downloaded_from"],
                    "dimensions": dimensions,
                    "file_size": len(body),
                    "final_score": final_score,
                    "temp_path": temp_path,
                }
                if best_success is None or current["final_score"] > best_success["final_score"]:
                    if best_success and os.path.exists(best_success["temp_path"]):
                        os.remove(best_success["temp_path"])
                    best_success = current
                else:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
            except Exception as exc:
                last_error = exc
                attempts.append({
                    "url": candidate.get("url"),
                    "source": candidate.get("source"),
                    "score": candidate.get("score"),
                    "error": str(exc),
                })
                if os.path.exists(temp_path):
                    os.remove(temp_path)

        if best_success:
            os.replace(best_success["temp_path"], output_path)
            return {
                "candidate": best_success["candidate"],
                "downloaded_from": best_success["downloaded_from"],
                "dimensions": best_success["dimensions"],
                "file_size": best_success["file_size"],
                "final_score": best_success["final_score"],
                "attempts": attempts,
            }

        raise Exception(
            "All ranked candidates failed"
            + (f" after {len(attempts)} attempts" if attempts else "")
            + (f". Last error: {last_error}" if last_error else "")
        )

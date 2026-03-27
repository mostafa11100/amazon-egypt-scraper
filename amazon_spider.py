"""
Amazon Egypt Scraper — GitHub Actions edition
Playwright + Xvfb (same approach as Noon — bypasses bot detection)
Scrapes listing pages only (no product page visits) for speed at scale.

Usage:
  python amazon_spider.py --group_index 0 --limit 2000
"""
import argparse
import json
import re
import time
import random
from datetime import datetime

PRODUCT_LIMIT = 2000  # per group — 27 groups × 2000 = up to 54,000 products

# ── 27 groups covering all Amazon Egypt categories ────────────────────────────
CATEGORY_GROUPS = {
    "0":  ("Mobiles",              "https://www.amazon.eg/s?i=mobile"),
    "1":  ("Computers",           "https://www.amazon.eg/s?i=computers"),
    "2":  ("Electronics",         "https://www.amazon.eg/s?i=electronics"),
    "3":  ("Appliances",          "https://www.amazon.eg/s?i=appliances"),
    "4":  ("Kitchen",             "https://www.amazon.eg/s?i=kitchen"),
    "5":  ("Men's Fashion",       "https://www.amazon.eg/s?i=fashion-mens-clothing"),
    "6":  ("Women's Fashion",     "https://www.amazon.eg/s?i=fashion-womens-clothing"),
    "7":  ("Girls Fashion",       "https://www.amazon.eg/s?i=fashion-girls-clothing"),
    "8":  ("Shoes",               "https://www.amazon.eg/s?i=shoes"),
    "9":  ("Beauty",              "https://www.amazon.eg/s?i=beauty"),
    "10": ("Health & Personal",   "https://www.amazon.eg/s?i=hpc"),
    "11": ("Sports",              "https://www.amazon.eg/s?i=sporting-goods"),
    "12": ("Toys & Games",        "https://www.amazon.eg/s?i=toys-and-games"),
    "13": ("Baby Products",       "https://www.amazon.eg/s?i=baby-products"),
    "14": ("Luggage",             "https://www.amazon.eg/s?i=luggage"),
    "15": ("Watches",             "https://www.amazon.eg/s?i=watches"),
    "16": ("Automotive",          "https://www.amazon.eg/s?i=automotive"),
    "17": ("Office Products",     "https://www.amazon.eg/s?i=office-products"),
    "18": ("Pet Supplies",        "https://www.amazon.eg/s?i=pet-supplies"),
    "19": ("Grocery",             "https://www.amazon.eg/s?i=grocery"),
    "20": ("Books",               "https://www.amazon.eg/s?i=stripbooks"),
    "21": ("Video Games",         "https://www.amazon.eg/s?i=videogames"),
    "22": ("Tools",               "https://www.amazon.eg/s?i=tools"),
    "23": ("Musical Instruments", "https://www.amazon.eg/s?i=musical-instruments"),
    "24": ("Garden",              "https://www.amazon.eg/s?i=garden"),
    "25": ("Movies & TV",         "https://www.amazon.eg/s?i=movies-tv"),
    "26": ("Jewelry",             "https://www.amazon.eg/s?i=jewelry"),
}


# ── Product extractor from listing page HTML ──────────────────────────────────

def extract_products(html, cat_name):
    """Parse Amazon listing page and return list of product dicts."""
    from scrapy import Selector
    sel = Selector(text=html)
    products = []

    for card in sel.css("div[data-component-type='s-search-result']"):
        asin = card.attrib.get("data-asin", "").strip()
        if not asin:
            continue

        title = (
            card.css("h2 span.a-size-base-plus::text").get()
            or card.css("h2 span.a-size-medium::text").get()
            or card.css("h2 span::text").get()
            or ""
        ).strip()
        if not title:
            continue

        # Current price — prefer the displayed big price
        current_price = (
            card.css("span.a-price[data-a-size='xl'] span.a-offscreen::text").get()
            or card.css("span.a-price[data-a-size='l'] span.a-offscreen::text").get()
            or card.css("span.a-price span.a-offscreen::text").get()
            or ""
        ).strip()

        # Original price (crossed-out)
        original_price = (
            card.css("span.a-price.a-text-price span.a-offscreen::text").get()
            or ""
        ).strip()
        # Don't duplicate if same as current
        if original_price == current_price:
            original_price = ""

        # Discount badge
        discount = (
            card.css("span.a-badge-text::text").get()
            or card.css("span.s-coupon-highlight-color::text").get()
            or ""
        ).strip()
        if "%" not in discount:
            discount = ""

        # Rating
        rating_raw = card.css("span.a-icon-alt::text").get(default="")
        m = re.search(r"([\d.]+)\s*out of", rating_raw)
        rating = m.group(1) if m else ""

        # Reviews count
        reviews = (
            card.css("span.a-size-base.s-underline-text::text").get()
            or card.css("a span.a-size-base::text").get()
            or ""
        ).strip()

        # Brand
        brand = (
            card.css("span.a-size-base-plus.a-color-base::text").get()
            or card.css("h2 a span.a-size-base-plus::text").get()
            or ""
        ).strip()

        # Image
        image = (
            card.css("img.s-image::attr(src)").get()
            or ""
        )

        # Product URL
        href = card.css("h2 a.a-link-normal::attr(href)").get(default="")
        if href and not href.startswith("http"):
            href = "https://www.amazon.eg" + href
        # Clean URL to /dp/ASIN only
        if "/dp/" in href:
            product_url = "https://www.amazon.eg/dp/" + asin
        else:
            product_url = href

        # Badges
        prime = "Yes" if card.css("i.a-icon-prime, span.s-prime").get() else ""
        amazon_choice = "Yes" if card.css("span.s-label-popover-default").get() else ""
        sponsored = "Yes" if card.css("span[data-component-type='s-sponsored-label-info-icon'], span.s-label-popover-default").get() else ""

        products.append({
            "platform":        "amazon",
            "asin":            asin,
            "title":           title,
            "brand":           brand,
            "category":        cat_name,
            "current_price":   current_price,
            "original_price":  original_price,
            "discount":        discount,
            "rating":          rating,
            "reviews_count":   reviews,
            "prime_eligible":  prime,
            "amazon_choice":   amazon_choice,
            "sponsored":       sponsored,
            "main_image":      image,
            "product_url":     product_url,
            "scraped_at":      datetime.now().isoformat(),
        })

    return products


# ── Main scraper ──────────────────────────────────────────────────────────────

def scrape_group(group_index, limit=PRODUCT_LIMIT):
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    from playwright_stealth import Stealth

    group_key = str(group_index)
    if group_key not in CATEGORY_GROUPS:
        print(f"Unknown group_index: {group_index}")
        return []

    cat_name, base_url = CATEGORY_GROUPS[group_key]
    print(f"\n[Group {group_index}] '{cat_name}'  limit={limit}")

    all_products = []
    seen_asins   = set()

    stealth = Stealth(
        navigator_webdriver=True,
        chrome_runtime=True,
        webgl_vendor=True,
        navigator_languages=True,
        navigator_platform=True,
        navigator_plugins=True,
    )

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=False,   # headed + Xvfb on GitHub Actions = bypasses bot detection
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--window-size=1920,1080",
            ],
        )
        ctx = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="Africa/Cairo",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            },
        )
        page = ctx.new_page()
        stealth.apply_stealth_sync(page)

        # Warm up on Amazon homepage
        print("  Warming up on homepage...")
        try:
            page.goto("https://www.amazon.eg/", wait_until="domcontentloaded", timeout=30_000)
            time.sleep(random.uniform(2, 3))
            print("  Warmup OK")
        except Exception as e:
            print(f"  Warmup failed: {e} (continuing)")

        # Paginate through listing pages
        for page_num in range(1, 400):
            if len(all_products) >= limit:
                break

            url = base_url if page_num == 1 else f"{base_url}&page={page_num}"
            print(f"  Page {page_num}: {url}")

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=45_000)
            except PWTimeout:
                print(f"  [TIMEOUT] page {page_num}")
                break
            except Exception as e:
                print(f"  [ERROR] {e}")
                break

            time.sleep(random.uniform(2, 3))
            page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
            time.sleep(1)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(1)

            html  = page.content()
            title = page.title()
            print(f"  Title: {title[:100]}")
            print(f"  HTML length: {len(html)}")

            # Save first page for inspection
            if page_num == 1:
                with open(f"debug_{group_index}.html", "w", encoding="utf-8") as fh:
                    fh.write(html)

            # Count cards with various selectors to diagnose
            from scrapy import Selector as _S
            _sel = _S(text=html)
            print(f"  s-search-result divs: {len(_sel.css('div[data-component-type=s-search-result]'))}")
            print(f"  s-result-item divs:   {len(_sel.css('div[data-asin]'))}")

            products = extract_products(html, cat_name)
            print(f"  Extracted: {len(products)} cards")

            new_count = 0
            for p in products:
                if len(all_products) >= limit:
                    break
                if p["asin"] not in seen_asins:
                    seen_asins.add(p["asin"])
                    all_products.append(p)
                    new_count += 1

            print(f"  +{new_count} new  (total {len(all_products)}/{limit})")

            if new_count == 0:
                if page_num == 1:
                    print("  Page 1 returned 0 products — may be blocked or empty category.")
                else:
                    print(f"  No new products on page {page_num} — category exhausted.")
                break

            time.sleep(random.uniform(1.5, 3))

        browser.close()

    print(f"\n[Group {group_index}] Done: {len(all_products)} products")
    return all_products


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--group_index", type=int, required=True)
    parser.add_argument("--limit",       type=int, default=PRODUCT_LIMIT)
    args = parser.parse_args()

    products = scrape_group(args.group_index, args.limit)

    out = f"products_{args.group_index}.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(products, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(products)} products -> {out}")


if __name__ == "__main__":
    main()

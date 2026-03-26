"""
Discovers all Amazon Egypt category URLs by scraping the navigation sidebar.
Saves results to categories.json — used by amazon_spider as start_urls.
"""
import scrapy
import json

# Top-level department seeds (cover all of Amazon Egypt)
SEED_DEPARTMENTS = [
    ("Electronics",          "https://www.amazon.eg/s?i=electronics"),
    ("Computers",            "https://www.amazon.eg/s?i=computers"),
    ("Mobiles",              "https://www.amazon.eg/s?i=mobile"),
    ("Home & Kitchen",       "https://www.amazon.eg/s?i=kitchen"),
    ("Large Appliances",     "https://www.amazon.eg/s?i=appliances"),
    ("Fashion Men",          "https://www.amazon.eg/s?i=fashion-mens-clothing"),
    ("Fashion Women",        "https://www.amazon.eg/s?i=fashion-womens-clothing"),
    ("Kids Fashion",         "https://www.amazon.eg/s?i=fashion-girls-clothing"),
    ("Shoes",                "https://www.amazon.eg/s?i=shoes"),
    ("Bags & Luggage",       "https://www.amazon.eg/s?i=luggage"),
    ("Beauty",               "https://www.amazon.eg/s?i=beauty"),
    ("Health & Personal",    "https://www.amazon.eg/s?i=hpc"),
    ("Sports & Outdoors",    "https://www.amazon.eg/s?i=sporting-goods"),
    ("Toys & Games",         "https://www.amazon.eg/s?i=toys-and-games"),
    ("Baby Products",        "https://www.amazon.eg/s?i=baby-products"),
    ("Books",                "https://www.amazon.eg/s?i=stripbooks"),
    ("Automotive",           "https://www.amazon.eg/s?i=automotive"),
    ("Pet Supplies",         "https://www.amazon.eg/s?i=pet-supplies"),
    ("Office Products",      "https://www.amazon.eg/s?i=office-products"),
    ("Video Games",          "https://www.amazon.eg/s?i=videogames"),
    ("Grocery & Food",       "https://www.amazon.eg/s?i=grocery"),
    ("Watches",              "https://www.amazon.eg/s?i=watches"),
    ("Jewelry",              "https://www.amazon.eg/s?i=jewelry"),
    ("Tools & DIY",          "https://www.amazon.eg/s?i=tools"),
    ("Musical Instruments",  "https://www.amazon.eg/s?i=musical-instruments"),
    ("Movies & TV",          "https://www.amazon.eg/s?i=movies-tv"),
    ("Software",             "https://www.amazon.eg/s?i=software"),
    ("Garden & Outdoor",     "https://www.amazon.eg/s?i=garden"),
    ("Industrial",           "https://www.amazon.eg/s?i=industrial"),
    ("Stationery",           "https://www.amazon.eg/s?i=arts-crafts"),
]


class CategoryDiscoverySpider(scrapy.Spider):
    name = "category_discovery"
    allowed_domains = ["amazon.eg"]

    custom_settings = {
        "CONCURRENT_REQUESTS": 8,
        "DOWNLOAD_DELAY": 0.5,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "COOKIES_ENABLED": False,
        "RETRY_TIMES": 2,
        "LOG_LEVEL": "INFO",
        "DOWNLOADER_MIDDLEWARES": {
            "amazon_scraper.middlewares.RotateUserAgentMiddleware": 400,
        },
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._seen_urls = set()
        self._categories = []

    def start_requests(self):
        for dept_name, url in SEED_DEPARTMENTS:
            yield scrapy.Request(
                url, callback=self.parse_sidebar,
                meta={"dept": dept_name, "seed_url": url}
            )

    def _add(self, name, url):
        # Normalise URL — drop everything after & except node/i/bbn
        if url in self._seen_urls:
            return
        self._seen_urls.add(url)
        self._categories.append({"name": name, "url": url})
        self.logger.info(f"  + {name}")

    def parse_sidebar(self, response):
        dept = response.meta["dept"]
        seed_url = response.meta["seed_url"]

        # Always include the department-level URL itself
        self._add(dept, seed_url)

        # Extract sub-category links from the left sidebar
        for link in response.css(
            "#departments a, "
            "div[cel_widget_id='LEFT_REFINEMENT_CONTAINER'] a, "
            "ul.a-unordered-list.a-vertical a"
        ):
            raw_name = " ".join(link.css("::text").getall()).strip()
            href = link.attrib.get("href", "")

            if not raw_name or not href:
                continue

            # Remove product count like "(1,234)"
            import re
            name = raw_name.split("\n")[0].strip()
            name = re.sub(r"\s*\([\d,]+\)\s*$", "", name).strip()
            if not name:
                continue

            if href.startswith("/"):
                href = "https://www.amazon.eg" + href
            if "amazon.eg" not in href:
                continue

            # ── Skip filter/refinement URLs ──────────────────────────────────
            # Real categories: /s?i=..., /b?node=..., /s?...&bbn=...
            # Filters have rh= (refinements like price, brand, shipping)
            if "rh=" in href and "rh=n%3A" not in href:
                continue  # price/brand/shipping filter — not a real category
            if not any(x in href for x in ["/s?", "/b?", "node="]):
                continue

            # Skip filter-like names (price ranges, shipping options, etc.)
            skip_patterns = [
                "EGP & above", "to 25 EGP", "to 50 EGP", "to 100 EGP",
                "to 200 EGP", "to 300 EGP", "to 400 EGP", "to 500 EGP",
                "Free Shipping", "Fulfilled by Amazon", "Prime Eligible",
                "New Arrivals", "Today's Deals",
            ]
            if any(p.lower() in name.lower() for p in skip_patterns):
                continue

            full_name = f"{dept} > {name}"
            self._add(full_name, href)

    def closed(self, reason):
        with open("categories.json", "w", encoding="utf-8") as f:
            json.dump(self._categories, f, ensure_ascii=False, indent=2)
        self.logger.info(
            f"\n{'='*50}\n"
            f"  Discovered: {len(self._categories)} categories\n"
            f"  Saved to: categories.json\n"
            f"{'='*50}"
        )

import scrapy
import re
from scrapy.exceptions import CloseSpider

PRODUCT_LIMIT = 5000

# 10 groups — one category each — for maximum parallel GitHub Actions jobs
CATEGORY_GROUPS = {
    "A": ["https://www.amazon.eg/s?k=mobile+phones"],
    "B": ["https://www.amazon.eg/s?k=laptops"],
    "C": ["https://www.amazon.eg/s?k=televisions"],
    "D": ["https://www.amazon.eg/s?k=refrigerators"],
    "E": ["https://www.amazon.eg/s?k=washing+machines"],
    "F": ["https://www.amazon.eg/s?k=air+conditioners"],
    "G": ["https://www.amazon.eg/s?k=headphones"],
    "H": ["https://www.amazon.eg/s?k=cameras"],
    "I": ["https://www.amazon.eg/s?k=tablets"],
    "J": ["https://www.amazon.eg/s?k=gaming"],
}


class AmazonEgyptSpider(scrapy.Spider):
    name = "amazon_eg"
    allowed_domains = ["amazon.eg"]

    custom_settings = {
        "CONCURRENT_REQUESTS": 16,
        "DOWNLOAD_DELAY": 0.2,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.2,
        "AUTOTHROTTLE_MAX_DELAY": 5,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 8.0,
        "COOKIES_ENABLED": False,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [429, 503, 500],
        "LOG_LEVEL": "INFO",
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "ar-EG,ar;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        },
        "DOWNLOADER_MIDDLEWARES": {
            "amazon_scraper.middlewares.RotateUserAgentMiddleware": 400,
        },
    }

    def __init__(self, group="A", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.group = group.upper()
        self.products_scraped = 0
        self.start_urls = CATEGORY_GROUPS.get(self.group, CATEGORY_GROUPS["A"])
        output_file = f"products_{self.group}.json"
        self.custom_settings["FEEDS"] = {
            output_file: {
                "format": "json",
                "encoding": "utf8",
                "indent": 4,
                "overwrite": True,
            }
        }
        self.logger.info(f"Spider started - group={self.group} limit={PRODUCT_LIMIT}")

    # ── Step 1: Category search page ────────────────────────────────────────
    def parse(self, response):
        if self.products_scraped >= PRODUCT_LIMIT:
            return

        category_name = response.url.split("k=")[-1].replace("+", " ").title()

        product_links = response.css(
            "a.a-link-normal.s-no-outline::attr(href), "
            "h2 a.a-link-normal::attr(href)"
        ).getall()

        self.logger.info(
            f"[{self.group}] '{category_name}': {len(product_links)} links "
            f"(scraped: {self.products_scraped}/{PRODUCT_LIMIT})"
        )

        for link in product_links:
            if self.products_scraped >= PRODUCT_LIMIT:
                return
            if "/dp/" in link:
                if not link.startswith("http"):
                    link = "https://www.amazon.eg" + link
                yield scrapy.Request(
                    link,
                    callback=self.parse_product,
                    meta={"category_name": category_name},
                )

        if self.products_scraped < PRODUCT_LIMIT:
            next_page = response.css(
                "a.s-pagination-next::attr(href), li.a-last a::attr(href)"
            ).get()
            if next_page:
                if not next_page.startswith("http"):
                    next_page = "https://www.amazon.eg" + next_page
                yield scrapy.Request(next_page, callback=self.parse)

    # ── Step 2: Product page ─────────────────────────────────────────────────
    def parse_product(self, response):
        if self.products_scraped >= PRODUCT_LIMIT:
            return

        try:
            title = response.css("#productTitle::text").get(default="").strip()
            if not title:
                return

            price_whole    = response.css("span.a-price-whole::text").get(default="0")
            price_fraction = response.css("span.a-price-fraction::text").get(default="00")
            price = f"{price_whole.strip().replace(',', '')}.{price_fraction.strip()} EGP"

            rating        = response.css("span.a-icon-alt::text").get(default="No Rating")
            reviews_count = response.css("#acrCustomerReviewText::text").get(default="0 ratings")

            breadcrumbs = response.css(
                "#wayfinding-breadcrumbs_feature_div li span a::text"
            ).getall()
            category = " > ".join([b.strip() for b in breadcrumbs]) or response.meta.get("category_name", "Unknown")

            main_image = response.css(
                "#imgTagWrapperId img::attr(src), #landingImage::attr(src)"
            ).get(default="")
            all_images = list(set(response.css("img.a-dynamic-image::attr(src)").getall()))

            bullets = response.css("#feature-bullets li span.a-list-item::text").getall()
            description = " | ".join(
                [b.strip() for b in bullets if b.strip() and "›" not in b]
            )

            tech_specs = {}
            for row in response.css(
                "#productDetails_techSpec_section_1 tr, "
                "#productDetails_db_sections tr, "
                "table.prodDetTable tr"
            ):
                label = row.css("th::text").get()
                value = " ".join(row.css("td *::text").getall()).strip()
                if label and value:
                    tech_specs[label.strip()] = re.sub(r"\s+", " ", value)

            variations = {}
            variation_labels = response.css("#twister label.a-form-label::text").getall()
            variation_values = response.css(
                "#twister .a-row span.selection::text, "
                "#twister .swatchSelect .a-button-text::text"
            ).getall()
            for i, label in enumerate(variation_labels):
                variations[label.strip().rstrip(":")] = (
                    variation_values[i].strip() if i < len(variation_values) else "N/A"
                )

            brand = response.css("#bylineInfo::text, #brand::text").get(default="").strip()
            asin  = ""
            if "/dp/" in response.url:
                asin = response.url.split("/dp/")[1].split("/")[0].split("?")[0]

            availability = response.css("#availability span::text").get(default="Unknown").strip()

            # ── Shipping & seller info ────────────────────────────────────────
            # delivery date / free shipping message
            delivery_msg = " ".join(
                response.css(
                    "#mir-layout-DELIVERY_BLOCK span.a-text-bold::text, "
                    "#deliveryMessageMirId::text, "
                    "#fast-track-message::text"
                ).getall()
            ).strip()
            delivery_date = re.sub(r"\s+", " ", delivery_msg) or "N/A"

            # is shipping free?
            full_delivery_text = " ".join(
                response.css(
                    "#mir-layout-DELIVERY_BLOCK *::text, "
                    "#deliveryMessageMirId *::text"
                ).getall()
            ).lower()
            shipping_price = "FREE" if "free" in full_delivery_text or "مجاني" in full_delivery_text else (
                response.css("#price-shipping-message .a-color-secondary::text").get(default="").strip() or "N/A"
            )

            # prime eligible
            prime_eligible = "Yes" if response.css(
                "i.a-icon-prime, #primeBadge_feature_div, #pe-our-price-text"
            ).get() else "No"

            # ships from / sold by — from the buybox table rows
            ships_from = "N/A"
            sold_by     = "N/A"
            for row in response.css("#tabular-buybox .tabular-buybox-container > div"):
                label = row.css(".tabular-buybox-text:first-child span::text").get(default="").strip()
                value = row.css(".tabular-buybox-text:last-child span::text, "
                                ".tabular-buybox-text:last-child a::text").get(default="").strip()
                if not value:
                    continue
                label_lower = label.lower()
                if "ships from" in label_lower or "يشحن من" in label_lower:
                    ships_from = value
                elif "sold by" in label_lower or "يباع من" in label_lower:
                    sold_by = value

            # fallback for sold_by
            if sold_by == "N/A":
                sold_by = response.css("#merchant-info a::text, #sellerProfileTriggerId::text").get(default="N/A").strip()

            self.products_scraped += 1
            self.logger.info(f"[{self.group}][{self.products_scraped}/{PRODUCT_LIMIT}] {title[:60]}")

            item = {
                "asin": asin, "title": title, "brand": brand, "category": category,
                "price_egp": price, "rating": rating, "reviews_count": reviews_count,
                "availability": availability,
                "shipping_price": shipping_price, "prime_eligible": prime_eligible,
                "ships_from": ships_from, "sold_by": sold_by,
                "delivery_date": delivery_date,
                "main_image": main_image,
                "all_images": all_images, "description": description,
                "tech_specs": tech_specs, "variations": variations,
                "product_url": response.url,
            }

            if self.products_scraped >= PRODUCT_LIMIT:
                yield item
                raise CloseSpider(f"[{self.group}] Reached limit of {PRODUCT_LIMIT}")

            yield item

        except CloseSpider:
            raise
        except Exception as e:
            self.logger.error(f"Error parsing {response.url}: {e}")

import scrapy
import re
import json
import os
from scrapy.exceptions import CloseSpider

PRODUCT_LIMIT = 500

# Fallback hardcoded groups (used if categories.json is not available)
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
        "CONCURRENT_REQUESTS": 32,           # doubled from 16
        "DOWNLOAD_DELAY": 0.1,               # reduced from 0.2
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.1,
        "AUTOTHROTTLE_MAX_DELAY": 5,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 16.0,  # doubled from 8
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

    def __init__(self, group="A", group_index=None, total_groups="20", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.products_scraped = 0

        if group_index is not None and os.path.exists("categories.json"):
            # ── Dynamic mode: categories discovered by category_spider ───────
            with open("categories.json", encoding="utf-8") as f:
                all_cats = json.load(f)
            gi = int(group_index)
            tg = int(total_groups)
            # Round-robin distribution so each group gets an even mix of depts
            self.start_urls = [c["url"] for i, c in enumerate(all_cats) if i % tg == gi]
            self.group = str(gi)
            self.logger.info(
                f"Dynamic mode — group {gi}/{tg}, "
                f"{len(self.start_urls)} categories, limit={PRODUCT_LIMIT}"
            )
        else:
            # ── Fallback: hardcoded groups ────────────────────────────────────
            self.group = group.upper()
            self.start_urls = CATEGORY_GROUPS.get(self.group, CATEGORY_GROUPS["A"])
            self.logger.info(
                f"Fallback mode — group={self.group}, limit={PRODUCT_LIMIT}"
            )

    # ── Step 1: Category / search page ──────────────────────────────────────
    def parse(self, response):
        if self.products_scraped >= PRODUCT_LIMIT:
            return

        # Extract category name from URL
        if "k=" in response.url:
            category_name = response.url.split("k=")[-1].split("&")[0].replace("+", " ").title()
        elif "i=" in response.url:
            category_name = response.url.split("i=")[-1].split("&")[0].replace("-", " ").title()
        else:
            category_name = response.css("h1.a-size-large::text, h1 span::text").get(default="Unknown").strip()

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

            # ── Price ────────────────────────────────────────────────────────
            price_whole    = response.css("span.a-price-whole::text").get(default="0")
            price_fraction = response.css("span.a-price-fraction::text").get(default="00")
            price = f"{price_whole.strip().replace(',', '')}.{price_fraction.strip()} EGP"

            # Original (before-discount) price
            original_price = response.css(
                "span.a-price.a-text-price span.a-offscreen::text"
            ).get(default="").strip()

            # Discount percentage
            discount_percent = response.css(
                ".savingsPercentage::text, "
                "span.a-color-price.a-size-base.a-text-bold::text"
            ).get(default="").strip()
            if "%" not in discount_percent:
                discount_percent = ""

            # ── Ratings ──────────────────────────────────────────────────────
            rating        = response.css("span.a-icon-alt::text").get(default="No Rating")
            reviews_count = response.css("#acrCustomerReviewText::text").get(default="0 ratings")

            # ── Category breadcrumb ───────────────────────────────────────────
            breadcrumbs = response.css(
                "#wayfinding-breadcrumbs_feature_div li span a::text"
            ).getall()
            category = " > ".join([b.strip() for b in breadcrumbs]) or response.meta.get("category_name", "Unknown")

            # ── Images ───────────────────────────────────────────────────────
            main_image = response.css(
                "#imgTagWrapperId img::attr(src), #landingImage::attr(src)"
            ).get(default="")
            all_images = list(set(response.css("img.a-dynamic-image::attr(src)").getall()))

            # ── Description / bullets ─────────────────────────────────────────
            bullets = response.css("#feature-bullets li span.a-list-item::text").getall()
            description = " | ".join(
                [b.strip() for b in bullets if b.strip() and "›" not in b]
            )

            # ── Tech specs table ──────────────────────────────────────────────
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

            # Extract common fields from tech_specs if present
            def _spec(keys):
                for k in keys:
                    for sk, sv in tech_specs.items():
                        if k.lower() in sk.lower():
                            return sv
                return ""

            weight     = _spec(["weight", "الوزن"])
            dimensions = _spec(["dimension", "الأبعاد", "product dimensions"])
            model_num  = _spec(["model number", "item model", "رقم الموديل"])
            country    = _spec(["country of origin", "بلد المنشأ"])
            warranty   = _spec(["warranty", "الضمان"])
            date_avail = _spec(["date first available", "تاريخ التوفر"])

            # ── Variations ───────────────────────────────────────────────────
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

            # ── Brand / ASIN / Availability ──────────────────────────────────
            brand = response.css("#bylineInfo::text, #brand::text").get(default="").strip()
            asin  = ""
            if "/dp/" in response.url:
                asin = response.url.split("/dp/")[1].split("/")[0].split("?")[0]
            availability = response.css("#availability span::text").get(default="Unknown").strip()

            # ── Extra badges ─────────────────────────────────────────────────
            amazon_choice = "Yes" if response.css(
                "#acBadge_feature_div, .ac-badge-wrapper"
            ).get() else "No"

            # best_seller_rank — check tech_specs first, then page elements
            best_seller_rank = _spec(["best sellers rank", "bestsellers rank", "أكثر مبيعاً"])
            if not best_seller_rank:
                bsr_texts = response.css(
                    "#SalesRank td::text, #SalesRank *::text, "
                    "#detailBulletsWrapper_feature_div li::text, "
                    "#detailBulletsWrapper_feature_div span::text"
                ).getall()
                bsr_combined = " ".join(bsr_texts)
                if "best seller" in bsr_combined.lower() or "أكثر مبيعاً" in bsr_combined.lower():
                    best_seller_rank = re.sub(r"\s+", " ", bsr_combined).strip()[:200]

            # coupon — try multiple selectors
            coupon = (
                response.css("#couponBadge_feature_div .a-color-price::text").get()
                or response.css("#couponFeatureDivId .a-color-price::text").get()
                or response.css("#vlptContainer .a-color-price::text").get()
                or response.css(".couponBadge::text").get()
                or response.css("#couponText::text").get()
                or ""
            )
            coupon = coupon.strip()

            # ── Shipping & seller ─────────────────────────────────────────────
            full_delivery_text = " ".join(
                response.css(
                    "#mir-layout-DELIVERY_BLOCK *::text, "
                    "#deliveryMessageMirId *::text"
                ).getall()
            ).lower()
            shipping_price = "FREE" if ("free" in full_delivery_text or "مجاني" in full_delivery_text) else (
                response.css("#price-shipping-message .a-color-secondary::text").get(default="").strip() or "N/A"
            )

            delivery_msg = " ".join(
                response.css(
                    "#mir-layout-DELIVERY_BLOCK span.a-text-bold::text, "
                    "#deliveryMessageMirId::text, "
                    "#fast-track-message::text"
                ).getall()
            ).strip()
            delivery_date = re.sub(r"\s+", " ", delivery_msg) or "N/A"

            prime_eligible = "Yes" if response.css(
                "i.a-icon-prime, #primeBadge_feature_div, #pe-our-price-text"
            ).get() else "No"

            # ships_from / sold_by — scan all buybox rows by text content
            ships_from = "N/A"
            sold_by    = "N/A"
            buybox_rows = response.css(
                "#tabular-buybox .a-row, "
                "#tabular-buybox tr, "
                "#tabular-buybox .tabular-buybox-container > div"
            )
            for row in buybox_rows:
                texts = [t.strip() for t in row.css("::text").getall() if t.strip()]
                if len(texts) < 2:
                    continue
                label_text = texts[0].lower()
                value_text = texts[-1]
                if "ships from" in label_text or "يشحن" in label_text:
                    ships_from = value_text
                elif "sold by" in label_text or "يباع" in label_text:
                    sold_by = value_text

            # fallbacks
            if ships_from == "N/A":
                ships_from = response.css(
                    "#shipsFromSoldBy_feature_div span::text"
                ).get(default="N/A").strip()
            if sold_by == "N/A":
                sold_by = response.css(
                    "#merchant-info a::text, "
                    "#sellerProfileTriggerId::text, "
                    "#soldByThirdParty a::text"
                ).get(default="N/A").strip()

            # ── Yield item ────────────────────────────────────────────────────
            self.products_scraped += 1
            self.logger.info(f"[{self.group}][{self.products_scraped}/{PRODUCT_LIMIT}] {title[:60]}")

            item = {
                "asin":             asin,
                "title":            title,
                "brand":            brand,
                "category":         category,
                # Price
                "price_egp":        price,
                "original_price":   original_price,
                "discount_percent": discount_percent,
                # Ratings
                "rating":           rating,
                "reviews_count":    reviews_count,
                # Availability
                "availability":     availability,
                "amazon_choice":    amazon_choice,
                "best_seller_rank": best_seller_rank,
                "coupon":           coupon,
                # Shipping
                "shipping_price":   shipping_price,
                "prime_eligible":   prime_eligible,
                "ships_from":       ships_from,
                "sold_by":          sold_by,
                "delivery_date":    delivery_date,
                # Images
                "main_image":       main_image,
                "all_images":       all_images,
                # Details
                "description":      description,
                "weight":           weight,
                "dimensions":       dimensions,
                "model_number":     model_num,
                "country_of_origin":country,
                "warranty":         warranty,
                "date_first_available": date_avail,
                "tech_specs":       tech_specs,
                "variations":       variations,
                "product_url":      response.url,
            }

            if self.products_scraped >= PRODUCT_LIMIT:
                yield item
                raise CloseSpider(f"[{self.group}] Reached limit of {PRODUCT_LIMIT}")

            yield item

        except CloseSpider:
            raise
        except Exception as e:
            self.logger.error(f"Error parsing {response.url}: {e}")

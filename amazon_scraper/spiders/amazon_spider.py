import scrapy
import re

PRODUCT_LIMIT = 100


class AmazonEgyptSpider(scrapy.Spider):
    name = "amazon_eg"
    allowed_domains = ["amazon.eg"]
    start_urls = ["https://www.amazon.eg/"]

    custom_settings = {
        "FEEDS": {
            "products_full_data.json": {
                "format": "json",
                "encoding": "utf8",
                "indent": 4,
                "overwrite": True,
            }
        },
        "CONCURRENT_REQUESTS": 8,
        "DOWNLOAD_DELAY": 0.5,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "COOKIES_ENABLED": False,
        "RETRY_TIMES": 2,
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.products_scraped = 0

    # ── Step 1: Homepage → extract category links ──────────────────────────
    def parse(self, response):
        self.logger.info("Parsing homepage for categories...")

        category_links = response.css(
            "a[href*='/b?node='], a[href*='/b/?node='], a[href*='node=']::attr(href)"
        ).getall()

        nav_links = response.css(
            "#nav-flyout-shopAll a::attr(href), "
            ".nav-hasPanel a::attr(href), "
            "a.nav-a::attr(href)"
        ).getall()

        all_links = list(set(category_links + nav_links))
        category_urls = []

        for link in all_links:
            if link and ("node=" in link or "/b?" in link or "/b/" in link):
                if not link.startswith("http"):
                    link = "https://www.amazon.eg" + link
                category_urls.append(link)

        self.logger.info(f"Found {len(category_urls)} category URLs")

        if not category_urls:
            self.logger.info("Using fallback category list")
            category_urls = [
                "https://www.amazon.eg/s?k=mobile+phones",
                "https://www.amazon.eg/s?k=laptops",
                "https://www.amazon.eg/s?k=televisions",
                "https://www.amazon.eg/s?k=refrigerators",
                "https://www.amazon.eg/s?k=washing+machines",
                "https://www.amazon.eg/s?k=air+conditioners",
                "https://www.amazon.eg/s?k=headphones",
                "https://www.amazon.eg/s?k=cameras",
                "https://www.amazon.eg/s?k=tablets",
                "https://www.amazon.eg/s?k=gaming",
            ]

        for url in category_urls[:10]:
            yield scrapy.Request(
                url,
                callback=self.parse_category,
                meta={"category_url": url},
            )

    # ── Step 2: Category page → extract product links ──────────────────────
    def parse_category(self, response):
        if self.products_scraped >= PRODUCT_LIMIT:
            return

        category_name = (
            response.css("h1.a-size-large::text, #s-result-count::text").get(default="")
            or response.url.split("k=")[-1].replace("+", " ").title()
        )

        product_links = response.css(
            "a.a-link-normal.s-no-outline::attr(href), "
            "h2 a.a-link-normal::attr(href)"
        ).getall()

        self.logger.info(
            f"Category '{category_name}': found {len(product_links)} products "
            f"(scraped so far: {self.products_scraped}/{PRODUCT_LIMIT})"
        )

        for link in product_links:
            if self.products_scraped >= PRODUCT_LIMIT:
                self.logger.info(f"Reached limit of {PRODUCT_LIMIT} products. Stopping.")
                return
            if "/dp/" in link:
                if not link.startswith("http"):
                    link = "https://www.amazon.eg" + link
                yield scrapy.Request(
                    link,
                    callback=self.parse_product,
                    meta={"category_name": category_name},
                )

        # Pagination only if still need more products
        if self.products_scraped < PRODUCT_LIMIT:
            next_page = response.css(
                "a.s-pagination-next::attr(href), li.a-last a::attr(href)"
            ).get()
            if next_page:
                if not next_page.startswith("http"):
                    next_page = "https://www.amazon.eg" + next_page
                yield scrapy.Request(
                    next_page,
                    callback=self.parse_category,
                    meta={"category_url": response.meta.get("category_url", "")},
                )

    # ── Step 3: Product page → extract full details ─────────────────────────
    def parse_product(self, response):
        if self.products_scraped >= PRODUCT_LIMIT:
            return

        try:
            title = response.css("#productTitle::text").get(default="").strip()
            if not title:
                return

            price_whole = response.css("span.a-price-whole::text").get(default="0")
            price_fraction = response.css("span.a-price-fraction::text").get(default="00")
            price = f"{price_whole.strip().replace(',', '')}.{price_fraction.strip()} EGP"

            rating = response.css("span.a-icon-alt::text").get(default="No Rating")
            reviews_count = response.css("#acrCustomerReviewText::text").get(default="0 ratings")

            breadcrumbs = response.css(
                "#wayfinding-breadcrumbs_feature_div li span a::text"
            ).getall()
            category = " > ".join([b.strip() for b in breadcrumbs]) or response.meta.get(
                "category_name", "Unknown"
            )

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
            asin = ""
            if "/dp/" in response.url:
                asin = response.url.split("/dp/")[1].split("/")[0].split("?")[0]

            availability = response.css("#availability span::text").get(default="Unknown").strip()

            self.products_scraped += 1
            self.logger.info(f"[{self.products_scraped}/{PRODUCT_LIMIT}] Scraped: {title[:60]}")

            yield {
                "asin": asin,
                "title": title,
                "brand": brand,
                "category": category,
                "price_egp": price,
                "rating": rating,
                "reviews_count": reviews_count,
                "availability": availability,
                "main_image": main_image,
                "all_images": all_images,
                "description": description,
                "tech_specs": tech_specs,
                "variations": variations,
                "product_url": response.url,
            }

        except Exception as e:
            self.logger.error(f"Error parsing {response.url}: {e}")

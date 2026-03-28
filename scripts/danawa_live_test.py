"""Live crawl test — runs DanawaCrawler against real Danawa pages."""

from src.crawlers.danawa import DanawaCrawler


def main() -> None:
    crawler = DanawaCrawler()
    results = crawler.crawl_raw()

    print(f"\n{'='*60}")
    print(f"Total products crawled: {len(results)}")
    print(f"{'='*60}")

    for i, raw in enumerate(results, 1):
        print(
            f"  [{i:2d}] {raw.category:4s} | {raw.price_text:>15s} | {raw.product_name[:50]}"
        )

    # Validation
    categories = {r.category for r in results}
    expected = {"CPU", "GPU", "RAM", "SSD"}
    missing = expected - categories
    if missing:
        print(f"\nWARNING: Missing categories: {missing}")
    else:
        print(f"\nAll 4 categories present: {sorted(categories)}")

    if len(results) == 10:
        print("PASS: Expected 10 products, got 10")
    else:
        print(f"WARN: Expected 10 products, got {len(results)}")


if __name__ == "__main__":
    main()

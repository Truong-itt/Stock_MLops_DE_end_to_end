#!/usr/bin/env python3
"""
reprocess_sentiment.py
──────────────────────
Script để re-process sentiment cho toàn bộ tin tức trong database.
Gửi tất cả articles lên Kafka topic 'news-sentiment' để sentiment_worker xử lý.

Cách chạy:
    python reprocess_sentiment.py                    # Tất cả tin tức
    python reprocess_sentiment.py --symbol AAPL     # Chỉ một mã
    python reprocess_sentiment.py --limit 500       # Giới hạn số lượng
    python reprocess_sentiment.py --only-missing    # Chỉ các tin chưa có FinBERT sentiment
"""

import argparse
import json
import sys
import time
from datetime import datetime
from typing import List, Dict

from cassandra.cluster import Cluster
from confluent_kafka import Producer

# ═══════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════
SCYLLA_HOSTS = ["scylla-node1", "scylla-node2", "scylla-node3"]
SCYLLA_PORT = 9042
SCYLLA_KS = "stock_data"

KAFKA_BOOTSTRAP = "kafka-1:29092,kafka-2:29092,kafka-3:29092"
KAFKA_TOPIC = "news-sentiment"


# ═══════════════════════════════════════════════════════════════════════
# CONNECTIONS
# ═══════════════════════════════════════════════════════════════════════
def connect_scylla():
    """Connect to ScyllaDB."""
    print("Connecting to ScyllaDB...")
    cluster = Cluster(SCYLLA_HOSTS, port=SCYLLA_PORT)
    session = cluster.connect(SCYLLA_KS)
    print("✓ Connected to ScyllaDB")
    return cluster, session


def create_kafka_producer():
    """Create Kafka producer."""
    print("Creating Kafka producer...")
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "retries": 3,
        "linger.ms": 50,
        "batch.size": 65536,  # 64KB batches for efficiency
    })
    print("✓ Kafka producer created")
    return producer


# ═══════════════════════════════════════════════════════════════════════
# DATA FETCHING
# ═══════════════════════════════════════════════════════════════════════
def get_all_stock_codes(session) -> List[str]:
    """Get all distinct stock codes from news table."""
    rows = session.execute("SELECT DISTINCT stock_code FROM stock_news")
    codes = [r.stock_code for r in rows if r.stock_code]
    print(f"Found {len(codes)} unique stock codes")
    return codes


def get_news_for_symbol(session, stock_code: str, only_missing: bool = False) -> List[Dict]:
    """Get all news articles for a symbol."""
    rows = session.execute(
        "SELECT * FROM stock_news WHERE stock_code = %s",
        [stock_code]
    )
    
    articles = []
    for r in rows:
        # Skip if only_missing and already has FinBERT sentiment
        if only_missing and r.sentiment_label:
            continue
        
        articles.append({
            "stock_code": r.stock_code,
            "article_id": r.article_id,
            "title": r.title or "",
            "content": r.content or "",
            "link": r.link or "",
            "date": r.date.isoformat() if r.date else None,
        })
    
    return articles


# ═══════════════════════════════════════════════════════════════════════
# KAFKA PUBLISHING
# ═══════════════════════════════════════════════════════════════════════
delivery_count = {"success": 0, "failed": 0}


def delivery_callback(err, msg):
    """Kafka delivery callback."""
    if err:
        delivery_count["failed"] += 1
    else:
        delivery_count["success"] += 1


def publish_articles(producer, articles: List[Dict]) -> int:
    """Publish articles to Kafka topic."""
    global delivery_count
    delivery_count = {"success": 0, "failed": 0}
    
    for i, article in enumerate(articles):
        try:
            producer.produce(
                KAFKA_TOPIC,
                key=article["article_id"].encode("utf-8") if article["article_id"] else None,
                value=json.dumps(article).encode("utf-8"),
                callback=delivery_callback,
            )
            
            # Poll periodically to trigger callbacks and prevent queue buildup
            if (i + 1) % 100 == 0:
                producer.poll(0)
                
        except Exception as e:
            print(f"  Error publishing article: {e}")
            delivery_count["failed"] += 1
    
    # Flush remaining messages
    producer.flush()
    
    return delivery_count["success"]


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Re-process sentiment for news articles")
    parser.add_argument("--symbol", "-s", type=str, help="Specific stock symbol to process")
    parser.add_argument("--limit", "-l", type=int, default=0, help="Max articles to process (0 = unlimited)")
    parser.add_argument("--only-missing", "-m", action="store_true", help="Only process articles without FinBERT sentiment")
    parser.add_argument("--dry-run", "-d", action="store_true", help="Count articles without publishing")
    args = parser.parse_args()
    
    print("=" * 60)
    print("SENTIMENT REPROCESS SCRIPT")
    print("=" * 60)
    print(f"Options:")
    print(f"  Symbol: {args.symbol or 'ALL'}")
    print(f"  Limit: {args.limit or 'unlimited'}")
    print(f"  Only missing: {args.only_missing}")
    print(f"  Dry run: {args.dry_run}")
    print()
    
    # Connect to services
    cluster, session = connect_scylla()
    
    if not args.dry_run:
        producer = create_kafka_producer()
    else:
        producer = None
    
    print()
    
    # Get stock codes to process
    if args.symbol:
        stock_codes = [args.symbol.upper()]
    else:
        stock_codes = get_all_stock_codes(session)
    
    # Process each stock
    total_articles = 0
    total_published = 0
    
    for i, code in enumerate(stock_codes):
        articles = get_news_for_symbol(session, code, args.only_missing)
        
        if not articles:
            continue
        
        # Apply limit
        if args.limit > 0 and total_articles + len(articles) > args.limit:
            articles = articles[:args.limit - total_articles]
        
        total_articles += len(articles)
        
        if args.dry_run:
            print(f"  [{i+1}/{len(stock_codes)}] {code}: {len(articles)} articles (dry run)")
        else:
            published = publish_articles(producer, articles)
            total_published += published
            print(f"  [{i+1}/{len(stock_codes)}] {code}: {len(articles)} articles, {published} published")
        
        # Check limit
        if args.limit > 0 and total_articles >= args.limit:
            print(f"\n  Reached limit of {args.limit} articles")
            break
        
        # Rate limiting
        time.sleep(0.1)
    
    # Summary
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Total articles found: {total_articles}")
    if not args.dry_run:
        print(f"  Successfully published: {total_published}")
        print(f"  Failed: {delivery_count['failed']}")
    print()
    
    # Cleanup
    if producer:
        producer.flush()
    cluster.shutdown()
    
    print("Done!")


if __name__ == "__main__":
    main()

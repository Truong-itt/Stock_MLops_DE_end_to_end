"""
news_worker.py
──────────────
Crawl tin tức chứng khoán từ Yahoo Finance RSS / Google News,
phân tích sentiment đơn giản, ghi vào ScyllaDB stock_news.
Publish tin mới lên Kafka topic 'news-sentiment' để phân tích FinBERT.

Chạy mỗi 5 phút, mỗi lần lấy tin mới cho tất cả symbols.
"""

import hashlib
import json
import signal
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from cassandra.cluster import Cluster
from confluent_kafka import Producer

from logger import get_logger

logger = get_logger()

# ═══════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════
SCYLLA_HOSTS = ["scylla-node1", "scylla-node2", "scylla-node3"]
SCYLLA_PORT  = 9042
SCYLLA_KS    = "stock_data"

KAFKA_BOOTSTRAP = "kafka-1:29092,kafka-2:29092,kafka-3:29092"
KAFKA_TOPIC_SENTIMENT = "news-sentiment"

POLL_INTERVAL_SEC = 300     # 5 phút

VIETNAM_STOCKS = [
    "VCB", "BID", "FPT", "HPG", "CTG", "VHM", "TCB", "VPB", "VNM", "MBB",
    "GAS", "ACB", "MSN", "GVR", "LPB", "SSB", "STB", "VIB", "MWG", "HDB",
    "PLX", "POW", "SAB", "BCM", "PDR", "KDH", "NVL", "DGC", "SHB", "EIB",
]

INTERNATIONAL_STOCKS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "BRK-B", "LLY", "AVGO",
    "JPM", "V", "UNH", "WMT", "MA", "XOM", "JNJ", "PG", "HD", "COST",
    "NFLX", "AMD", "INTC", "DIS", "PYPL", "BA", "CRM", "ORCL", "CSCO", "ABT",
]

ALL_STOCKS = VIETNAM_STOCKS + INTERNATIONAL_STOCKS

# Keyword lists for simple sentiment
POSITIVE_WORDS = {
    "surge", "soar", "rally", "gain", "rise", "jump", "upgrade", "bullish",
    "beat", "outperform", "profit", "growth", "tăng", "lãi", "đột phá",
    "tích cực", "khởi sắc", "hồi phục", "vượt", "kỷ lục",
}
NEGATIVE_WORDS = {
    "fall", "drop", "crash", "decline", "loss", "plunge", "downgrade", "bearish",
    "miss", "underperform", "risk", "warning", "giảm", "lỗ", "sụt",
    "tiêu cực", "rủi ro", "bán tháo", "thua", "cảnh báo",
}


# ═══════════════════════════════════════════════════════════════════════
# SCYLLA CONNECTION
# ═══════════════════════════════════════════════════════════════════════
def connect_scylla():
    for attempt in range(30):
        try:
            cluster = Cluster(SCYLLA_HOSTS, port=SCYLLA_PORT)
            session = cluster.connect(SCYLLA_KS)
            logger.info("Connected to ScyllaDB")
            return cluster, session
        except Exception as e:
            logger.warning("ScyllaDB not ready (%d/30): %s", attempt + 1, e)
            time.sleep(5)
    raise RuntimeError("Cannot connect to ScyllaDB")


# ═══════════════════════════════════════════════════════════════════════
# KAFKA PRODUCER
# ═══════════════════════════════════════════════════════════════════════
def create_kafka_producer():
    """Create Kafka producer with retry logic."""
    for attempt in range(30):
        try:
            producer = Producer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "acks": "all",
                "retries": 3,
                "linger.ms": 10,
            })
            logger.info("Kafka producer created")
            return producer
        except Exception as e:
            logger.warning("Kafka not ready (%d/30): %s", attempt + 1, e)
            time.sleep(5)
    logger.error("Cannot connect to Kafka, continuing without sentiment publishing")
    return None


def delivery_callback(err, msg):
    """Kafka delivery callback."""
    if err:
        logger.warning("Kafka delivery failed: %s", err)


def publish_to_kafka(producer, article: Dict):
    """Publish article to Kafka for sentiment analysis."""
    if not producer:
        return
    
    try:
        # Convert datetime to ISO string for JSON serialization
        data = article.copy()
        if isinstance(data.get("date"), datetime):
            data["date"] = data["date"].isoformat()
        if isinstance(data.get("crawled_at"), datetime):
            data["crawled_at"] = data["crawled_at"].isoformat()
        
        producer.produce(
            KAFKA_TOPIC_SENTIMENT,
            key=article["article_id"].encode("utf-8"),
            value=json.dumps(data).encode("utf-8"),
            callback=delivery_callback
        )
        producer.poll(0)  # Trigger callbacks
    except Exception as e:
        logger.warning("Failed to publish to Kafka: %s", e)


# ═══════════════════════════════════════════════════════════════════════
# SENTIMENT (đơn giản keyword-based)
# ═══════════════════════════════════════════════════════════════════════
def compute_sentiment(text: str) -> float:
    """
    Trả về score [-1.0, 1.0]:
      > 0 = positive, < 0 = negative, 0 = neutral
    """
    if not text:
        return 0.0
    words = text.lower().split()
    pos = sum(1 for w in words if w in POSITIVE_WORDS)
    neg = sum(1 for w in words if w in NEGATIVE_WORDS)
    total = pos + neg
    if total == 0:
        return 0.0
    return round((pos - neg) / total, 4)


# ═══════════════════════════════════════════════════════════════════════
# NEWS FETCHERS
# ═══════════════════════════════════════════════════════════════════════
def make_article_id(link: str, title: str) -> str:
    """Tạo article_id ổn định từ link + title."""
    raw = f"{link}|{title}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]


def fetch_yahoo_rss(symbol: str) -> List[Dict]:
    """Lấy tin từ Yahoo Finance RSS."""
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
    articles = []
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return articles

        soup = BeautifulSoup(resp.content, "lxml-xml")
        items = soup.find_all("item")

        for item in items[:10]:  # tối đa 10 bài mỗi symbol
            title = item.find("title")
            link  = item.find("link")
            desc  = item.find("description")
            pub   = item.find("pubDate")

            title_text = title.get_text(strip=True) if title else ""
            link_text  = link.get_text(strip=True) if link else ""
            desc_text  = desc.get_text(strip=True) if desc else ""

            # Parse pubDate
            pub_dt = datetime.now(timezone.utc)
            if pub:
                try:
                    from email.utils import parsedate_to_datetime
                    pub_dt = parsedate_to_datetime(pub.get_text(strip=True))
                except Exception:
                    pass

            full_text = f"{title_text} {desc_text}"
            sentiment = compute_sentiment(full_text)

            articles.append({
                "stock_code":      symbol,
                "date":            pub_dt,
                "article_id":      make_article_id(link_text, title_text),
                "title":           title_text,
                "content":         desc_text[:2000],  # giới hạn 2000 ký tự
                "link":            link_text,
                "pdf_link":        None,
                "is_pdf":          False,
                "sentiment_score": sentiment,
                "crawled_at":      datetime.now(timezone.utc),
            })

    except Exception as e:
        logger.warning("Yahoo RSS error for %s: %s", symbol, e)

    return articles


def fetch_google_news(symbol: str) -> List[Dict]:
    """Lấy tin từ Google News RSS (fallback)."""
    query = f"{symbol} stock"
    url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
    articles = []
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return articles

        soup = BeautifulSoup(resp.content, "lxml-xml")
        items = soup.find_all("item")

        for item in items[:5]:  # tối đa 5 bài
            title = item.find("title")
            link  = item.find("link")
            pub   = item.find("pubDate")

            title_text = title.get_text(strip=True) if title else ""
            link_text  = link.get_text(strip=True) if link else ""

            pub_dt = datetime.now(timezone.utc)
            if pub:
                try:
                    from email.utils import parsedate_to_datetime
                    pub_dt = parsedate_to_datetime(pub.get_text(strip=True))
                except Exception:
                    pass

            sentiment = compute_sentiment(title_text)

            articles.append({
                "stock_code":      symbol,
                "date":            pub_dt,
                "article_id":      make_article_id(link_text, title_text),
                "title":           title_text,
                "content":         None,
                "link":            link_text,
                "pdf_link":        None,
                "is_pdf":          False,
                "sentiment_score": sentiment,
                "crawled_at":      datetime.now(timezone.utc),
            })

    except Exception as e:
        logger.warning("Google News error for %s: %s", symbol, e)

    return articles


# ═══════════════════════════════════════════════════════════════════════
# WRITE TO SCYLLA
# ═══════════════════════════════════════════════════════════════════════
def save_articles(scylla_session, articles: List[Dict], kafka_producer=None) -> int:
    """Ghi articles vào stock_news, skip nếu đã tồn tại. Publish tin mới lên Kafka."""
    insert_cql = """
        INSERT INTO stock_news
            (stock_code, date, article_id, title, content, link,
             pdf_link, is_pdf, sentiment_score, crawled_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        IF NOT EXISTS
    """
    prepared = scylla_session.prepare(insert_cql)

    inserted = 0
    new_articles = []
    
    for art in articles:
        try:
            result = scylla_session.execute(prepared, (
                art["stock_code"],
                art["date"],
                art["article_id"],
                art["title"],
                art["content"],
                art["link"],
                art["pdf_link"],
                art["is_pdf"],
                art["sentiment_score"],
                art["crawled_at"],
            ))
            # IF NOT EXISTS trả về [applied] = True nếu chèn mới
            if result and hasattr(result, 'one'):
                row = result.one()
                if row and getattr(row, 'applied', row[0]):
                    inserted += 1
                    new_articles.append(art)
            else:
                inserted += 1
                new_articles.append(art)
        except Exception as e:
            logger.error("Insert news error (%s): %s", art.get("stock_code"), e)

    # Publish new articles to Kafka for FinBERT sentiment analysis
    if kafka_producer and new_articles:
        for art in new_articles:
            publish_to_kafka(kafka_producer, art)
        kafka_producer.flush()  # Ensure all messages are sent
        logger.info("[KAFKA] Published %d articles to %s", len(new_articles), KAFKA_TOPIC_SENTIMENT)

    return inserted


# ═══════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════
def main():
    logger.info("Starting news_worker ...")

    cluster, scylla_session = connect_scylla()
    
    # Initialize Kafka producer for sentiment analysis pipeline
    kafka_producer = create_kafka_producer()

    running = True

    def _stop(sig, frame):
        nonlocal running
        logger.info("Received signal %s, stopping ...", sig)
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    while running:
        total_fetched = 0
        total_inserted = 0

        for symbol in ALL_STOCKS:
            # Thử Yahoo Finance RSS trước
            articles = fetch_yahoo_rss(symbol)

            # Bổ sung Google News nếu chưa đủ 5 tin
            if len(articles) < 5:
                google_articles = fetch_google_news(symbol)
                # Loại bỏ trùng lặp bằng article_id
                existing_ids = {a["article_id"] for a in articles}
                for ga in google_articles:
                    if ga["article_id"] not in existing_ids:
                        articles.append(ga)
                        if len(articles) >= 5:
                            break

            if articles:
                n = save_articles(scylla_session, articles, kafka_producer)
                total_fetched += len(articles)
                total_inserted += n
                if n > 0:
                    logger.info("[NEWS] %s: %d fetched, %d new", symbol, len(articles), n)

            # Rate limiting
            time.sleep(1)

            if not running:
                break

        logger.info("[NEWS CYCLE] Total fetched=%d  new=%d", total_fetched, total_inserted)

        # Đợi đến lần chạy tiếp theo
        for _ in range(POLL_INTERVAL_SEC):
            if not running:
                break
            time.sleep(1)

    # Cleanup
    try:
        if kafka_producer:
            kafka_producer.flush()
        scylla_session.shutdown()
        cluster.shutdown()
    except Exception:
        pass
    logger.info("news_worker stopped.")


if __name__ == "__main__":
    main()

"""
sentiment_worker.py
───────────────────
Kafka consumer for news sentiment analysis using FinBERT.
- Consumes news from 'news-sentiment' topic
- Translates Vietnamese to English using MyMemory API
- Runs FinBERT sentiment analysis
- Updates sentiment_score in ScyllaDB

Author: truongitt
"""

import json
import signal
import time
import re
from datetime import datetime, timezone
from typing import Optional, Tuple

import requests
from confluent_kafka import Consumer, KafkaError, KafkaException

from cassandra.cluster import Cluster

from logger import get_logger

logger = get_logger()

# ═══════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════
KAFKA_BOOTSTRAP = "kafka-1:29092,kafka-2:29092,kafka-3:29092"
KAFKA_TOPIC = "news-sentiment"
KAFKA_GROUP_ID = "sentiment-worker-group"

SCYLLA_HOSTS = ["scylla-node1", "scylla-node2", "scylla-node3"]
SCYLLA_PORT = 9042
SCYLLA_KS = "stock_data"

MYMEMORY_API = "https://api.mymemory.translated.net/get"

# Vietnamese detection pattern
VIETNAMESE_PATTERN = re.compile(r'[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]', re.IGNORECASE)

running = True


def signal_handler(sig, frame):
    global running
    logger.info("Shutdown signal received")
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


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
# LANGUAGE DETECTION & TRANSLATION
# ═══════════════════════════════════════════════════════════════════════
def is_vietnamese(text: str) -> bool:
    """Detect if text contains Vietnamese characters."""
    if not text:
        return False
    return bool(VIETNAMESE_PATTERN.search(text))


def translate_to_english(text: str) -> str:
    """
    Translate Vietnamese text to English using MyMemory API.
    Returns original text if translation fails.
    """
    if not text or len(text) < 5:
        return text
    
    try:
        # Split into chunks (API has 500 char limit)
        chunks = []
        max_len = 450
        remaining = text
        
        while remaining:
            if len(remaining) <= max_len:
                chunks.append(remaining)
                break
            # Find break point at sentence or space
            break_point = remaining.rfind('. ', 0, max_len)
            if break_point < 50:
                break_point = remaining.rfind(' ', 0, max_len)
            if break_point < 50:
                break_point = max_len
            chunks.append(remaining[:break_point + 1])
            remaining = remaining[break_point + 1:].strip()
        
        translated_chunks = []
        for chunk in chunks:
            resp = requests.get(
                MYMEMORY_API,
                params={"q": chunk, "langpair": "vi|en"},
                timeout=10
            )
            data = resp.json()
            if data.get("responseStatus") == 200 and data.get("responseData", {}).get("translatedText"):
                translated_chunks.append(data["responseData"]["translatedText"])
            else:
                translated_chunks.append(chunk)  # fallback to original
            time.sleep(0.3)  # Rate limiting
        
        return " ".join(translated_chunks)
    except Exception as e:
        logger.warning("Translation error: %s", e)
        return text


# ═══════════════════════════════════════════════════════════════════════
# FINBERT SENTIMENT ANALYSIS
# ═══════════════════════════════════════════════════════════════════════
class FinBertAnalyzer:
    """FinBERT-based sentiment analyzer for financial news."""
    
    def __init__(self):
        self.classifier = None
        self._load_model()
    
    def _load_model(self):
        """Load FinBERT model. Lazy loading on first use."""
        try:
            from transformers import pipeline
            logger.info("Loading FinBERT model...")
            self.classifier = pipeline(
                "sentiment-analysis",
                model="ProsusAI/finbert",
                truncation=True,
                max_length=512
            )
            logger.info("FinBERT model loaded successfully")
        except Exception as e:
            logger.error("Failed to load FinBERT model: %s", e)
            self.classifier = None
    
    def analyze(self, text: str) -> Tuple[float, str]:
        """
        Analyze sentiment of text.
        Returns: (score, label)
            score: -1.0 to 1.0 (negative to positive)
            label: 'positive', 'negative', 'neutral'
        """
        if not self.classifier or not text:
            return 0.0, "neutral"
        
        try:
            # Truncate text if too long
            if len(text) > 2000:
                text = text[:2000]
            
            result = self.classifier(text)[0]
            label = result["label"].lower()
            confidence = result["score"]
            
            # Convert to score [-1, 1]
            if label == "positive":
                score = confidence
            elif label == "negative":
                score = -confidence
            else:
                score = 0.0
            
            return round(score, 4), label
        except Exception as e:
            logger.warning("Sentiment analysis error: %s", e)
            return 0.0, "neutral"


# ═══════════════════════════════════════════════════════════════════════
# KAFKA CONSUMER
# ═══════════════════════════════════════════════════════════════════════
def create_kafka_consumer():
    """Create Kafka consumer with retry logic."""
    for attempt in range(30):
        try:
            consumer = Consumer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "group.id": KAFKA_GROUP_ID,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
                "session.timeout.ms": 30000,
                "max.poll.interval.ms": 300000,  # 5 minutes for processing
            })
            consumer.subscribe([KAFKA_TOPIC])
            logger.info("Kafka consumer created, subscribed to: %s", KAFKA_TOPIC)
            return consumer
        except Exception as e:
            logger.warning("Kafka not ready (%d/30): %s", attempt + 1, e)
            time.sleep(5)
    raise RuntimeError("Cannot connect to Kafka")


# ═══════════════════════════════════════════════════════════════════════
# UPDATE DATABASE
# ═══════════════════════════════════════════════════════════════════════
def update_sentiment_in_db(session, news_item: dict, score: float, label: str):
    """Update sentiment score in ScyllaDB."""
    try:
        query = """
        UPDATE stock_news 
        SET sentiment_score = %s, sentiment_label = %s, sentiment_updated_at = %s
        WHERE stock_code = %s AND date = %s AND article_id = %s
        """
        session.execute(query, (
            score,
            label,
            datetime.now(timezone.utc),
            news_item["stock_code"],
            news_item["date"],
            news_item["article_id"]
        ))
        logger.info(
            "Updated sentiment for %s/%s: %.4f (%s)",
            news_item["stock_code"], news_item["article_id"][:8], score, label
        )
    except Exception as e:
        logger.error("Failed to update sentiment in DB: %s", e)


# ═══════════════════════════════════════════════════════════════════════
# MAIN PROCESSING LOOP
# ═══════════════════════════════════════════════════════════════════════
def process_message(msg, analyzer: FinBertAnalyzer, session) -> bool:
    """Process a single Kafka message."""
    try:
        value = msg.value()
        if value is None:
            return False
        
        news_item = json.loads(value.decode("utf-8"))
        
        stock_code = news_item.get("stock_code", "")
        article_id = news_item.get("article_id", "")
        title = news_item.get("title", "")
        content = news_item.get("content", "")
        
        # Combine title and content for analysis
        full_text = f"{title} {content}".strip()
        
        if not full_text:
            logger.warning("Empty text for %s/%s, skipping", stock_code, article_id)
            return False
        
        # Detect language and translate if Vietnamese
        if is_vietnamese(full_text):
            logger.info("Vietnamese detected for %s, translating...", stock_code)
            full_text = translate_to_english(full_text)
        
        # Run FinBERT sentiment analysis
        score, label = analyzer.analyze(full_text)
        
        # Update database
        update_sentiment_in_db(session, news_item, score, label)
        
        return True
    except json.JSONDecodeError as e:
        logger.warning("Invalid JSON message: %s", e)
        return False
    except Exception as e:
        logger.error("Error processing message: %s", e)
        return False


def main():
    global running
    
    logger.info("=" * 60)
    logger.info("SENTIMENT WORKER STARTING")
    logger.info("=" * 60)
    
    # Connect to ScyllaDB
    cluster, session = connect_scylla()
    
    # Initialize FinBERT analyzer
    analyzer = FinBertAnalyzer()
    
    # Create Kafka consumer
    consumer = create_kafka_consumer()
    
    processed_count = 0
    
    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error("Kafka error: %s", msg.error())
                    continue
            
            if process_message(msg, analyzer, session):
                processed_count += 1
                if processed_count % 10 == 0:
                    logger.info("Processed %d messages", processed_count)
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        consumer.close()
        cluster.shutdown()
        logger.info("Shutdown complete. Processed %d messages total.", processed_count)


if __name__ == "__main__":
    main()

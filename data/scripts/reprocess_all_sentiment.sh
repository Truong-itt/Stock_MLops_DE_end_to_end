#!/bin/bash
# reprocess_all_sentiment.sh
# Script to trigger re-processing of all news sentiment using FinBERT
#
# Usage:
#   ./reprocess_all_sentiment.sh                    # All articles
#   ./reprocess_all_sentiment.sh --symbol AAPL     # Single symbol
#   ./reprocess_all_sentiment.sh --only-missing    # Only articles without FinBERT sentiment
#   ./reprocess_all_sentiment.sh --limit 1000      # Limit number of articles

echo "=========================================="
echo "SENTIMENT REPROCESS TRIGGER"
echo "=========================================="

# Option 1: Run via Docker container (recommended)
if docker ps | grep -q "sentiment-worker"; then
    echo "Running via sentiment-worker container..."
    docker exec sentiment-worker python reprocess_sentiment.py "$@"
    exit $?
fi

# Option 2: Run via news-worker container
if docker ps | grep -q "news-worker"; then
    echo "Running via news-worker container..."
    docker exec news-worker python reprocess_sentiment.py "$@"
    exit $?
fi

# Option 3: Run via any data container
if docker ps | grep -q "producer"; then
    echo "Running via producer container..."
    docker exec producer python reprocess_sentiment.py "$@"
    exit $?
fi

# Option 4: Call API endpoint
echo "No suitable container found. Trying API endpoint..."
curl -X POST "http://localhost:8020/api/sentiment/reprocess?limit=1000" -H "Content-Type: application/json"
echo ""

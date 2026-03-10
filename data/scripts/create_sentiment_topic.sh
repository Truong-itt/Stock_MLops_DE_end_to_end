#!/bin/bash
# create_sentiment_topic.sh - Create Kafka topic for news sentiment analysis

echo "Creating Kafka topic: news-sentiment"

docker exec kafka-1 kafka-topics --create \
    --bootstrap-server localhost:29092 \
    --topic news-sentiment \
    --partitions 3 \
    --replication-factor 3 \
    --if-not-exists

echo "Topic created. Listing topics:"
docker exec kafka-1 kafka-topics --list --bootstrap-server localhost:29092

echo "Topic details:"
docker exec kafka-1 kafka-topics --describe \
    --bootstrap-server localhost:29092 \
    --topic news-sentiment

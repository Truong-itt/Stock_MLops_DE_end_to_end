FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    confluent-kafka[avro,schema-registry] \
    yfinance \
    requests \
    beautifulsoup4 \
    lxml \
    confluent-kafka \
    clickhouse-connect \
    cassandra-driver \
    pytz

# COPY /recordings/ws_20260302.jsonl /app/
COPY stream_data.py .
COPY agg_worker.py .
COPY news_worker.py .
COPY sentiment_worker.py .
COPY reprocess_sentiment.py .
COPY data_gap_tool.py .
COPY logger.py .
COPY symbol_registry.py .
# COPY utils.py .

CMD ["python", "stream_data.py"]

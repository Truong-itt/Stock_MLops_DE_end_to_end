#!/bin/bash
# migrate_add_sentiment_columns.sh
# Add sentiment_label and sentiment_updated_at columns to stock_news table

echo "Adding sentiment columns to stock_news table..."

docker exec -it scylla-node1 cqlsh -e "
USE stock_data;

-- Add sentiment_label column (if not exists)
ALTER TABLE stock_news ADD sentiment_label text;

-- Add sentiment_updated_at column (if not exists)  
ALTER TABLE stock_news ADD sentiment_updated_at timestamp;

-- Verify schema
DESCRIBE TABLE stock_news;
"

echo "Migration complete!"

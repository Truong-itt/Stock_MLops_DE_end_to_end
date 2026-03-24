import logging
import os
import time

import clickhouse_connect


logger = logging.getLogger("backend.clickhouse")

MAX_RETRIES = 15
RETRY_DELAY = 3


class ClickHouseDB:
    """Small ClickHouse client used for BOCPD analytics routes."""

    def __init__(self):
        self.host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
        self.port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
        self.username = os.getenv("CLICKHOUSE_USER", "default")
        self.password = os.getenv("CLICKHOUSE_PASSWORD", "truongittstock")
        self.database = os.getenv("CLICKHOUSE_DB", "stock_warehouse")
        self.client = None

    def connect(self):
        if self.client is not None:
            return
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(
                    "Connecting to ClickHouse at %s:%s (attempt %d/%d)",
                    self.host,
                    self.port,
                    attempt,
                    MAX_RETRIES,
                )
                self.client = clickhouse_connect.get_client(
                    host=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                    database=self.database,
                )
                logger.info("Connected to ClickHouse successfully")
                return
            except Exception as exc:
                logger.warning("ClickHouse not ready (%d/%d): %s", attempt, MAX_RETRIES, exc)
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
        raise RuntimeError("Cannot connect to ClickHouse after retries")

    def close(self):
        if self.client is None:
            return
        try:
            self.client.close()
        except Exception:
            pass
        self.client = None

    def is_connected(self) -> bool:
        return self.client is not None

    def query(self, sql: str):
        if self.client is None:
            self.connect()
        result = self.client.query(sql)
        columns = list(result.column_names or [])
        return [dict(zip(columns, row)) for row in result.result_rows]

    def command(self, sql: str):
        if self.client is None:
            self.connect()
        return self.client.command(sql)


ch_db = ClickHouseDB()

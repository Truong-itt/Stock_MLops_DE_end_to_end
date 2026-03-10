import os
import time
import logging
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scylla-setup")

class ScyllaDBSetup:
    def __init__(self):
        in_docker = os.environ.get("RUNNING_IN_DOCKER", "false").lower() == "true"

        # Allow override contact points from env
        # cp_env = os.environ.get("SCYLLA_CONTACT_POINTS")

        # if cp_env:
        #     self.contact_points = [x.strip() for x in cp_env.split(",")]
        # else:
        self.contact_points = (
            ["scylla-node1", "scylla-node2", "scylla-node3"]
            if in_docker
            else ["localhost"]
        )

        self.port = 9042
        self.keyspace = "stock_data"

    # CONNECT WITH RETRY
    def connect(self, max_retries=30, delay=5):
        logger.info(f"Connecting to cluster at {self.contact_points}:{self.port}")

        for attempt in range(1, max_retries + 1):
            try:
                cluster = Cluster(
                    contact_points=self.contact_points,
                    port=self.port,
                    protocol_version=4
                )
                session = cluster.connect()

                logger.info("Connected successfully to ScyllaDB")

                # Detect datacenter automatically
                host = list(cluster.metadata.all_hosts())[0]
                self.local_dc = host.datacenter
                logger.info(f"Detected datacenter: {self.local_dc}")

                return cluster, session

            except Exception as e:
                logger.warning(f"Attempt {attempt} failed: {e}")
                time.sleep(delay)

        raise Exception("Could not connect to Scylla cluster")

    # CREATE KEYSPACE
    def create_keyspace(self, session):
        logger.info("Creating keyspace...")

        query = f"""
        CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
        WITH replication = {{
            'class': 'NetworkTopologyStrategy',
            '{self.local_dc}': 3
        }}
        AND durable_writes = true;
        """

        session.execute(query)
        logger.info("Keyspace ready.")

    # CREATE TABLES
    def create_tables(self, session):
        logger.info("Creating tables...")
        session.execute(f"USE {self.keyspace}")

        tables = [

            # RAW TICKS
            """
            CREATE TABLE IF NOT EXISTS stock_prices (
                symbol text,
                timestamp text,
                price double,
                exchange text,
                quote_type int,
                market_hours int,
                change_percent double,
                day_volume bigint,
                change double,
                last_size bigint,
                price_hint text,
                producer_timestamp bigint,
                PRIMARY KEY (symbol, timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)
            AND cdc = {
                'enabled': true,
                'preimage': false,
                'postimage': false,
                'ttl': '120'
            };
            """,

            # AGGREGATED INTERVAL DATA
            """
            CREATE TABLE IF NOT EXISTS stock_prices_agg (
                symbol text,
                bucket_date date,
                interval text,
                ts timestamp,
                open double,
                high double,
                low double,
                close double,
                volume bigint,
                vwap double,
                PRIMARY KEY ((symbol, bucket_date, interval), ts)
            ) WITH CLUSTERING ORDER BY (ts ASC);
            """,

            # DAILY SUMMARY
            """
            CREATE TABLE IF NOT EXISTS stock_daily_summary (
                symbol text,
                trade_date date,
                open double,
                high double,
                low double,
                close double,
                volume bigint,
                change double,
                change_percent double,
                vwap double,
                exchange text,
                quote_type int,
                market_hours int,
                PRIMARY KEY (symbol, trade_date)
            ) WITH CLUSTERING ORDER BY (trade_date DESC);
            """,

            # LATEST PRICE
            """
            CREATE TABLE IF NOT EXISTS stock_latest_prices (
                symbol text PRIMARY KEY,
                price double,
                timestamp timestamp,
                exchange text,
                quote_type int,
                market_hours int,
                change_percent double,
                day_volume bigint,
                change double,
                last_size bigint,
                price_hint text,
                producer_timestamp bigint
            ) WITH cdc = {
                'enabled': true,
                'preimage': false,
                'postimage': false,
                'ttl': '120'
            };
            """,

            # NEWS TABLE
            """
            CREATE TABLE IF NOT EXISTS stock_news (
                stock_code text,
                date timestamp,
                article_id text,
                title text,
                link text,
                is_pdf boolean,
                content text,
                sentiment_score float,
                sentiment_label text,
                sentiment_updated_at timestamp,
                pdf_link text,
                crawled_at timestamp,
                PRIMARY KEY (stock_code, date, article_id)
            ) WITH CLUSTERING ORDER BY (date DESC, article_id DESC);
            """
        ]

        for t in tables:
            session.execute(t)

        logger.info("Tables created.")

    # CREATE INDEXES
    def create_indexes(self, session):
        logger.info("Creating indexes...")

        indexes = [
            "CREATE INDEX IF NOT EXISTS ON stock_prices (exchange);",
            "CREATE INDEX IF NOT EXISTS ON stock_daily_summary (trade_date);"
        ]

        for idx in indexes:
            try:
                session.execute(idx)
            except Exception as e:
                logger.warning(f"Index creation warning: {e}")

        logger.info("Indexes done.")

    # VERIFY
    def verify(self, session):
        logger.info("Verifying schema...")

        rows = session.execute(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s",
            [self.keyspace]
        )

        tables = [r.table_name for r in rows]
        logger.info(f"Tables in keyspace: {tables}")

    # MAIN SETUP FLOW
    def setup(self):
        logger.info("Starting Scylla setup...")

        cluster = None

        try:
            cluster, session = self.connect()
            self.create_keyspace(session)
            self.create_tables(session)

            logger.info("Waiting for CDC metadata sync...")
            time.sleep(10)

            self.create_indexes(session)
            self.verify(session)

            logger.info("Scylla setup completed successfully.")

        except Exception as e:
            logger.error(f"Setup failed: {e}")
            raise

        finally:
            if cluster:
                cluster.shutdown()


def main():
    setup = ScyllaDBSetup()
    setup.setup()


if __name__ == "__main__":
    main()
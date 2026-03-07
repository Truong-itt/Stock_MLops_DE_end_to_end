import os
import time
import logging
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, dict_factory

logger = logging.getLogger("backend.database")

MAX_RETRIES = 30
RETRY_DELAY = 4  # seconds


class ScyllaDB:
    """Singleton ScyllaDB connection manager with auto-retry."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True

        self.contact_points = os.getenv(
            "SCYLLA_CONTACT_POINTS", "scylla-node1,scylla-node2,scylla-node3"
        ).split(",")
        self.port = int(os.getenv("SCYLLA_PORT", "9042"))
        self.keyspace = os.getenv("SCYLLA_KEYSPACE", "stock_data")
        self.cluster = None
        self.session = None

    def connect(self):
        """Connect with retry loop — waits for ScyllaDB to become ready."""
        hosts = [cp.strip() for cp in self.contact_points]
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(
                    "Connecting to ScyllaDB at %s:%s (attempt %d/%d)",
                    hosts, self.port, attempt, MAX_RETRIES,
                )
                self.cluster = Cluster(
                    contact_points=hosts,
                    port=self.port,
                    protocol_version=4,
                )
                self.session = self.cluster.connect(self.keyspace)
                self.session.row_factory = dict_factory
                logger.info("Connected to ScyllaDB successfully")
                return
            except Exception as e:
                logger.warning(
                    "ScyllaDB not ready (%d/%d): %s", attempt, MAX_RETRIES, e
                )
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
        raise RuntimeError("Cannot connect to ScyllaDB after %d retries" % MAX_RETRIES)

    def close(self):
        if self.cluster:
            self.cluster.shutdown()
            logger.info("ScyllaDB connection closed")

    def execute(self, query, params=None, timeout=15):
        stmt = SimpleStatement(query, fetch_size=5000)
        return self.session.execute(stmt, parameters=params, timeout=timeout)


db = ScyllaDB()

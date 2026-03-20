import logging
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, dict_factory

from bocpd import BOCPDConfig, ZeroMeanGaussianVarianceBOCPD
from symbol_registry import SymbolRegistry


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("changepoint.worker")

SCYLLA_CONTACT_POINTS = os.getenv(
    "SCYLLA_CONTACT_POINTS", "scylla-node1,scylla-node2,scylla-node3"
).split(",")
SCYLLA_PORT = int(os.getenv("SCYLLA_PORT", "9042"))
SCYLLA_KEYSPACE = os.getenv("SCYLLA_KEYSPACE", "stock_data")
POLL_INTERVAL = float(os.getenv("BOCPD_POLL_INTERVAL", "2.0"))
BOOTSTRAP_LIMIT = int(os.getenv("BOCPD_BOOTSTRAP_LIMIT", "120"))
BOOTSTRAP_HISTORY_LIMIT = int(os.getenv("BOCPD_BOOTSTRAP_HISTORY_LIMIT", "80"))


@dataclass
class SymbolState:
    model: ZeroMeanGaussianVarianceBOCPD
    last_price: Optional[float] = None
    last_event_time: Optional[datetime] = None
    bootstrapped: bool = False


class ChangepointWorker:
    def __init__(self):
        self.cluster = None
        self.session = None
        self.registry = SymbolRegistry()
        self.states: Dict[str, SymbolState] = {}
        self.all_symbols: List[str] = []
        self.model_config = BOCPDConfig(
            alpha0=float(os.getenv("BOCPD_ALPHA0", "1.0")),
            beta0=float(os.getenv("BOCPD_BETA0", "0.0001")),
            hazard_lambda=float(os.getenv("BOCPD_HAZARD_LAMBDA", "90")),
            max_run_length=int(os.getenv("BOCPD_MAX_RUN_LENGTH", "180")),
            tail_mass_threshold=float(os.getenv("BOCPD_TAIL_MASS_THRESHOLD", "0.000001")),
        )

    def connect(self, max_retries: int = 30, delay: int = 4) -> None:
        hosts = [cp.strip() for cp in SCYLLA_CONTACT_POINTS if cp.strip()]
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    "Connecting to ScyllaDB at %s:%s (attempt %d/%d)",
                    hosts,
                    SCYLLA_PORT,
                    attempt,
                    max_retries,
                )
                self.cluster = Cluster(
                    contact_points=hosts,
                    port=SCYLLA_PORT,
                    protocol_version=4,
                )
                self.session = self.cluster.connect(SCYLLA_KEYSPACE)
                self.session.row_factory = dict_factory
                logger.info("Connected to ScyllaDB")
                return
            except Exception as exc:
                logger.warning("Scylla connect failed (%d/%d): %s", attempt, max_retries, exc)
                if attempt < max_retries:
                    time.sleep(delay)
        raise RuntimeError("Cannot connect to ScyllaDB")

    def close(self) -> None:
        if self.cluster:
            self.cluster.shutdown()
            logger.info("ScyllaDB connection closed")

    def execute(self, query: str, params=None, timeout: int = 15):
        stmt = SimpleStatement(query, fetch_size=5000)
        return self.session.execute(stmt, parameters=params, timeout=timeout)

    def ensure_tables(self) -> None:
        logger.info("Ensuring changepoint tables exist")
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_changepoint_latest (
                symbol text PRIMARY KEY,
                event_time timestamp,
                price double,
                return_value double,
                cp_prob double,
                expected_run_length double,
                map_run_length int,
                predictive_volatility double,
                innovation_zscore double,
                whale_score double,
                hazard double,
                evidence double,
                regime_label text,
                source text
            );
            """
        )
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_changepoint_history (
                symbol text,
                bucket_date date,
                event_time timestamp,
                price double,
                return_value double,
                cp_prob double,
                expected_run_length double,
                map_run_length int,
                predictive_volatility double,
                innovation_zscore double,
                whale_score double,
                hazard double,
                evidence double,
                regime_label text,
                source text,
                PRIMARY KEY ((symbol, bucket_date), event_time)
            ) WITH CLUSTERING ORDER BY (event_time DESC);
            """
        )

    def parse_event_time(self, raw_value) -> Optional[datetime]:
        if raw_value is None:
            return None
        if isinstance(raw_value, datetime):
            return raw_value if raw_value.tzinfo else raw_value.replace(tzinfo=timezone.utc)
        if isinstance(raw_value, (int, float)):
            value = int(raw_value)
        else:
            text = str(raw_value).strip()
            if not text:
                return None
            if text.isdigit():
                value = int(text)
            else:
                try:
                    return datetime.fromisoformat(text.replace("Z", "+00:00"))
                except ValueError:
                    return None
        if value < 10_000_000_000:
            value *= 1000
        return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)

    def compute_return(self, previous_price: float, current_price: float) -> Optional[float]:
        # Paper formula (14): R_t = p_t / p_{t-1} - 1
        if previous_price is None or current_price is None:
            return None
        if previous_price <= 0 or current_price <= 0:
            return None
        return (current_price / previous_price) - 1.0

    def upsert_result(self, symbol: str, event_time: datetime, price: float, return_value: float, result: Dict, source: str) -> None:
        bucket_day = event_time.date()
        self.execute(
            """
            INSERT INTO stock_changepoint_latest (
                symbol, event_time, price, return_value, cp_prob,
                expected_run_length, map_run_length, predictive_volatility,
                innovation_zscore, whale_score, hazard, evidence,
                regime_label, source
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                symbol,
                event_time,
                price,
                return_value,
                result["cp_prob"],
                result["expected_run_length"],
                result["map_run_length"],
                result["predictive_volatility"],
                result["innovation_zscore"],
                result["whale_score"],
                result["hazard"],
                result["evidence"],
                result["regime_label"],
                source,
            ],
        )
        self.execute(
            """
            INSERT INTO stock_changepoint_history (
                symbol, bucket_date, event_time, price, return_value, cp_prob,
                expected_run_length, map_run_length, predictive_volatility,
                innovation_zscore, whale_score, hazard, evidence,
                regime_label, source
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                symbol,
                bucket_day,
                event_time,
                price,
                return_value,
                result["cp_prob"],
                result["expected_run_length"],
                result["map_run_length"],
                result["predictive_volatility"],
                result["innovation_zscore"],
                result["whale_score"],
                result["hazard"],
                result["evidence"],
                result["regime_label"],
                source,
            ],
        )

    def create_state(self) -> SymbolState:
        return SymbolState(model=ZeroMeanGaussianVarianceBOCPD(self.model_config))

    def get_state(self, symbol: str) -> SymbolState:
        if symbol not in self.states:
            self.states[symbol] = self.create_state()
        return self.states[symbol]

    def bootstrap_symbol(self, symbol: str) -> None:
        state = self.get_state(symbol)
        if state.bootstrapped:
            return

        try:
            rows = list(
                self.execute(
                    "SELECT timestamp, price FROM stock_prices WHERE symbol = %s LIMIT %s",
                    [symbol, BOOTSTRAP_LIMIT],
                )
            )
        except Exception as exc:
            logger.warning("Bootstrap query failed for %s: %s", symbol, exc)
            rows = []

        ordered = []
        for row in rows:
            event_time = self.parse_event_time(row.get("timestamp"))
            price = row.get("price")
            if event_time and price is not None:
                ordered.append((event_time, float(price)))
        ordered.sort(key=lambda item: item[0])

        emitted = 0
        for idx in range(1, len(ordered)):
            prev_time, prev_price = ordered[idx - 1]
            event_time, current_price = ordered[idx]
            return_value = self.compute_return(prev_price, current_price)
            if return_value is None:
                continue
            result = state.model.update(return_value)
            state.last_price = current_price
            state.last_event_time = event_time

            # Keep startup load bounded: only write the tail needed by the UI.
            if len(ordered) - idx <= BOOTSTRAP_HISTORY_LIMIT:
                self.upsert_result(symbol, event_time, current_price, return_value, result, "bootstrap")
                emitted += 1

        if ordered:
            state.last_event_time, state.last_price = ordered[-1]
        else:
            latest_rows = list(
                self.execute(
                    "SELECT symbol, price, timestamp FROM stock_latest_prices WHERE symbol = %s",
                    [symbol],
                )
            )
            if latest_rows:
                latest_row = latest_rows[0]
                state.last_price = float(latest_row["price"]) if latest_row.get("price") is not None else None
                state.last_event_time = self.parse_event_time(latest_row.get("timestamp"))

        state.bootstrapped = True
        logger.info("Bootstrapped %s with %d BOCPD updates", symbol, emitted)

    def sync_registry(self) -> None:
        self.registry.reload_if_changed()
        symbols = self.registry.get_all_symbols()
        if symbols != self.all_symbols:
            self.all_symbols = symbols
            logger.info("Tracking %d symbols for changepoint detection", len(self.all_symbols))
        for symbol in self.all_symbols:
            if symbol not in self.states:
                self.bootstrap_symbol(symbol)

    def process_live_rows(self) -> None:
        rows = list(
            self.execute(
                """
                SELECT symbol, price, timestamp
                FROM stock_latest_prices
                """
            )
        )

        for row in rows:
            symbol = row.get("symbol")
            if symbol not in self.all_symbols:
                continue

            price = row.get("price")
            event_time = self.parse_event_time(row.get("timestamp"))
            if price is None or event_time is None:
                continue

            state = self.get_state(symbol)
            price = float(price)

            if state.last_event_time == event_time and state.last_price == price:
                continue

            if state.last_price is None:
                state.last_price = price
                state.last_event_time = event_time
                continue

            return_value = self.compute_return(state.last_price, price)
            if return_value is None:
                state.last_price = price
                state.last_event_time = event_time
                continue

            result = state.model.update(return_value)
            self.upsert_result(symbol, event_time, price, return_value, result, "live")

            state.last_price = price
            state.last_event_time = event_time

    def run(self) -> None:
        self.connect()
        self.ensure_tables()
        self.sync_registry()
        logger.info("BOCPD worker started")

        try:
            while True:
                self.sync_registry()
                self.process_live_rows()
                time.sleep(POLL_INTERVAL)
        finally:
            self.close()


def main() -> None:
    worker = ChangepointWorker()
    worker.run()


if __name__ == "__main__":
    main()

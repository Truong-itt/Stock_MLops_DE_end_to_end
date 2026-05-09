import logging
import os
import sys
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from zoneinfo import ZoneInfo


DEFAULT_LOG_FORMAT = "%(asctime)s @%(name)s [%(levelname)s]: %(message)s"


class TZFormatter(logging.Formatter):
    def __init__(self, fmt: str, datefmt: str, timezone_name: str):
        super().__init__(fmt=fmt, datefmt=datefmt)
        try:
            self.zone = ZoneInfo(timezone_name)
        except Exception:
            self.zone = ZoneInfo("UTC")

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, self.zone)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()


def _build_formatter(log_format: str, timezone_name: str) -> logging.Formatter:
    return TZFormatter(
        fmt=log_format,
        datefmt="%Y-%m-%d %H:%M:%S",
        timezone_name=timezone_name,
    )


def configure_root_logging(
    logs_dir: str = "/var/log/whale_ml",
    log_filename: str = "whale_ml.log",
    log_format: str = DEFAULT_LOG_FORMAT,
    keep_days: int = 30,
    timezone_name: str = "Asia/Ho_Chi_Minh",
    level: int = logging.INFO,
) -> None:
    if getattr(configure_root_logging, "_configured", False):
        return

    logs_path = Path(logs_dir) / log_filename
    logs_path.parent.mkdir(parents=True, exist_ok=True)

    formatter = _build_formatter(log_format=log_format, timezone_name=timezone_name)

    file_handler = TimedRotatingFileHandler(
        filename=str(logs_path),
        when="D",
        interval=1,
        backupCount=max(int(keep_days), 1),
        encoding="utf-8",
        utc=False,
    )
    file_handler.suffix = "%Y-%m-%d"
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    existing_file = False
    existing_stream = False
    target_file = str(logs_path.resolve())
    for handler in root_logger.handlers:
        if isinstance(handler, TimedRotatingFileHandler) and getattr(handler, "baseFilename", "") == target_file:
            existing_file = True
        if isinstance(handler, logging.StreamHandler) and getattr(handler, "stream", None) is sys.stdout:
            existing_stream = True

    if not existing_file:
        root_logger.addHandler(file_handler)
    if not existing_stream:
        root_logger.addHandler(stream_handler)

    configure_root_logging._configured = True


def get_logger(
    logs_dir: str = "/var/log/whale_ml",
    log_filename: str = "whale_ml.log",
    log_format: str = DEFAULT_LOG_FORMAT,
    keep_days: int = 30,
) -> logging.Logger:
    timezone_name = "Asia/Ho_Chi_Minh"
    configure_root_logging(
        logs_dir=logs_dir,
        log_filename=log_filename,
        log_format=log_format,
        keep_days=keep_days,
        timezone_name=timezone_name,
    )
    return logging.getLogger("whale_ml.service")

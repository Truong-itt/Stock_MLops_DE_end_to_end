import logging
import os
import sys
from datetime import datetime
import pytz
from logging.handlers import TimedRotatingFileHandler

def get_logger(
        logs_dir: str = '/var/log/stockraw/',
        log_filename: str = None,
        log_format: str = '%(asctime)s @%(name)s [%(levelname)s]:    %(message)s',
        keep_days: int = 30
    ) -> logging.Logger:
    """
    Logger:
    - Rotate log mỗi ngày
    - Giữ tối đa keep_days
    - Tự xoá log cũ
    - Timezone Asia/Ho_Chi_Minh
    """

    # Lấy tên script nếu không truyền filename
    if log_filename is None:
        script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        log_filename = f"{script_name}.log"

    logs_path = os.path.join(logs_dir, log_filename)
    os.makedirs(os.path.dirname(logs_path), exist_ok=True)

    logger = logging.getLogger(log_filename)
    logger.setLevel(logging.INFO)

    # Tránh log bị nhân đôi
    if logger.handlers:
        return logger

    # Formatter timezone HCM
    class HCMFormatter(logging.Formatter):
        def __init__(self, fmt=None, datefmt=None, style='%'):
            super().__init__(fmt, datefmt, style)
            self.zone = pytz.timezone("Asia/Ho_Chi_Minh")

        def formatTime(self, record, datefmt=None):
            dt = datetime.fromtimestamp(record.created, self.zone)
            return dt.strftime(datefmt) if datefmt else dt.isoformat()

    formatter = HCMFormatter(
        log_format,
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Rotate mỗi ngày – giữ 30 ngày
    handler = TimedRotatingFileHandler(
        filename=logs_path,
        when="D",
        interval=1,
        backupCount=keep_days,
        encoding="utf-8",
        utc=False
    )

    handler.suffix = "%Y-%m-%d"
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    
    # Also log to stdout for Docker
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    logger.propagate = False

    return logger

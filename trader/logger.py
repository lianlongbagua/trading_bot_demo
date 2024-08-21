import logging
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler


def setup_logging(log_file="trading_app.log"):
    class UTCFormatter(logging.Formatter):
        def formatTime(self, record, datefmt=None):
            dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M:%S UTC")

    formatter = UTCFormatter("%(asctime)s - %(levelname)s - %(message)s")

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # File handler
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5
    )
    file_handler.setFormatter(formatter)

    # Set logging level for httpx
    logging.getLogger("httpx").setLevel(logging.CRITICAL)

    return console_handler, file_handler


class LoggedClass:
    def __init__(self, name):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        # Only add handlers if they don't exist
        if not self.logger.handlers:
            console_handler, file_handler = setup_logging()
            self.logger.addHandler(console_handler)
            self.logger.addHandler(file_handler)

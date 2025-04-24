import os
import logging
from logging.handlers import TimedRotatingFileHandler
from conf.config import LOG_FOLDER, LOGGING_VERBOSE

# Ensure log directory exists
os.makedirs(LOG_FOLDER, exist_ok=True)

def setup_logger(name="ApplicationLogger"):
    log_path = os.path.join(LOG_FOLDER, "alur.log")
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # 🧠 Prevent duplicate log entries
    logger.propagate = False

    # ✅ Only attach handlers once
    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        file_handler = TimedRotatingFileHandler(
            log_path, when="midnight", interval=1, backupCount=30, encoding="utf-8", utc=True
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger

# Shared logger instance
logger = setup_logger()

# 🔇 Verbose wrapper
def verbose(msg: str):
    if LOGGING_VERBOSE:
        logger.info(msg)

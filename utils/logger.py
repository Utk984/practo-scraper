import logging
import os
import sys
from logging.handlers import RotatingFileHandler

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)


# Configure logger
def setup_logger(name, log_file, level=logging.INFO):
    """Function to set up a logger with both file and console handlers"""
    # Create a custom logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Create handlers
    # Console handler
    # c_handler = logging.StreamHandler(sys.stdout)
    # c_handler.setLevel(level)

    # File handler with rotation (10MB max size, keep 5 backup files)
    f_handler = RotatingFileHandler(
        os.path.join("logs", log_file), maxBytes=10 * 1024 * 1024, backupCount=5
    )
    f_handler.setLevel(level)

    # Create formatters and add them to handlers
    # c_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)

    # Add handlers to the logger if they're not already there
    if not logger.handlers:
        # logger.addHandler(c_handler)
        logger.addHandler(f_handler)

    return logger


# Main application logger
app_logger = setup_logger("practo", "practo.log")

# Database logger
db_logger = setup_logger("practo.db", "db.log")

# API request logger
request_logger = setup_logger("practo.request", "requests.log")

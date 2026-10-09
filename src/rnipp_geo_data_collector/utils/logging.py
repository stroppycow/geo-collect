import logging
import sys
from datetime import datetime
from pathlib import Path


class ISOFormatter(logging.Formatter):
    """Formatter using an ISO 8601 timestamp."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        return (
            datetime.fromtimestamp(record.created)
            .astimezone()
            .isoformat(timespec="milliseconds")
        )


def configure_logging(
    loglevel: str = "INFO",
    log_stdout: bool = True,
    log_file: str | Path | None = None,
) -> None:
    """Configure the root logger with an ISO 8601 date format.

    Log records are emitted to stdout, to a file, or both.
    """
    formatter = ISOFormatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    root_logger = logging.getLogger()
    root_logger.setLevel(loglevel.upper())
    root_logger.handlers.clear()

    if log_stdout:
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(formatter)
        root_logger.addHandler(stdout_handler)

    if log_file is not None:
        log_file_path = Path(log_file)
        log_file_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    if not root_logger.handlers:
        root_logger.addHandler(logging.NullHandler())

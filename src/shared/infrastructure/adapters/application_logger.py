import logging
import os
from shared.application.ports.logger_port import LoggerPort
from shared.application.ports.progress_bar_port import ProgressBarPort

log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

class ApplicationLogger(LoggerPort):
    
    def __init__(self, progress_bar: ProgressBarPort | None = None):
        self._logger = logging.getLogger(__name__)
        self._progress_bar = progress_bar

    def set_progress_bar(self, progress_bar: ProgressBarPort) -> None:
        self._progress_bar = progress_bar

    def debug(self, message: str) -> None:
        if self._progress_bar:
            self._progress_bar.write(message)
        else:
            self._logger.debug(f"Debug: {message}")

    def info(self, message: str) -> None:
        if self._progress_bar:
            self._progress_bar.write(message)
        else:
            self._logger.info(f"Info: {message}")

    def error(self, message: str) -> None:
        if self._progress_bar:
            self._progress_bar.write(message)
        else:
            self._logger.error(f"Error: {message}")

    def warning(self, message: str) -> None:
        if self._progress_bar:
            self._progress_bar.write(message)
        else:
            self._logger.warning(f"Warning: {message}")
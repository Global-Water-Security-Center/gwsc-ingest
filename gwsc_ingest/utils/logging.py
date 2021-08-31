import logging
import sys


def setup_basic_logging(log_level=logging.DEBUG):
    """
    Configure basic console logging for commandline executed scripts.

    Args:
        log_level: The log level to log at. Defaults to logging.DEBUG.
    """
    formatter = GwscConsoleFormatter()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logging.root.addHandler(handler)
    logging.root.setLevel(log_level)


class GwscConsoleFormatter(logging.Formatter):
    """
    Format messages differently based on log level.
    """
    def format(self, record):
        if record.levelno == logging.INFO:
            self._style._fmt = "%(message)s"
        else:
            self._style._fmt = "%(levelname)s: %(message)s"
        return super().format(record)

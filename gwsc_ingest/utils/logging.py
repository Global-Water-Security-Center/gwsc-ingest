import logging


def setup_basic_logging(log_level=logging.DEBUG):
    """
    Configure basic console logging for commandline executed scripts.

    Args:
        log_level: The log level to log at. Defaults to logging.DEBUG.
    """
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s::%(levelname)s::%(module)s::%(lineno)d::%(message)s',
        datefmt='%Y-%m-%d %I:%M:%S'
    )

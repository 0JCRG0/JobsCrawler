import logging


def get_custom_logger(name: str) -> logging.Logger:
    """
    Get a custom logger with a specific name.
    Parameters
    ----------
    name: str
        The name of the logger.
    Returns
    -------
    logging.Logger
        The custom logger.
    """
    logging.basicConfig(level=logging.INFO)
    if not logging.getLogger(name).handlers:
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        logger.propagate = False

        # Add the formatter to the logger
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(fmt='%(levelname)s: %(asctime)s - "%(name)s" | %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
        )

        logger.addHandler(handler)
    else:
        logger = logging.getLogger(name)

    return logger
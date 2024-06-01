import logging
from typing import Type

# Create a custom formatter
class LogLevelFormatter(logging.Formatter):
    def format(self, record):
        if record.levelno == logging.DEBUG:
            self._style._fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        elif record.levelno == logging.INFO:
            self._style._fmt = "%(levelname)s: %(message)s" 
        else:
            self._style._fmt = "%(message)s"
        return super().format(record)

def get_logger(name: str) -> Type[logging.Logger]:

    # create a logger instance
    return logging.getLogger(name)


def configure_logger(logger: Type[logging.Logger]) -> None:

    # logger configuration
    # basic
    # logging.basicConfig(
    #     level=logging.INFO,
    #     # format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    #     format="%(message)s",
    # )

    handler: Type[logging.StreamHandler] = logging.StreamHandler()
    formatter: Type[logging.Formatter] = logging.Formatter()
    handler.setFormatter(formatter)

    # attach handler to logger
    logger.addHandler(handler)

    # Set logging level
    logger.setLevel(logging.INFO)


# configure_logger()
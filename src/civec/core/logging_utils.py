import logging

_logging_configured = False


def configure_logging(service_name: str, level: int = logging.INFO) -> logging.Logger:
    global _logging_configured
    if not _logging_configured:
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
        _logging_configured = True
    logger = logging.getLogger(service_name)
    return logger

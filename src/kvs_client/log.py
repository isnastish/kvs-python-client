import logging
import traceback
import json

class JsonFormatter(logging.Formatter):
    def format(self, record) -> str:
        msg = {
            "level": record.levelname,
            "module": record.name,
        }

        if isinstance(record.msg, dict):
            msg = record.msg | msg
        else:
            msg["message"] = record.getMessage()

        if record.exc_info:
            exc_type, e, tb = record.exc_info
            msg["exception_type"] = exc_type.__name__
            msg["exception_info"] = str(e)
            msg["traceback"] = "\n".join(traceback.format_tb(tb))

        return json.dumps(msg)


class LogAugmentationFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return True


LOGGING_CONFIG = {
    "version": 1,
    "formatters": {
        "json": {
            "()": JsonFormatter,
        },
    },
    "filters": {
        "kvs": {
            "()": LogAugmentationFilter,
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "json",
            "filters": ["kvs"],
            "stream": "ext://sys.stderr",
        },
    },
    "loggers": {
        "": {"handlers": ["console"], "level": "INFO"},
    },
    "disable_existing_loggers": False,
}

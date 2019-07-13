import os
import time
import logging.config
import schedule

from update.run import index_update, finance_update


def run():
    # 生产者
    # schedule.every(3).seconds.do(index_update)
    schedule.every(3).seconds.do(finance_update)

    # 开始调度任务之前执行第一次
    # index_update()
    finance_update()

    while True:
        # 消费者
        schedule.run_pending()
        time.sleep(10)


# 模块日志配置
logging.config.dictConfig({
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "simple": {
            "format": "[%(levelname)1.1s %(asctime)s|%(module)s|%(funcName)s|%(lineno)d] %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        }
    },
    "handlers": {
        "console": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": 'simple',
            "stream": "ext://sys.stdout"
        },
        "main_file_log": {
            "level": "DEBUG",
            "class": "logging.handlers.TimedRotatingFileHandler",
            "filename": os.path.join(os.getcwd(), "logs/main.log"),
            "formatter": "simple",
            "when": "D",
            "backupCount": 5
        },

        "index_file_log": {
            "level": "DEBUG",
            "class": "logging.handlers.TimedRotatingFileHandler",
            "filename": os.path.join(os.getcwd(), "logs/index/index.log"),
            "formatter": "simple",
            "when": "D",
            "backupCount": 5
        },

        "finance_file_log": {
            "level": "DEBUG",
            "class": "logging.handlers.TimedRotatingFileHandler",
            "filename": os.path.join(os.getcwd(), "logs/finance/finance.log"),
            "formatter": "simple",
            "when": "D",
            "backupCount": 5
        },
    },
    "loggers": {
        "main_log": {
            "level": "DEBUG",
            "handlers": ["console", "main_file_log"]
        },
        "index_log": {
            "level": "DEBUG",
            "handlers": ["console", "index_file_log"]
        },
        "finance_log": {
            "level": "DEBUG",
            "handlers": ["console", "finance_file_log"]
        },
    }
})


logger = logging.getLogger("main_log")


run()

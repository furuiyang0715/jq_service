import os
import threading
import time
import logging.config
import schedule

from update.calendars.hotfix import hot_inc
from update.run import index_update, finance_update, calendars_inc, calendars_detection


def run_threaded(job_func):
    """确保任务能在线程中运行完毕"""
    job_thread = threading.Thread(target=job_func)
    job_thread.start()


def run():
    # 生产者
    schedule.every(30).days.do(run_threaded, index_update)
    schedule.every(5).days.do(run_threaded, finance_update)
    schedule.every().day.at("02:00").do(run_threaded, calendars_inc)
    schedule.every(3).seconds.do(run_threaded, calendars_detection)

    # 开始调度任务之前执行第一次
    index_update()
    finance_update()
    calendars_inc()

    # 在执行之前进行热修复
    hot_inc()

    while True:
        # 消费者
        logger.info(schedule.jobs)
        schedule.run_pending()
        time.sleep(3)


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
            "filename": os.path.join(os.getcwd(), "update/logs/main.log"),
            # "filename": "./logs/main.log",
            "formatter": "simple",
            "when": "D",
            "backupCount": 5
        },

        "index_file_log": {
            "level": "DEBUG",
            "class": "logging.handlers.TimedRotatingFileHandler",
            "filename": os.path.join(os.getcwd(), "update/logs/index/index.log"),
            # "filename": "./logs/index/index.log",
            "formatter": "simple",
            "when": "D",
            "backupCount": 5
        },

        "finance_file_log": {
            "level": "DEBUG",
            "class": "logging.handlers.TimedRotatingFileHandler",
            "filename": os.path.join(os.getcwd(), "update/logs/finance/finance.log"),
            # "filename": "./logs/finance/finance.log",
            "formatter": "simple",
            "when": "D",
            "backupCount": 5
        },
        "inc_file_log": {
            "level": "DEBUG",
            "class": "logging.handlers.TimedRotatingFileHandler",
            "filename": os.path.join(os.getcwd(), "update/logs/calendars/inc.log"),
            # "filename": "./logs/calendars/inc.log",
            "formatter": "simple",
            "when": "D",
            "backupCount": 5
        },
        "detection_file_log": {
            "level": "DEBUG",
            "class": "logging.handlers.TimedRotatingFileHandler",
            "filename": os.path.join(os.getcwd(), "update/logs/calendars/detection.log"),
            # "filename": "./logs/calendars/detection.log",
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
        "inc_log": {
            "level": "DEBUG",
            "handlers": ["console", "inc_file_log"]
        },
        "detection_log": {
            "level": "DEBUG",
            "handlers": ["console", "detection_file_log"]
        },
    }
})


logger = logging.getLogger("main_log")

import os
import time
import logging.config
import schedule

from update.run import index_update, finance_update, calendars_inc, calendars_detection


def run():
    # 生产者

    # print("into")

    # schedule.every(30).days.do(index_update)
    # schedule.every(5).days.do(finance_update)
    # schedule.every().day.at("02:00").do(calendars_inc)
    schedule.every().minute.do(calendars_detection)

    # 开始调度任务之前执行第一次
    # index_update()
    # finance_update()
    # calendars_inc()
    # calendars_detection()

    while True:
        # 消费者
        # print("1------>", time.time())
        # print(schedule.jobs)
        logger.info(schedule.jobs)
        schedule.run_pending()
        # print("2------>", time.time())
        time.sleep(300)


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


# run()
# print(os.getcwd())
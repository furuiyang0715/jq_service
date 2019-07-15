import datetime
import logging
# from raven import Client
from update.calendars.detection import task
from update.calendars.inc_sync import inc_update
from update.finance.base_sync import BaseFinanceSync
from update.index.index_sync import IndexSync

# sentry = Client("https://330e494ccd22497db605a102491c0423@sentry.io/1501024")
from update.utils import sentry

logger = logging.getLogger("main_log")


def calendars_inc():
    logger.info(f"现在是 {datetime.datetime.today()}, 开始增量更新 calendars 数据")
    sentry.captureMessage(f"现在是 {datetime.datetime.today()}, 开始增量更新 calendars 数据")
    try:
        inc_update()
    except Exception:
        sentry.captureException(exc_info=True)


def calendars_detection():
    logger.info(f"现在是 {datetime.datetime.today()}, 开始检测拉取更新 calendars 数据")
    try:
        task()
        pass
    except Exception:
        sentry.captureException(exc_info=True)


def finance_update():
    logger.info(f"现在是 {datetime.datetime.today()}, 开始更新 finance 数据")
    sentry.captureMessage(f"现在是 {datetime.datetime.today()}, 开始更新 finance 数据。")
    try:
        BaseFinanceSync().daily_sync()
    except Exception:
        sentry.captureException(exc_info=True)


def index_update():
    logger.info(f"现在是 {datetime.datetime.today()}, 开始更新 index 数据。")
    sentry.captureMessage(f"现在是 {datetime.datetime.today()}, 开始更新 index 数据。")
    try:
        IndexSync().daily_sync()
    except Exception:
        sentry.captureException(exc_info=True)

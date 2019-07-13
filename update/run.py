import datetime
import logging
from raven import Client

from update.finance.base_sync import BaseFinanceSync
from update.index.index_sync import IndexSync

sentry = Client("https://330e494ccd22497db605a102491c0423@sentry.io/1501024")

logger = logging.getLogger("main_log")


def calendars_inc():

    pass


def calendars_detection():

    pass


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

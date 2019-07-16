import datetime
import logging
from update.calendars.check_data import check
from update.calendars.detection import task
from update.finance.base_sync import BaseFinanceSync
from update.index.index_sync import IndexSync
from update.utils import sentry, catch_exceptions
logger = logging.getLogger("main_log")


@catch_exceptions(cancel_on_failure=True)
def calendars_check():
    logger.info(f"现在是 {datetime.datetime.today()}, 开始增量 calendars 数据")
    sentry.captureMessage(f"现在是 {datetime.datetime.today()}, 开始增量 calendars 数据")
    check()


@catch_exceptions(cancel_on_failure=True)
def calendars_detection():
    # sentry.captureMessage(f"现在是 {datetime.datetime.today()}, 开始拉取 calendars 数据")
    logger.info(f"现在是 {datetime.datetime.today()}, 开始拉取 calendars 数据")
    task()


@catch_exceptions(cancel_on_failure=True)
def finance_update():
    logger.info(f"现在是 {datetime.datetime.today()}, 开始更新 finance 数据")
    sentry.captureMessage(f"现在是 {datetime.datetime.today()}, 开始更新 finance 数据。")
    BaseFinanceSync().daily_sync()


@catch_exceptions(cancel_on_failure=True)
def index_update():
    logger.info(f"现在是 {datetime.datetime.today()}, 开始更新 index 数据。")
    sentry.captureMessage(f"现在是 {datetime.datetime.today()}, 开始更新 index 数据。")
    IndexSync().daily_sync()

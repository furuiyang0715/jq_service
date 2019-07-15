import datetime
import logging
from update.calendars.detection import task
from update.calendars.inc_sync import inc_update
from update.finance.base_sync import BaseFinanceSync
from update.index.index_sync import IndexSync
from update.utils import sentry, catch_exceptions

logger = logging.getLogger("main_log")


@catch_exceptions(cancel_on_failure=True)
def calendars_inc():
    logger.info(f"现在是 {datetime.datetime.today()}, 开始增量更新 calendars 数据")
    sentry.captureMessage(f"现在是 {datetime.datetime.today()}, 开始增量更新 calendars 数据")
    inc_update()
    # try:
    #     inc_update()
    # except Exception:
    #     sentry.captureException(exc_info=True)


@catch_exceptions(cancel_on_failure=True)
def calendars_detection():
    logger.info(f"现在是 {datetime.datetime.today()}, 开始检测拉取更新 calendars 数据")
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

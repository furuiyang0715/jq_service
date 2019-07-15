import datetime
import functools
import logging

import schedule

from update.calendars.detection import task
from update.calendars.inc_sync import inc_update
from update.finance.base_sync import BaseFinanceSync
from update.index.index_sync import IndexSync
from update.utils import sentry

logger = logging.getLogger("main_log")


def catch_exceptions(job_func, cancel_on_failure=False):
    @functools.wraps(job_func)
    def wrapper(*args, **kwargs):
        try:
            return job_func(*args, **kwargs)
        except:
            import traceback
            logger.info(traceback.format_exc())
            # 向 sentry 发送错误信息
            sentry.captureException(exc_info=True)
            if cancel_on_failure:
                return schedule.CancelJob
    return wrapper


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


# @catch_exceptions(cancel_on_failure=True)
# def bad_task():
#     return 1 / 0

# schedule.every(5).minutes.do(bad_task)

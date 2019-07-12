import sys

from logbook import Logger, StreamHandler
StreamHandler(sys.stdout).push_application()

logger = Logger('Logbook')


def calendars_update():
    logger.info("开始更新交易日历")
    pass


def index_update():
    logger.info("开始更新指数数据")
    pass


def finance_update():
    logger.info("开始更新金融数据")
    pass


def update():
    # 主程序

    # 对于交易日历数据的更新
    calendars_update()

    # 对于 index 数据的更新
    index_update()

    # 对于金融数据的更新
    finance_update()


if __name__ == "__main__":
    update()
    pass
import datetime
import time
import logging

from update import utils
from update.calendars.all_codes import all_codes
from update.calendars.gen_delisted_days import gen_delisted_info, gen_delisted_days
from update.calendars.gen_market_days import gen_sh000001
from update.calendars.gen_sus_days import gen_inc_code_sus

logger = logging.getLogger()


def log_method_time_usage(func):
    def wrapped(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        dt = time.time()-start
        if dt > 0.1:
            logger.info(f"[TimeUsage] {func.__module__}.{func.__name__} usage: {dt}")
        return result
    return wrapped


def gen_diff(market_start, end, timestamp):
    """

    :param market_start:
    :param end:
    :param timestamp:
    :return:
    """
    market_sus = gen_sh000001(market_start, end, timestamp)
    # for code in all_codes:
    for code in ["300576"]:
        start = None
        # start = utils.gen_last_mongo_date(code)
        if not start:
            start = market_start
        sus = gen_inc_code_sus(code, start, end, timestamp)
        delisted_infos = gen_delisted_info(code, timestamp)
        delisted = gen_delisted_days(delisted_infos, end)
        if not sus:
            sus = []
        single_sus = set(market_sus + sus + delisted) - set(market_sus)

        logger.info(f"code: {code}")
        logger.info(f"single_sus: {single_sus}")

        yield code, single_sus


def bulk_insert(code, sus):
    logger.info(f"{code} 进入增加流程")
    coll = utils.gen_calendars_coll()
    bulks = list()
    # 将 code 转换为 带前缀的格式
    f_code = utils.code_convert(code)
    for s in sus:
        bulks.append({"code": f_code, "date": s, "date_int": utils.yyyymmdd_date(s), "ok": False})
    try:
        ret = coll.insert_many(bulks)
    except Exception as e:
        # 批量插入出错的话
        logger.warning(f"批量插入有误，错误的原因是 {e}")


def bulk_delete(code, sus):
    logger.info(f"{code} 进入删除流程")
    coll = utils.gen_calendars_coll()
    f_code = utils.code_convert(code)
    try:
        ret = coll.delete_many({"code": f_code, "date": {"$in": list(sus)}})
    except Exception as e:
        logger.info(f'批量删除有误，错误的原因是 {e}')


def check_mongo_diff(code, single_sus, ALREADYCHECK=False, DEL=False):
    """
    DEL 是个标识位 表示是否根据 singe_sus 来调整删除原有数据 默认是只增加 不删除
    ALREADYCHECK 也是个标志位 表示在插入新数据时，是否对数据库已经有该数据进行检查 默认是增量更新
    数据都是原来没有插入过的 不检查
    :param code:
    :param single_sus:
    :param ALREADYCHECK:
    :param DEL:
    :return:
    """
    logger.info(f"股票代码是：{code} ")
    already_sus = list()

    if ALREADYCHECK:  # 需要对已经有的数据进行插入重复检查
        coll = utils.gen_calendars_coll()
        f_code = utils.code_convert(code)
        cursor = coll.find({"code": f_code, "ok": False}, {"date": 1, "_id": 0})
        already_sus = [r.get("date") for r in cursor]
        logger.info(f"already_sus :{already_sus}")
        add_sus = set(single_sus) - set(already_sus)  # 需要插入的
        logger.info(f"需要新插入的数据: {add_sus} \n 插入数量是： {len(add_sus)}")
    else:
        add_sus = single_sus

    if DEL:  # 需要检测后面可能又被删除数据
        del_sus = set(already_sus) - set(single_sus)  # 需要删除的
    else:
        del_sus = set()
    return add_sus, del_sus


@log_method_time_usage
def inc():
    end_time = utils.gen_limit_date()
    timestamp = datetime.datetime.now()
    market_start = utils.market_first_day()
    logger.info(f" market_start: {market_start}")
    logger.info(f" end_time: {end_time}")
    logger.info(f" timestamp: {timestamp}")

    for code, single_sus in gen_diff(market_start, end_time, timestamp):
        add_sus, del_sus = check_mongo_diff(code, single_sus, ALREADYCHECK=True, DEL=True)

        if add_sus:
            logger.info("="*200)
            logger.info(f"需要新插入的数据: {add_sus} \n 插入数量是： {len(add_sus)}")
            bulk_insert(code, add_sus)
        if del_sus:
            logger.info("*"*200)
            logger.info(f"需要新删除的数据: {del_sus} \n 插入数量是： {len(del_sus)}")
            bulk_delete(code, del_sus)


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    inc()

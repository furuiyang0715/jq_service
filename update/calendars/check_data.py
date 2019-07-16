import datetime
import sys
import logging
from update import utils
from update.calendars.gen_delisted_days import gen_delisted_info, gen_delisted_days
from update.calendars.gen_market_days import gen_sh000001
from update.calendars.gen_sus_days import gen_inc_code_sus

logger = logging.getLogger("inc_log")
MARKET_LIMIT_DATE = datetime.datetime(2020, 1, 1)
error_code = list()
cld = utils.gen_calendars_coll()


def convert_front_map(codes):
    def convert(code):
        if code[0] == "0" or code[0] == "3":
            return "SZ" + code
        elif code[0] == "6":
            return "SH" + code
        else:
            logger.warning("wrong code: ", code)
            sys.exit(1)
    res_map = dict()
    for c in codes:
        res_map.update({c: convert(c)})

    return res_map


def market_first_day():
    start = None
    conn = utils.DC()
    query_sql = "select Date from const_tradingday where SecuMarket=83 order by Date asc limit 1"
    try:
        with conn.cursor() as cursor:
            cursor.execute(query_sql)
            res = cursor.fetchall()
            for column in res:
                start = column[0]
    finally:
        conn.commit()
    return start


def check():
    # 确定一个快照时间戳
    timestamp = datetime.datetime.now()
    logger.info(f"开始检查数据的一致性，本次检查的快照时间戳是 {timestamp}")
    # 检验的截止时间
    limit_date = utils.gen_limit_date()
    # 拿到所有的codes
    codes = utils.gen_sync_codes()
    codes_map = convert_front_map(codes)
    market_start = market_first_day()

    for code in codes:
        logger.info("")
        logger.info(f"code: {code}")
        # 转换为前缀模式
        f_code = codes_map.get(code)

        logger.info("市场停牌： ")
        market_sus = gen_sh000001(market_start, limit_date, timestamp)
        if market_sus:
            logger.info(f"market_sus_0: {market_sus[0]}")
            logger.info(f"market_sus_-1: {market_sus[-1]}")

        logger.info("个股停牌： ")
        code_sus = gen_inc_code_sus(code, market_start, limit_date, timestamp)
        if code_sus:
            logger.info(f"code_sus_0: {code_sus[0]}")
            logger.info(f"code_sus_-1: {code_sus[-1]}")
        else:
            logger.info(f"{code} no suspended days")

        logger.info("个股退市： ")
        infos = gen_delisted_info(code, timestamp)
        delisted = gen_delisted_days(infos, limit_date)
        if delisted == "no_records":
            error_code.append(code)
            continue
        if delisted:
            logger.info(f"delisted_0: {delisted[0]}")
            logger.info(f"delisted_-1: {delisted[-1]}")
        else:
            logger.info(f"{code} no delisted")

        # 在最新的 mysql 数据库中查询出的 all_sus
        all_sus = sorted(set(market_sus + code_sus + delisted) - set(market_sus))
        all_sus = [utils.yyyymmdd_date(dt) for dt in all_sus]

        # 生成 mongo 数据进行核对
        cursor = cld.find({"code": f_code, "ok": False}, {"date_int": 1, "_id": 0})
        mongo_sus = [j.get("date_int") for j in cursor]

        # 进行两项数据的核对
        logger.info(f"len(all_sus) : {len(all_sus)}")
        logger.info(f"len(mongo_sus): {len(mongo_sus)}")

        if set(all_sus) == set(mongo_sus):
            logger.info("check right!")
        else:
            logger.info("check wrong!")

            # update
            real_sus_dates = set(all_sus) - set(mongo_sus)
            logger.info(f"real_sus_dates: {real_sus_dates}")
            for sus in real_sus_dates:
                # 有则更新
                if list(cld.find({"code": f_code, "date_int": sus})):
                    cld.update_one({"code": f_code, "date_int": sus}, {"$set": {"ok": False}})
                else:
                    # 无则插入
                    data = {"code": f_code, "date_int": sus, 'date': back_convert_date_int(sus), "ok": False}
                    cld.insert(data)

            real_trading_dates = set(mongo_sus) - set(all_sus)
            logger.info(f"real_trading_dates: {real_trading_dates}")
            if real_trading_dates:
                cld.delete_many({"code": f_code, "date_int": {"$in": list(real_trading_dates)}})


def back_convert_date_int(date_int):
    """
    convert date_int to datetime
    :param date_int:
    :return:
    """
    _year, _ret = divmod(date_int, 10000)
    _month, _day = divmod(_ret, 100)
    return datetime.datetime(_year, _month, _day)


# if __name__ == "__main__":
#     logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
#     check()


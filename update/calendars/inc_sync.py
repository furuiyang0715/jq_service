from update import utils
from update.utils import sentry

nolists = list()
import logging
logger = logging.getLogger("inc_log")


# def get_last_date(code):
#     MongoUri = myconfig.get("MONGO_URL", "mongodb://172.17.0.1:27017")
#     db = pymongo.MongoClient(MongoUri)
#     # cld = db.stock.calendar
#
#     coll_name = myconfig.get("MONGO_DBNAME", "stock")
#     cld = db[coll_name]["calendar"]
#
#     f_code = code_convert(code)
#     cursor = cld.find({"code": f_code}, {"date": 1}).sort([("date", pymongo.DESCENDING)]).limit(1)
#     try:
#         date = cursor.next().get("date")
#     except:
#         date = None
#     return date


# def gen_dates(b_date, days):
#     day = datetime.timedelta(days=1)
#     for i in range(days):
#         yield b_date + day*i


# def get_date_list(start=None, end=None):
#     data = list()
#     for d in gen_dates(start, (end-start).days+1):
#         data.append(d)
#     return data


# def yyyymmdd_date(dt: datetime) -> int:
#     return dt.year * 10 ** 4 + dt.month * 10 ** 2 + dt.day
#
#
# def code_convert(code):
#     if code[0] == "0" or code[0] == "3":
#         return "SZ" + code
#     elif code[0] == "6":
#         return "SH" + code
#     else:
#         logger.warning(f"wrong code: {code}")
#         sys.exit(1)
#
#
# def inc_insert(code, day, ok):
#     MongoUri = myconfig.get("MONGO_URL", "mongodb://172.17.0.1:27017")
#     db = pymongo.MongoClient(MongoUri)
#
#     coll_name = myconfig.get("MONGO_DBNAME", "stock")
#     cld = db[coll_name]["calendar"]
#
#     data = {"code": code_convert(code),
#             "date": day,
#             "date_int": yyyymmdd_date(day),
#             "ok": ok}
#     logger.info(data)
#     logging.info("")
#     logging.info("")
#     logging.info("")
#     cld.insert_one(data)


def inc_sync(code, limit_date):
    # conf = {
    #     "user": "dcr",
    #     "password": "acBWtXqmj2cNrHzrWTAciuxLJEreb*4EgK4",
    #     "host": "139.159.176.118",
    #     "port": 3306,
    #     "sqlDBname": "datacenter"
    # }
    # mysql_string = f"mysql+pymysql://{conf['user']}:{conf['password']}@{conf['host']}:\
    #     {conf.get('port')}/{conf['sqlDBname']}?charset=gbk"
    # DATACENTER = create_engine(mysql_string)

    DATACENTER = utils.DC2()
    # 获取某只股票上一次在 mongo 数据库中的最大记录
    # last_date = get_last_date(code)
    last_date = utils.gen_last_mongo_date(code)

    logger.info(last_date)
    if not last_date:
        logger.info(f"请先进行首次更新{code}")
        # nollists 中记录有问题的code
        nolists.append(code)

    elif last_date == limit_date:
    # elif False:
        logger.info(f'{code} 无需更新')

    else:
        # 生成包含起止时间的日期列表
        inc_days = utils.get_date_list(last_date, limit_date)
        # 去除已经入库的起始时间
        inc_days.remove(last_date)

        logger.info(f"code: {code}")
        logger.info(f"inc days: {inc_days}")
        for inc_day in inc_days:
            logger.info(f"inc_day: {inc_day}")
            # (1) 判断是否是交易日
            query1 = f"""select IfTradingDay from const_tradingday where Date = '{inc_day}' 
            and CMFTime = (select max(CMFTime) from const_tradingday where Date = '{inc_day}');"""
            if_trading_day = DATACENTER.execute(query1).first()[0]
            # print(if_trading_day)
            logger.info(f"if_trading_day: {if_trading_day}")
            if if_trading_day == 2:
                ok = False
                utils.inc_insert(code, inc_day, ok)
                continue
            # (2) 判断是否是停牌日
            query2 = f"""select NoticeStartDate, NoticeEndDate from stk_specialnotice_new
             where SecuCode = {code} 
             and NoticeTypei = 18 
             and NoticeTypeii != 1703
             and NoticeStartDate <= '{inc_day}'
             order by NoticeStartDate desc
             limit 1
             ;"""
            cursor = DATACENTER.execute(query2).first()
            if not cursor:
                ok = True
                utils.inc_insert(code, inc_day, ok)
                continue
            notice_end_date = DATACENTER.execute(query2).first()[1]
            logging.info(f"notice_end_date: {notice_end_date}")
            if not notice_end_date:
                if_suspended_day = True
            elif notice_end_date < inc_day:
                if_suspended_day = False
            else:
                if_suspended_day = True
            if if_suspended_day:
                ok = False
                utils.inc_insert(code, inc_day, ok)
                continue
            # (3) 判断是否退市日
            query3 = f"""SELECT 
            A.InnerCode,A.SecuCode, 
--             A.ListedDate,
            B.ChangeDate,B.ChangeType 
            from stk_liststatus B,const_secumainall A 
            WHERE A.InnerCode=B.InnerCode 
            AND A.SecuMarket IN (83,90) 
            AND A.SecuCategory=1 
            AND B.ChangeType IN (1,2,3,4,5,6) 
            AND A.SecuCode = '{code}'
            AND B.ChangeDate <= '{inc_day}'
            order by B.ChangeDate DESC 
            limit 1
            ; """
            cursor = DATACENTER.execute(query3).first()
            if not cursor:
                utils.inc_insert(code, inc_day, True)
                continue
            # 判断是否是退市日
            # 1-上市，2-暂停上市，3-恢复上市，4-终止上市，5-摘牌，6-退市整理期
            type = DATACENTER.execute(query3).first()[3]
            # print(type)
            logger.info(f'type: {type}')
            # sys.exit(0)
            if type in (1, 3):
                if_listed_day = False
            else:
                if_listed_day = True
            if if_listed_day:
                ok = False
                utils.inc_insert(code, inc_day, ok)
                continue
            utils.inc_insert(code, inc_day, True)


# def gen_sync_codes():
#     from all_codes import all
#     codes = all
#     return codes


# def gen_limit_date():
#     limit_date = datetime.datetime.combine(datetime.date.today(), datetime.time.min) + datetime.timedelta(days=1)
#     return limit_date


def inc_update():
    # 生成当日的下一天作为同步的截止日期
    limit_date = utils.gen_limit_date()
    # 被同步的全部股票代码
    codes = utils.gen_sync_codes()
    for code in codes:
        logger.info(f"{code} begin to inc update")
        inc_sync(code, limit_date)
    logger.info(f"本次更新交易日历出现异常的代码:{nolists}")
    if nolists:
        sentry.captureMessage(f"本次更新交易日历出现异常的代码: {nolists}")


# if __name__ == "__main__":
#     # 生成当日的下一天作为同步的截止日期
#     limit_date = utils.gen_limit_date()
#     # 被同步的全部股票代码
#     codes = utils.gen_sync_codes()
#     for code in codes:
#         logger.info(f"{code} begin to inc update")
#         inc_sync(code, limit_date)
#     logger.info(nolists)

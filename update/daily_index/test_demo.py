import datetime
import logging
import pprint
import time

from sqlalchemy import create_engine

from update import utils

MYSQL_USER = "dcr"
MYSQL_PASSWORD = "acBWtXqmj2cNrHzrWTAciuxLJEreb*4EgK4"
MYSQL_HOST = "139.159.176.118"
MYSQL_PORT = 3306
MYSQL_DB = "datacenter"


def DC():

    #  mysql_string = f"""mysql+pymysql://{cf['user']}:{cf['password']}@{cf['host']}:{cf.get('port')
    #     }/{cf['sqlDBname']}?charset=gbk"""
    mysql_string = f"""mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/
                       {MYSQL_DB}?charset=gbk"""

    # print(mysql_string)

    cli = create_engine(mysql_string)

    return cli

# conn = DC()
# check_all_indexes_query = "select distinct(IndexCode) from datacenter.index_weight;"
# all_indexes = list((i[0] for i in conn.execute(check_all_indexes_query).fetchall()))
# index_we_need = [
#         "000001",  # 上证指数
#         "399001",  # 深证成指
#         "399005",  # 中小板指
#         "399006",  # 创业板指
#         "399004",  # 深证100R
#         "399007",  # 深证300
#         "399008",  # 中小300
#         "000016",  # 上证50
#         "000010",  # 上证180
#         "000009",  # 上证380
#         "000300",  # 沪深300
#         "000903",  # 中证100
#         "000904",  # 中证200
#         "000905",  # 中证500
#         "000922",  # 中证红利
#         "000969",  # 300非周
#         "399372",  # 大盘成长
#         "399373",  # 大盘价值
#         "399374",  # 中盘成长
#         "399375",  # 中盘价值
#         "399376",  # 小盘成长
#         "399377",  # 小盘价值
#         "000015",  # 红利指数
#         "000019",  # 治理指数
#         "000043",  # 超大盘
#         "000044",  # 上证中盘
#         "399346",  # 深证成长
#         "399324",  # 深证红利
#         "399328",  # 深证治理
#         "399348",  # 深证价值
#         "399678",  # 深次新股
#         "399016",  # 深证创新
#         "399370",  # 国证成长
#         "399366",  # 国证大宗
#         "399320",  # 国证服务
#         "399321",  # 国证红利
#         "399359",  # 国证基建
#         "399371",  # 国证价值
#         "399362",  # 国证民营
#         "399365",  # 国证农业
#         "399361",  # 国证商业
#         "399322",  # 国证治理
#         "399367",  # 巨潮地产
#         "399364",  # 中金消费
#         "399319",  # 资源优势
#         "399673",  # 创业板50
#         "399012",  # 创业300
#         "399018",  # 创业创新
#         "399608",  # 科技100
#         "399612",  # 中创100
#         "399550",  # 央视50
#         "399360",  # 新硬件
#         "399363",  # 计算机指
#         "399415",  # I100
#         "000847"  # 腾讯济安
# ]
# common = set(index_we_need) & set(list(all_indexes))
# print(len(common))  # 19

### 查询语句
# SELECT B.*,A.SecuCode from const_secumainall A,index_weight B WHERE A.SecuCategory=1 AND A.SecuMarket IN(83,90)
# AND A.ListedSector IN(1,2,6,7) AND A.InnerCode=B.InnerCode  AND B.IndexCode='000300' AND B.Date='2019-07-25' AND B.Flag=3;


#  id       | Date                | IndexCode | InnerCode | Weight   | Flag | CMFTime | CREATETIMEJZ        | UPDATETIMEJZ        | SecuCode


"""
SELECT B.Date, B.IndexCode, A.SecuCode, B.Weight from const_secumainall A,index_weight B WHERE A.SecuCategory=1 AND A.SecuMarket IN(83,90)
AND A.ListedSector IN(1,2,6,7) AND A.InnerCode=B.InnerCode  AND B.IndexCode='000300' AND B.Date='2019-07-25' AND B.Flag=3;
"""


# | 2019-07-25 00:00:00 | 000300    | 002939   | 0.041107 |
# | 2019-07-25 00:00:00 | 000300    | 002925   | 0.042504 |
# | 2019-07-25 00:00:00 | 000300    | 002945   | 0.036589 |
# | 2019-07-25 00:00:00 | 000300    | 601066   | 0.069895 |
# | 2019-07-25 00:00:00 | 000300    | 603259   | 0.077515 |
# | 2019-07-25 00:00:00 | 000300    | 601319   | 0.077126 |
# | 2019-07-25 00:00:00 | 000300    | 601298   | 0.032472 |
# | 2019-07-25 00:00:00 | 000300    | 002938   | 0.072417 |
# | 2019-07-25 00:00:00 | 000300    | 601577   | 0.027887 |
# | 2019-07-25 00:00:00 | 000300    | 601138   | 0.127380 |
# +---------------------+-----------+----------+----------+


logger = logging.getLogger()
conn = DC()
check_all_indexes_query = "select distinct(IndexCode) from datacenter.index_weight;"
all_indexes = list((i[0] for i in conn.execute(check_all_indexes_query).fetchall()))
logger.info(f"当前查询数据为{all_indexes}")
index_we_need = [
        "000001",  # 上证指数
        "399001",  # 深证成指
        "399005",  # 中小板指
        "399006",  # 创业板指
        "399004",  # 深证100R
        "399007",  # 深证300
        "399008",  # 中小300
        "000016",  # 上证50
        "000010",  # 上证180
        "000009",  # 上证380
        "000300",  # 沪深300
        "000903",  # 中证100
        "000904",  # 中证200
        "000905",  # 中证500
        "000922",  # 中证红利
        "000969",  # 300非周
        "399372",  # 大盘成长
        "399373",  # 大盘价值
        "399374",  # 中盘成长
        "399375",  # 中盘价值
        "399376",  # 小盘成长
        "399377",  # 小盘价值
        "000015",  # 红利指数
        "000019",  # 治理指数
        "000043",  # 超大盘
        "000044",  # 上证中盘
        "399346",  # 深证成长
        "399324",  # 深证红利
        "399328",  # 深证治理
        "399348",  # 深证价值
        "399678",  # 深次新股
        "399016",  # 深证创新
        "399370",  # 国证成长
        "399366",  # 国证大宗
        "399320",  # 国证服务
        "399321",  # 国证红利
        "399359",  # 国证基建
        "399371",  # 国证价值
        "399362",  # 国证民营
        "399365",  # 国证农业
        "399361",  # 国证商业
        "399322",  # 国证治理
        "399367",  # 巨潮地产
        "399364",  # 中金消费
        "399319",  # 资源优势
        "399673",  # 创业板50
        "399012",  # 创业300
        "399018",  # 创业创新
        "399608",  # 科技100
        "399612",  # 中创100
        "399550",  # 央视50
        "399360",  # 新硬件
        "399363",  # 计算机指
        "399415",  # I100
        "000847"  # 腾讯济安
]

daily_indexes = set(index_we_need) & set(list(all_indexes))
logger.info(f"当前日更新数据为{daily_indexes}")

month_indexes = set(index_we_need)-daily_indexes
logger.info(f"当前月更新数据为{month_indexes}")


def process_daily(dt: datetime.datetime, indexes):
    """

    :param dt:
    :param indexes:
    :return:
    """
    dt = dt.strftime("%Y-%m-%d %H:%M:%S")
    conn = DC()

    # 日频处理流
    for index_code in indexes:
        logger.info(f"index_code: {index_code}")
        conn.execute("use datacenter;")
        query_sql = f"""
        SELECT B.Date, B.IndexCode, A.SecuCode, B.Weight from const_secumainall A,index_weight B
        WHERE A.SecuCategory=1 AND A.SecuMarket IN(83,90) AND A.ListedSector IN(1,2,6,7)
        AND A.InnerCode=B.InnerCode  AND B.IndexCode= '{index_code}' AND B.Date='{dt}' AND B.Flag=3;
        """
        logger.info(f"query_sql: \n {query_sql}")
        res = conn.execute(query_sql).fetchall()
        # print(len(list(res)))
        # 拼接 infos
        infos = dict()
        for r in res:
            # print(r)  # (datetime.datetime(2019, 7, 25, 0, 0), '000015', '601998', Decimal('1.579791'))
            infos.update({utils.code_convert(r[2]): r[3]})
        # print(pprint.pformat(infos))
        # "_id" : ObjectId("5d0a458f47b9f71e6648b7a2"),
        # 	"date" : ISODate("2019-06-19T00:00:00Z"),
        # 	"index" : "SZ399319",
        # 	"index_info" : {
        data = {
            "date": dt,
            "index": utils.code_convert(index_code),
            "index_info": infos,
        }
        yield data


now = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)
logger.info(f"对{now}的日频数据进行 process_daily 处理")

datas = list(process_daily(now, daily_indexes))

cld = utils.gen_index_new_coll()
try:
    cld.insert_many(datas)
except Exception as e:
    logger.info(f"{now} daily 的批量插入失败，失败的原因是 {e}")
    raise


def process_month():
    # 月频处理流
    pass



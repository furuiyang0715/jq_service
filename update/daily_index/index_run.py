import datetime
import logging

from update import utils
from update.utils import DC2

logger = logging.getLogger()
conn = DC2()
cld = utils.gen_index_new_coll()

now = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)

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


def process_daily(dt: datetime.datetime, indexes):
    """
    日频处理流
    :param dt:
    :param indexes:
    :return:
    """
    dt = dt.strftime("%Y-%m-%d %H:%M:%S")
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
        infos = dict()
        for r in res:
            infos.update({utils.code_convert(r[2]): r[3]})
        data = {
            "date": dt,
            "index": utils.code_convert(index_code),
            "index_info": infos,
        }
        yield data


def process_month(dt: datetime.datetime, indexes):
    """
    月频处理流
    :param dt:
    :param indexes:
    :return:
    """
    # # 查询 InnerCode 与 SeCucode 之间的关系
    # sql1 = f"""select SeCucode, InnerCode from datacenter.const_secumainall where SecuCategory=4
    # AND SecuCode in {tuple(indexes)};"""
    # ret = conn.execute(sql1).fetchall()
    # # 拼接
    # inner_map = dict()
    # for r in ret:
    #     inner_map.update({
    #         utils.code_convert(r[0]): r[1]
    #     })
    # # print(inner_map)  # {'SZ000001': 1, 'SZ000019': 6513, 'SZ000847': 48352, 'SZ000904': 4976,

    # | 002930.XSHE |  0.843000 |
    # | 002933.XSHE |  0.483600 |
    for index_code in indexes:
        code_ = utils.convert_11code(utils.code_convert(index_code))   # 将 index_code 转换为后缀模式
        sql2 = f"""select SecuCode, Weight from datacenter.index_indexcomponentsweight where SecuCode = '{code_}'
        and EndDate = (SELECT max(EndDate) FROM datacenter.index_indexcomponentsweight where SecuCode = '{code_}');
                 """
        ret = conn.execute(sql2).fetchall()
        print(list(ret))



if __name__ == "__main__":
    check_all_indexes_query = "select distinct(IndexCode) from datacenter.index_weight;"
    all_indexes = list((i[0] for i in conn.execute(check_all_indexes_query).fetchall()))
    logger.info(f"当前查询数据为{all_indexes}")
    daily_indexes = set(index_we_need) & set(list(all_indexes))
    logger.info(f"当前日更新数据为{daily_indexes}")
    month_indexes = set(index_we_need) - daily_indexes
    logger.info(f"当前月更新数据为{month_indexes}")

    process_month(now, month_indexes)


def main():
    check_all_indexes_query = "select distinct(IndexCode) from datacenter.index_weight;"
    all_indexes = list((i[0] for i in conn.execute(check_all_indexes_query).fetchall()))
    logger.info(f"当前查询数据为{all_indexes}")

    daily_indexes = set(index_we_need) & set(list(all_indexes))
    logger.info(f"当前日更新数据为{daily_indexes}")

    month_indexes = set(index_we_need) - daily_indexes
    logger.info(f"当前月更新数据为{month_indexes}")

    logger.info(f"开始对{now}的日频数据进行 process_daily 处理")
    datas = list(process_daily(now, daily_indexes))
    try:
        cld.insert_many(datas)
    except Exception as e:
        logger.info(f"{now} daily 的批量插入失败，失败的原因是 {e}")
        raise

    # 规定每月月底对月频数据进行处理
    if (now + datetime.timedelta(days=1)).month != now.month:
        # 只有每月的最后一天符合条件
        process_month(now, month_indexes)

from sys_pack import sys , bs, pd, np
from sys_db_connector import db_connector as db
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import akshare as ak

base_info = "base_info"
id_info = "industrys_info"
k_data_info = "k_data_info"
k_data_medical = "k_data_medical"
k_data_finance = "k_data_finance"
k_data_outfits = "k_data_outfits"
k_data_agriculture = "k_data_agriculture"
k_data_electric = "k_data_electric"
k_data_electronic = "k_data_electronic"
k_data_automotive = "k_data_automotive"
k_data_other = "k_data_other"
season = {
    "01" : 1,
    "02" : 1,
    "03" : 1,
    "04" : 2,
    "05" : 2,
    "06" : 2,
    "07" : 3,
    "08" : 3,
    "09" : 3,
    "10" : 4,
    "11" : 4,
    "12" : 4
}
def industry():
    sys.login("Start")

    # 获取行业分类数据
    rs = bs.query_stock_industry()
    # rs = bs.query_stock_basic(code_name="浦发银行")
    print('query_stock_industry error_code:' + rs.error_code)
    print('query_stock_industry respond  error_msg:' + rs.error_msg)

    # 打印结果集
    industry_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起

        industry_list.append(sys.listToJson(rs.get_row_data(), rs.fields))

    # result = pd.DataFrame(industry_list, columns=rs.fields)
    # print(industry_list)
    start = datetime.datetime.now()
    idcol = db.link_start(id_info)
    db.rows_insert(idcol, industry_list)
    end = datetime.datetime.now()
    print("耗时：", end - start)  # 测试Mysql的排序时间
    sys.logout("End")

#       start_date='2015-01-01', end_date='2017-12-31',
def history_data(table = None,paras=None,start=None, end=None):

    sys.login("Start")

    #### 获取沪深A股估值指标(日频)数据 ####
    # peTTM    滚动市盈率
    # psTTM    滚动市销率
    # pcfNcfTTM    滚动市现率
    # pbMRQ    市净率
    codes = query(paras, table)

    # for c in codes:
    #     print(c)
    # param_list = list(map(lambda x: (x.get("code"), start, end), codes))
    # param_list = list(map(lambda x: x.get("code"), codes))
    # print(param_list)
    # result_list = []
    start_time = datetime.datetime.now()
    print("loading...")
    # with ThreadPoolExecutor(max_workers=6) as executor:
    #
    #     all_list = [executor.submit(test, code.get("code"), start, end) for code in codes]
    #     result = []
    #
    #     for future in as_completed(all_list):
    #         res = future.result()
    #         result.append(res)
    #     print(result)
    result = []
    for code in codes:
        with ThreadPoolExecutor(max_workers=8) as executor:
            all_list = [executor.submit(processing, code.get("code"), start, end, code.get("code"))]

            for future in as_completed(all_list):
                res = future.result()
                result.append(res)
                print("任务{} down load success".format(res[0]))

    end_time = datetime.datetime.now()
    print("1数据清洗耗时：", end_time - start_time)  # 测试时间

    sys.logout("End")

    # start_db = datetime.datetime.now()
    # kdcol = db.link_start(k_data_info)
    # db.rows_insert(kdcol, result_list)
    # end_db = datetime.datetime.now()
    # print("持久化耗时：", end_db - start_db)  # 测试时间


def history_data_2(table=None,paras=None,start=None, end=None, target=None):

    sys.login("Start")

    #### 获取沪深A股估值指标(日频)数据 ####
    # peTTM    滚动市盈率
    # psTTM    滚动市销率
    # pcfNcfTTM    滚动市现率
    # pbMRQ    市净率
    codes = query(paras, table)
    start_time = datetime.datetime.now()
    print("loading...")
    callback = []
    try:
        for code in codes:
            rs_k = bs.query_history_k_data_plus(code.get("code"),
                                                "date,code,close,peTTM,pbMRQ,psTTM,pcfNcfTTM",
                                                start_date=start, end_date=end,
                                                frequency="d", adjustflag="3")
            ori_season = 0
            rs_k.fields.append("peg")
            YOYEPSBasic = 0
            while (rs_k.error_code == '0') & rs_k.next():
                k_data = rs_k.get_row_data()
                year = k_data[0].split("-")[0]
                month = k_data[0].split("-")[1]
                pe = 0.0
                if len(k_data[3]) != 0:
                    pe = float(k_data[3])
                act_season = season[month]

                if ori_season != act_season:
                    ori_season = act_season
                    rs_growth = bs.query_growth_data(code=code.get("code"), year=year, quarter=4)
                    if (rs_growth.error_code == '0') & rs_growth.next():
                        g_data = rs_growth.get_row_data()
                        YOYEPSBasic = g_data[-2]
                        YOYEPSBasic = float(YOYEPSBasic)

                peg = 0.0
                if YOYEPSBasic != 0:
                    peg = pe / (YOYEPSBasic * 100.0)
                    k_data.append(peg)
                else:
                    peg = 0.0
                    k_data.append(peg)
                callback.append(sys.listToJson(k_data, rs_k.fields))
    except Exception as e:
        print(e.message)
        return None
    end_time = datetime.datetime.now()
    print("2数据清洗耗时：", end_time - start_time)  # 测试时间
    sys.logout("End")

    # print(callback)
    # start_db = datetime.datetime.now()
    # kdcol = db.link_start(target)
    # db.rows_insert(kdcol, callback)
    # end_db = datetime.datetime.now()
    # print("持久化耗时：", end_db - start_db)  # 测试时间
def forcast_report():
    sys.login("Start")

    #### 获取公司业绩预告 ####
    rs_forecast = bs.query_forecast_report("sh.600073", start_date="2015-01-01", end_date="2020-12-31")
    print('query_forecast_reprot respond error_code:' + rs_forecast.error_code)
    print('query_forecast_reprot respond  error_msg:' + rs_forecast.error_msg)
    rs_forecast_list = []
    while (rs_forecast.error_code == '0') & rs_forecast.next():
        # 分页查询，将每页信息合并在一起
        rs_forecast_list.append(rs_forecast.get_row_data())
    # result_forecast = pd.DataFrame(rs_forecast_list, columns=rs_forecast.fields)
    print(rs_forecast_list)
    # end = datetime.datetime.now()
    # print("耗时：", end - start)  # 测试Mysql的排序时间
    sys.logout("End")

def growth(code, year=None, season=None):
    sys.login("Start")
    # 成长能力
    growth_list = []
    rs_growth = bs.query_growth_data(code=code, year=year, quarter=season)
    while (rs_growth.error_code == '0') & rs_growth.next():
        growth_list.append(rs_growth.get_row_data())
        print(rs_growth.get_row_data())
        # return rs_growth.get_row_data()[5]
    # result_growth = pd.DataFrame(growth_list, columns=rs_growth.fields)
    # 打印输出
    return growth_list
    sys.logout("End")

def query(industry = None, table = None):
    filter = {"_id":0,"code":1}
    start = datetime.datetime.now()
    col = db.link_start(table)
    results = db.rows_select(col,params=industry,filter=filter)
    end = datetime.datetime.now()
    print("耗时：", end - start)  # 测试Mysql的排序时间

    return results
    # for result in results:
    #     print(result.get("code"))
def test(code, start, end, index):
    print("code {} finished at {}".format(index, time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())))
    callback = []
    rs_k = bs.query_history_k_data_plus(code,
                                        "date,code,close,peTTM,pbMRQ,psTTM,pcfNcfTTM",
                                        start_date=start, end_date=end,
                                        frequency="d", adjustflag="3")
    while (rs_k.error_code == '0') & rs_k.next():
        k_data = rs_k.get_row_data()

        callback.append(sys.listToJson(k_data, rs_k.fields));
    return callback


def processing(code, start, end, index):
    # code = params[0]
    # start = params[1]
    # end = params[2]
    try:
        rs_k = bs.query_history_k_data_plus(code,
                                            "date,code,close,peTTM,pbMRQ,psTTM,pcfNcfTTM",
                                            start_date=start, end_date=end,
                                            frequency="d", adjustflag="3")

        ori_season = 0
        rs_k.fields.append("peg")
        YOYEPSBasic = 0
        callback = []
        while (rs_k.error_code == '0') & rs_k.next():
            k_data = rs_k.get_row_data()
            year = k_data[0].split("-")[0]
            month = k_data[0].split("-")[1]
            pe = 0.0
            if len(k_data[3]) != 0:
                pe = float(k_data[3])
            act_season = season[month]

            if ori_season != act_season:
                ori_season = act_season
                rs_growth = bs.query_growth_data(code=code, year=year, quarter=4)
                if (rs_growth.error_code == '0') & rs_growth.next():
                    g_data = rs_growth.get_row_data()
                    YOYEPSBasic = g_data[-2]
                    YOYEPSBasic = float(YOYEPSBasic)

            peg = 0.0
            if YOYEPSBasic != 0:
                peg = pe / (YOYEPSBasic * 100.0)
                k_data.append(peg)
            else:
                peg = 0.0
                k_data.append(peg)
            # print(sys.listToJson(k_data, rs_k.fields))
            callback.append(sys.listToJson(k_data, rs_k.fields))
            # return sys.listToJson(k_data, rs_k.fields)
        return callback
    except Exception as e:
        print(e.message)
        return None

if __name__=='__main__':
    paras = {"industry":"汽车"}
    # industry()
    # results =  query(paras, id_info)
    # print(result_list)
    # for result in results:
    #     print(result)
    # paras = {"industry" : "食品饮料"}
    # history_data(table=id_info,paras=paras,start="2000-01-01",end="2020-08-20")
    history_data_2(table=id_info,paras=paras,start="2000-01-01",end="2020-08-20", target=k_data_automotive)
    # growth("sh.600000",2007,4)
    # forcast_report()

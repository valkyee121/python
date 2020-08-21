from sys_pack import sys , bs, pd, np
from sys_db_connector import db_connector as db
import datetime
from itertools import starmap


base_info = "base_info"
id_info = "industrys_info"
k_data_info = "k_data_info"
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
    codes = query(paras, table)
    sys.login("Start")

    #### 获取沪深A股估值指标(日频)数据 ####
    # peTTM    滚动市盈率
    # psTTM    滚动市销率
    # pcfNcfTTM    滚动市现率
    # pbMRQ    市净率

    # for c in codes:
    #     print(c)
    param_list = list(map(lambda x: (x.get("code"), start, end), codes))
    print(param_list)
    # result_list = []
    start_time = datetime.datetime.now()
    print("loading...")
    # for code in codes:
    # print(param_list)
    #
    # starmap(lambda x, y, z: test(x, y, z), param_list)
    result_list = starmap(processing, param_list)
    print(result_list)
    end_time = datetime.datetime.now()
    print("数据清洗耗时：", end_time - start_time)  # 测试Mysql的排序时间
    sys.logout("End")

    # start_db = datetime.datetime.now()
    # kdcol = db.link_start(k_data_info)
    # db.rows_insert(kdcol, result_list)
    # end_db = datetime.datetime.now()
    # print("持久化耗时：", end_db - start_db)  # 测试Mysql的排序时间
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
        # growth_list.append()
        print(rs_growth.get_row_data())
        # return rs_growth.get_row_data()[5]
    # result_growth = pd.DataFrame(growth_list, columns=rs_growth.fields)
    # 打印输出

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
def test(code, start, end):
    print(code)

def processing(code, start, end):
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
if __name__ == '__main__':
    paras = {"industry":"食品饮料"}
    # industry()
    # results =  query(paras, id_info)
    # print(result_list)
    # for result in results:
    #     print(result)
    # paras = {"industry" : "食品饮料"}
    history_data(table=id_info,paras=paras,start="2000-01-01",end="2020-08-20")
    # growth("sh.600000",2007,4)
    # forcast_report()

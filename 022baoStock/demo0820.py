from sys_pack import sys, bs
from sys_db_connector import db_connector as db

def test1():
    mylist = [{"code" : "SZ00002", "name" : "RFYY"}]
    myDb = db.link_start("base_info")
    db.rows_insert(myDb, mylist)
def test2():
    list = ['sh.600005', '2016-04-26', '2015-12-31', '-0.219783', '-0.016735', '-6.810500', '-6.952000', '-6.976290']
    print(float("66.66")/(float(list[-2]) * 100.0))
def test3():
    sys.login("start")
    year = [2010, 2011, 2012, 2013, 2014, 2015, 1016]
    for y in year:
        rs_growth = bs.query_growth_data(code="sh.600007", year=y, quarter=4)
        print(y)
        if (rs_growth.error_code == '0') & rs_growth.next():
            print(float(rs_growth.get_row_data()[-2]))
    sys.logout("end")
def test4():
    sys.login("start")

    rs_k = bs.query_history_k_data_plus(code="sh.600007",
                                        fields="date,code,close,peTTM,pbMRQ,psTTM,pcfNcfTTM",
                                        start_date="2000-01-01", end_date="2020-08-19",
                                        frequency="d", adjustflag="3")
    ori_season = 0
    # rs_k.fields.append("peg")
    YOYEPSBasic = 0
    # print(type(rs_k.get_data()))
    for data in rs_k:
        print(data)
        # if (rs_k.error_code == '0') & rs_k.next():
        #     k_data = data
        #     print(k_data)

            # result = list()
            # print(map(lambda x: str(x), rs_k.get_data()))

    # while (rs_k.error_code == '0') & rs_k.next():
    #     k_data = rs_k.get_row_data()
    #     print(k_data)

    sys.logout("end")
if __name__ == '__main__':
    test4()
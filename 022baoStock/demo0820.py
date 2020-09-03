from sys_pack import sys, bs, np
from sys_db_connector import db_connector as db
from queue import Queue as queue
from concurrent.futures import ThreadPoolExecutor
import time
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

def test5(q):
    # 生产者
    for i in range(0, 10):
        q.put(i)
        print("放入%d" % i)

    # 消费者
    for j in range(0, 10):
        res = q.get(j)
        print("取出%d" % res)
        # 由消费者调用，每一个get()得到一个任务后，由task_done()通知队列此任务已经处理完成
        q.task_done()
    # 阻塞等待
    q.join()
def spider(page):
    time.sleep(page)
    return page
def main_thread_executor():
    with ThreadPoolExecutor(max_workers=3) as t:
        i = 1
        for result in t.map(spider,[1,2,3,4,5]):
            print("task{}:{}".format(i, result))
            i += 1
def test6():
    str = ('sh.600059', '2000-01-01', '2020-08-20'),
    print(type(str))
if __name__ == '__main__':
    test6()
    np.random.randn()
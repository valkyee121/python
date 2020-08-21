import baostock as bs
import pandas as pd
import numpy as np
import json
class sys:

    def login(cls):
        lg = bs.login()
        # 显示登陆返回信息
        print('login respond error_code:' + lg.error_code)
        print('login respond  error_msg:' + lg.error_msg)

    def logout(cls):
        bs.logout()

    def to_json2(df, orient='split'):
        df_json = df.to_json(orient=orient, force_ascii=False)
        return json.loads(df_json)

    def listToJson(self, keyList):
        keys = [str(x) for x in keyList]
        list_json = dict(zip(keys, self))
        # str_json = json.dumps(list_json, ensure_ascii=False)  # json转为string
        return list_json



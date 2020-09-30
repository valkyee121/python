from sys_db_connector import db_connector as db
import datetime
import pandas as pd
import numpy as np
import json
# 导入图表绘制、图标展示模块
from bokeh.plotting import figure, output_file, show
from bokeh.io import output_notebook
from bokeh.models import ColumnDataSource, DatetimeTickFormatter,HoverTool
from bokeh.palettes import Spectral4
from matplotlib import pyplot as plt
import seaborn as sns
from concurrent.futures import ThreadPoolExecutor, as_completed
pd.set_option('display.max_columns',1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth',1000)

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
k_data_foods = "k_data_foods"

def query(code = None, table = None):
    filter = {"_id": 0}
    # start = datetime.datetime.now()
    col = db.link_start(table)
    results = db.rows_select(col, params=code, filter=filter)
    # end = datetime.datetime.now()
    return results
def mem_usage(pandas_obj):
    if isinstance(pandas_obj,pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else: # we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2 # convert bytes to megabytes
    return "{:03.2f} MB".format(usage_mb)

def drawing(data=None):
    df = pd.DataFrame(list(data))


    ori_date = df.date
    ori_code = df.code

    df['date'] = pd.to_datetime(ori_date, format='%Y-%m-%d')
    df['code'] = ori_code.astype('category')

    df_obj = df.select_dtypes(include=['object'])
    convert_obj = df_obj.apply(pd.to_numeric, downcast='float')
    df[convert_obj.columns] = convert_obj
    df = df.set_index(keys=['date'])
    ti = df.loc[:,['close','peTTM','pbMRQ','peg']]
    ti.index.name = 'index'
    source = ColumnDataSource(data=ti)
    print(ti)
    start = datetime.datetime.now()
    ti2 = pd.DataFrame(ti.values.T,index=ti.columns,columns=ti.index)
    end = datetime.datetime.now()
    print("transform timing：", end - start)
    print(ti2)
    hover = HoverTool(tooltips=[
        ("index", "@index"),
        ("pe", ti.columns[0]),
        ("pb", ti.columns[1]),
    ])
    p = figure(plot_width=1920, plot_height=1080,x_axis_type='datetime',tools=[hover,'box_select,reset,wheel_zoom,pan,crosshair'])
    # p.multi_line([ti.index, ti.index,ti.index],   # 第一条线的横坐标和第二条线的横坐标
    #              [ti['peTTM'], df['pbMRQ'], df['peg']],  # 注意x，y值的设置 → [x1,x2,x3,..], [y1,y2,y3,...]  第一条线的Y值和第二条线的Y值
    #              color=["firebrick", "navy", "green"],  # 可同时设置 → color= "firebrick"；也可以统一弄成一个颜色。
    #              alpha=[0.8, 0.6, 0.6],  # 可同时设置 → alpha = 0.6
    #              line_width=[2, 1, 1],  # 可同时设置 → line_width = 2
    #              )
    for col, color in zip(ti.columns.tolist(), Spectral4):

        if col == 'close':
            p.patch(ti.index, ti[col],  # 设置x，y值
                    line_width=1, line_alpha=0.8, line_color=color, line_dash=[100, 4],  # 线型基本设置
                    fill_color='black', fill_alpha=0.2 , legend=col,
                    )
            # 绘制面积图
            # .patch将会把所有点连接成一个闭合面
        else:
            p.line(ti.index, ti[col], line_width=2, color=color, alpha=0.8, legend=col,
                   muted_color=color, muted_alpha=0.2)  # 设置消隐后的显示颜色、透明度 可以设置muted_color = 'black'


    # output_notebook()
    p.xaxis.formatter = DatetimeTickFormatter(
        days=["%Y-%m-%d"],
        months=["%Y-%m"],
        years=["%Y"],
    )
    show(p)
    # for dtype in ['float','object','datetime']:
    #     selected_dtype = df.select_dtypes(include=[dtype])
    #     mean_usage_b = selected_dtype.memory_usage(deep=True).mean()
    #     mean_usage_mb = mean_usage_b / 1024 ** 2
    #     print("Average memory usage for {} columns: {:03.2f} MB".format(dtype, mean_usage_mb))


if __name__ == '__main__':
    paras = {"code": "sh.600132"}
    callback_list = query(code=paras,table=k_data_foods)
    df = pd.DataFrame(list(callback_list))
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df['code'] = df['code'].astype('category')

    df_obj = df.select_dtypes(include=['object','float64'])
    convert_obj = df_obj.apply(pd.to_numeric, downcast='float')
    df[convert_obj.columns] = convert_obj

    df['peg'] = df['peg'].astype(np.float32)
    print(df.describe())
    elements = ['close','YOYEquity','dupontAssetStoEquity',
                'dupontNitogr','dupontEbittogr','npMargin','epsTTM']
    df1 = df.loc[:,elements]
    df1.plot()


    # f,ax = plt.subplots(figsize=(24,24))
    # sns.heatmap(df.corr(), annot=True, linewidths=.5, fmt='.2f', ax=ax)
    plt.show()

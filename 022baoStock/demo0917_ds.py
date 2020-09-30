import numpy as np                  # linear algebra
import pandas as pd                 # data processing, CSV file I/O (e.g. pd.read_csv)
import matplotlib.pyplot as plt     # drawing
import seaborn as sns               # visualization tool

# Input data files are available in the "../input/" directory.
# For example, running this (by clicking run or pressing Shift+Enter) will list the files in the input directory
# from subprocess import check_output
# print(check_output(["ls", "D://WorkSpace//pyc//dataset"]).decode("utf8"))

import os
# for dirname, _, filenames in os.walk("D:\WorkSpace\pyc\dataset"):
#     for filename in filenames:
#         print(os.path.join(dirname, filename))
#         # Any results you write to the current directory are saved as output.

pd.set_option('display.max_columns',1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth',1000)


# csv加载
def load_data():
    data = pd.read_csv('D:\WorkSpace\pyc\dataset\pokemon.csv')
    return data

# 相关性
def make_corr_map(dataSet = None):
    f,ax = plt.subplots(figsize=(18,18))
    sns.heatmap(dataSet.corr(), annot=True, linewidths=.5, fmt='.2f', ax=ax)
    plt.show()

# Line Plot
# color = color, label = label, linewidth = width of line, alpha = opacity,
# grid = grid, linestyle = sytle of line
def mat_line_plot(dataSet = None):
    dataSet.Speed.plot(kind='line', color='green', label='Speed', linewidth=1,
                       alpha=0.5, grid=True, linestyle='-')
    dataSet.Defense.plot(kind='line', color='k', label='Defense', linewidth=1.2,
                         alpha=0.5, grid=True, linestyle=':')
    plt.legend(loc='upper right')
    plt.xlabel('x axis')
    plt.ylabel('y axis')
    plt.title('Line Plot')
    plt.show()

# "data = data": 不同变量 相同地址
# "data = data.copy()": 不同变量 不同地址 data为新对象
def data_types(data=None):
    print(data.info())
    print("**************************")
    print(data['Type 1'].value_counts())
    print("**************************")
    print(data.describe())
    print("**************************")
    data.boxplot(column='HP', by='Legendary')
    data_new = data.head()
    melted = pd.melt(frame=data_new, id_vars='Name', value_vars=['Attack', 'Defense', 'Speed'])
    print(melted)
    print("**************************")
    data_pivot = melted.pivot(index='Name', columns='variable', values='value')
    print(data_pivot)
    print("**************************")
    print(data['Type 2'].value_counts(dropna=False))
    print("**************************")
    data1 = data.copy()
    print(data1.dropna(inplace=True))
    print(data.head())
    print(data.info())
    print(data.corr())
    # make_corr_map(data)
    # mat_line_plot(data)

def scratch():
    country = ["Spain", "Deutschland"]
    population = ["11", "10"]
    list_label = ["country", "population"]
    list_col = [country, population]
    zipped = list(zip(list_label, list_col))
    data_dict = dict(zipped)
    df = pd.DataFrame(data_dict)
    print(df)
    df["capital"] = ["madrid", "Berlin"]
    print(df)

def visual_exploratory_data_analusis(data = None):
    print(data.head(10))
    data1 = data.loc[:,["Attack","Defense","Speed"]]
    data1.plot(subplots= True)
    plt.show()

if __name__ == '__main__':
    # data = load_data()
    # # scratch()
    # visual_exploratory_data_analusis(data)
    data = ""
    fdata = "";
    if data.isspace() or data == "":
        pass
    else:
        fdata = np.float(data)
    print(fdata)
from sys_db_connector import db_connector as db
from bokeh.plotting import figure, output_file, show
import datetime
import torch
from torch import nn


def running():
    testmap = {}
    for i in range(10000000):
        s = str(i)
        testmap[s] = i
        t = testmap[s]
        if t % 100000 == 0:
            pass



if __name__ == '__main__':
    tensor = torch.rand(4)
    print(tensor)
    N = tensor.size(0)
    num_classes = 4
    one_hot = torch.zeros(N, num_classes).long()
    print(type(one_hot))
    one_hot.scatter_(dim=1, index=torch.unsqueeze(tensor, dim=1),
                     src=torch.ones(N, num_classes).long())
    print(one_hot)
    # for i in range(3):
    #     start = datetime.datetime.now()
    #     running()
    #     end = datetime.datetime.now()
    #     print("Round %d: Time duration: %s"  %(i, (end - start)) )
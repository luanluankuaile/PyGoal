import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import logging

def test():
    handler=logging.StreamHandler()
    logging.basicConfig(level=logging.INFO,handlers=[handler])
    x = np.arange(1, 11)
    y = 2 * x + 5
    plt.title("Matplotlib demo")
    plt.xlabel("x axis caption")
    plt.ylabel("y axis caption")
    plt.plot(x, y,'ob')
    plt.show()
    x = np.arange(0, 3 * np.pi, 0.1)
    y = np.sin(x)
    plt.title("sine wave form")
    # 使用 matplotlib 来绘制点
    plt.plot(x, y,'ob')
    plt.show()
    csv_read=pd.read_csv('C:\BaiduNetdiskDownload\WEKA\Advertising.csv')
    logging.info(csv_read)

test()
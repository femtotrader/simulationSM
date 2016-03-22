#!/usr/bin/python
#arg month 12 year 2015 sl 0.0011 tp 0.0015 infile 201512.csv outfile log_trades.txt 
#This software use Savinsky method and running average to take and decision.
#Stop is used also

import sys
import argparse
import pandas as pd
import numpy as np
import time
import datetime as dt
from datetime import date, datetime, time, timedelta
from scipy.signal import savgol_filter
import matplotlib.pyplot as plt
from matplotlib.finance import candlestick_ohlc
from pandas.lib import Timestamp
#from datetime import timedelta, datetime, time, date



##############################      DEFINITION OF SETTINGS

FORMAT = "%Y%m%d %H%M%S%f"
PATH = sys.path[0]
OUTFILE = PATH + "//" + sys.argv[2]
WAITING = 7200
INFILE = PATH + "//" + sys.argv[4]
MONTH = sys.argv[6]
PATH_FIGURES = "/home/oliverio/Desktop/BT_parallel/figures"
################## READ FILE AND CONVERT IN A INDEX TIME WITH 1 SECOND
row = 0
timeTemp = dt.datetime(2007, 12, 6,1,1)
names = ["operation", "timeIn",  "timeOut","priceIn", "priceOut", "RSI1H_0", "RSI1H_-1", "RSI1H_-2", "RA1H_50", "RA1H_20", "RSI15MIN_0", "RSI15MIN_-1", "RSI15MIN_-2", "RA15MIN_50", "RA15MIN_50-1", "RA15MIN_20", "RA15MIN_20-1", "RSI5MIN_0", "RA5MIN_50", "RA5MIN_50-1", "RA5MIN_50-2", "RA5MIN_20", "RA5MIN_20-1", "RA5MIN_20-2"]

data = pd.read_csv(INFILE, header = None, names=names, sep=",", date_parser = lambda x: pd.to_datetime(x, format = FORMAT))
data["timeIn"] = pd.to_datetime(data["timeIn"])
bin_list = range(100)
data.reset_index(level=0, inplace=True)
data.index = data.timeIn
del data["timeIn"]


dataBuyG = data[data["operation"] == "BUY_GAGNANT"]
dataBuyP = data[data["operation"] == "BUY_PERDANT"]
dataSellG = data[data["operation"] == "SELL_GAGNANT"]
dataSellP = data[data["operation"] == "SELL_PERDANT"]
print dataSellG.size, dataSellP.size
print dataBuyG.size, dataBuyP.size

dataBuyG = dataBuyG[dataBuyG["priceIn"] > dataBuyG["RA1H_50"]]
dataBuyG = dataBuyG[dataBuyG["priceIn"] < dataBuyG["RA1H_20"]]
dataBuyP = dataBuyP[dataBuyP["priceIn"] > dataBuyP["RA1H_50"]]
dataBuyP = dataBuyP[dataBuyP["priceIn"] < dataBuyP["RA1H_20"]]
print dataBuyG.size, dataBuyP.size
#print dataSellP.head(n=10)
f = plt.figure(1, dpi = 300)
plt.hist(dataBuyG["RSI1H_0"], bins=bin_list, histtype="step", color="blue")
plt.hist(dataBuyP["RSI1H_0"], bins=bin_list, histtype="step", color="red")
plt.xlabel("RSI 1H")
plt.savefig(PATH_FIGURES + "/" + "RSI_BUY_1H_" + MONTH + "_2015.eps", format="eps")
f.show()



RSI = 50
dataBuyG = dataBuyG[dataBuyG["RSI1H_0"] > RSI]
dataBuyG = dataBuyG[dataBuyG["RSI1H_0"] < (RSI + 1)]
dataBuyP = dataBuyP[dataBuyP["RSI1H_0"] > RSI]
dataBuyP = dataBuyP[dataBuyP["RSI1H_0"] < (RSI + 1)]
print dataBuyG.size, dataBuyP.size

f = plt.figure(2, dpi = 300)
plt.hist(dataBuyG["RSI1H_-1"], bins=bin_list, histtype="step", color="blue")
plt.hist(dataBuyP["RSI1H_-1"], bins=bin_list, histtype="step", color="red")
plt.xlabel("RSI 1H")
plt.savefig(PATH_FIGURES + "/" + "RSI_BUY_1H-1_" + MONTH + "_2015.eps", format="eps")
f.show()
"""
RSI =51
dataBuyG = dataBuyG[dataBuyG["RSI1H_-1"] > RSI]
dataBuyG = dataBuyG[dataBuyG["RSI1H_-1"] < RSI+1]
dataBuyP = dataBuyP[dataBuyP["RSI1H_-1"] > RSI]
dataBuyP = dataBuyP[dataBuyP["RSI1H_-1"] < RSI+1]
print dataBuyG.size, dataBuyP.size

dataBuyG = dataBuyG[dataBuyG["priceIn"] < dataBuyG["RA15MIN_50"]]
dataBuyG = dataBuyG[dataBuyG["priceIn"] > dataBuyG["RA15MIN_20"]]

dataBuyP = dataBuyP[dataBuyP["priceIn"] < dataBuyP["RA15MIN_50"]]
dataBuyP = dataBuyP[dataBuyP["priceIn"] > dataBuyP["RA15MIN_20"]]
f = plt.figure(3)
plt.hist(dataBuyG["RSI15MIN_0"], bins = bin_list, histtype = "step", color = "blue")
plt.hist(dataBuyP["RSI15MIN_0"], bins = bin_list, histtype = "step", color = "red")
f.show()
"""

"""
dataBuyG = dataBuyG[dataBuyG["RSI15MIN_0"] > 39.0]
dataBuyG = dataBuyG[dataBuyG["RSI15MIN_0"] < 43.0]

dataBuyP = dataBuyP[dataBuyP["RSI15MIN_0"] > 39.0]
dataBuyP = dataBuyP[dataBuyP["RSI15MIN_0"] < 43.0]
print dataBuyG.size, dataBuyP.size



f = plt.figure(4)
plt.hist(dataBuyG["RSI5MIN_0"], bins = bin_list, histtype = "step", color = "blue", normed = 1)
plt.hist(dataBuyP["RSI5MIN_0"], bins = bin_list, histtype = "step", color = "red", normed = 1)
f.show()
"""
"""
f = open(OUTFILE, 'a')

while (row < len(data)):
    diff = (Timestamp(data.timeIn[row]) - timeTemp).total_seconds()
#    print diff
#    print Timestamp(data.timeIn[row])
#    print diff
    if (diff >= WAITING):
     
#        print data["operation"]

        if (data["operation"][row] == "BUY_GAGNANT") | (data["operation"][row] == "BUY_PERDANT"):  
            if (data["RSI1H_0"][row] > 60) & (data["RSI1H_0"][row] < 61.5):
                if (data["RSI1H_-1"][row] > 59) & (data["RSI1H_-1"][row] < 60):
                    if (data["RSI15min"][row] < 55):
#                        if (data["RSI5min"][row] < 60):
                            f.write(data[row])
                            timeTemp = Timestamp(data.index[row])
    row+=1

f.close()

####################################################################################
####################################################################################
####################################################################################

"""
dataSellG = dataSellG[dataSellG["priceIn"] < dataSellG["RA1H_50"]]
dataSellG = dataSellG[dataSellG["priceIn"] > dataSellG["RA1H_20"]]
dataSellP = dataSellP[dataSellP["priceIn"] < dataSellP["RA1H_50"]]
dataSellP = dataSellP[dataSellP["priceIn"] > dataSellP["RA1H_20"]]


f = plt.figure(5, dpi = 300)
plt.hist(dataSellG["RSI1H_0"], bins=bin_list, histtype="step", color="blue")
plt.hist(dataSellP["RSI1H_0"], bins=bin_list, histtype="step", color="red")
plt.xlabel("RSI 1H")
plt.savefig(PATH_FIGURES + "/" + "RSI_SELL_1H_" + MONTH + "_2015.eps", format="eps")
f.show()


dataSellG = dataSellG[dataSellG["RSI1H_0"] > 47]
dataSellG = dataSellG[dataSellG["RSI1H_0"] < 48]
dataSellP = dataSellP[dataSellP["RSI1H_0"] > 47]
dataSellP = dataSellP[dataSellP["RSI1H_0"] < 48]
print dataSellG.size, dataSellP.size


f = plt.figure(6, dpi = 300)
plt.hist(dataSellG["RSI1H_-1"], bins=bin_list, histtype="step", color="blue")
plt.hist(dataSellP["RSI1H_-1"], bins=bin_list, histtype="step", color="red")
plt.xlabel("RSI 1H")
plt.savefig(PATH_FIGURES + "/" + "RSI_SELL_1H-1_" + MONTH + "_2015.eps", format="eps")
f.show()

"""
dataSellG = dataSellG[dataSellG["RSI1H_-1"] > 37]
dataSellG = dataSellG[dataSellG["RSI1H_-1"] < 38]
dataSellP = dataSellP[dataSellP["RSI1H_-1"] > 37]
dataSellP = dataSellP[dataSellP["RSI1H_-1"] < 38]



f = plt.figure(7)
plt.hist(dataSellG["RSI15MIN_0"], bins=bin_list, histtype="step", color="blue")
plt.hist(dataSellP["RSI15MIN_0"], bins=bin_list, histtype="step", color="red")
f.show()
"""
"""

dataSellG = dataSellG[dataSellG["RSI15MIN_0"] > 48.0]
dataSellG = dataSellG[dataSellG["RSI15MIN_0"] < 49.0]
dataSellP = dataSellP[dataSellP["RSI15MIN_0"] > 48.0]
dataSellP = dataSellP[dataSellP["RSI15MIN_0"] < 49.0]
print dataSellG.size, dataSellP.size
f = plt.figure(8)
plt.hist(dataSellG["RSI5MIN_0"], bins=bin_list, histtype="step", color="blue", normed=1)
plt.hist(dataSellP["RSI5MIN_0"], bins=bin_list, histtype="step", color="red", normed=1)
f.show()
"""

f = plt.figure(10)
plt.hist(dataBuyG.index.day, bins = range(29), histtype = "step", color = "blue")
plt.hist(dataBuyP.index.day, bins = range(29), histtype = "step", color = "red")
plt.hist(dataSellG.index.day, bins = range(29), histtype = "step", color = "brown")
plt.hist(dataSellP.index.day, bins = range(29), histtype = "step", color = "yellow")
f.show()




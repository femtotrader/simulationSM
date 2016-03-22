#!/usr/bin/python
import os, sys
import multiprocessing as mp
#import ipyparallel as ip
#sys.path.append("/usr/local/lib/python2.7/dist-packages")
import time
import pandas as pd
from pandas.tseries.offsets import *
from pandas.lib import Timestamp
import numpy as np
import datetime as dt
#from datetime import date, datetime, time, timedelta
#from rosetta.parallel.pandas_easy import groupby_to_series_to_frame
#import dask.dataframe as dd


start_time= time.time()

FORMAT = "%Y%m%d %H%M%S%f"
BAR1_5min = '5min'      # time scale
BAR2_15min = '15min'     # time scale
BAR3_1H = "1h"
PATH = sys.path[0]   
MONTH = sys.argv[10]  
INFILE = "//home//oliverio//DATA//" + sys.argv[2] + MONTH + ".csv"
OUTFILE = "//home//oliverio//DATA//02//"
BAR1_CHECK = sys.argv[4]
BAR2_CHECK = sys.argv[6]
BAR3_CHECK = sys.argv[8]

os.mkdir(OUTFILE, 0777)
data = pd.read_csv(INFILE, header = None, names=['bid', 'ask', 'other'], date_parser = lambda x: pd.to_datetime(x, format = FORMAT))

del data['other']

dbar = data.resample("S", how = 'mean', fill_method='ffill')
#dbar.to_csv(OUTFILE + "//" + "201504.csv", header = False)
dbar["weekDay"] = dbar.index.dayofweek
dbarTemp1 = dbar[dbar.index.month != float(MONTH)]
row = len(dbarTemp1)
print row
while row < len(dbar):
    rowInit = row - 3600
    rowEnd = rowInit + (24*3600) +3600
    rowCalculation = 0
    rowCalculationWeekend = 0
    if (dbar.weekDay[row] == 0): rowCalculationWeekend =  (96 * 3600)
    if (dbar.weekDay[row] == 1): rowCalculationWeekend =  (96 * 3600)
    if (dbar.weekDay[row] == 6): rowCalculationWeekend =  (96 * 3600)
    if BAR1_CHECK == "Y" : rowCalculation =  (130 * 300)
    if BAR2_CHECK == "Y" : rowCalculation =  (130 * 900)
    if BAR3_CHECK == "Y" : rowCalculation =  (150 * 3600)

    rowInit = rowInit - rowCalculation - rowCalculationWeekend
    
    day = dbar[rowInit:rowEnd]
    del day["weekDay"]
    day.to_csv(OUTFILE + "//" + MONTH + "_" + str(dbar.index.day[rowEnd-1]), header = False)
    row = rowEnd - 1
    print str(dbar.index.day[rowEnd]), rowInit, row
print  dbarTemp1.head()
print day.head()
print day.tail()



elapsed_time = time.time() - start_time


print "%f sec." %(elapsed_time)
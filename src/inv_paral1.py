#!/usr/bin/python
import os, sys
import multiprocessing as mp
#import ipyparallel as ip
#sys.path.append("/usr/local/lib/python2.7/dist-packages")
import time
import pandas as pd
from pandas.lib import Timestamp
import numpy as np
import datetime as dt
import calendar
#from datetime import date, datetime, time, timedelta
#from rosetta.parallel.pandas_easy import groupby_to_series_to_frame
#import dask.dataframe as dd



start_time= time.time()
FORMAT = "%Y-%m-%d %H:%M:%S"
BAR1 = '5min'      # time scale
BAR2 = '15min'     # time scale
BAR3 = "1h"
PATH = sys.path[0]
SL = float(sys.argv[2])
TP = float(sys.argv[4])
MONTH = sys.argv[6]
YEAR = sys.argv[8]
INFILE_PATH = "//home//oliverio//DATA//" + MONTH 

def RSI(series, period):
    delta = series.diff().dropna()
    u = delta * 0
    d = u.copy()
    u[delta > 0] = delta[delta > 0]
    d[delta < 0] = -delta[delta < 0]
    u[u.index[period-1]] = np.mean( u[:period] ) #first value is sum of avg gains
    u = u.drop(u.index[:(period-1)])
    d[d.index[period-1]] = np.mean( d[:period] ) #first value is sum of avg losses
    d = d.drop(d.index[:(period-1)])
    rs = pd.stats.moments.ewma(u, com=period-1, adjust=False) / \
         pd.stats.moments.ewma(d, com=period-1, adjust=False)
    return 100 - 100 / (1 + rs)


def count_files(path):
    count = 0
    for f in os.listdir(path):
        if os.path.isfile(os.path.join(path,f)): count += 1
    return count
    
def make_list(number_files):
    lista = []
    lista_str = []
    for i in range(1, int(number_files)+1): lista.append(str(i))
    days_weekEnd = [i.strip().split() for i in open(PATH + "//" + "weekEndDays.txt").readlines()]
    ss = set(days_weekEnd[int(MONTH)])
    fs = set(lista)
    ss.intersection(fs)
    ss.union(fs)
    lista = fs - ss.intersection(fs)
    lista = [int(i) for i in lista]
    lista = sorted(lista, key=int)
    for i in range(len(lista)):
        item = MONTH + "_" + str(lista[i])
        lista_str.append(item)
    print lista_str
    return lista_str

    
def read_data(path):
    beforeTime = dt.datetime.now()
    positionBuy = False
    positionSell = False
    trade_price = 0
    time_sell = dt.datetime(2007, 12, 6,1,1)
    time_buy = dt.datetime(2007, 12, 6,1,1)
    p_trades = 0
    n_trades = 0
    p_trades_buy = 0
    n_trades_buy = 0
    p_trades_sell = 0
    n_trades_sell = 0
    t_trades = 0
    gain_buy = 0
    loss_buy = 0
    gain_sell = 0
    loss_sell = 0

    INFILE = INFILE_PATH + "//" + path
    OUTFILE = PATH + "//" + "logTr_invProb" + path + ".txt" 
    
       
    dbar = pd.read_csv(INFILE, header = None, names=['bid', 'ask'], date_parser = lambda x: pd.to_datetime(x, format = FORMAT))
    dbar["hour"] = dbar.index.hour
    dbar["trade"] = float("NAN")
    dbar["weekDay"] = dbar.index.dayofweek
    dbarTemp1 = dbar[dbar.index.day != float(path[3:])]
    if path[3:] == "1": row = len(dbarTemp1)
    else: row = len(dbarTemp1) - 3600
    print  row, path[3:]
#    rowInit = row
#    print days, outfile
    days_weekEnd = [i.strip().split() for i in open(PATH + "//" + "weekEndDays.txt").readlines()]
    while (row < len(dbar)-3600): 
#    print row, dbar.bid.ix[row], dbar.index[row]
        if (dbar.hour[row] >= 23) | ((dbar.hour[row] >= 00) and (dbar.hour[row] <= 12)): inTime = True
        else: 
            row = len(dbar) - 1
            inTime = False
        if inTime == True:
            dbarTemp = dbar[row-532800:row]     
            dbar_5min = dbarTemp.resample(BAR1, how = 'last', fill_method='ffill')
            dbar_5min["weekDay"] = dbar_5min.index.dayofweek
            dbar_15min = dbarTemp.resample(BAR2, how = 'last', fill_method='ffill') 
            dbar_15min["weekDay"] = dbar_15min.index.dayofweek
            dbar_1h = dbarTemp.resample(BAR3, how = 'last', fill_method='ffill') 
            dbar_1h["weekDay"] = dbar_1h.index.dayofweek
            for item in days_weekEnd[int(MONTH)]: 
                dbar_5min = dbar_5min[dbar_5min.weekDay != 5]  
                dbar_5min = dbar_5min[dbar_5min.weekDay != 6]  
                dbar_15min = dbar_15min[dbar_15min.weekDay != 5]  
                dbar_15min = dbar_15min[dbar_15min.weekDay != 6]  
                dbar_1h = dbar_1h[dbar_1h.weekDay != 5]  
                dbar_1h = dbar_1h[dbar_1h.weekDay != 6]  
            dbar_5min["rsi"] = RSI(dbar_5min["bid"],21)
            dbar_5min['ra20'] = pd.rolling_mean(dbar_5min.bid, 20)
            dbar_5min['ra50'] = pd.rolling_mean(dbar_5min.bid, 50) 
              
            dbar_15min["rsi"] = RSI(dbar_15min["bid"],21)          
            dbar_15min['ra20'] = pd.rolling_mean(dbar_15min.bid, 20)             
            dbar_15min['ra50'] = pd.rolling_mean(dbar_15min.bid, 50)
            
            dbar_1h["rsi"] = RSI(dbar_1h["bid"],21)          
            dbar_1h['ra20'] = pd.rolling_mean(dbar_1h.bid, 20)             
            dbar_1h['ra50'] = pd.rolling_mean(dbar_1h.bid, 50)
            priceBid = (float(dbar.bid[row]))
            priceAsk = (float(dbar.bid[row]))
            rowTemp = row
            tradeSell = True
            tradeBuy = True
            f = open(OUTFILE, 'a')
            while (tradeSell == True) & (rowTemp < len(dbar)-100) :
                rowTemp += 1
                priceTempBid = (float(dbar.bid[rowTemp]))
                priceTempAsk = (float(dbar.ask[rowTemp]))
                dSell = priceTempAsk - priceBid
                if (dSell > SL): 
                    f.write("SELL_PERDANT,%s,%s,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f\n" %(Timestamp(dbar.index[row]), Timestamp(dbar.index[rowTemp]), float(priceBid), float(priceTempAsk), float(dbar_1h.rsi[-1]), float(dbar_1h.rsi[-2]), float(dbar_1h.rsi[-3]), float(dbar_1h.ra50[-1]), float(dbar_1h.ra20[-1]), float(dbar_15min.rsi[-1]), float(dbar_15min.rsi[-2]), float(dbar_15min.rsi[-3]), float(dbar_15min.ra50[-1]), float(dbar_15min.ra50[-2]), float(dbar_15min.ra20[-1]), float(dbar_15min.ra20[-2]), float(dbar_5min.rsi[-1]), float(dbar_5min.ra50[-1]), float(dbar_5min.ra50[-2]), float(dbar_5min.ra50[-3]), float(dbar_5min.ra20[-1]), float(dbar_5min.ra20[-2]), float(dbar_5min.ra20[-3])))
                    tradeSell = False
                if (dSell  <= -1 * TP): 
                    f.write("SELL_GAGNANT,%s,%s,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f\n" %(Timestamp(dbar.index[row]), Timestamp(dbar.index[rowTemp]), float(priceBid), float(priceTempAsk), float(dbar_1h.rsi[-1]), float(dbar_1h.rsi[-2]), float(dbar_1h.rsi[-3]), float(dbar_1h.ra50[-1]), float(dbar_1h.ra20[-1]), float(dbar_15min.rsi[-1]), float(dbar_15min.rsi[-2]), float(dbar_15min.rsi[-3]), float(dbar_15min.ra50[-1]), float(dbar_15min.ra50[-2]), float(dbar_15min.ra20[-1]), float(dbar_15min.ra20[-2]), float(dbar_5min.rsi[-1]), float(dbar_5min.ra50[-1]), float(dbar_5min.ra50[-2]), float(dbar_5min.ra50[-3]), float(dbar_5min.ra20[-1]), float(dbar_5min.ra20[-2]), float(dbar_5min.ra20[-3])))
                    tradeSell = False
        
            rowTemp = row
            while (tradeBuy == True) & (rowTemp < len(dbar) - 100):
                rowTemp += 1
                priceTempBid = (float(dbar.bid[rowTemp]))
                priceTempAsk = (float(dbar.ask[rowTemp]))
                dBuy = priceTempBid - priceAsk
                if (dBuy < (-1 * SL)): 
                    f.write("BUY_PERDANT,%s,%s,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f\n" %(Timestamp(dbar.index[row]), Timestamp(dbar.index[rowTemp]), float(priceBid), float(priceTempAsk), float(dbar_1h.rsi[-1]), float(dbar_1h.rsi[-2]), float(dbar_1h.rsi[-3]), float(dbar_1h.ra50[-1]), float(dbar_1h.ra20[-1]), float(dbar_15min.rsi[-1]), float(dbar_15min.rsi[-2]), float(dbar_15min.rsi[-3]), float(dbar_15min.ra50[-1]), float(dbar_15min.ra50[-2]), float(dbar_15min.ra20[-1]), float(dbar_15min.ra20[-2]), float(dbar_5min.rsi[-1]), float(dbar_5min.ra50[-1]), float(dbar_5min.ra50[-2]), float(dbar_5min.ra50[-3]), float(dbar_5min.ra20[-1]), float(dbar_5min.ra20[-2]), float(dbar_5min.ra20[-3])))
                    tradeBuy = False
                if (dBuy >= TP): 
                    f.write("BUY_GAGNANT,%s,%s,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f\n" %(Timestamp(dbar.index[row]), Timestamp(dbar.index[rowTemp]), float(priceBid), float(priceTempAsk), float(dbar_1h.rsi[-1]), float(dbar_1h.rsi[-2]), float(dbar_1h.rsi[-3]), float(dbar_1h.ra50[-1]), float(dbar_1h.ra20[-1]), float(dbar_15min.rsi[-1]), float(dbar_15min.rsi[-2]), float(dbar_15min.rsi[-3]), float(dbar_15min.ra50[-1]), float(dbar_15min.ra50[-2]), float(dbar_15min.ra20[-1]), float(dbar_15min.ra20[-2]), float(dbar_5min.rsi[-1]), float(dbar_5min.ra50[-1]), float(dbar_5min.ra50[-2]), float(dbar_5min.ra50[-3]), float(dbar_5min.ra20[-1]), float(dbar_5min.ra20[-2]), float(dbar_5min.ra20[-3])))
                    tradeBuy = False
        row+=1
    
#path_to_process = [INFILE01, INFILE02, INFILE03, INFILE04, INFILE05, INFILE06]
#path_to_process = [ INFILE02, INFILE06]

if __name__ == "__main__":
#    numberFiles = count_files(INFILE_PATH)
    numberFiles = ["02_6","02_10", "02_11","02_12","02_13","02_16","02_17", "02_18","02_19", "02_20","02_23", "02_24","02_25", "02_26"]
    
#    path_to_process = make_list(numberFiles)
    path_to_process = numberFiles
#    read_data(numberFiles[0])
    print numberFiles, path_to_process
    pool = mp.Pool(2)
    pool.map(read_data,path_to_process )
#data = pd.read_csv(INFILE, header = None, names=['bid', 'ask', 'other'], date_parser = lambda x: pd.to_datetime(x, format = FORMAT))
#data = dd.read_csv(INFILE)
#del data['other']

elapsed_time = time.time() - start_time


print "%f sec." %(elapsed_time)
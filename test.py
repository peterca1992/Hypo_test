#import pyodbc
#from pandas_datareader import data as web
import datetime as dt
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import timedelta
#import ffn

#import ssl
#ssl._create_default_https_context = ssl._create_unverified_context
#yf.pdr_override()

#%%
#清單
otc_list = pd.read_csv("D:\\Python_code\\工作內容\\2023\\0710_上市櫃清單\\df_list.csv")
otc_list["股票代號"] = otc_list.apply(lambda x : str(x["股票代號"]), axis = 1)


stock_list = ['1301','1702','2357','2385','2474','2520','2603','2615','2850','2881','2882','2891','3008','3388','3413','4107','5299','5306','5871','6121','6147','6176','6192','6239','6669','6670','8299','8464','9938']
stock_list_fix = []

for i in stock_list:
    
   stock_list_fix.append((i + otc_list[otc_list["股票代號"] == i]["上市櫃"]).iloc[0])


stock_list = stock_list_fix.copy()

del stock_list_fix



#%%
#資料
start = dt.datetime(2000, 1, 1)
end = dt.date.today() + timedelta(days = 1)

df_adj_total = pd.DataFrame()
df_close_total = pd.DataFrame()

for x in stock_list:
    
    name = x
    
    df = yf.download(name, start, end)
    df = df.reset_index()
    
    df_adj = df[["Date", "Adj Close"]]
    df_close = df[["Date", "Close"]]
    
    df_adj = df_adj.rename(columns = {"Adj Close" : x.split(".")[0]})
    df_close = df_close.rename(columns = {"Close" : x.split(".")[0]})
    
    if(x == stock_list[0]):
        
        df_adj_total = pd.concat([df_adj_total, df_adj], axis = 0)
        df_close_total = pd.concat([df_close_total, df_close], axis = 0)
    
    if(x != stock_list[0]):
        
        df_adj_total = pd.merge(df_adj_total, df_adj, how = "outer", on = "Date")
        df_close_total = pd.merge(df_close_total, df_close, how = "outer", on = "Date")


df_adj_total = df_adj_total.sort_values(by = "Date").reset_index(drop = True)
df_close_total = df_close_total.sort_values(by = "Date").reset_index(drop = True)

for j in df_close_total.columns[1:]:
    
    df_close_total[j] = df_close_total[j].fillna(method = "ffill")
    df_adj_total[j] = df_adj_total[j].fillna(method = "ffill")
    
    
del df, df_adj, df_close, end, i, name, start, x, j

    
#%%
df_adj_total = df_adj_total[df_adj_total["Date"] >= "2017/12/1"].reset_index(drop = True)
df_close_total = df_close_total[df_close_total["Date"] >= "2017/12/1"].reset_index(drop = True)

df_adj_ret = pd.DataFrame()
df_close_ret = pd.DataFrame()


for i in df_adj_total.columns[1:]:
    
    df_adj_ret_part = pd.DataFrame(np.log(df_adj_total[i].iloc[1:].reset_index(drop = True) / df_adj_total[i].iloc[:-1].reset_index(drop = True)))
    df_adj_ret_part = pd.concat([df_adj_total["Date"].iloc[1:].reset_index(drop = True), df_adj_ret_part], axis = 1)









    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
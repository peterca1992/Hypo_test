import datetime as dt
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import timedelta
from sklearn.linear_model import LinearRegression


#%%
#setting function

def pos_tr(x):
    
    if(x["price"] >= x["2sd"]):
        y = 0.2
        
    if(x["sd"] <= x["price"] and x["price"] < x["2sd"]):
        y = 0.4

    if(x["y"] <= x["price"] and x["price"] < x["sd"]):
        y = 0.6
        
    if(x["nsd"] < x["price"] and x["price"] < x["y"]):
        y = 0.8
        
    if(x["n2sd"] < x["price"] and x["price"] <= x["nsd"]):
        y = 1
    
    if(x["n2sd"] >= x["price"]):
        y = 1.1
          
    return y



#%%
#清單
otc_list = pd.read_csv("D:\\Python_code\\工作內容\\2023\\0710_上市櫃清單\\df_list.csv")
otc_list["股票代號"] = otc_list.apply(lambda x : str(x["股票代號"]), axis = 1)


stock_list = ['1301','1702','2357','2385','2474','2520','2603','2615','2850','2881','2882','2891','3008','3388','3413','4107','5299','5306','5871','6121','6147','6176','6192','6239','6669','6670','8299','8464','9938','2330']
stock_list_fix = []

for i in stock_list:
    
   stock_list_fix.append((i + otc_list[otc_list["股票代號"] == i]["上市櫃"]).iloc[0])


stock_list = stock_list_fix.copy()

del stock_list_fix



#%%
#撈資料
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
    
    
df_tw = yf.download("^TWII", start, end)


del df, df_adj, df_close, end, i, name, start, x, j

    
#%%
#報酬
df_adj_total = df_adj_total[df_adj_total["Date"] >= "2017/12/1"].reset_index(drop = True)
df_close_total = df_close_total[df_close_total["Date"] >= "2017/12/1"].reset_index(drop = True)

df_adj_ret = pd.DataFrame()


for i in df_adj_total.columns[1:]:
    
    df_adj_ret_part = pd.DataFrame(np.log(df_adj_total[i].iloc[1:].reset_index(drop = True) / df_adj_total[i].iloc[:-1].reset_index(drop = True)))
    df_adj_ret_part = pd.concat([df_adj_total["Date"].iloc[1:].reset_index(drop = True), df_adj_ret_part], axis = 1)
    
    if(i == df_adj_total.columns[1]):
        
        df_adj_ret = pd.concat([df_adj_ret, df_adj_ret_part], axis = 0)
    
    if(i != df_adj_total.columns[1]):
        
        df_adj_ret = pd.merge(df_adj_ret, df_adj_ret_part, how = "outer", on = "Date")


df_tw = df_tw.reset_index(drop = False)
df_tw_ret = pd.DataFrame(np.log(df_tw["Adj Close"].iloc[1:].reset_index(drop = True) / df_tw["Adj Close"].iloc[:-1].reset_index(drop = True)))
df_tw_ret["Date"] = df_tw["Date"].iloc[1:].reset_index(drop = True)
df_tw_ret = df_tw_ret[["Date", "Adj Close"]]
df_tw_ret.columns = ["Date", "TW"]

del df_adj_ret_part, i


#%%
#先找Beta
#先用已經找出的

beta_list = pd.read_csv("D:\\Python_code\\工作內容\\2023\\0710_320避險調整\\beta_list.csv")


#%%
pos_table = pd.DataFrame()

for i in stock_list:
    
    st_name = i.split(".")[0]
    
    df_adj_part = df_adj_total[["Date", st_name]]
    df_adj_part = df_adj_part.reset_index(drop = False)
    df_adj_part["index"] = df_adj_part["index"] + 1 
    #df_adj_part = pd.merge(df_adj_part, df_tw[["Date", "Adj Close"]], how = "left", on = "Date")

    df_pos_part = pd.DataFrame()
    
    for j in range(30, len(df_adj_part) + 1):
        
        x_train = np.array(df_adj_part['index'].iloc[(j - 30):j]).reshape(-1, 1)
        y_train = np.array(df_adj_part[st_name].iloc[(j - 30):j]).reshape(-1, 1)  
    
        lm = LinearRegression()
        df_m = lm.fit(x_train, y_train)
        
        yhat = lm.predict(x_train)
        
        df_yhat_table = pd.DataFrame(y_train).rename(columns = {0:"price"})
        
        df_yhat_table["2sd"] = yhat + 2*np.std(y_train - yhat)
        df_yhat_table["sd"] = yhat + np.std(y_train - yhat)
        df_yhat_table["y"] = yhat
        df_yhat_table["nsd"] = yhat - np.std(y_train - yhat)
        df_yhat_table["n2sd"] = yhat - 2*np.std(y_train - yhat)
        
        df_part["pos"] = df_part.apply(lambda x : pos_tr(x), axis = 1)
        
        df_part["Date"] = df_adj_part['Date'].iloc[(j - 66):j].reset_index(drop = True)
        
        if(j == 66):
        
            df_weight_part = pd.concat([df_weight_part, df_part[["Date", "pos"]]], axis = 0)
        
        if(j != 66):
        
            df_weight_part = pd.concat([df_weight_part, pd.DataFrame(df_part[["Date", "pos"]].iloc[-1]).T], axis = 0)








































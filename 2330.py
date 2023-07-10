import pyodbc
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

import pyodbc
import pandas as pd

import sys

sys.path.append('C:\\Users\\J1070116\\Desktop')
from WCFAdox import PCAX

#設定連線主機IP並產生物件
PX=PCAX("172.24.26.40")

#%%

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

sqlcmd = "select * from dbo.日收盤還原表排行 where 日期 >= '2012/1/1' and 股票代號 in ('TWA00','2330','3008')"
sqltables="日收盤還原表排行"
sql=PX.Sql_data(sqlcmd,sqltables)

df_or = sql.copy()

df = df_or[["日期", "股票代號", "收盤價", "漲跌"]]
df["日期"] = pd.to_datetime(df["日期"], format = "%Y%m%d")
df["收盤價"] = df.apply(lambda x : float(x["收盤價"]), axis = 1)
df["漲跌"] = df.apply(lambda x : float(x["漲跌"]), axis = 1)
df["Ret"] = np.log(df["收盤價"] /(df["收盤價"] - df["漲跌"]))


df_ret = df[["日期", "股票代號", "Ret"]].pivot_table(values='Ret', index='日期', columns = "股票代號").reset_index(drop = False)
df_ret["Date"] = df_ret["日期"].copy()
df_ret = df_ret[["Date", "2330", "3008", "TWA00"]]



#%%
#Beta
stock_list = ["2330","3008"]
ABeta_list_output_beta = pd.DataFrame()


day_for_reg = [22, 44, 66, 126, 252, 504]


for x in stock_list:
    
    ABeta_list_output_part_beta_indi = pd.DataFrame()
    
    for liner_day in day_for_reg:
        
        ABeta_list_beta_indi = []
        ABeta_date_list_beta_indi = []
        
        for j in range(liner_day,len(df_ret)+1):
            
            x_train_beta_indi = np.array(np.abs(df_ret['TWA00'].iloc[(j - liner_day):j])).reshape(-1, 1)
            y_train_beta_indi = np.array(np.abs(df_ret[x].iloc[(j - liner_day):j])).reshape(-1, 1)
        
            lm_beta_indi = LinearRegression()
            df_m_beta_indi = lm_beta_indi.fit(x_train_beta_indi, y_train_beta_indi)
            
            ABeta_list_beta_indi.append(df_m_beta_indi.coef_[0][0])
            ABeta_date_list_beta_indi.append(df_ret["Date"].iloc[j-1])
        
        ABeta_list_beta_indi = pd.DataFrame(ABeta_list_beta_indi)
        ABeta_date_list_beta_indi = pd.DataFrame(ABeta_date_list_beta_indi)
        
        ABeta_list_beta_indi = pd.concat([ABeta_date_list_beta_indi, ABeta_list_beta_indi], axis = 1)
        ABeta_list_beta_indi.columns = ["Date", "Beta_" + str(liner_day)]
        
        if(liner_day == day_for_reg[0]):
            ABeta_list_output_part_beta_indi = pd.concat([ABeta_list_output_part_beta_indi, ABeta_list_beta_indi], axis = 0)
    
        if(liner_day != day_for_reg[0]):
            ABeta_list_output_part_beta_indi = pd.merge(ABeta_list_output_part_beta_indi, ABeta_list_beta_indi, how = "left", on = "Date")
    
    ABeta_list_output_part_beta_indi["Stock"] = x
    
    ABeta_list_output_beta = pd.concat([ABeta_list_output_beta, ABeta_list_output_part_beta_indi], axis = 0)



#%%

ABeta_list_output_beta_part = ABeta_list_output_beta[ABeta_list_output_beta["Stock"] == "3008"] #########


sqlcmd = "select * from dbo.日收盤表排行 where 日期 >= '2012/1/1' and 股票代號 in ('TWA00','2330','3008')"
sqltables="日收盤表排行"
sql=PX.Sql_data(sqlcmd,sqltables)

df_nor = sql.copy()

df2 = df_nor[["日期", "股票代號", "收盤價", "漲跌"]]
df2["日期"] = pd.to_datetime(df2["日期"], format = "%Y%m%d")
df2["收盤價"] = df2.apply(lambda x : float(x["收盤價"]), axis = 1)
df2["漲跌"] = df2.apply(lambda x : float(x["漲跌"]), axis = 1)
df2["Ret"] = np.log(df2["收盤價"] /(df2["收盤價"] - df2["漲跌"]))


df_ret2 = df2[["日期", "股票代號", "Ret"]].pivot_table(values='Ret', index='日期', columns = "股票代號").reset_index(drop = False)
df_ret2["Date"] = df_ret2["日期"].copy()
df_ret2 = df_ret2[["Date", "2330", "3008", "TWA00"]]


df_ret3 = pd.merge(df_ret2, ABeta_list_output_beta_part[["Date", "Beta_252"]], how = "left", on = "Date")
df_ret3 = df_ret3[np.isnan(df_ret3["Beta_252"]) == False].reset_index(drop = True)
df_ret3["Ret2"] = df_ret3["3008"] - df_ret3["TWA00"] * df_ret3["Beta_252"]  #########


#原本的完
#%%
#測試加上五線譜調整避險比例
#用RS的五線譜切出來

df_price =  df[["日期", "股票代號", "收盤價"]].pivot_table(values='收盤價', index='日期', columns = "股票代號").reset_index(drop = False)
df_price = df_price.reset_index(drop = False)
df_price["index"] = df_price["index"] + 1

pos_columns = pd.DataFrame()

for i in range(66,(len(df_price)+1)):

    x_train = np.array(df_price['index'].iloc[(i - 66):i]).reshape(-1, 1)
    y_train = np.array(df_price['3008'].iloc[(i - 66):i]/df_price['TWA00'].iloc[(i - 66):i]).reshape(-1, 1)  #########

    lm = LinearRegression()
    df_m = lm.fit(x_train, y_train)
    
    yhat = lm.predict(x_train)
    
    df_part = pd.DataFrame(y_train).rename(columns = {0:"price"})
    
    df_part["2sd"] = yhat + 2*np.std(y_train - yhat)
    df_part["sd"] = yhat + np.std(y_train - yhat)
    df_part["y"] = yhat
    df_part["nsd"] = yhat - np.std(y_train - yhat)
    df_part["n2sd"] = yhat - 2*np.std(y_train - yhat)
    
    df_part["pos"] = df_part.apply(lambda x : pos_tr(x), axis = 1)
    
    
    df_part["Date"] = df_price['日期'].iloc[(i - 66):i].reset_index(drop = True)
    
    pos_columns = pd.concat([pos_columns, pd.DataFrame(df_part[["Date", "pos"]].iloc[-1]).T], axis = 0)



df_ret4 = pd.merge(df_ret3, pos_columns, how = "left", on = "Date")






































'''
Created on 2021年1月5日
@author:liuxl
'''
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
import time
import numpy as np

ts.set_token("e7411039d01fd931c7451478328dce2af025cc6761104a9aa455590d")

engine_ts = create_engine('mysql+pymysql://root:admin123@192.168.0.208:3306/stock?charset=utf8')
pro = ts.pro_api()


def read_data(sql):
    # sql = """SELECT * FROM stock_basic LIMIT 20"""
    df = pd.read_sql_query(sql, engine_ts)
    return df


'''每天可能有新股上市，每次都删除重新存入'''


def write_data(df):
    df.to_sql('stock_basic', engine_ts, index=False, if_exists='replace', chunksize=5000)


def write_data_daily(table_name, data_value):
    data_value.to_sql(table_name, engine_ts, index=False, if_exists='append', chunksize=5000)


'''只查询A股，上市的股'''


def get_data():
    ds = pro.stock_basic(list_status='L', market="主板")
    return ds


def get_date_stock(ts_codes):
    sql = "SELECT * FROM stock_basic where ts_code = 'data_value' "
    sql_d = sql.replace("data_value", ts_codes)
    stock_data = read_data(sql_d)
    return stock_data


def get_max_trade_time(table_name):
    sql = "SELECT  MAX(trade_date) FROM `table_name`"
    sql_d = sql.replace("table_name", table_name)
    stock_data = read_data(sql_d)
    return stock_data


def save_stock_data(stock_data):
    ts_code = stock_data[0]
    table_name = "stock_daily_" + ts_code
    if not engine_ts.has_table(table_name):
        start_time = stock_data[6]
        df = pro.daily(ts_code=ts_code, start_date=start_time, end_date=now_data)
        write_data_daily(table_name, df)
        print("新股票" + ts_code + "信息表")
    else:
        max_trade_time = get_max_trade_time(table_name)
        if (now_data > max_trade_time).__init__:
            start_date = max_trade_time["MAX(trade_date)"].array[0]
            df = pro.daily(ts_code=ts_code, start_date=start_date,
                           end_date=now_data)
            if not df["trade_date"].empty and (df["trade_date"].array[0] != start_date):
                print(ts_code + "更新" + now_data + "日期，股票信息")
                save_stock = df.drop(df["ts_code"].size - 1, axis=0)
                write_data_daily(table_name, save_stock)


if __name__ == '__main__':
    now_data = time.strftime('%Y%m%d', time.localtime(time.time()))
    all_data = get_data()
    write_data(all_data)
    test = np.array(all_data)
    for index in test.tolist():
        save_stock_data(index)

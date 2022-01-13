import pandas as pd
import time
import numpy as np
import mysql as db

'''用5、10、30、60、120、240均线'''
now_data = time.strftime('%Y%m%d', time.localtime(time.time()))
days_array = [5, 10, 20, 60, 120, 240]
max_days = 5


def get_stock_average(table_names, sum_value):
    days_int = 2 * sum_value
    days_value = str(days_int)
    stock_sql = """SELECT `close` FROM `table_name_value` ORDER BY trade_date desc LIMIT sum_value_d"""
    sql_init_table = stock_sql.replace("table_name_value", table_names)
    sql_init = sql_init_table.replace("sum_value_d", days_value)
    df = pd.read_sql_query(sql_init, db.engine_ts)
    return df


def get_stock_average_value(stock_list, sums):
    average_value = np.zeros(max_days)
    data_list = np.array(stock_list)
    for int_ in range(sums):
        if int_ < max_days:
            average_int_list = data_list[int_:sums + int_]
            average_int_ = np.mean(average_int_list)
            average_value[int_] = average_int_
            data_list[int_]
        else:
            continue
    return average_value


if __name__ == '__main__':
    all_sql = """SELECT * FROM stock_basic"""
    stock_all_data = db.read_data(all_sql)
    array_data = np.array(stock_all_data)
    for index in array_data.tolist():
        table_name = "stock_daily_" + index[0]
        days_sums = range(len(days_array))

        for days_sum in days_sums:
            stock_data_list = get_stock_average(table_name, days_array[days_sum])
            average_array = get_stock_average_value(stock_data_list, days_array[days_sum])

            print(average_array)

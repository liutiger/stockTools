import mysql as db
import time


def getAllDate():
    sql = """SELECT * FROM stock_basic """
    info = db.read_data(sql)
    return info


def get_isinstance(value):
    return isinstance(value, float)


'''历史代码'''
# name = row[1].array[2]
# lastTime = row[1].array[6]
# info = db.pro.daily_basic(ts_code=ts_code, trade_date='20220106',
#                          fields='ts_code,close,trade_date,turnover_rate,turnover_rate_f,volume_ratio,pe,pb')
# db.ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=lastTime, end_date=time_now)


if __name__ == '__main__':
    df = getAllDate()
    time_now = time.strftime('%Y%m%d', time.localtime(time.time()))
    # time_now = '20220100'
    ts_codes = ""
    ts_f = ","
    count = 0
for row in df.iterrows():
    count = count + 1
    ts_code = row[1].array[0]
    if len(ts_codes) == 0:
        ts_codes = ts_codes + ts_code
    else:
        ts_codes = ts_codes + ts_f + ts_code
    if count >= 500:
        count = 0
        now_stock = db.pro.daily(ts_code=ts_codes, start_date=time_now, end_date=time_now)
        time.sleep(0.06)
        ts_codes = ""
        for index in now_stock.iterrows():
            '''涨幅2.5~5,量比大于1，换手率在5～10之间，市值在50亿以上'''
            index_value = index[1].array[8]
            if 2.5 < index_value < 5:
                time.sleep(0.06)
                stock = db.pro.daily_basic(ts_code=index[1].array[0], trade_date=time_now,
                                           fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pb,circ_mv')
                stock_array = stock.loc[0].array
                if get_isinstance(stock_array[3]) and stock_array[3] >= 1 \
                        and get_isinstance(stock_array[2]) and 4.5 <= stock_array[2] <= 10 \
                        and get_isinstance(stock_array[5]) and stock_array[6] >= 500000:
                    '''量持续 下方有均线上方无套劳 全天不破分时均线且强于大盘分时'''
                    print(index[1].array[0])
    else:
        ts_codes = ts_codes + ts_f + ts_code

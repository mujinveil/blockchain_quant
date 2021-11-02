# encoding="utf-8"
import datetime
import json
import time
import requests
import numpy as np
import pandas as pd
from threading import Thread
from tools.databasePool import POOL, r0
from tools.future_trade import buy_open, sell_open, buy_close, sell_close
from tools.get_future_market_info import get_perpetualprice


# 获取K线数据
def get_klinedata(platform, symbol, granularity):
    if platform == "huobi":  # 2000条数据，时间粒度1min, 5min, 15min, 30min, 60min, 4hour, 1day, 1mon, 1week, 1year
        huobi_granularity_dict = {60: "1min", 300: "5min", 900: "15min", 1800: "30min", 3600: "60min",
                                  14400: "4hour", 86400: "1day", 604800: "1week", 2592000: "mon",
                                  946080000: "1year"}
        for _ in range(3):
            try:
                res = requests.get("https://api.huobi.pro/market/history/kline?period={}&size=2000&symbol={}".format(
                    huobi_granularity_dict[granularity], symbol.replace("_", "")), timeout=1)
                df = pd.DataFrame()
                if res.status_code == 200:
                    data = json.loads(res.content.decode())["data"][::-1]
                    df['close'] = [i['close'] for i in data]
                    df['time_vs'] = [time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(i['id'])) for i in data]
                    break
            except Exception as e:
                print(e)
                df = pd.DataFrame()
        return df


# 获取推文数据
def read_data():
    conn = POOL.connection()
    cur = conn.cursor()
    df = pd.read_sql("SELECT * FROM twitter_stream where time_vs>'2021-09-10 08:00:00' ORDER BY id DESC", conn)
    df['tweet'] = df['tweet'].apply(lambda x: str(x).lower())
    df = df[df['tweet'].str.contains('ethereum')]
    df.drop_duplicates(subset=['tweet'], keep='first', inplace=True)
    df.sort_values(by='time_vs', inplace=True)
    df.set_index('time_vs', inplace=True)
    df['sentiment_smooth'] = df['sentiment'].rolling(2000).mean()
    df = df.resample(rule='5T').mean()
    return df


# 获取以太坊与推文情感数据的相关系数
def get_corr(data):
    corrlist = []
    for i in range(len(data)):
        if i <= 15:
            corr_value = 0
        else:
            df = data.head(i + 1)
            corr_value = df['close'].corr(df['sentiment_smooth'])
        corrlist.append(corr_value)
    data['corr'] = corrlist
    return data


def process_data(df):
    # 以太坊数据
    eth_data = get_klinedata("huobi", "eth_usdt", 900)
    eth_data.sort_values(by='time_vs', inplace=True)
    eth_data.set_index('time_vs', inplace=True)
    # 合并以太坊与推文数据
    data = pd.merge(eth_data, df, how='left', left_index=True, right_index=True)
    data.dropna(inplace=True)
    data = get_corr(data)
    # 如果相关系数持续增长
    data['corr_increase'] = data['corr'] - data['corr'].shift(5)
    data['position'] = np.where(data['corr_increase'] > 0, 1, 0)
    data['MA_30'] = data['close'].rolling(30).mean()
    data['trend'] = np.where(data['close'] > data['MA_30'], 1, -1)
    data['position'] = data['trend'] * data['position']
    if data['corr_increase'].iloc[-1] > 0.4:
        if data['position'] == 1:
            return 1
        elif data['position'] == -1:
            return -1
    elif data['corr_increase'].iloc[-1] <= 0:
        return 0


# 定期清理sql数据
def clear_data(n):
    now_time = time.localtime()
    if now_time.tm_hour != 9 or now_time.tm_min != 37:
        return
    conn = POOL.connection()
    cur = conn.cursor()
    # 获取n天前时间
    today = datetime.datetime.now()
    offset = datetime.timedelta(days=n)
    before_date = (today - offset).strftime("%Y-%m-%d %H:%M:%S")
    print(before_date)
    delete_sql = "delete from twitter_stream where time_vs<%s"
    cur.execute(delete_sql, (before_date))
    conn.commit()


# 主交易函数
def trade(strategydata):
    flag = strategydata['flag']
    platform = strategydata['platform']
    symbol = strategydata['symbol']
    df = read_data()
    signal = process_data(df)
    signal = 1
    close = get_perpetualprice(platform, symbol)
    if flag == 0:
        if signal == 1:
            # 买入做多eth
            buy_open(strategydata, close)
        elif signal == -1:
            # 卖出做空eth
            sell_open(strategydata, close)
    if signal == 0:
        # 平多仓eth
        if flag == 1:
            buy_close(strategydata, close)
        # 平空仓eth
        elif flag == -1:
            sell_close(strategydata, close)


if __name__ == "__main__":
    while True:
        try:
            strategy_list = r0.hvals("Sentiment_strategy")
            strategy_list = [json.loads(i) for i in strategy_list]
            T = []
            for strategy_info in strategy_list:
                T.append(Thread(target=trade, args=(strategy_info,)))
            T.append(Thread(target=clear_data, args=(4,)))
            for t in T:
                t.start()
            for t in T:
                t.join()
        except Exception as e:
            print(e)
        finally:
            time.sleep(1)
    # strategydata={
    #         "userUuid": "402",
    #         "platform": "T8ex",
    #         "symbol": "eth",
    #         "apiAccountId": 10213,
    #         "strategyId": 9980,
    #         "firstSheets": 3,
    #         "leverage": 3,
    #         "flag": -1,
    #         "strategyType": "28",
    #         "entryPrice": "3290.58"
    #     }
    # trade(strategydata)

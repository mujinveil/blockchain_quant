# encoding="utf-8"
import json
import time
from threading import Thread
import numpy as np
import pandas as pd
import requests
import statsmodels.api as sm
import sys
sys.path.append("..")
from tools.Config import huobifuture_api_url
from tools.databasePool import r0
from tools.future_trade import buy_open, buy_close
from tools.get_future_market_info import get_perpetualprice


def get_future_klinedata(symbol):
    # 获取500根4小时均线
    now = int(time.time())
    start_time = now - 3600 * 500 * 4
    contract_code = "{}-usdt".format(symbol).upper()
    url = huobifuture_api_url + '/linear-swap-ex/market/history/kline?contract_code={}&period=4hour&from={}&to={}'.format(
        contract_code, start_time, now)
    res = requests.get(url)
    resdict = json.loads(res.content.decode())
    df = pd.DataFrame()
    df['close'] = [i['close'] for i in resdict['data']]
    df['high'] = [i['high'] for i in resdict['data']]
    df['low'] = [i['low'] for i in resdict['data']]
    return df


def rsrs(df):
    highs = df.high
    lows = df.low
    ans = []
    for i in range(len(highs))[18:]:
        data_high = highs.iloc[i - 18 + 1:i + 1]
        data_low = lows.iloc[i - 18 + 1:i + 1]
        X = sm.add_constant(data_low)
        model = sm.OLS(data_high, X)
        results = model.fit()
        ans.append(results.params[1])
    return ans


def rss_signal(ans):
    ans_mean = np.mean(ans[:-1])
    ans_std = np.std(ans[:-1])
    buythreshold = ans_mean + ans_std
    sellthreshold = ans_mean - ans_std
    if ans[-1] > buythreshold:
        return 1  # 做多
    elif ans[-1] < sellthreshold:
        return -1  # 做多平仓
    else:
        return 0


def trade(strategydata):
    flag = strategydata['flag']
    platform = strategydata['platform']
    symbol = strategydata['symbol']
    df = get_future_klinedata(symbol)
    close = get_perpetualprice(platform, symbol)
    ans = rsrs(df)
    signal = rss_signal(ans)
    if flag == 0 and signal == 1:
        buy_open(strategydata, close)
    elif flag == 1 and signal == -1:
        buy_close(strategydata, close)


if __name__ == "__main__":
    while True:
        try:
            strategy_list = r0.hvals("rss_strategy")
            strategy_list = [json.loads(i) for i in strategy_list]
            print(strategy_list)
            T = []
            for strategy_info in strategy_list:
                T.append(Thread(target=trade, args=(strategy_info,)))
            for t in T:
                t.start()
            for t in T:
                t.join()
        except Exception as e:
            print(e)
        finally:
            time.sleep(1)
    # strategydata = {
    #     "userUuid": "536",
    #     "apiAccountId": 10210,
    #     "strategyId": 883,
    #     "flag": 0,
    #     "platform": "huobi",
    #     "symbol": "eth",
    #     "leverage": 5,
    #     "firstSheets": 5,
    #     "entryPrice":0
    # }
    # trade(strategydata)

# encoding="utf-8"
import json
import sys
import time
from threading import Thread
import numpy as np
import pandas as pd
import requests
sys.path.append("..")
from tools.Config import updateCover_url
from tools.databasePool import r2
from tools.tool import buy_multiple_symbols
from tools.strategy_clearout import currency_stop_out


# 获取k线数据（现货）
def get_klinedata(platform, symbol, granularity):
    df = pd.DataFrame()
    # [时间，开盘价，收盘价，交易量]
    if platform == "huobi":  # 200条数据，时间粒度1min, 5min, 15min, 30min, 60min, 4hour, 1day, 1mon, 1week, 1year
        huobi_granularity_dict = {60: "1min", 300: "5min", 900: "15min", 1800: "30min", 3600: "60min",
                                  14400: "4hour", 86400: "1day", 604800: "1week", 2592000: "mon",
                                  946080000: "1year"}
        for _ in range(3):
            try:
                response = requests.get(
                    "https://api.huobi.pro/market/history/kline?period={}&size=300&symbol={}".format(
                        huobi_granularity_dict[granularity], symbol.replace("_", "")), timeout=3)
                if response.status_code == 200:
                    res = response.json()['data'][::-1]
                    df['close'] = [i['close'] for i in res]
                    break
            except Exception as e:
                print(e)
    elif platform == "binance":
        for _ in range(3):
            try:
                # 200条数据，时间粒度1m, 5m, 15m, 30m, 1h, 4h, 1d
                binance_granularity_dict = {60: "1m", 300: "5m", 900: "15m", 1800: "30m", 3600: "1h",
                                            14400: "4h", 86400: "1d"}
                response = requests.get("https://www.binancezh.cc/api/v3/klines?symbol={}&interval={}&limit=300".format(
                    symbol.upper().replace("_", ""), binance_granularity_dict[granularity]), timeout=1)
                if response.status_code == 200:
                    data = response.json()
                    df['close'] = [float(i[4]) for i in data]
                    break
            except Exception as e:
                print(e)
    return df


# 获取多个币种的200日收盘价
def get_symbols_klinedata(platform, symbols, granularity):
    klinedata = None
    for symbol in symbols:
        df = get_klinedata(platform, "{}_usdt".format(symbol), granularity)
        df.rename(columns={'close': symbol}, inplace=True)
        klinedata = df if klinedata is None else pd.concat([klinedata, df], axis=1)
    return klinedata


# 复合动量因子
def compound_momentum(symbols, df):
    judge_list = None
    for symbol in symbols:
        df['return'] = np.log(df[symbol] / df[symbol].shift())
        up_days = np.sum(df['return'].values > 0)
        down_days = np.sum(df['return'].values < 0)
        return_all = df['return'].sum()
        judge = pd.DataFrame({symbol: [up_days / (up_days + down_days) * return_all]})
        # judge=pd.DataFrame({symbol:[1*return_all]})
        judge_list = judge.T if judge_list is None else pd.concat([judge_list, judge.T], axis=0)
    judge_list.columns = ['return']
    judge_list.sort_values(by=['return'], ascending=False, inplace=True)
    return judge_list


# 收益率偏度因子
def skew_factor(symbols, df):
    skew_list = None
    for symbol in symbols:
        # df=get_future_klinedata0("T8ex",symbol)
        ret = np.log(df[symbol] / df[symbol].shift())
        ret.columns = [symbol]
        ret_skew = pd.DataFrame({symbol: [ret.skew()]})  # 计算偏度后转置，index为【future】,即期货种类代码
        skew_list = ret_skew.T if skew_list is None else pd.concat([skew_list, ret_skew.T], axis=0)
    skew_list.columns = ['skew']
    skew_list.sort_values(by=['skew'], ascending=False, inplace=True)
    return skew_list


def weighted_factor_score(factor, factor_name, weight):
    # 给定每只期货合约的因子数据及权重，返回加权后的打分结果
    factor.columns = [factor_name]
    factor.iloc[:2] = 1
    factor.iloc[2:] = 0
    # factor.iloc[-2:] = 0
    factor = factor * weight
    return factor


def multi_factor_signal(symbols, df):
    judge_list = weighted_factor_score(compound_momentum(symbols, df), '复合动量', 1)
    skew_list = weighted_factor_score(skew_factor(symbols, df), '偏度', 1)
    result = pd.concat([judge_list, skew_list], axis=1)
    result = result.sum(axis=1).sort_values(ascending=False)
    result.iloc[:2] = 1
    result.iloc[2:] = 0
    # result.iloc[-2:] = -1
    return result


def trade(strategydata):
    localtime = time.localtime(time.time())
    if localtime.tm_hour != 8 or localtime.tm_min != 0 or localtime.tm_sec != 0:
        return
    flag = strategydata['flag']
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    strategyId = strategydata['strategyId']
    platform = strategydata['platform']
    strategyType = strategydata['strategyType']
    amount = strategydata['amount']
    df = get_symbols_klinedata("huobi", symbols, 86400)
    res = multi_factor_signal(symbols, df)
    buy_pool = list(res.index[:2])
    buy_symbols = ["{}_usdt".format(i) for i in buy_pool]
    buy_list = [{'symbol': buy_symbols[0], 'numberDeal': amount},
                {'symbol': buy_symbols[1], 'numberDeal': amount}]
    if flag == 0:  # 无持仓
        buy_multiple_symbols(userUuid, apiAccountId, strategyId, platform, strategyType, buy_list, updateCover_url)
        strategydata['flag'] = 1
        r2.hset("Momentum_strategy", strategyId, json.dumps(strategydata))
    if flag == 1:  # 有持仓
        print("该账户有持仓")
        # 检查是否需要调仓换股
        momentum_list = r2.hvals('momentum_label:{}'.format(strategyId))
        momentum_info = [json.loads(i)['symbol'] for i in momentum_list]
        if set(momentum_info) != set(buy_symbols):  # 待买入股票池与持仓不同,需调仓换股
            # 先清仓
            currency_stop_out(strategydata)
            # 再买入股票
            buy_multiple_symbols(userUuid, apiAccountId, strategyId, platform, strategyType, buy_list, updateCover_url)


if __name__ == "__main__":
    symbols = ['btc', 'eth', 'link', 'eos', 'fil', 'ltc', 'dot', 'doge']
    while True:
        try:
            strategy_list = r2.hvals("Momentum_strategy")
            strategy_list = [json.loads(i) for i in strategy_list]
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
    #     "userUuid": "398051ac70ef4da9aafd33ce0b95195f",
    #     "apiAccountId": 10209,
    #     "strategyId": 785,
    #     "platform": "T8ex",
    #     "strategyType": 24,
    #     "amount": 30,
    #     "flag": 1
    # }
    # trade(strategydata)

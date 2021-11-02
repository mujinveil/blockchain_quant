# encoding=utf-8
import json
import time
import sys
sys.path.append("..")
from threading import Thread
from tools.databasePool import r0
from tools.future_trade import buy_open, sell_close, sell_open, buy_close
from tools.get_future_market_info import get_perpetualprice, get_huobifuture_klinedata


# 震荡区间
def range(df, N, k1, k2):
    df['HH'] = df['high'].shift().rolling(N).max()
    df['HC'] = df['close'].shift().rolling(N).max()
    df['LC'] = df['close'].shift().rolling(N).min()
    df['LL'] = df['low'].shift().rolling(N).min()
    df.fillna(0, inplace=True)
    df['vix_1'] = df['HH'] - df['LC']
    df['vix_2'] = df['HC'] - df['LL']
    df['range'] = df[['vix_1', 'vix_2']].max(axis=1)
    df['upper_'] = df['open'] + k1 * df['range']
    df['low_'] = df['open'] - k2 * df['range']
    df.drop(['vix_1', 'vix_2'], axis=1, inplace=True)
    return df


# 突破上轨或者下轨
def break_signal(df, close):
    if close > df['upper_'].iloc[-2]:
        return 1
    elif close < df['low_'].iloc[-2]:
        return -1
    else:
        return 0


# 交易主函数
def trade(strategydata):
    # localtime = time.localtime(time.time())
    # if localtime.tm_hour != 11:
    #     return
    flag = strategydata['flag']
    platform = strategydata['platform']
    symbol = strategydata['symbol']
    N = strategydata['backward']
    k1 = strategydata['buyThreshold']
    k2 = strategydata['sellThreshold']
    close = get_perpetualprice(platform, symbol)
    df = get_huobifuture_klinedata(symbol)
    df = range(df, N, k1, k2)
    signal = break_signal(df, close)
    if signal == 1:
        if flag == 0:
            buy_open(strategydata, close)
        elif flag == -1:
            sell_close(strategydata, close)
    elif signal == -1:
        if flag == 0:
            sell_open(strategydata, close)
        elif flag == 1:
            buy_close(strategydata, close)


if __name__ == "__main__":
    while True:
        try:
            strategy_list = r0.hvals("dual_thrust")
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

    # strategydata = {"userUuid": "6e7c88272f554956a35d8ed2cf833201",
    #                 "apiAccountId": 10177,
    #                 "strategyId": 1099,
    #                 "amount": 1,
    #                 "flag": 0,
    #                 "platform": "T8ex",
    #                 "symbol": "BTC",
    #                 "leverRate":5}

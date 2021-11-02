# encoding="utf-8"
import json
import time
from threading import Thread
import numpy as np
import sys
sys.path.append("..")
from tools.Kline_analyze import MACD
from tools.databasePool import r0
from tools.future_trade import buy_open, sell_open, buy_close, sell_close
from tools.get_future_market_info import get_perpetualprice, get_huobifuture_klinedata


def macd_signal(df):
    close = np.array(df['close'].values.tolist())
    diff, dea, macd = MACD(close, 3, 7, 7)
    if diff[-1] > 0 and dea[-1] > 0 and macd[-1] > 0:
        return 1
    elif diff[-1] < 0 and dea[-1] < 0 and macd[-1] < 0:
        return -1
    else:
        return 0


def atr_signal(df):
    for i in range(0, len(df)):
        df.loc[df.index[i], 'TR'] = max((df['close'][i] - df['low'][i]), abs(df['high'][i] - df['close'].shift()[i]),
                                        abs(df['low'][i] - df['close'].shift()[i]))
    df['atr'] = df['TR'].rolling(10).mean()
    df['high_20'] = np.array(df['high'].rolling(20).max())
    df['low_20'] = np.array(df['low'].rolling(20).min())
    df['high_diff'] = df['high_20'] - df['close']
    df['low_diff'] = df['close'] - df['low_20']
    df['dontbuy'] = np.where(df['high_diff'] > 3 * df['atr'], 1, -1)
    df['dontsell'] = np.where(df['low_diff'] > 3 * df['atr'], 1, -1)
    dontbuy_signal = df['dontbuy'].iloc[-1]
    dontsell_signal = df['dontsell'].iloc[-1]
    return dontbuy_signal, dontsell_signal


def trade_signal(macd, atr):
    if macd == 1 and atr[0] != 1:
        return 1
    elif macd == -1 and atr[1] != 1:
        return -1
    else:
        return 0


# 开仓买入
def trade(strategydata):
    localtime = time.localtime(time.time())
    if localtime.tm_hour != 8 or localtime.tm_min != 0 or localtime.tm_sec != 0:
        return
    symbol = strategydata['symbol']
    flag = strategydata['flag']
    platform = strategydata['platform']
    df = get_huobifuture_klinedata(symbol)
    macd = macd_signal(df)
    atr = atr_signal(df)
    signal = trade_signal(macd, atr)
    close = get_perpetualprice(platform, symbol)
    if flag == 0:
        if signal == 1:
            buy_open(strategydata, close)
        elif signal == -1:
            sell_open(strategydata, close)
    elif flag == 1:
        if signal != 1:
            buy_close(strategydata, close)
    elif flag == -1:
        if signal != -1:
            sell_close(strategydata, close)


if __name__ == "__main__":
    while True:
        try:
            strategy_list = r0.hvals("macdatrstrategy")
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

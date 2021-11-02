# encoding="utf-8"
import json
import time
from threading import Thread
import numpy as np
import sys
sys.path.append("..")
from tools.Kline_analyze import EMA
from tools.databasePool import r0
from tools.future_trade import buy_open, sell_open, buy_close, sell_close
from tools.get_future_market_info import get_huobifuture_klinedata, get_perpetualprice


def keltner_channel(df):
    # 最高价、最低价、收盘价均值
    df['hlc'] = (df['close'] + df['high'] + df['low']) / 3
    hlc = np.array(df['hlc'])
    # 最高价/最低价/收盘价均值的指数平均值
    df['hlc_ema'] = EMA(14, hlc)
    # ATR计算
    for i in range(0, len(df)):
        df.loc[df.index[i], 'TR'] = max((df['close'][i] - df['low'][i]), abs(df['high'][i] - df['close'].shift()[i]),
                                        abs(df['low'][i] - df['close'].shift()[i]))
    df['atr'] = df['TR'].rolling(14).mean()
    # 上轨、下轨
    df['up'] = df['hlc_ema'] + 0.8 * df['atr']
    df['down'] = df['hlc_ema'] - 0.8 * df['atr']
    return df


def keltner_signal(close, df, flag):
    if flag == 0:  # 无持仓
        if close > df['up'].iloc[-2]:  # 开多
            return 1
        elif close < df['down'].iloc[-2]:  # 开空
            return -1
        else:
            return 0
    elif flag == 1:  # 持有多单
        if close < df['hlc_ema'].iloc[-2]:  # 平多
            return 0
        else:
            return 1
    elif flag == -1:  # 持有空单
        if close > df['hlc_ema'].iloc[-2]:  # 平空
            return 0
        else:
            return -1


def trade(strategydata):
    localtime = time.localtime(time.time())
    if localtime.tm_hour != 8 or localtime.tm_min != 0 or localtime.tm_sec != 0:
        return
    flag = strategydata['flag']
    platform = strategydata['platform']
    symbol = strategydata['symbol']
    close = get_perpetualprice(platform, symbol)
    df = get_huobifuture_klinedata(symbol)
    df = keltner_channel(df)
    signal = keltner_signal(close, df, flag)
    if flag == 0:
        if signal == 1:
            buy_open(strategydata, close)
        elif signal == -1:
            sell_open(strategydata, close)

    elif flag == 1:
        if signal == 0:
            buy_close(strategydata, close)

    elif flag == -1:
        if signal == 0:
            sell_close(strategydata, close)


if __name__ == "__main__":
    while True:
        try:
            strategy_list = r0.hvals("keltner_strategy")
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

    # 测试服
    # strategydata={
    #     "userUuid": "396",
    #     "apiAccountId": 10200,
    #     "strategyId": 1260,
    #     "flag": 0,
    #     "platform": "T8ex",
    #     "symbol": "eth",
    #     "leverage": 3,
    #     "firstSheets": 2,
    #     "entryPrice": "2406.76"
    # }

    # 正式服
    # strategydata = {
    #     "userUuid": "536",
    #     "apiAccountId": 10210,
    #     "strategyId": 884,
    #     "flag": 0,
    #     "platform": "huobi",
    #     "symbol": "eth",
    #     "leverage": 5,
    #     "firstSheets": 2,
    #     "entryPrice": 0
    # }
    # trade(strategydata)

# encoding="utf-8"
import json
import sys
import time

sys.path.append("..")
from threading import Thread
from tools.databasePool import r0
from tools.future_trade import buy_open, sell_open, buy_close, sell_close
from tools.get_future_market_info import get_perpetualprice, top_position_ratio


# 大户持仓日变化
def top_position_change(df):
    df['long_change'] = df['longratio'] - df['longratio'].shift()
    if df['long_change'].iloc[-1] > 0:
        # 大户多仓增加，跟踪做多
        return 1
    elif df['long_change'].iloc[-1] <= 0:
        # 大户空仓增加，跟踪做空
        return -1


# 交易主函数
def trade(strategydata):
    localtime = time.localtime(time.time())
    if localtime.tm_hour != 8 or localtime.tm_min != 0 or localtime.tm_sec != 0:
        return
    symbol = strategydata['symbol']
    platform = strategydata['platform']
    flag = strategydata['flag']
    # 只获取火币大户持仓变化，币安的数据无效
    pos_ratio_table = top_position_ratio("huobi", symbol)
    close = get_perpetualprice(platform, symbol)
    signal = top_position_change(pos_ratio_table)
    # 当前持仓为0
    if flag == 0:
        # 跟随大户做多
        if signal == 1:
            buy_open(strategydata, close)
        # 跟随大户做空
        elif signal == -1:
            sell_open(strategydata, close)
    # 持有多仓
    elif flag == 1:
        # 大户多仓减少，空仓增加,平多仓，开空仓
        if signal == -1:
            buy_close(strategydata, close)
            sell_open(strategydata, close)
        # 如果信号依然为1，则继续持有多仓
        elif signal != -1:
            pass
    # 持有空仓
    elif flag == -1:
        # 大户空仓减少，多仓增加，平空仓，开多仓
        if signal == 1:
            sell_close(strategydata, close)
            buy_open(strategydata, close)
        # 如果信号依然为-1，则继续持有空仓
        elif signal != 1:
            pass


if __name__ == "__main__":
    while True:
        try:
            strategy_list = r0.hvals("spiderweb_strategy")
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
    #     "userUuid": "402",
    #     "apiAccountId": 10213,
    #     "strategyId": 1268,
    #     "flag": -1,
    #     "symbol": "eth",
    #     "platform": "T8ex",
    #     "leverage": 3,
    #     "firstSheets": 1,
    #     "entryPrice": "3267.01"
    # }
    # close = get_perpetualprice("T8ex", "eth")
    # sell_close(strategydata, close)
    # trade(strategydata)

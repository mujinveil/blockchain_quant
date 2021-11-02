# encoding="utf-8"
import json
import time
from threading import Thread
import numpy as np
import requests
from tools.Config import future_remain_url, contract_size_dict, futureamountlimit
from tools.databasePool import r0
from tools.future_trade import turtle_buy_open, turtle_sell_open, turtle_buy_close, turtle_sell_close
from tools.get_future_market_info import get_huobifuture_klinedata, get_perpetualprice


# 判断当前价格是否突破唐安奇通道上下轨
def check_break(price_list, price, T):
    up = max(price_list['high'].iloc[-T - 1:-2])
    down = min(price_list['low'].iloc[-T - 1:-2])
    if price > up:
        return 1
    elif price < down:
        return -1
    else:
        return 0


# 判断是否需要止损
def check_stop_signal(price_list, T):
    up = max(price_list['high'].iloc[-T - 1:-2])
    down = min(price_list['low'].iloc[-T - 1:-2])
    if price_list['high'].iloc[-1] > up:
        return 1
    elif price_list['low'].iloc[-1] < down:
        return -1
    else:
        return 0


# 计算ATR
def get_ATR(price_list, T):
    TR_list = [max(price_list['high'].iloc[i] - price_list['low'].iloc[i],
                   abs(price_list['high'].iloc[i] - price_list['close'].iloc[i - 1]),
                   abs(price_list['close'].iloc[i - 1] - price_list['low'].iloc[i])) for i in range(1, T + 1)]
    ATR = np.array(TR_list).mean()
    return ATR


# 判断下一步是否加仓或者止损平仓
def get_next_signal(price, last_price, ATR, position):
    if (price >= last_price + 0.5 * ATR and position == 1) or (price <= last_price - 0.5 * ATR and position == -1):
        # 多头或空头加仓
        return 1
    elif (price <= last_price - 2 * ATR and position == 1) or (price >= last_price + 2 * ATR and position == -1):
        # 多头或空头止损平仓
        return -1
    else:
        return 0


# 是否重置最高价与最低价
# def set_price_mark(strategydata, close):
#     flag = strategydata['flag']
#     strategyId = strategydata['strategyId']
#     if flag == 1 and strategydata['price_mark'] < close:
#         strategydata['price_mark'] = close
#         r0.hset("Turtle_strategy", strategyId, json.dumps(strategydata))
#     elif flag == -1 and strategydata['price_mark'] > close:
#         strategydata['price_mark'] = close
#         r0.hset("Turtle_strategy", strategyId, json.dumps(strategydata))
#
#
# # 是否平仓
# def get_risk_signal(strategydata, close):
#     flag = strategydata['flag']
#     if flag == -1:
#         if close >= 1.05 * strategydata['price_mark']:
#             return True
#         else:
#             return False
#     elif flag == 1:
#         if close < 0.95 * strategydata['price_mark']:
#             return True
#         else:
#             return False


# 获取每次加仓的张数
def get_unit(strategydata, ATR, contract_size):
    # 获得账户总权益
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    platform = strategydata['platform']
    symbol = strategydata['symbol']
    position_param = {"userUuid": userUuid, "apiAccountId": apiAccountId, "platform": platform, "symbol": symbol}
    remainres = requests.get(future_remain_url, params=position_param)
    remaindict = remainres.json()
    if remaindict['response'] and "power" in remaindict['response'].keys():
        remain_amount = abs(remaindict['response']['power'])
    else:
        remain_amount = strategydata['amount']
    # 计算每单头寸
    if platform == "binance":
        unit = remain_amount * 0.01 / ATR
        unit = round(unit, futureamountlimit[symbol][platform])
    else:
        unit = int(remain_amount * 0.01 / ATR / contract_size)
    return unit


# 交易主函数
def trade(strategydata):
    strategyId = strategydata['strategyId']
    platform = strategydata['platform']
    symbol = strategydata['symbol']
    flag = strategydata['flag']
    last_price = strategydata['last_price']
    add_time = strategydata['add_time']
    limit_unit = strategydata['maxPositionNum']
    contract_size = float(contract_size_dict[symbol][platform])  # 合约面值
    price_list = get_huobifuture_klinedata(symbol)
    # 如果没有数据，返回
    if len(price_list) == 0:
        return
    close_price = float(price_list['close'].iloc[-1])
    ATR = get_ATR(price_list, 26)
    # 开仓,获得开仓信号
    if flag == 0:
        open_signal = check_break(price_list, close_price, 20)
        if open_signal == 1:  # 多头开仓
            unit = get_unit(strategydata, ATR, contract_size)
            turtle_buy_open(strategydata, close_price, unit)
        elif open_signal == -1:  # 空头开仓
            unit = get_unit(strategydata, ATR, contract_size)
            turtle_sell_open(strategydata, close_price, unit)

    # 判断加仓或止损
    elif flag != 0:
        signal = get_next_signal(close_price, last_price, ATR, flag)
        # 判断加仓且持仓没有到达上限
        if signal == 1 and add_time < limit_unit:
            unit = get_unit(strategydata, ATR, contract_size)
            # 多头加仓
            if flag == 1:
                turtle_buy_open(strategydata, close_price, unit)
            # 空头加仓
            elif flag == -1:
                turtle_sell_open(strategydata, close_price, unit)
        # 判断平仓止损
        elif signal == -1:
            if flag == 1:
                # 多头平仓
                turtle_buy_close(strategydata, close_price)
            elif flag == -1:
                # 空头平仓
                turtle_sell_close(strategydata, close_price)
        else:
            # 方式一：跌破二十日最低价
            signal = check_stop_signal(price_list, 10)
            if flag == 1 and signal == -1:
                # 多头平仓
                turtle_buy_close(strategydata, close_price)
            elif flag == -1 and signal == 1:
                # 空头平仓
                turtle_sell_close(strategydata, close_price)
            # 方式二：跟踪最高价，从最高价回落5%
            # # 判断今日是否出现最高价(多头)或者最低价(空头)
            # set_price_mark(strategydata, close_price)
            # # 得到止损信号
            # signal = get_risk_signal(strategydata, close_price)
            # if signal:
            #     if flag == 1:
            #         turtle_buy_close(strategydata, close_price)
            #     elif flag == -1:
            #         turtle_sell_close(strategydata, close_price)


if __name__ == "__main__":
    while True:
        try:
            strategy_list = r0.hvals("Turtle_strategy")
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
    #   "userUuid": "402",
    #    "platform":  "T8ex",
    #    "apiAccountId":  10213,
    #    "strategyId":  9980,
    #    "amount":  1000,
    #    "strategyType":  26,
    #    "symbol":  "eth",
    #    "leverage":  3,
    #    "limit_unit":  4,
    #    "flag":  1,
    #    "last_price":  3111.99,
    #    "add_time":  1,
    #    "buy_value":  18679.56,
    #    "buy_num":  6,
    #    "sell_value":  0,
    #    "sell_num":  0,
    #    "price_mark":  3111.99
    # }
    # trade(strategydata)

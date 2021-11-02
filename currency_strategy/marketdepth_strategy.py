# -*- coding: utf-8 -*-
# @Time : 2020/3/20 10:01
# 下买卖单逻辑
import json
import random
import sys
import time
from threading import Thread
sys.path.append("..")
from tools.Config import pricelimit, amountlimit
from tools.databasePool import r1
from tools.get_market_info import get_currentprice1, get_currentprice0, get_T8ex_orders
from tools.handle import place_all_orders, cancel_1, query_1, \
    bulk_currency_buy_orders, bulk_currency_sell_orders, bulk_currency_cancel_orders


def init_market_strategy(market_strategy_data):
    userUuid = market_strategy_data["userUuid"]  # 获取用户唯一id
    strategyId = market_strategy_data["strategyId"]  # 获取策略id
    status = market_strategy_data["status"]  # 获取状态1开启，0关闭，3手动停止
    apiAccountId1 = market_strategy_data["apiAccountId1"]  # API id 1
    apiAccountId2 = market_strategy_data["apiAccountId2"]  # API id 2
    platform = market_strategy_data["platform"]  # 交易所
    symbol = market_strategy_data["symbol"]  # 交易对
    amount_min = market_strategy_data["amount_min"]  # 每笔最小交易量
    amount_max = market_strategy_data["amount_max"]  # 每笔最大交易量
    diffprice_min = market_strategy_data["diffprice_min"]  # 最小价差
    diffprice_max = market_strategy_data["diffprice_max"]  # 最大价差
    order_amount = market_strategy_data["order_amount"]  # 挂单数量
    allow_diffprice = market_strategy_data["allow_diffprice"]  # 允许盘口价差
    apiAccountIdlist = [apiAccountId1, apiAccountId2]
    try:
        buyId = random.choice(apiAccountIdlist)
        sellId = [i for i in apiAccountIdlist if i != buyId][0]
        currentprice = get_currentprice1("huobi", symbol)
        buylist = []
        selllist = []
        buyprice = currentprice - 1
        sellprice = currentprice + 1
        for i in range(order_amount):
            buyprice = round(buyprice - random.uniform(diffprice_min, diffprice_max), pricelimit[symbol][platform])
            buyamount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
            buydata = {
                "userUuid": userUuid,
                "apiAccountId": buyId,
                "strategyId": strategyId,
                "platform": platform,
                "symbol": symbol,
                "amount": buyamount,
                "price": buyprice
            }
            buylist.append(buydata)
            sellprice = round(sellprice + random.uniform(diffprice_min, diffprice_max), pricelimit[symbol][platform])
            sellamount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
            selldata = {
                "userUuid": userUuid,
                "apiAccountId": sellId,
                "strategyId": strategyId,
                "platform": platform,
                "symbol": symbol,
                "amount": sellamount,
                "price": sellprice
            }
            selllist.append(selldata)
        place_all_orders(buylist, selllist)
    except Exception as e:
        info = "初始化深度策略报错{}".format(e)
        print(info)


def stop_market_strategy(market_strategy_data):
    symbol = market_strategy_data["symbol"]  # 交易对
    try:
        res1 = r1.hgetall("T8ex_{}_buyorders".format(symbol))
        res1 = [json.loads(i) for i in res1.values()]
        res2 = r1.hgetall("T8ex_{}_sellorders".format(symbol))
        res2 = [json.loads(i) for i in res2.values()]
        for i in res1:
            cancel_1(i["userUuid"], i["apiAccountId"], i["strategyId"], i["platform"], i["symbol"], i["orderId"],
                     i["direction"])
        for i in res2:
            cancel_1(i["userUuid"], i["apiAccountId"], i["strategyId"], i["platform"], i["symbol"], i["orderId"],
                     i["direction"])
    except Exception as e:
        info = "停止深度策略报错{}".format(e)
        print(info)


def market_strategy(market_strategy_data):
    userUuid = market_strategy_data["userUuid"]  # 获取用户唯一id
    strategyId = market_strategy_data["strategyId"]  # 获取策略id
    status = market_strategy_data["status"]  # 获取状态1开启，0关闭，3手动停止
    apiAccountId1 = market_strategy_data["apiAccountId1"]  # 子账户id 1
    apiAccountId2 = market_strategy_data["apiAccountId2"]  # 子账户id 2
    platform = market_strategy_data["platform"]  # 交易所
    symbol = market_strategy_data["symbol"]  # 交易对
    amount_min = market_strategy_data["amount_min"]  # 每笔最小交易量
    amount_max = market_strategy_data["amount_max"]  # 每笔最大交易量
    diffprice_min = market_strategy_data["diffprice_min"]  # 下单价最小间距
    diffprice_max = market_strategy_data["diffprice_max"]  # 下单价最大间距
    order_amount = market_strategy_data["order_amount"]  # 挂单数量
    allow_diffprice = market_strategy_data["allow_diffprice"]  # 允许盘口价差
    apiAccountIdlist = [apiAccountId1, apiAccountId2]
    buyId = random.choice(apiAccountIdlist)
    sellId = [i for i in apiAccountIdlist if i != buyId][0]
    if status == 1:
        """1-盘口差价过大,则补单"""
        try:
            # 获取深度买一卖一价以及最新成交价
            lastprice = get_currentprice0(platform, symbol)
            res = get_T8ex_orders(symbol)
            buyprice1 = 0
            sellprice1 = 0
            if res["data"]["bid"] != [] and res["data"]["ask"] != []:
                buyprice1 = res["data"]["bid"][0]["price"]
                sellprice1 = res["data"]["ask"][0]["price"]
            if sellprice1 - buyprice1 > allow_diffprice:
                print("{}-买一卖一差价{},补单".format(symbol, sellprice1 - buyprice1))
                dif1 = lastprice - buyprice1
                dif2 = sellprice1 - lastprice
                buyprice = lastprice - diffprice_max  # 折价处理
                buylist = []
                n = int(dif1 / (allow_diffprice / 5)) if int(dif1 / (allow_diffprice / 5)) < 5 else 5
                for i in range(n):
                    print("{}-补买单{}".format(symbol, i))
                    buyprice = round(buyprice - random.uniform(diffprice_min, diffprice_max),pricelimit[symbol][platform])
                    buyamount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
                    if symbol == "shib_usdt":
                        buyamount = int(buyamount)
                    buydata = {
                        "userUuid": userUuid,
                        "apiAccountId": buyId,
                        "strategyId": strategyId,
                        "platform": platform,
                        "symbol": symbol,
                        "amount": buyamount,
                        "price": buyprice,
                        "direction": 1,
                        "source": 8
                    }
                    buylist.append(buydata)
                bulk_currency_buy_orders(buylist[:10])
                # place_buy_orders(buylist)
                sellprice = lastprice + diffprice_max  # 溢价处理
                selllist = []
                m = int(dif2 / (allow_diffprice / 5)) if int(dif2 / (allow_diffprice / 5)) < 5 else 5
                for i in range(m):
                    print("{}-补卖单{}".format(symbol, i))
                    sellprice = round(sellprice + random.uniform(diffprice_min, diffprice_max),
                                      pricelimit[symbol][platform])
                    sellamount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
                    if symbol == "shib_usdt":
                        sellamount = int(sellamount)
                    selldata = {
                        "userUuid": userUuid,
                        "apiAccountId": sellId,
                        "strategyId": strategyId,
                        "platform": platform,
                        "symbol": symbol,
                        "amount": sellamount,
                        "price": sellprice,
                        "direction": 2,
                        "source": 8
                    }
                    selllist.append(selldata)
                bulk_currency_sell_orders(selllist[:10])
                # place_sell_orders(selllist)
        except Exception as e:
            info = "盘口价差大补单出错{}".format(e)
            print(info)
        time.sleep(3)
        """2-盘口订单少,补单"""
        try:
            lastprice = get_currentprice0(platform, symbol)
            res = get_T8ex_orders(symbol)
            if len(res["data"]["bid"]) < order_amount:
                print("*******{}-【盘口】买单{}个，小于{}，往低价新增买单********".format(symbol, len(res["data"]["bid"]), order_amount))
                buyprice = lastprice - diffprice_max
                buylist = []
                for i in range(order_amount - len(res["data"]["bid"])):
                    buyprice = round(buyprice - random.uniform(diffprice_min, diffprice_max),
                                     pricelimit[symbol][platform])
                    buyamount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
                    if symbol == "shib_usdt":
                        buyamount = int(buyamount)
                    buydata = {
                        "userUuid": userUuid,
                        "apiAccountId": buyId,
                        "strategyId": strategyId,
                        "platform": platform,
                        "symbol": symbol,
                        "amount": buyamount,
                        "price": buyprice,
                        "direction": 1,
                        "source": 8
                    }
                    buylist.append(buydata)
                bulk_currency_buy_orders(buylist[:10])
                # place_buy_orders(buylist)
            if len(res["data"]["ask"]) < order_amount:
                print("*******{}-【盘口】卖单{}个，小于{}，往高价新增卖单********".format(symbol, len(res["data"]["ask"]), order_amount))
                sellprice = lastprice + diffprice_max
                selllist = []
                for i in range(order_amount - len(res["data"]["ask"])):
                    sellprice = round(sellprice + random.uniform(diffprice_min, diffprice_max),
                                      pricelimit[symbol][platform])
                    sellamount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
                    if symbol == "shib_usdt":
                        sellamount = int(sellamount)
                    selldata = {
                        "userUuid": userUuid,
                        "apiAccountId": sellId,
                        "strategyId": strategyId,
                        "platform": platform,
                        "symbol": symbol,
                        "amount": sellamount,
                        "price": sellprice,
                        "direction": 2,
                        "source": 8
                    }
                    selllist.append(selldata)
                bulk_currency_sell_orders(selllist[:10])
                # place_sell_orders(selllist)
        except Exception as e:
            info = "订单少,补单出错{}".format(e)
            print(info)
        time.sleep(3)
        """3-查询订单,如果有成交就从redis中去掉"""
        try:
            lastprice = get_currentprice0(platform, symbol)
            res1 = r1.hgetall("T8ex_{}_buyorders".format(symbol))
            res1 = [json.loads(i) for i in res1.values()]
            res2 = r1.hgetall("T8ex_{}_sellorders".format(symbol))
            res2 = [json.loads(i) for i in res2.values()]
            for i in res1:
                if i["price"] >= lastprice:
                    query_1(i["userUuid"], i["apiAccountId"], i["strategyId"], i["platform"], i["symbol"], i["orderId"],
                            i["direction"])
            for j in res2:
                if j["price"] <= lastprice:
                    query_1(j["userUuid"], j["apiAccountId"], j["strategyId"], j["platform"], j["symbol"], j["orderId"],
                            j["direction"])
        except Exception as e:
            info = "查询redis订单出错{}".format(e)
            print(info)
        time.sleep(3)
        """4-如果数据库未成交买单大于20个，则取消价格较小的买单,避免资金占用过多"""
        try:
            res1 = r1.hgetall("T8ex_{}_buyorders".format(symbol))
            res1 = [json.loads(i) for i in res1.values()]
            res1 = sorted(res1, key=lambda keys: keys['price'])
            res2 = r1.hgetall("T8ex_{}_sellorders".format(symbol))
            res2 = [json.loads(i) for i in res2.values()]
            res2 = sorted(res2, key=lambda keys: keys['price'])
            if len(res1) > order_amount:
                print("*******{}-数据库买单{}个，大于{}，取消价格较小的买单********".format(symbol, len(res1), order_amount))
                cancellist = res1[:- order_amount]
                # 需要分两批撤
                user1cancellist = [i for i in cancellist if i['apiAccountId'] == apiAccountId1][:10]
                user2cancellist = [i for i in cancellist if i['apiAccountId'] == apiAccountId2][:10]
                bulk_currency_cancel_orders(user1cancellist)
                bulk_currency_cancel_orders(user2cancellist)
            if len(res2) > order_amount:
                print("*******{}-数据库卖单{}个，大于{}，取消价格较大的卖单********".format(symbol, len(res2), order_amount))
                cancellist = res2[order_amount:]
                # 需要分两批撤
                user1cancellist = [i for i in cancellist if i['apiAccountId'] == apiAccountId1][:10]
                user2cancellist = [i for i in cancellist if i['apiAccountId'] == apiAccountId2][:10]
                bulk_currency_cancel_orders(user1cancellist)
                bulk_currency_cancel_orders(user2cancellist)
        except Exception as e:
            info = "redis中订单多,取消部分{}".format(e)
            print(info)

    print(
        "#####################################{}-{}执行完毕***********************************************".format(platform,
                                                                                                               symbol))
    time.sleep(2)


def run_market_strategy():
    while True:
        try:
            marketdatalist = r1.hvals("Market_Strategy")
            marketdatalist = [json.loads(i) for i in marketdatalist]
            if not marketdatalist:
                time.sleep(2)
            else:
                T = []
                for marketdata in marketdatalist:
                    # symbol = marketdata['symbol']
                    # if symbol not in ["dec_usdt","ds_dec"]:
                    #     continue
                    if marketdata["status"] == 1:
                        t = Thread(target=market_strategy, args=(marketdata,))
                        T.append(t)
                        t.start()
                    elif marketdata["status"] == 2:
                        r1.hdel("Market_Strategy", marketdata["strategyId"])
                        t = Thread(target=stop_market_strategy, args=(marketdata,))
                        t.start()
                for t in T:
                    t.join()
                time.sleep(5)
        except Exception as e:
            info = "提供深度策略报错{}".format(e)
            print(info)


if __name__ == '__main__':
    run_market_strategy()

'''
{
  "strategyId": 82,
   "userUuid":  "68",
   "userId":  null,
   "apiAccountId1":  10187,
   "apiAccountId2":  10188,
   "platform":  "bitget",
   "symbol":  "matic_usdt",
   "amount_min":  0.1,
   "amount_max":  1,
   "diffprice_min":  0.001,
   "diffprice_max":  0.01,
   "order_amount":  20,
   "allow_diffprice":  0.002,
   "status":  1
}

'''

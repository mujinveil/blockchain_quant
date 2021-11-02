# encoding='utf-8'
import json
import random
import time
from threading import Thread
import sys
sys.path.append("..")
from tools.Config import futurepricelimit
from tools.databasePool import r6
from tools.future_handle import query_1, place_cancel_orders,bulk_buy_orders, bulk_sell_orders, bulk_cancel_orders
from tools.get_future_market_info import get_T8ex_contract_orders, get_perpetualprice


def stop_market_strategy(market_strategy_data):
    symbol = market_strategy_data["symbol"]  # 交易对
    try:
        res1 = r6.hgetall("T8ex_{}_buyorders".format(symbol))
        res1 = [json.loads(i) for i in res1.values()]
        res2 = r6.hgetall("T8ex_{}_sellorders".format(symbol))
        res2 = [json.loads(i) for i in res2.values()]
        cancelorderList = res1 + res2
        place_cancel_orders(cancelorderList)
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
    leverate = market_strategy_data['leverate']
    apiAccountIdlist = [apiAccountId1, apiAccountId2]
    buyId = apiAccountIdlist[0]  # random.choice(apiAccountIdlist)
    sellId = apiAccountIdlist[1]  # [i for i in apiAccountIdlist if i != buyId][0]
    if status == 1:
        """2-盘口订单少,补单"""
        try:
            lastprice = get_perpetualprice(platform, symbol)
            res = get_T8ex_contract_orders(symbol)
            if len(res["data"]["bid"]) < order_amount:
                print("*******{}-【盘口】买单{}个，小于{}，往低价新增买单********".format(symbol, len(res["data"]["bid"]), order_amount))
                buyprice = lastprice - diffprice_max
                buylist = []
                for i in range(order_amount - len(res["data"]["bid"])):
                    buyprice = round(buyprice - random.uniform(diffprice_min, diffprice_max),futurepricelimit[symbol][platform])
                    buyamount = random.randint(amount_min, amount_max)
                    if buyId == apiAccountId1:
                        buydata = {
                            "userUuid": userUuid,
                            "apiAccountId": buyId,
                            "symbol": symbol,
                            "platform": platform,
                            "amount": buyamount,
                            "price": buyprice,
                            "direction": 1,
                            "orderPriceType": 1,
                            "offset": 1,
                            "leverRate": leverate
                        }
                    else:
                        buydata = {
                            "userUuid": userUuid,
                            "apiAccountId": buyId,
                            "symbol": symbol,
                            "platform": platform,
                            "amount": buyamount,
                            "price": buyprice,
                            "direction": 1,
                            "orderPriceType": 1,
                            "offset": 2,
                            "leverRate": leverate
                        }
                    buylist.append(buydata)
                bulk_buy_orders(buylist[:10])
            if len(res["data"]["ask"]) < order_amount:
                print("*******{}-【盘口】卖单{}个，小于{}，往高价新增卖单********".format(symbol, len(res["data"]["ask"]), order_amount))
                sellprice = lastprice + diffprice_max
                selllist = []
                for i in range(order_amount - len(res["data"]["ask"])):
                    sellprice = round(sellprice + random.uniform(diffprice_min, diffprice_max),
                                      futurepricelimit[symbol][platform])
                    sellamount = random.randint(amount_min, amount_max)
                    if sellId == apiAccountId2:
                        selldata = {
                            "userUuid": userUuid,
                            "apiAccountId": sellId,
                            "symbol": symbol,
                            "platform": platform,
                            "amount": sellamount,
                            "price": sellprice,
                            "direction": 2,
                            "orderPriceType": 1,
                            "offset": 3,
                            "leverRate": leverate
                        }
                    else:
                        selldata = {
                            "userUuid": userUuid,
                            "apiAccountId": sellId,
                            "symbol": symbol,
                            "platform": platform,
                            "amount": sellamount,
                            "price": sellprice,
                            "direction": 2,
                            "orderPriceType": 1,
                            "offset": 4,
                            "leverRate": leverate
                        }
                    selllist.append(selldata)
                bulk_sell_orders(selllist[:10])
        except Exception as e:
            info = "订单少,补单出错{}".format(e)
            print(info)
        time.sleep(2)
        """1-盘口差价过大,则补单"""
        try:
            # 获取深度买一卖一价以及最新成交价
            userUuid = market_strategy_data["userUuid"]
            buyprice1 = 0
            sellprice1 = 0
            lastprice = get_perpetualprice(platform, symbol)
            res = get_T8ex_contract_orders(symbol)
            if res["data"]["bid"] != [] and res["data"]["ask"] != []:
                buyprice1 = res["data"]["bid"][0]["price"]
                sellprice1 = res["data"]["ask"][0]["price"]
            print(lastprice, buyprice1, sellprice1)
            if sellprice1 - buyprice1 > allow_diffprice:
                print("{}-买一卖一差价{},补单".format(symbol, sellprice1 - buyprice1))
                dif1 = lastprice - buyprice1
                dif2 = sellprice1 - lastprice
                buyprice = lastprice - diffprice_max  # 折价处理
                buylist = []
                n = int(dif1 / (allow_diffprice / 5)) if int(dif1 / (allow_diffprice / 5)) < 5 else 5
                for i in range(n):
                    print("{}-补买单{}".format(symbol, i))
                    buyprice = round(buyprice - random.uniform(diffprice_min, diffprice_max),
                                     futurepricelimit[symbol][platform])
                    buyamount = random.randint(amount_min, amount_max)
                    if buyId == apiAccountId1:
                        buydata = {
                            "userUuid": userUuid,
                            "apiAccountId": buyId,
                            "symbol": symbol,
                            "platform": platform,
                            "amount": buyamount,
                            "price": buyprice,
                            "direction": 1,
                            "orderPriceType": 1,
                            "offset": 1,
                            "leverRate": leverate
                        }
                    buylist.append(buydata)
                bulk_buy_orders(buylist[:10])
                sellprice = lastprice + diffprice_max  # 溢价处理
                selllist = []
                m = int(dif2 / (allow_diffprice / 5)) if int(dif2 / (allow_diffprice / 5)) < 5 else 5
                for i in range(m):
                    print("{}-补卖单{}".format(symbol, i))
                    sellprice = round(sellprice + random.uniform(diffprice_min, diffprice_max),
                                      futurepricelimit[symbol][platform])
                    sellamount = random.randint(amount_min, amount_max)
                    if sellId == apiAccountId2:
                        selldata = {
                            "userUuid": userUuid,
                            "apiAccountId":sellId,
                            "symbol": symbol,
                            "platform": platform,
                            "amount": sellamount,
                            "price": sellprice,
                            "direction": 2,
                            "orderPriceType": 1,
                            "offset": 3,
                            "leverRate": leverate
                        }
                    selllist.append(selldata)
                bulk_sell_orders(selllist[:10])
        except Exception as e:
            info = "盘口价差大补单出错{}".format(e)
            print(info)
        time.sleep(3)
        """3-查询订单,如果有成交就从redis中去掉"""
        try:
            lastprice = get_perpetualprice(platform, symbol)
            res1 = r6.hgetall("T8ex_{}_buyorders".format(symbol))
            res1 = [json.loads(i) for i in res1.values()]
            res2 = r6.hgetall("T8ex_{}_sellorders".format(symbol))
            res2 = [json.loads(i) for i in res2.values()]
            for i in res1:
                if i["price"] >= lastprice:
                    query_1(i["userUuid"], i["apiAccountId"], i["platform"], i["orderId"], i["direction"],
                            i["symbol"], )
            for j in res2:
                if j["price"] <= lastprice:
                    query_1(i["userUuid"], i["apiAccountId"], i["platform"], i["orderId"], i["direction"],
                            i["symbol"], )
        except Exception as e:
            info = "查询redis订单出错{}".format(e)
            print(info)
        time.sleep(3)
        """4-如果数据库未成交买单大于20个，则取消价格较小的买单,避免资金占用过多"""
        try:
            res1 = r6.hgetall("T8ex_{}_buyorders".format(symbol))
            res1 = [json.loads(i) for i in res1.values()]
            res1 = sorted(res1, key=lambda keys: keys['price'])
            res2 = r6.hgetall("T8ex_{}_sellorders".format(symbol))
            res2 = [json.loads(i) for i in res2.values()]
            res2 = sorted(res2, key=lambda keys: keys['price'])
            if len(res1) > order_amount:
                print("*******{}-数据库买单{}个，大于{}，取消价格较小的买单********".format(symbol, len(res1), order_amount))
                cancellist = res1[:- order_amount][:10]
                bulk_cancel_orders(cancellist)
            if len(res2) > order_amount:
                print("*******{}-数据库卖单{}个，大于{}，取消价格较大的卖单********".format(symbol, len(res2), order_amount))
                cancellist = res2[order_amount:][:10]
                bulk_cancel_orders(cancellist)
        except Exception as e:
            info = "redis中订单多,取消部分{}".format(e)
            print(info)
    print(
        "#####################################{}-{}执行完毕***********************************************".format(platform,symbol))
    time.sleep(2)


def run_market_strategy():
    while True:
        try:
            marketdatalist = r6.hvals("Future_Market_Strategy")
            marketdatalist = [json.loads(i) for i in marketdatalist]
            if not marketdatalist:
                time.sleep(2)
            else:
                T = []
                for marketdata in marketdatalist:
                    symbol = marketdata['symbol']
                    #if symbol not in ["link", "eth", "ltc", "doge", "fil"]:
                    if symbol !="eth":
                        continue
                    if marketdata["status"] == 1:
                        t = Thread(target=market_strategy, args=(marketdata,))
                        T.append(t)
                        t.start()
                    elif marketdata["status"] == 2:
                        r6.hdel("Future_Market_Strategy", marketdata["strategyId"])
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

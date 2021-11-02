# encoding=utf-8
# 循环按火币最新价挂单撤单
import json
import random
import time
from threading import Thread
from tools.handle import buy, sell, cancel
from tools.Config import amountlimit
from tools.databasePool import r1
from tools.get_market_info import get_currentprice0, get_currentprice1, get_bitget_orders


def run_follow(followdata):
    userUuid = followdata["userUuid"]  # 获取用户唯一id
    strategyId = followdata["strategyId"]  # 获取策略id
    status = followdata["status"]  # 获取状态1开启，0关闭，3手动停止
    apiAccountId1 = followdata["apiAccountId1"]  # API id 1
    apiAccountId2 = followdata["apiAccountId2"]  # API id 2
    platform = followdata["platform"]  # 交易所
    symbol = followdata["symbol"]  # 交易对
    amount_min = followdata["amount_min"]  # 每笔最小交易量
    amount_max = followdata["amount_max"]  # 每笔最大交易量
    apiAccountIdlist = [apiAccountId1, apiAccountId2]
    try:
        if status == 1:  # 策略开启
            """方案一，对比买卖现价"""
            buyId = random.choice(apiAccountIdlist)  # 随机选一个id
            sellId = [i for i in apiAccountIdlist if i != buyId][0]  # 非buyid 中的id
            currentprice, side = 0, 1
            if symbol == "matic_usdt":
                if currentprice == 0:
                    currentprice = get_currentprice1("huobi", "matic_usdt")
                    side = random.choice(["buy", "sell"])
                print(symbol, currentprice, side)

            if currentprice != 0:
                currentprice1 = get_currentprice0(platform, symbol)
                print("{}-{}-最新价{}".format(platform, symbol, currentprice1))
                if currentprice1 != 0:
                    # 如果huobi比bitget价格大于0.2%(根据卖盘下买单，将价格拉至与huobi基本持平),此处后期可加入对冲逻辑
                    if currentprice - currentprice1 >= currentprice * 0.002:
                        print("{}-huobi比bitget价格大{}".format(symbol, currentprice - currentprice1))
                        res = get_bitget_orders(symbol)
                        asks = res["data"]["ask"]
                        if asks:
                            buyamount = round(sum([i["amount"] for i in asks if i["price"] <= currentprice]),
                                              amountlimit[symbol][platform])
                            if buyamount == 0:
                                buyamount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
                            buyprice = currentprice
                            buyorderid = buy(userUuid, buyId, strategyId, platform, symbol, buyamount, buyprice)
                            cancel(userUuid, buyId, strategyId, platform, symbol, buyorderid)
                    # 如果huobi比bitget价格小于0.2%(根据买盘下卖单，将价格拉至于huobi基本持平),此处后期可加入对冲逻辑
                    if currentprice1 - currentprice >= currentprice * 0.002:
                        print("{}-huobi比bitget价格小{}".format(symbol, currentprice1 - currentprice))
                        res = get_bitget_orders(symbol)
                        bids = res["data"]["bid"]
                        if bids:
                            sellamount = round(sum([i["amount"] for i in bids if i["price"] >= currentprice]),
                                               amountlimit[symbol][platform])
                            if sellamount == 0:
                                sellamount = round(random.uniform(amount_min, amount_max),
                                                   amountlimit[symbol][platform])
                            sellprice = currentprice
                            sellorderid = sell(userUuid, sellId, strategyId, platform, symbol, sellamount, sellprice)
                            cancel(userUuid, sellId, strategyId, platform, symbol, sellorderid)
                # 判断完上面情行，继续刷单
                amount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
                if side == "sell":
                    sellorderid = sell(userUuid, sellId, strategyId, platform, symbol, amount, currentprice)
                    buyorderid = buy(userUuid, buyId, strategyId, platform, symbol, amount, currentprice)
                    cancel(userUuid, sellId, strategyId, platform, symbol, sellorderid)
                    cancel(userUuid, buyId, strategyId, platform, symbol, buyorderid)
                elif side == "buy":
                    buyorderid = buy(userUuid, buyId, strategyId, platform, symbol, amount, currentprice)
                    sellorderid = sell(userUuid, sellId, strategyId, platform, symbol, amount, currentprice)
                    cancel(userUuid, buyId, strategyId, platform, symbol, buyorderid)
                    cancel(userUuid, sellId, strategyId, platform, symbol, sellorderid)
        # """方案二、对比标的价和本交易所买一卖一价"""
        # buyId = random.choice(apiAccountIdlist)
        # sellId = [i for i in apiAccountIdlist if i != buyId][0]
        # currentprice,side = 0,1
        # if symbol != "btc_sgdt" and symbol != "eth_sgdt":
        #     currentprice, side = get_okex_price_and_side(symbol)
        #     if currentprice == 0:
        #         currentprice = get_currentprice1("huobi", symbol)
        #         side = random.choice(["buy", "sell"])
        #     print(symbol,currentprice,side)
        # if symbol == "btc_sgdt":
        #     currentprice, side = get_okex_price_and_side("btc_usdt")
        #     if currentprice == 0:
        #         currentprice = get_currentprice1("huobi", "btc_usdt")
        #         side = random.choice(["buy", "sell"])
        #     currentprice = round(currentprice * 1.424,2)
        #     print(symbol, currentprice, side)
        # elif symbol == "eth_sgdt":
        #     currentprice, side = get_okex_price_and_side("eth_usdt")
        #     if currentprice == 0:
        #         currentprice = get_currentprice1("huobi", "eth_usdt")
        #         side = random.choice(["buy", "sell"])
        #     currentprice = round(currentprice * 1.424,2)
        #     print(symbol, currentprice, side)
        # if currentprice != 0:
        #     res = get_market_depth(platform,symbol)   # 获取befinx买卖信息
        #     buy1_price = res["buyprice"]
        #     buy1_amount = res["buyquantity"]
        #     sell1_price = res["sellprice"]
        #     sell1_amount = res["sellquantity"]
        #     # 如果okex现价比befinx买一价还小，则按befinx买一价撮合成交
        #     if currentprice < buy1_price and buy1_amount !=0:
        #         # if buy1_amount > amount_max:
        #         #     buy1_amount = amount_max
        #         sellorderid = sell(userUuid, buyId, strategyId, platform, symbol, buy1_amount, buy1_price)
        #     # 如果okex现价比befinx卖一价还大，则按befinx卖一价撮合成交
        #     if currentprice > sell1_price and sell1_amount != 0:
        #         # if sell1_amount > amount_max:
        #         #     sell1_amount = amount_max
        #         buyorderid = buy(userUuid, sellId, strategyId, platform, symbol, sell1_amount, sell1_price)
        #     else:
        #         # 判断完上面情行，继续刷单
        #         amount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
        #         if side == "sell":
        #             sellorderid = sell(userUuid, sellId, strategyId, platform, symbol, amount, currentprice)
        #             buyorderid = buy(userUuid, buyId, strategyId, platform, symbol, amount, currentprice)
        #             # cancel(userUuid, sellId, strategyId, platform, symbol, sellorderid)
        #             # cancel(userUuid, buyId, strategyId, platform, symbol, buyorderid)
        #             # cancel_all(userUuid, buyId, sellId, strategyId, platform, symbol, buyorderid, sellorderid)
        #         elif side == "buy":
        #             buyorderid = buy(userUuid, buyId, strategyId, platform, symbol, amount, currentprice)
        #             sellorderid = sell(userUuid, sellId, strategyId, platform, symbol, amount, currentprice)
        #             # cancel(userUuid, buyId, strategyId, platform, symbol, buyorderid)
        #             # cancel(userUuid, sellId, strategyId, platform, symbol, sellorderid)
        #             # cancel_all(userUuid, buyId, sellId, strategyId, platform, symbol, buyorderid, sellorderid)

    except Exception as e:
        print(e)
    finally:
        time.sleep(1)


def goDoFollowStrategy():
    while True:
        try:
            followdatalist = r1.hvals("Follow_Strategy")
            followdatalist = [json.loads(i) for i in followdatalist]
            if not followdatalist:
                time.sleep(2)
            else:
                T = []
                for followdata in followdatalist:
                    t = Thread(target=run_follow, args=(followdata,))
                    T.append(t)
                    t.start()
                for t in T:
                    t.join()
                time.sleep(0.5)
        except Exception as e:
            info = "行情自动跟随策略报错{}".format(e)
            print(info)


if __name__ == '__main__':
    goDoFollowStrategy()


"""{
  "strategyId": 1606897069356,
  "userUuid": "398051ac70ef4da9aafd33ce0b95195f",
  "userId":   null,
  "apiAccountId1":   10153,
  "apiAccountId2":   10152,
  "platform": "bitget",
  "symbol": "matic_usdt",
  "amount_min":   0.1,
  "amount_max":   1,
  "order_amount":   2,
  "allow_diffprice":   10,
  "status":   1
}"""

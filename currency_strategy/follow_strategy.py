# encoding=utf-8
# 循环按火币最新价挂单撤单
import json
import random
import sys
import time
from threading import Thread
sys.path.append("..")
from tools.Config import amountlimit, befinx_self_symbol, pricelimit
from tools.databasePool import r1
from tools.get_market_info import get_okex_price_and_side, get_currentprice0, get_currentprice1, get_T8ex_orders
from tools.handle import buy, sell, cancel


def cancel_all(userUuid, apiAccountId1, apiAccountId2, strategyId, platform, symbol, orderId1, orderId2):
    T = []
    t1 = Thread(target=cancel, args=(userUuid, apiAccountId1, strategyId, platform, symbol, orderId1))
    t2 = Thread(target=cancel, args=(userUuid, apiAccountId2, strategyId, platform, symbol, orderId2))
    T.append(t1)
    T.append(t2)
    for t in T:
        t.start()
    for t in T:
        t.join()


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
            if symbol not in befinx_self_symbol:
                if currentprice == 0:
                    currentprice = get_currentprice1("huobi", symbol)
                    currentprice = round(currentprice, pricelimit[symbol][platform])
                    side = random.choice(["buy", "sell"])
                print(symbol, currentprice, side)
            if symbol == "btc_sgdt":
                currentprice, side = get_okex_price_and_side("btc_usdt")
                if currentprice == 0:
                    currentprice = get_currentprice1("binance", "btc_usdt")
                    side = random.choice(["buy", "sell"])
                currentprice = round(currentprice * 1.424, 2)
                print(symbol, currentprice, side)
            if symbol == "eth_sgdt":
                currentprice, side = get_okex_price_and_side("eth_usdt")
                if currentprice == 0:
                    currentprice = get_currentprice1("binance", "eth_usdt")
                    side = random.choice(["buy", "sell"])
                currentprice = round(currentprice * 1.424, 2)
                print(symbol, currentprice, side)
            if symbol == "gech_sgdt":
                # 以xrp_usdt为参照
                currentprice, side = get_okex_price_and_side("xrp_usdt")
                if currentprice == 0:
                    currentprice = round(get_currentprice1("binance", "xrp_usdt"), pricelimit[symbol][platform])
                    side = random.choice(["buy", "sell"])
                print(symbol, currentprice, side)
            if currentprice != 0:
                currentprice1 = get_currentprice0(platform, symbol)  # T8当前价
                print("{}-{}-最新价{}".format(platform, symbol, currentprice1))
                if currentprice1 != 0:
                    # 如果huobi比bitget价格大于0.2%(根据卖盘下买单，将价格拉至与huobi基本持平),此处后期可加入对冲逻辑
                    if currentprice * 0.1 > currentprice - currentprice1 >= currentprice * 0.002:
                        print("{}-huobi比T8ex价格大{}".format(symbol, currentprice - currentprice1))
                        res = get_T8ex_orders(symbol)
                        asks = res["data"]["ask"]
                        if asks:
                            buyamount = round(sum([i["amount"] for i in asks if i["price"] <= currentprice]),
                                              amountlimit[symbol][platform])
                            if buyamount == 0:
                                buyamount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
                            if symbol == "shib_usdt":
                                buyamount = int(buyamount*0.01)
                            buyprice = currentprice
                            buyorderid = buy(userUuid, buyId, strategyId, platform, symbol, buyamount, buyprice)
                            if buyorderid:
                                time.sleep(2)
                                cancel(userUuid, buyId, strategyId, platform, symbol, buyorderid)
                    # 如果huobi比bitget价格小于0.2%(根据买盘下卖单，将价格拉至于huobi基本持平),此处后期可加入对冲逻辑
                    if currentprice * 0.1 > currentprice1 - currentprice >= currentprice * 0.002:
                        print("{}-T8ex比huobi价格大{}".format(symbol, currentprice1 - currentprice))
                        res = get_T8ex_orders(symbol)
                        bids = res["data"]["bid"]
                        if bids:
                            sellamount = round(sum([i["amount"] for i in bids if i["price"] >= currentprice]),
                                               amountlimit[symbol][platform])
                            if sellamount == 0:
                                sellamount = round(random.uniform(amount_min, amount_max),
                                                   amountlimit[symbol][platform])
                            if symbol == "shib_usdt":
                                sellamount = int(sellamount*0.01)
                            sellprice = currentprice
                            print('用户{}子账户{}下溢价卖单数量{},下单价格{}'.format(userUuid, sellId, sellamount, sellprice))
                            sellorderid = sell(userUuid, sellId, strategyId, platform, symbol, sellamount, sellprice)
                            if sellorderid:
                                time.sleep(2)
                                cancel(userUuid, sellId, strategyId, platform, symbol, sellorderid)
                # 判断完上面情行，继续刷单
                amount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
                if symbol == "shib_usdt":
                    amount = int(amount)
                if side == "sell":
                    sell(userUuid, sellId, strategyId, platform, symbol, amount, currentprice)
                    buy(userUuid, buyId, strategyId, platform, symbol, amount, currentprice)
                elif side == "buy":
                    buy(userUuid, buyId, strategyId, platform, symbol, amount, currentprice)
                    sell(userUuid, sellId, strategyId, platform, symbol, amount, currentprice)
    except Exception as e:
        print(e)
    finally:
        time.sleep(2)


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
                    # symbol = followdata['symbol']
                    # if symbol not in ["btc_usdt"]:
                    #     continue
                    t = Thread(target=run_follow, args=(followdata,))
                    T.append(t)
                    t.start()
                for t in T:
                    t.join()
                time.sleep(5)
        except Exception as e:
            info = "行情自动跟随策略报错{}".format(e)
            print(info)


if __name__ == '__main__':
    goDoFollowStrategy()

"""{
  "strategyId": 80,
  "userUuid": "68",
  "userId":   null,
  "apiAccountId1":   10187,
  "apiAccountId2":   10188,
  "platform": "bitget",
  "symbol": "matic_usdt",
  "amount_min":   0.1,
  "amount_max":   1,
  "order_amount":   2,
  "allow_diffprice": 0.002,
  "status":   1
}"""

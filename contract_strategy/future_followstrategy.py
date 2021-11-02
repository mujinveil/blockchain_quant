# encoding=utf-8
# 循环按火币最新价挂单撤单
import json
import random
import sys
import time
sys.path.append("..")
from threading import Thread
from tools.databasePool import r6
from tools.future_handle import buy, sell, cancel
from tools.get_future_market_info import get_perpetualprice, get_T8ex_contract_orders


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
    apiAccountIdlist = [apiAccountId1, apiAccountId2]  # apiAccountId1 用于做多，apiAccountId2用于做空
    leverate = followdata['leverate']  # 杠杆倍数
    try:
        if status == 1:  # 策略开启
            """方案一，对比买卖现价"""
            # buyId = random.choice(apiAccountIdlist)  # 随机选一个id
            # sellId = [i for i in apiAccountIdlist if i != buyId][0]  # 非buyid 中的id
            buyId = apiAccountId1
            sellId = apiAccountId2
            currentprice, side = 0, 1
            if currentprice == 0:
                currentprice = get_perpetualprice("huobi", symbol)
                side = random.choice(["buy", "sell"])
            print(symbol, currentprice, side)
            if currentprice != 0:
                currentprice1 = get_perpetualprice(platform, symbol)
                print("{}-{}-最新价{}".format(platform, symbol, currentprice1))
                if currentprice1 != 0:
                    # 如果huobi比bitget价格大于0.2%(根据卖盘下买单，将价格拉至与huobi基本持平),此处后期可加入对冲逻辑
                    if currentprice - currentprice1 >= currentprice * 0.002:
                        print("{}-huobi比T8ex价格大{}".format(symbol, currentprice - currentprice1))
                        res = get_T8ex_contract_orders(symbol)
                        asks = res["data"]["ask"]
                        if asks:
                            buyamount = int(sum([i["amount"] for i in asks if i["price"] <= currentprice]) * 1)
                            if buyamount == 0:
                                buyamount = random.randint(amount_min, amount_max)
                            buyprice = currentprice
                            if buyId == apiAccountId1:
                                buyorderid = buy(userUuid, buyId, symbol, platform, buyamount, buyprice, 1, 1, 1,
                                                 leverate)
                            else:
                                buyorderid = buy(userUuid, buyId, symbol, platform, buyamount, buyprice, 1, 1, 2,
                                                 leverate)
                            if buyorderid:
                                time.sleep(2)
                                cancel(userUuid, buyId, platform, buyorderid)
                    # 如果huobi比bitget价格小于0.2%(根据买盘下卖单，将价格拉至于huobi基本持平),此处后期可加入对冲逻辑
                    if currentprice1 - currentprice >= currentprice * 0.002:
                        print("{}-T8ex比huobi价格大{}".format(symbol, currentprice1 - currentprice))
                        res = get_T8ex_contract_orders(symbol)
                        print(res)
                        bids = res["data"]["bid"]
                        if bids:
                            sellamount = int(sum([i["amount"] for i in bids if i["price"] >= currentprice]) * 1)
                            if sellamount == 0:
                                sellamount = random.randint(amount_min, amount_max)
                            sellprice = currentprice
                            print('用户{}子账户{}下溢价卖单数量{},下单价格{}'.format(userUuid, sellId, sellamount, sellprice))
                            if sellId == apiAccountId2:
                                sellorderid = sell(userUuid, sellId, symbol, platform, sellamount, sellprice, 2, 1, 3,
                                                   leverate)
                            else:
                                sellorderid = sell(userUuid, sellId, symbol, platform, sellamount, sellprice, 2, 1, 4,
                                                   leverate)
                            if sellorderid:
                                time.sleep(2)
                                cancel(userUuid, sellId, platform, sellorderid)
                # 判断完上面情行，继续刷单
                amount = random.randint(amount_min, amount_max)
                if side == "sell":
                    if sellId == apiAccountId2:
                        sell(userUuid, sellId, symbol, platform, amount, currentprice, 2, 1, 3, leverate)
                    else:
                        sell(userUuid, sellId, symbol, platform, amount, currentprice, 2, 1, 4, leverate)
                    if buyId == apiAccountId1:
                        buy(userUuid, buyId, symbol, platform, amount, currentprice, 1, 1, 1, leverate)
                    else:
                        buy(userUuid, buyId, symbol, platform, amount, currentprice, 1, 1, 2, leverate)
                elif side == "buy":
                    if buyId == apiAccountId1:
                        buy(userUuid, buyId, symbol, platform, amount, currentprice, 1, 1, 1, leverate)
                    else:
                        buy(userUuid, buyId, symbol, platform, amount, currentprice, 1, 1, 2, leverate)
                    if sellId == apiAccountId2:
                        sell(userUuid, sellId, symbol, platform, amount, currentprice, 2, 1, 3, leverate)
                    else:
                        sell(userUuid, sellId, symbol, platform, amount, currentprice, 2, 1, 4, leverate)
    except Exception as e:
        print(e)
    finally:
        time.sleep(1)


def goDoFollowStrategy():
    while True:
        try:
            followdatalist = r6.hvals("Future_Follow_Strategy")
            followdatalist = [json.loads(i) for i in followdatalist]
            if not followdatalist:
                time.sleep(2)
            else:
                T = []
                for followdata in followdatalist:
                    symbol = followdata['symbol']
                    # if symbol not in ["link", "eth", "ltc","doge","fil"]:
                    if symbol != "eth":
                        continue
                    t = Thread(target=run_follow, args=(followdata,))
                    T.append(t)
                    t.start()
                for t in T:
                    t.join()
                time.sleep(2)
        except Exception as e:
            info = "行情自动跟随策略报错{}".format(e)
            print(info)


if __name__ == "__main__":
    goDoFollowStrategy()

# encoding=utf-8
# 循环按照自己定的价格区间挂单撤单
import json
import random
import sys
import time
from threading import Thread
sys.path.append("..")
from tools.Config import amountlimit, pricelimit
from tools.databasePool import r1
from tools.get_market_info import get_currentprice0, get_T8ex_orders, get_currentprice1
from tools.handle import buy, sell, cancel


def run_pricing(pricingdata):
    userUuid = pricingdata["userUuid"]  # 获取用户唯一id
    strategyId = pricingdata["strategyId"]  # 获取策略id
    status = pricingdata["status"]  # 获取状态1开启，0关闭，3手动停止
    apiAccountId1 = pricingdata["apiAccountId1"]  # API id 1
    apiAccountId2 = pricingdata["apiAccountId2"]  # API id 2
    platform = pricingdata["platform"]  # 交易所
    symbol = pricingdata["symbol"]  # 交易对
    amount_min = pricingdata["amount_min"]  # 每笔最小交易量
    amount_max = pricingdata["amount_max"]  # 每笔最大交易量
    apiAccountIdlist = [apiAccountId1, apiAccountId2]
    try:
        if status == 1:  # 策略开启
            """方案一，对比买卖现价"""
            buyId = random.choice(apiAccountIdlist)  # 随机选一个id
            sellId = [i for i in apiAccountIdlist if i != buyId][0]  # 非buyid 中的id
            currentprice, side = 0, 1
            if symbol == 'dec_usdt':
                currentprice = random.uniform(0.999, 1.001)
                currentprice = round(currentprice, pricelimit[symbol][platform])
                side = random.choice(["buy", "sell"])
                print(symbol, currentprice, side)
            if symbol == "ds_dec":
                # 依据月份与日期定价，前半月价格在0-1%之间波动，后半月价格在1%-1.3%波动
                now_time = time.localtime()
                month = now_time.tm_mon
                day = now_time.tm_mday
                delta_month = int(month - 9)
                if day < 27:  # 前半月
                    # 获取比特币当日的开盘价与当前价
                    btc_close = get_currentprice1("huobi","btc_usdt")
                    if not btc_close:
                        return
                    increase = btc_close/47000
                    currentprice = 2 * (1.012 ** delta_month) * increase
                    currentprice = round(currentprice, pricelimit[symbol][platform])
                    side = random.choice(["buy", "sell"])
                    print(symbol, currentprice, side)
                elif day >= 28:
                    increase = random.uniform(1.01, 1.013)
                    currentprice = 2 * (1.012 ** delta_month) * increase
                    currentprice = round(currentprice, pricelimit[symbol][platform])
                    side = random.choice(["buy", "sell"])
                    print(symbol, currentprice, side)
            if currentprice != 0:
                currentprice1 = get_currentprice0(platform, symbol)  # T8当前价
                print("{}-{}-最新价{}".format(platform, symbol, currentprice1))
                if currentprice1 != 0:
                    # 如果huobi比T8价格大于0.1%(根据卖盘下买单，将价格拉至与huobi基本持平),此处后期可加入对冲逻辑
                    if currentprice - currentprice1 >= currentprice * 0.001:
                        print("{}-huobi比T8ex价格大{}".format(symbol, currentprice - currentprice1))
                        res = get_T8ex_orders(symbol)
                        asks = res["data"]["ask"]
                        if asks:
                            buyamount = round(sum([i["amount"] for i in asks if i["price"] <= currentprice]),
                                              amountlimit[symbol][platform])
                            if buyamount == 0:
                                buyamount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
                            buyprice = currentprice
                            buyorderid = buy(userUuid, buyId, strategyId, platform, symbol, buyamount, buyprice)
                            if buyorderid:
                                time.sleep(2)
                                cancel(userUuid, buyId, strategyId, platform, symbol, buyorderid)
                    # 如果huobi比T8价格小于0.1%(根据买盘下卖单，将价格拉至于huobi基本持平),此处后期可加入对冲逻辑
                    if currentprice1 - currentprice >= currentprice * 0.001:
                        print("{}-T8ex比huobi价格大{}".format(symbol, currentprice1 - currentprice))
                        res = get_T8ex_orders(symbol)
                        bids = res["data"]["bid"]
                        if bids:
                            sellamount = round(sum([i["amount"] for i in bids if i["price"] >= currentprice]),
                                               amountlimit[symbol][platform])
                            if sellamount == 0:
                                sellamount = round(random.uniform(amount_min, amount_max),
                                                   amountlimit[symbol][platform])
                            sellprice = currentprice
                            print('用户{}子账户{}下溢价卖单数量{},下单价格{}'.format(userUuid, sellId, sellamount, sellprice))
                            sellorderid = sell(userUuid, sellId, strategyId, platform, symbol, sellamount, sellprice)
                            if sellorderid:
                                time.sleep(2)
                                cancel(userUuid, sellId, strategyId, platform, symbol, sellorderid)
            # 判断完上面情行，继续刷单
            amount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
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


def goDoPricingStrategy():
    while True:
        try:
            pricingdatalist = r1.hvals("Pricing_Strategy")
            pricingdatalist = [json.loads(i) for i in pricingdatalist]
            if not pricingdatalist:
                time.sleep(2)
            else:
                T = []
                for pricingdata in pricingdatalist:
                    symbol = pricingdata['symbol']
                    if symbol not in ["ds_dec","dec_usdt"]:
                        continue
                    t = Thread(target=run_pricing, args=(pricingdata,))
                    T.append(t)
                    t.start()
                for t in T:
                    t.join()
                time.sleep(5)
        except Exception as e:
            info = "自有币种定价策略报错{}".format(e)
            print(info)


if __name__ == '__main__':
    goDoPricingStrategy()


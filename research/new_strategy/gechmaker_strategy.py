from tools.get_market_info import get_currentprice0
import random, time, json
from threading import Thread
from tools.databasePool import r1
from tools.Config import amountlimit, pricelimit
from market_maker.bitget_marketmaker import buy, sell, query


def trade(marketmakerdata, num):
    try:
        userUuid = marketmakerdata["userUuid"]  # 获取用户唯一id
        strategyId = marketmakerdata["strategyId"]  # 获取策略id
        status = marketmakerdata["status"]  # 获取状态1开启，0关闭，3手动停止
        apiAccountId = marketmakerdata["apiAccountId"]  # api——id
        platform = marketmakerdata["platform"]  # 交易所
        symbol = marketmakerdata["symbol"]  # 交易对
        amount_min = marketmakerdata["amount_min"]  # 每笔最小交易量
        amount_max = marketmakerdata["amount_max"]  # 每笔最大交易量
        initial_price = marketmakerdata["initial_price"]  # 初始价格
        diffprice_min = marketmakerdata["diffprice_min"]  # 最小价差
        diffprice_max = marketmakerdata["diffprice_max"]  # 最大价差
        range = marketmakerdata["range"]  # 下跌幅度如5%
        lastprice = get_currentprice0(platform, symbol)
        price_change = (lastprice - initial_price) / initial_price
        if price_change >= range:
            amount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
            price = round(lastprice + random.uniform(diffprice_min, diffprice_max),
                          pricelimit[symbol][platform])
            buyorderid = buy(userUuid, apiAccountId, strategyId, platform, symbol, amount, price)
            time.sleep(0.2)
            # 查询订单，如果未成交，则补齐买单
            numberDeal = query(userUuid, apiAccountId, strategyId, platform, symbol, buyorderid)
            if numberDeal < amount:
                buyamount = round(amount - numberDeal, amountlimit[symbol][platform])
                buyorderid = buy(userUuid, apiAccountId, strategyId, platform, symbol, buyamount, price)
            marketmakerdata['initial_price'] = lastprice
            r1.hset("GechMaker_Strategy", strategyId, json.dumps(marketmakerdata))
        elif price_change <= -1 * range:
            amount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
            price = round(lastprice - random.uniform(diffprice_min, diffprice_max),
                          pricelimit[symbol][platform])
            sellorderid = sell(userUuid, apiAccountId, strategyId, platform, symbol, amount, price)
            # 查询订单，如果未成交，则补齐卖单
            time.sleep(0.2)
            numberDeal = query(userUuid, apiAccountId, strategyId, platform, symbol, sellorderid)
            if numberDeal < amount:
                sellamount = round(amount - numberDeal, amountlimit[symbol][platform])
                sellorderid = buy(userUuid, apiAccountId, strategyId, platform, symbol, sellamount, price)
            marketmakerdata['initial_price'] = lastprice
            r1.hset("GechMaker_Strategy", strategyId, json.dumps(marketmakerdata))
        else:
            if num % 60 == 0:
                sidelist = ['sell', 'sell', 'sell', 'buy', 'buy']
                side = random.choice(sidelist)
                if side == 'sell':
                    amount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
                    price = round(lastprice - random.uniform(diffprice_min, diffprice_max),
                                  pricelimit[symbol][platform])
                    sellorderid = sell(userUuid, apiAccountId, strategyId, platform, symbol, amount, price)
                    # 查询订单，如果未成交，则下对应的买单
                    time.sleep(0.2)
                    numberDeal = query(userUuid, apiAccountId, strategyId, platform, symbol, sellorderid)
                    if numberDeal < amount:
                        sellamount = round(amount - numberDeal, amountlimit[symbol][platform])
                        sellorderid = buy(userUuid, apiAccountId, strategyId, platform, symbol, sellamount, price)
                elif side == 'buy':
                    amount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
                    price = round(lastprice + random.uniform(diffprice_min, diffprice_max),
                                  pricelimit[symbol][platform])
                    buyorderid = buy(userUuid, apiAccountId, strategyId, platform, symbol, amount, price)
                    time.sleep(0.2)
                    numberDeal = query(userUuid, apiAccountId, strategyId, platform, symbol, buyorderid)
                    if numberDeal < amount:
                        buyamount = round(amount - numberDeal, amountlimit[symbol][platform])
                        buyorderid = buy(userUuid, apiAccountId, strategyId, platform, symbol, buyamount, price)
    except:
        pass
    finally:
        time.sleep(2)


def goGechMakerStrategy():
    num = 1
    while True:
        try:
            marketmakerdatalist = r1.hvals("GechMaker_Strategy")
            marketmakerdatalist = [json.loads(i) for i in marketmakerdatalist]
            if marketmakerdatalist == []:
                time.sleep(2)
            else:
                T = []
                for marketmakerdata in marketmakerdatalist:
                    t = Thread(target=trade(), args=(marketmakerdata))
                    T.append(t)
                    t.start()
                for t in T:
                    t.join()
        except Exception as e:
            info = "Gech策略报错{}".format(e)
            print(info)
        finally:
            num += 1


if __name__ == "__main__":
    # goGechMakerStrategy()
    symbol = "gech_busd"
    #get_currentprice0('befinx', symbol)
    userUuid = "6e7c88272f554956a35d8ed2cf833201"
    strategyId = '10056'
    apiAccountId = '10156'
    platform = 'befinx'
    buyamount = 0.01
    price = 4.3
    buy(userUuid, apiAccountId, strategyId, platform, symbol, buyamount, price)

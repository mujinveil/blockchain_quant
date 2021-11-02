# encoding=utf-8

# 循环按火币最新价挂单撤单
import json
import random
import time
from threading import Thread
from tools.Config import amountlimit, pricelimit
from tools.databasePool import r1
from tools.handle import buy, sell, query
from tools.get_market_info import get_currentprice0


def run_marketmaker(marketmakerdata, num):
    try:
        type = marketmakerdata["type"]  # 1区间震荡（逻辑与行情跟随相同）2拉升3回调4震荡上行5震荡下行
        if type == 1:
            status = marketmakerdata["status"]  # 获取状态1开启，0关闭，2暂停，3手动停止
            if status == 1:
                userUuid = marketmakerdata["userUuid"]  # 获取用户唯一id
                strategyId = marketmakerdata["strategyId"]  # 获取策略id
                apiAccountId = marketmakerdata["apiAccountId"]  # api——id
                platform = marketmakerdata["platform"]  # 交易所
                symbol = marketmakerdata["symbol"]  # 交易对
                tradeCoin = symbol.split("_")[0]  # 交易币
                valueCoin = symbol.split("_")[1]  # 计价币
                amount_min = marketmakerdata["amount_min"]  # 每笔最小交易量
                amount_max = marketmakerdata["amount_max"]  # 每笔最大交易量
                starttime = marketmakerdata["starttime"]  # 开启时间精确到s
                spendtime = marketmakerdata["spendtime"]  # 运行时间
                initial_price = marketmakerdata["initial_price"]  # 初始价格
                sidelist = marketmakerdata["sidelist"]  # 控制买卖单次数
                sideflag = marketmakerdata["sideflag"]  # 标记1主买，2主卖
                diffprice_min = marketmakerdata["diffprice_min"]  # 最小价差
                diffprice_max = marketmakerdata["diffprice_max"]  # 最大价差
                range = marketmakerdata["range"]  # 上下波动范围，如-5% - 5%
                time_space = marketmakerdata["time_space"]  # 买卖方向切换时间间距
                if num % time_space == 0:
                    print("num为{}，time_space为{}，切换买卖概率".format(num, time_space))
                    if sideflag == 1:
                        marketmakerdata["sidelist"] = ["sell", "sell", "sell", "buy", "buy"]
                        marketmakerdata["sideflag"] = 2
                        marketmakerdata["time_space"] = random.choice([60, 300, 900])
                        r1.hset("MaketMaker_Strategy", strategyId, json.dumps(marketmakerdata))
                    elif sideflag == 2:
                        marketmakerdata["sidelist"] = ["sell", "sell", "buy", "buy", "buy"]
                        marketmakerdata["sideflag"] = 1
                        marketmakerdata["time_space"] = random.choice([120, 600, 1800])
                        r1.hset("MaketMaker_Strategy", strategyId, json.dumps(marketmakerdata))
                # 对比当前价与初始价的涨跌幅度
                lastprice = get_currentprice0(platform, symbol)
                if lastprice != 0:
                    current_range = (lastprice - initial_price) / initial_price
                    print("********最新价", lastprice, "涨跌幅", current_range)
                    # 如果当前涨跌幅大于预设值，则下卖单，降低成交价
                    if current_range > range:
                        print("如果当前涨跌幅大于预设值，则下卖单，降低成交价")
                        if sideflag == 1:
                            marketmakerdata["sidelist"] = ["sell", "sell", "sell", "buy", "buy"]
                            marketmakerdata["sideflag"] = 2
                            r1.hset("MaketMaker_Strategy", strategyId, json.dumps(marketmakerdata))
                    # 如果当前涨跌幅小于预设值，则下买单，拉高成交价
                    elif current_range < -1 * range:
                        print("如果当前涨跌幅小于预设值，则下买单，拉高成交价")
                        if sideflag == 2:
                            marketmakerdata["sidelist"] = ["sell", "sell", "buy", "buy", "buy"]
                            marketmakerdata["sideflag"] = 1
                            r1.hset("MaketMaker_Strategy", strategyId, json.dumps(marketmakerdata))
                    # 如果涨跌幅位于区间值，
                    side = random.choice(sidelist)
                    print("------", side)
                    if side == "buy":
                        amount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
                        price = round(lastprice + random.uniform(diffprice_min, diffprice_max),
                                      pricelimit[symbol][platform])
                        buyorderid = buy(userUuid, apiAccountId, strategyId, platform, symbol, amount, price)
                        # 查询订单，如果未成交，则下对应的卖单
                        time.sleep(0.2)
                        numberDeal = query(userUuid, apiAccountId, strategyId, platform, symbol, buyorderid)
                        if numberDeal < amount:
                            sellamount = round(amount - numberDeal, amountlimit[symbol][platform])
                            sellorderid = sell(userUuid, apiAccountId, strategyId, platform, symbol, sellamount, price)
                    if side == "sell":
                        amount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
                        price = round(lastprice - random.uniform(diffprice_min, diffprice_max),
                                      pricelimit[symbol][platform])
                        sellorderid = sell(userUuid, apiAccountId, strategyId, platform, symbol, amount, price)
                        # 查询订单，如果未成交，则下对应的买单
                        time.sleep(0.2)
                        numberDeal = query(userUuid, apiAccountId, strategyId, platform, symbol, sellorderid)
                        if numberDeal < amount:
                            buyamount = round(amount - numberDeal, amountlimit[symbol][platform])
                            buyorderid = buy(userUuid, apiAccountId, strategyId, platform, symbol, buyamount, price)
        elif type == 2:
            userUuid = marketmakerdata["userUuid"]  # 获取用户唯一id
            strategyId = marketmakerdata["strategyId"]  # 获取策略id
            status = marketmakerdata["status"]  # 获取状态1开启，0关闭，3手动停止
            apiAccountId = marketmakerdata["apiAccountId"]  # api——id
            platform = marketmakerdata["platform"]  # 交易所
            symbol = marketmakerdata["symbol"]  # 交易对
            tradeCoin = symbol.split("_")[0]  # 交易币
            valueCoin = symbol.split("_")[1]  # 计价币
            amount_min = marketmakerdata["amount_min"]  # 每笔最小交易量
            amount_max = marketmakerdata["amount_max"]  # 每笔最大交易量
            starttime = marketmakerdata["starttime"]  # 开启时间精确到s
            spendtime = marketmakerdata["spendtime"]  # 运行时间，单位s
            initial_price = marketmakerdata["initial_price"]  # 初始价格
            sidelist = marketmakerdata["sidelist"]  # 控制买卖单次数
            sideflag = marketmakerdata["sideflag"]  # 标记1主买，2主卖
            diffprice_min = marketmakerdata["diffprice_min"]  # 最小价差
            diffprice_max = marketmakerdata["diffprice_max"]  # 最大价差
            range = marketmakerdata["range"]  # 上涨幅度如5%
            lastprice = get_currentprice0(platform, symbol)
            if lastprice != 0:
                current_range = (lastprice - initial_price) / initial_price  # 对比当前价与初始价的涨跌幅度
                run_time = int(time.time()) - starttime
                print("**********最新价", lastprice, "涨跌幅", current_range)
                # 如果当前涨跌幅大于预设值，或者运行时间到了，则调用java停止接口，状态为“已完成”
                if current_range >= range or run_time >= spendtime:
                    print("当前涨幅{}，预设值{}，实际运行时间{}，预设时间{}".format(current_range, range, run_time, spendtime))
                    # 调用java停止接口（或者删掉此策略，改为震荡趋势运行）
                    # requests.get(stopMarketMarketStrategy_url, params={"strategyId": strategyId})
                    marketmakerdata["initial_price"] = lastprice
                    marketmakerdata["type"] = 1
                    marketmakerdata["sidelist"] = ["sell", "sell", "buy", "buy", "buy"]
                    marketmakerdata["sideflag"] = 1
                    marketmakerdata["time_space"] = 60
                    r1.hset("MaketMaker_Strategy", strategyId, json.dumps(marketmakerdata))
                else:
                    side = random.choice(sidelist)
                    print("----------", side)
                    if side == "buy":
                        amount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
                        price = round(lastprice + random.uniform(diffprice_min, diffprice_max),
                                      pricelimit[symbol][platform])
                        buyorderid = buy(userUuid, apiAccountId, strategyId, platform, symbol, amount, price)
                        # 查询订单，如果未成交，则下对应的卖单
                        time.sleep(0.2)
                        numberDeal = query(userUuid, apiAccountId, strategyId, platform, symbol, buyorderid)
                        if numberDeal < amount:
                            sellamount = round(amount - numberDeal, amountlimit[symbol][platform])
                            sellorderid = sell(userUuid, apiAccountId, strategyId, platform, symbol, sellamount,
                                               price)
                    if side == "sell":
                        amount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
                        price = round(lastprice - random.uniform(diffprice_min, diffprice_max),
                                      pricelimit[symbol][platform])
                        sellorderid = sell(userUuid, apiAccountId, strategyId, platform, symbol, amount, price)
                        # 查询订单，如果未成交，则下对应的买单
                        time.sleep(0.2)
                        numberDeal = query(userUuid, apiAccountId, strategyId, platform, symbol, sellorderid)
                        if numberDeal < amount:
                            buyamount = round(amount - numberDeal, amountlimit[symbol][platform])
                            buyorderid = buy(userUuid, apiAccountId, strategyId, platform, symbol, buyamount, price)
        elif type == 3:
            userUuid = marketmakerdata["userUuid"]  # 获取用户唯一id
            strategyId = marketmakerdata["strategyId"]  # 获取策略id
            status = marketmakerdata["status"]  # 获取状态1开启，0关闭，3手动停止
            apiAccountId = marketmakerdata["apiAccountId"]  # api——id
            platform = marketmakerdata["platform"]  # 交易所
            symbol = marketmakerdata["symbol"]  # 交易对
            tradeCoin = symbol.split("_")[0]  # 交易币
            valueCoin = symbol.split("_")[1]  # 计价币
            amount_min = marketmakerdata["amount_min"]  # 每笔最小交易量
            amount_max = marketmakerdata["amount_max"]  # 每笔最大交易量
            starttime = marketmakerdata["starttime"]  # 开启时间精确到s
            spendtime = marketmakerdata["spendtime"]  # 运行时间，单位s
            initial_price = marketmakerdata["initial_price"]  # 初始价格
            sidelist = marketmakerdata["sidelist"]  # 控制买卖单次数
            sideflag = marketmakerdata["sideflag"]  # 标记1主买，2主卖
            diffprice_min = marketmakerdata["diffprice_min"]  # 最小价差
            diffprice_max = marketmakerdata["diffprice_max"]  # 最大价差
            range = marketmakerdata["range"]  # 下跌幅度如5%
            lastprice = get_currentprice0(platform, symbol)
            if lastprice != 0:
                current_range = (lastprice - initial_price) / initial_price  # 对比当前价与初始价的涨跌幅度
                run_time = int(time.time()) - starttime
                print("**********最新价", lastprice, "涨跌幅", current_range)
                # 如果当前涨跌幅大于预设值，或者运行时间到了，则调用java停止接口，状态为“已完成”
                if current_range <= -1 * range or run_time >= spendtime:
                    print("当前跌幅{}，预设值{}，实际运行时间{}，预设时间{}".format(current_range, range, run_time, spendtime))
                    # 调用java停止接口（或者删掉此策略，改为震荡趋势运行）
                    # requests.get(stopMarketMarketStrategy_url, params={"strategyId": strategyId})
                    marketmakerdata["initial_price"] = lastprice
                    marketmakerdata["type"] = 1
                    marketmakerdata["sidelist"] = ["sell", "sell", "buy", "buy", "buy"]
                    marketmakerdata["sideflag"] = 1
                    marketmakerdata["time_space"] = 60
                    r1.hset("MaketMaker_Strategy", strategyId, json.dumps(marketmakerdata))
                else:
                    side = random.choice(sidelist)
                    print("------------", side)
                    if side == "buy":
                        amount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
                        price = round(lastprice + random.uniform(diffprice_min, diffprice_max),
                                      pricelimit[symbol][platform])
                        buyorderid = buy(userUuid, apiAccountId, strategyId, platform, symbol, amount, price)
                        # 查询订单，如果未成交，则下对应的卖单
                        time.sleep(0.2)
                        numberDeal = query(userUuid, apiAccountId, strategyId, platform, symbol, buyorderid)
                        if numberDeal < amount:
                            sellamount = round(amount - numberDeal, amountlimit[symbol][platform])
                            sellorderid = sell(userUuid, apiAccountId, strategyId, platform, symbol, sellamount,
                                               price)
                    if side == "sell":
                        amount = round(random.uniform(amount_min, amount_max), amountlimit[symbol][platform])
                        price = round(lastprice - random.uniform(diffprice_min, diffprice_max),
                                      pricelimit[symbol][platform])
                        sellorderid = sell(userUuid, apiAccountId, strategyId, platform, symbol, amount, price)
                        # 查询订单，如果未成交，则下对应的买单
                        time.sleep(0.2)
                        numberDeal = query(userUuid, apiAccountId, strategyId, platform, symbol, sellorderid)
                        if numberDeal < amount:
                            buyamount = round(amount - numberDeal, amountlimit[symbol][platform])
                            buyorderid = buy(userUuid, apiAccountId, strategyId, platform, symbol, buyamount, price)
    except Exception as e:
        print(e)
    finally:
        time.sleep(2)


def goDoMarketMakerStrategy():
    num = 1
    while True:
        try:
            marketmakerdatalist = r1.hvals("MaketMaker_Strategy")
            marketmakerdatalist = [json.loads(i) for i in marketmakerdatalist]
            print(marketmakerdatalist)
            if not marketmakerdatalist:
                time.sleep(2)
            else:
                T = []
                for marketmakerdata in marketmakerdatalist:
                    t = Thread(target=run_marketmaker, args=(marketmakerdata, num))
                    T.append(t)
                    t.start()
                for t in T:
                    t.join()
        except Exception as e:
            info = "做市策略报错{}".format(e)
            print(info)
        finally:
            num += 1


if __name__ == '__main__':
    goDoMarketMakerStrategy()

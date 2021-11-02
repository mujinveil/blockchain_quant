import time
import numpy as np
import requests, json, random
from threading import Thread
from loggerConfig import logger
from tools.Config import Trade_url, amountlimit, premiumdict, pricelimit, \
    updateCover_url
from tools.get_market_info import get_currentprice0
from tools.databasePool import r2, POOL

np.set_printoptions(suppress=True)  # 取消科学计数法


def sumProfit(strategyId):
    totalProfit = 0
    totalprofitRate = 0
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        cur.execute('select sum(profit) from coverlist where strategyId=%s and direction=2 and status=1', (strategyId,))
        total_profit = cur.fetchone()[0]
        cur.execute(
            'select sum(trade_price*trade_amount) from coverlist where strategyId=%s and direction=2 and status=1',
            (strategyId,))
        total_amount = cur.fetchone()[0]
        if total_profit:
            totalProfit = float(total_profit)
        if total_profit and total_amount:
            totalprofitRate = float(total_profit / total_amount)
        cur.close()
        conn.close()
    except Exception as e:
        logger.error('策略{}在查询利润时出错{}'.format(strategyId, e))
    print(totalProfit, totalprofitRate)
    return totalProfit, totalprofitRate


def traceLevel(strategydata):
    entry_price = strategydata['entry_price']
    coverRatio = strategydata['coverRatio'].split("-")
    strategyId = strategydata['strategyId']
    for index in range(len(coverRatio)):
        coverprice = entry_price * (1 - float(coverRatio[index]))
        label = {'covertraceprice': coverprice, 'stopprice': coverprice, 'coverprice': coverprice, 'touchtag': 0}
        r2.hset('coverlevel', '{0}-{1}'.format(strategyId, index), json.dumps(label))


# 首次进场，执行买单
def startbuy(strategydata):
    # 只在初次进场时买入，或在止盈后时再次买入
    if strategydata['flag'] == 1:
        return
    userUuid = strategydata["userUuid"]  # 用户id
    apiAccountId = strategydata["apiAccountId"]  # 用户子账户id
    strategyId = strategydata["strategyId"]  # 策略id
    platform = strategydata["platform"]  # 交易平台
    symbol = strategydata["symbol"]  # 交易对
    orderAmount = strategydata['orderAmount']  # 首单额度
    orderQuantities = strategydata['orderQuantities']  # 做单数量
    profitStopRatio = strategydata['profitStopRatio']  # 止盈比例
    callbackRatio = strategydata['callbackRatio']  # 止盈回调
    coverRatio = strategydata['coverRatio'].split('-')  # 补仓跌幅
    coverCallbackRatio = strategydata['coverCallbackRatio']  # 补仓回调比例
    i = '用户{}子账户{}开始新一轮补仓策略{}，设置的首单额度{}，做单数量{}，止盈比例{}，止盈回调比例{}，补仓跌幅{}，补仓回调比例{}'.format(userUuid, apiAccountId,
                                                                                       strategyId,
                                                                                       orderAmount, orderQuantities,
                                                                                       profitStopRatio,
                                                                                       callbackRatio, coverRatio,
                                                                                       coverCallbackRatio)
    print(i)
    logger.info(i)
    # 参数初始化
    strategydata['flag'] = 0
    strategydata['touchtag'] = 0
    strategydata['buy_value'] = 0
    strategydata['buy_num'] = 0
    strategydata["stopprice"] = 0
    strategydata['mostprice'] = 0
    current_price = get_currentprice0(platform, symbol)
    strategydata['entry_price'] = current_price
    conn = POOL.connection()
    cur = conn.cursor()

    buy_price = round(current_price + premiumdict[symbol] / 2, pricelimit[symbol][platform])
    # buy_price = round(current_price * 1.01, pricelimit[symbol][platform])
    amount = orderAmount / buy_price
    x, y = str(amount).split('.')
    amount = float(x + '.' + y[0:amountlimit[symbol][platform]])
    if amount == 0:
        return
    ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    tradeparams = {"direction": 1, "amount": amount, "symbol": symbol, "platform": platform, "price": buy_price,
                   "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11, "strategyId": strategyId,
                   'tradetype': 2}
    print("开始下买单，下单价格{}".format(buy_price))
    traderes = requests.post(Trade_url, data=tradeparams)
    trade_dict = json.loads(traderes.content.decode())
    orderId = trade_dict["response"]["orderid"]  # 获取订单id  #例如'E161786658748980'
    insertsql = "INSERT INTO coverlist(userUuid,apiAccountId,strategyId,platform,symbol,direction,orderid," \
                "order_amount,order_price,order_time,status,uniqueId,tradetype,coverlevel) VALUES(%s, %s, %s, %s, " \
                "%s,%s,%s, %s, %s, %s, %s,%s,%s,%s) "
    insertdata = (
        userUuid, apiAccountId, strategyId, platform, symbol, 1, orderId, amount, buy_price, ordertime, 0, 11, 2, 0)
    cur.execute(insertsql, insertdata)
    conn.commit()
    time.sleep(3)
    if choice:
        tradeprice = buy_price
        tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        buyfee = amount * tradeprice * 0.002
        # 设置补仓价格档位
        traceLevel(strategydata)
        strategydata['buy_value'] = amount * tradeprice
        strategydata['buy_num'] = amount
        strategydata['entry_price'] = tradeprice
        strategydata['flag'] = 1
        r2.hset('Cover_strategy', strategyId, json.dumps(strategydata))
        # 存入数据库
        updatesql = "update coverlist set trade_amount=%s,trade_price=%s,trade_time=%s,status=%s,fee=%s where " \
                    "strategyId=%s and orderid=%s "
        cur.execute(updatesql, (amount, tradeprice, tradetime, 1, buyfee, strategyId, orderId))
        params = {'strategyId': strategyId, 'averagePrice': tradeprice}
        res = requests.post(updateCover_url, data=params)
    else:
        cur.execute("update coverlist set status=2 where strategyId=%s and orderid=%s and status=0",
                    (strategyId, orderId))
    conn.commit()
    cur.close()
    conn.close()


def stopOut(strategyId, ):
    strategydata = json.loads(r2.hget('Cover_strategy', strategyId))
    if strategydata['flag'] == 0:
        return
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    symbol = strategydata['symbol']
    platform = strategydata["platform"]
    amount = strategydata['buy_num']
    currentprice = get_currentprice0(platform, symbol)
    if amount != 0:
        entry_price_avg = strategydata['buy_value'] / strategydata['buy_num']
    else:
        entry_price_avg = strategydata['entry_price']
    totalprofit = 0
    totalprofitRate = 0
    conn = POOL.connection()
    cur = conn.cursor()

    # 下卖单
    ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # 折价卖出
    sellprice = round(currentprice - premiumdict[symbol] / 2, pricelimit[symbol][platform])
    # sellprice = round(currentprice * 0.99, pricelimit[symbol][platform])
    print("开始强制卖出，卖出价格{}".format(sellprice))
    sellparams = {"direction": 2, "amount": amount, "symbol": symbol, "platform": platform, "price": sellprice,
                  "userUuid": userUuid, "apiAccountId": apiAccountId, "source": 11, "strategyId": strategyId,
                  'tradetype': 2}
    res = requests.post(Trade_url, data=sellparams)
    resdict = json.loads(res.content.decode())
    orderId = resdict["response"]["orderid"]  # 获取订单id
    sellinsertsql = "INSERT INTO coverlist(strategyId,userUuid,apiAccountId,platform,symbol,direction," \
                    "orderid,order_amount,order_price,order_time," \
                    "status,uniqueId,tradetype,coverlevel) VALUES(%s, %s, %s, %s, %s, %s,%s,%s,%s, %s, %s, " \
                    "%s,%s,%s) "
    cur.execute(sellinsertsql, (
        strategyId, userUuid, apiAccountId, platform, symbol, 2, orderId, amount, sellprice,
        ordertime, 0, 11, 2, 0))
    time.sleep(3)
    if 1:
        tradeprice = sellprice
        tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        sellfee = amount * tradeprice * 0.002
        profit = round((tradeprice - entry_price_avg - (tradeprice + entry_price_avg) * 0.002) * amount, 8)
        profitRate = round(profit / (entry_price_avg * amount), 8)
        updatesql = "update coverlist set trade_amount=%s,trade_price=%s,trade_time=%s,profit=%s,profitRate=%s," \
                    "status=%s,fee=%s where " \
                    "strategyId=%s and orderid=%s "
        cur.execute(updatesql,
                    (amount, tradeprice, tradetime, profit, profitRate, 1, sellfee, strategyId, orderId))
        conn.commit()
        totalprofit, totalprofitRate = sumProfit(strategyId)
        i = "用户{}补仓策略{}，买入均价{}，在价位{}时强制平仓，盈利{}，盈利率{}".format(userUuid, strategyId,
                                                             entry_price_avg,
                                                             currentprice,
                                                             totalprofit,
                                                             totalprofitRate)
        print(i)
        logger.info(i)
    conn.commit()
    cur.close()
    conn.close()
    return totalprofit, totalprofitRate


# 执行止盈卖单
def tracesell(strategyId):
    strategydata = json.loads(r2.hget('Cover_strategy', strategyId))
    if strategydata['flag'] == 0:
        return
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    symbol = strategydata['symbol']
    platform = strategydata["platform"]
    profitStopRatio = strategydata['profitStopRatio']
    callbackRatio = strategydata['callbackRatio']
    coverRatio = strategydata['coverRatio'].split('-')
    amount = strategydata['buy_num']
    currentprice = get_currentprice0(platform, symbol)
    print('用户{}策略{}开始追踪止盈，当前行情价{}'.format(userUuid, strategyId, currentprice))
    if amount != 0:
        entry_price_avg = strategydata['buy_value'] / strategydata['buy_num']
    else:
        entry_price_avg = strategydata['entry_price']
    if strategydata['touchtag'] == 1 and currentprice > strategydata['mostprice']:
        print("当前行情价{}更新最高价与止盈价".format(currentprice))
        strategydata['mostprice'] = currentprice
        strategydata['stopprice'] = currentprice * (1 - callbackRatio)
        r2.hset('Cover_strategy', strategyId, json.dumps(strategydata))
    if currentprice >= entry_price_avg * (1 + profitStopRatio) and (
            strategydata['touchtag'] == None or strategydata['touchtag'] == 0):
        print('当前行情价{}触发激活价，作标记'.format(currentprice))
        strategydata['touchtag'] = 1
        r2.hset('Cover_strategy', strategyId, json.dumps(strategydata))
    # 当价格触碰了激活价并回落到止盈价时
    if strategydata["stopprice"] is not None and strategydata["touchtag"] == 1 and strategydata[
        "stopprice"] >= currentprice > entry_price_avg:
        i = '用户{}策略{}当前价格{}触碰了激活价并回落到止盈价,开始止盈卖出'.format(userUuid, strategyId, currentprice)
        print(i)
        logger.info(i)
        conn = POOL.connection()
        cur = conn.cursor()
        # 下卖单
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # 折价卖出
        sellprice = round(currentprice - premiumdict[symbol] / 2, pricelimit[symbol][platform])
        # sellprice = round(currentprice * 0.99, pricelimit[symbol][platform])
        sellparams = {"direction": 2, "amount": amount, "symbol": symbol, "platform": platform, "price": sellprice,
                      "userUuid": userUuid, "apiAccountId": apiAccountId, "source": 11, "strategyId": strategyId,
                      'tradetype': 2}
        # print("开始下止盈卖单，下单价格{}".format(sellprice))
        res = requests.post(Trade_url, data=sellparams)
        resdict = json.loads(res.content.decode())
        orderId = resdict["response"]["orderid"]  # 获取订单id
        sellinsertsql = "INSERT INTO coverlist(strategyId,userUuid,apiAccountId,platform,symbol,direction," \
                        "orderid,order_amount,order_price,order_time," \
                        "status,uniqueId,tradetype,coverlevel) VALUES(%s, %s, %s, %s, %s, %s,%s,%s,%s, %s, %s, " \
                        "%s,%s,%s) "
        cur.execute(sellinsertsql, (
            strategyId, userUuid, apiAccountId, platform, symbol, 2, orderId, amount, sellprice,
            ordertime, 0, 11, 2, 0))
        conn.commit()
        time.sleep(3)
        if choice:
            tradeprice = sellprice
            tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            sellfee = amount * tradeprice * 0.002
            profit = round(((tradeprice - entry_price_avg) - (tradeprice + entry_price_avg) * 0.002) * amount, 8)
            profitRate = round(profit / (entry_price_avg * amount), 8)
            i = "用户{}新一轮补仓策略{}，基准价{}，价格涨到最高价{}后,又回调{}%至{},追踪止盈，盈利{}，盈利率{}".format(userUuid, strategyId,
                                                                                  entry_price_avg,
                                                                                  strategydata["mostprice"],
                                                                                  callbackRatio * 100,
                                                                                  strategydata["stopprice"], profit,
                                                                                  profitRate)
            print(i)
            logger.info(i)
            updatesql = "update coverlist set trade_amount=%s,trade_price=%s,trade_time=%s,profit=%s," \
                        "profitRate=%s,status=%s,fee=%s where " \
                        "strategyId=%s and orderid=%s "
            cur.execute(updatesql, (amount, tradeprice, tradetime, profit, profitRate, 1, sellfee, strategyId, orderId))
            strategydata['flag'] = 0
            r2.hset('Cover_strategy', strategyId, json.dumps(strategydata))
            for i in range(len(coverRatio)):
                r2.hdel('coverlevel', '{}-{}'.format(strategyId, i))
            totalprofit, totalprofitRate = sumProfit(strategyId)
            params = {'strategyId': strategyId, 'profit': totalprofit, 'profitRate': totalprofitRate}
            res = requests.post(updateCover_url, data=params)
        else:
            cur.execute("update coverlist set status=2 where strategyId=%s and orderid=%s and status=0",
                        (strategyId, orderId))
        conn.commit()
        cur.close()
        conn.close()


def tracebuy(strategydata, index):
    strategyId = strategydata['strategyId']
    cover_label = r2.hget('coverlevel', '{0}-{1}'.format(strategyId, index))
    if strategydata['flag'] == 0 or (not cover_label):
        return
    # label_next = r2.hget('coverlevel', '{0}-{1}'.format(strategyId, index + 1))
    # next_cover_price = json.loads(label_next)['coverprice'] if label_next else 0
    coverRatio = strategydata['coverRatio'].split("-")
    if index + 1 < len(coverRatio):
        next_cover_price = strategydata['entry_price'] * (1 - float(coverRatio[index + 1]))
    else:
        next_cover_price = 0
    cover_label = json.loads(cover_label)
    covercallbackratio = strategydata['coverCallbackRatio']
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    count = strategydata['orderAmount']
    symbol = strategydata['symbol']
    entry_price = strategydata['entry_price']
    platform = strategydata["platform"]
    currentprice = get_currentprice0(platform, symbol)
    print('用户{}策略{}开始追踪补仓，当前行情价{}'.format(userUuid, strategyId, currentprice))
    # 当价格在此档位区间，并触碰到了最低价
    if cover_label['covertraceprice'] > currentprice > next_cover_price:
        print('当前行情价{}更新最低价与抄底价'.format(currentprice))
        cover_label['covertraceprice'] = currentprice
        cover_label['stopprice'] = currentprice * (1 + covercallbackratio)
        r2.hset("coverlevel", '{0}-{1}'.format(strategyId, index), json.dumps(cover_label))
    # 当价格触碰到了激活价
    if currentprice <= cover_label['coverprice'] and (
            cover_label['touchtag'] == None or cover_label['touchtag'] == 0):
        print('当前行情价{}触发激活价，作标记'.format(currentprice))
        cover_label['touchtag'] = 1
        r2.hset('coverlevel', '{0}-{1}'.format(strategyId, index), json.dumps(cover_label))
    # 当价格触碰了激活价并回升到抄底价时
    if cover_label["stopprice"] is not None and cover_label["touchtag"] == 1 and cover_label[
        "stopprice"] <= currentprice < entry_price:
        conn = POOL.connection()
        cur = conn.cursor()
        i = '用户{}策略{}当前价格{}触碰了激活价并回升到抄底价'.format(userUuid, strategyId, currentprice)
        print(i)
        logger.info(i)
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # 溢价买入
        buyprice = round(currentprice + premiumdict[symbol] / 2, pricelimit[symbol][platform])
        # buyprice = round(currentprice * 1.01, pricelimit[symbol][platform])
        amount = count / buyprice
        x, y = str(amount).split('.')
        amount = float(x + '.' + y[0:amountlimit[symbol][platform]])
        buyparams = {"direction": 1, "amount": amount, "symbol": symbol, "platform": platform, "price": buyprice,
                     "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11, "strategyId": strategyId,
                     'tradetype': 2}
        print("开始下追踪补仓单，下单价格{}".format(buyprice))
        res = requests.post(Trade_url, data=buyparams)
        resdict = json.loads(res.content.decode())
        orderId = resdict["response"]["orderid"]  # 获取订单id
        insertsql = "INSERT INTO coverlist(userUuid,apiAccountId,strategyId,platform,symbol,direction,orderid," \
                    "order_amount,order_price,order_time,status,uniqueId,tradetype,coverlevel) VALUES(%s, %s, %s, " \
                    "%s, %s,%s,%s, %s, %s, %s," \
                    " %s,%s,%s,%s) "
        insertdata = (
            userUuid, apiAccountId, strategyId, platform, symbol, 1, orderId, amount, buyprice, ordertime, 0,
            11, 2, index + 1)
        cur.execute(insertsql, insertdata)
        conn.commit()
        time.sleep(3)
        if choice:
            tradeprice = buyprice
            tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            buyfee = tradeprice * amount * 0.002
            # 存入数据库
            updatesql = "update coverlist set trade_amount=%s,trade_price=%s,trade_time=%s,status=%s,fee=%s where " \
                        "strategyId=%s and orderid=%s"
            cur.execute(updatesql, (amount, tradeprice, tradetime, 1, buyfee, strategyId, orderId))
            strategydata['buy_value'] += amount * tradeprice
            strategydata['buy_num'] += amount
            r2.hset('Cover_strategy', strategyId, json.dumps(strategydata))
            r2.hdel('coverlevel', '{0}-{1}'.format(strategyId, index))
            # 补仓后修改买入均价
            params = {'strategyId': strategyId, 'averagePrice': strategydata['buy_value'] / strategydata['buy_num']}
            res = requests.post(updateCover_url, data=params)
        else:
            cur.execute("update coverlist set status=2 where strategyId=%s and orderid=%s and status=0",
                        (strategyId, orderId))
        conn.commit()
        cur.close()
        conn.close()


if __name__ == '__main__':
    successful_rate = [1, 1, 1, 1, 1, 0, 0, 0, 0, 0]
    while True:  # 需测试的情形 1.一个策略单个线程，2.一个策略多个线程，3.多个账户跑此策略
        try:
            choice = random.choice(successful_rate)
            strategy_list = r2.hvals("Cover_strategy")
            strategy_list = [json.loads(i) for i in strategy_list]
            T = []
            for strategy_info in strategy_list:
                T.append(Thread(target=startbuy, args=(strategy_info,)))
                for index in range(len(strategy_info['coverRatio'])):
                    T.append(Thread(target=tracebuy, args=(strategy_info, index,)))
                T.append(Thread(target=tracesell, args=(strategy_info['strategyId'],)))
            for t in T:
                t.start()
            for t in T:
                t.join()
                # startbuy(strategy_info)
                # for index in range(len(strategy_info['coverRatio'])):
                #     tracebuy(strategy_info, index)
                # tracesell(strategy_info['strategyId'])
        except Exception as e:
            print(e)
        finally:
            time.sleep(1)

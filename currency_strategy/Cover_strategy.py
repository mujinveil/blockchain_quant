import json
import time
from threading import Thread
import numpy as np
import requests
import sys
sys.path.append("..")
from loggerConfig import logger
from tools.Config import Queryorder_url, Trade_url, amountlimit, pricelimit, \
    updateCover_url, Cancel_url, Fee, Query_tradeprice_url
from tools.databasePool import r2, r5, POOL
from tools.get_market_info import get_currentprice1

np.set_printoptions(suppress=True)  # 取消科学计数法


def traceLevel(strategydata):
    entry_price = strategydata['entry_price']
    coverRatio = strategydata['coverRatio'].split("-")
    strategyId = strategydata['strategyId']
    for i in range(len(coverRatio)):
        coverprice = entry_price * (1 - float(coverRatio[i]))
        label = {'covertraceprice': coverprice, 'stopprice': coverprice, 'coverprice': coverprice, 'touchtag': 0}
        r2.hset('coverlevel', '{0}-{1}'.format(strategyId, i), json.dumps(label))


def sumProfit(userUuid, strategyId, init_amount):
    totalprofit = 0
    totalprofitRate = 0
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        cur.execute('select sum(profit) from coverlist where strategyId=%s and direction=2 and status=1',
                    (strategyId,))
        total_profit = cur.fetchone()[0]
        if total_profit:
            totalprofit = float(total_profit)
            totalprofitRate = round(totalprofit / init_amount, 8)
    except Exception as e:
        logger.error('用户{}策略{}在查询利润时出错{}'.format(userUuid, strategyId, e))
    finally:
        cur.close()
        conn.close()
        return totalprofit, totalprofitRate


def cancel_order(userUuid, apiAccountId, strategyId, platform, symbol, orderId, direction):
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        cancelbuyparams = {"direction": direction, "symbol": symbol, "platform": platform, "orderId": orderId,
                           "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11, "strategyId": strategyId}
        cancelres = requests.post(Cancel_url, data=cancelbuyparams)
        res = json.loads(cancelres.content.decode())
        if res["code"] == 1:
            cur.execute("update coverlist set status=2 where strategyId=%s and orderid=%s", (strategyId, orderId))
        else:
            i = "用户{}策略{}撤销{}平台订单出错,原因{}".format(userUuid, strategyId, platform, res['message'])
            print(i)
            logger.info(i)
    except Exception as e:
        i = "用户{}策略{}撤销{}平台订单出错{}".format(userUuid, strategyId, platform, e)
        print(i)
        logger.info(i)
    finally:
        conn.commit()
        cur.close()
        conn.close()


# 首次进场，执行买单
def startBuy(strategydata):
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
    current_price = get_currentprice1(platform, symbol)
    strategydata['entry_price'] = current_price
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        # 计算下单金额（溢价买入）
        # buy_price = round(current_price + premiumdict[symbol] / 2, pricelimit[symbol][platform])
        buy_price = round(current_price * 1.01, pricelimit[symbol][platform])
        amount = orderAmount / buy_price
        x, y = str(amount).split('.')
        amount = float(x + '.' + y[0:amountlimit[symbol][platform]])
        if amount == 0:
            return
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        tradeparams = {"direction": 1, "amount": amount, "symbol": symbol, "platform": platform, "price": buy_price,
                       "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11, "strategyId": strategyId,
                       'tradetype': 1}
        print(tradeparams)
        # 将下单参数传给跟单策略
        r5.hset('order_param_13', strategyId, json.dumps(tradeparams))
        i = '用户{}策略{}开始开仓，下买单，价格{}'.format(userUuid, strategyId, buy_price)
        print(i)
        logger.info(i)
        traderes = requests.post(Trade_url, data=tradeparams)
        trade_dict = json.loads(traderes.content.decode())
        print(trade_dict)
        orderId = trade_dict["response"]["orderid"]  # 获取订单id
        insertsql = "INSERT INTO coverlist(userUuid,apiAccountId,strategyId,platform,symbol,direction,orderid," \
                    "order_amount,order_price,order_time,status,uniqueId,tradetype,coverlevel) VALUES(%s, %s, %s, %s, " \
                    "%s,%s,%s, %s, %s, %s, %s,%s,%s,%s) "
        insertdata = (
            userUuid, apiAccountId, strategyId, platform, symbol, 1, orderId, amount, buy_price, ordertime, 0, 11, 1, 0)
        cur.execute(insertsql, insertdata)
        conn.commit()
        print("订单插入数据库")
        time.sleep(3)
        # 3秒后查询订单是否成交
        queryparams = {"direction": 1, "symbol": symbol, "platform": platform, "orderId": orderId,
                       "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11, "strategyId": strategyId}
        res = requests.post(Queryorder_url, data=queryparams)
        querydict = json.loads(res.content.decode())
        status = querydict['response']['status']
        if status == 'closed':
            numberDeal = round(float(querydict["response"]["numberDeal"]) * 0.998,8)
            queryparams1 = {"platform": platform, "symbol": symbol, "orderId": orderId, "apiId": apiAccountId,
                            "userUuid": userUuid, "strategyId": strategyId}
            res = requests.post(Query_tradeprice_url, data=queryparams1)
            queryresdict = json.loads(res.content.decode())
            fee = Fee[platform]['buyfee']
            try:
                tradeprice = queryresdict["response"]["avgPrice"]
                tradetime = queryresdict["response"]["createdDate"]
                buyfee = round(numberDeal * tradeprice * fee, 8)
            except:
                tradeprice = buy_price
                tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                buyfee = round(numberDeal * tradeprice * fee, 8)
            # 设置补仓价格档位
            traceLevel(strategydata)
            strategydata['buy_value'] = numberDeal * tradeprice
            strategydata['buy_num'] = numberDeal
            strategydata['entry_price'] = tradeprice
            strategydata['flag'] = 1
            r2.hset('Cover_strategy', strategyId, json.dumps(strategydata))
            # 存入数据库
            updatesql = "update coverlist set trade_amount=%s,trade_price=%s,trade_time=%s,status=%s,fee=%s where " \
                        "strategyId=%s and orderid=%s "
            cur.execute(updatesql, (numberDeal, tradeprice, tradetime, 1, buyfee, strategyId, orderId))
            conn.commit()
            print('订单成交插入数据库')
            params = {'strategyId': strategyId, 'averagePrice': tradeprice}
            res = requests.post(updateCover_url, data=params)
            resdict = json.loads(res.content.decode())
            print(resdict)
        elif status == 'open':
            cancel_order(userUuid, apiAccountId, strategyId, platform, symbol, orderId, 1)
    except Exception as e:
        logger.info("用户{}补仓策略{}进场下买单时出错{}".format(userUuid, strategyId, e))
        # 调用java停止策略接口
        updateparams = {'strategyId': strategyId, "status": 4}
        # res1 = requests.post(updateCover_url, data=updateparams)
        # print(json.loads(res1.content.decode()))
    finally:
        conn.commit()
        cur.close()
        conn.close()


# 强制清仓卖出
def stopOut(strategyId):
    strategydata = json.loads(r2.hget('Cover_strategy', strategyId))
    totalprofit = 0
    totalprofitRate = 0
    if strategydata['flag'] == 0:
        return totalprofit, totalprofitRate
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    symbol = strategydata['symbol']
    platform = strategydata["platform"]
    amount = round(strategydata['buy_num'],amountlimit[platform][symbol])
    coverRatio = strategydata['coverRatio'].split("-")
    orderAmount = strategydata['orderAmount']
    incrementalType = int(strategydata['incrementalType'])
    if incrementalType == 0:
        init_amount = orderAmount * (len(coverRatio) + 1)
    elif incrementalType == 1:
        startIndex = int(strategydata['startIndex'])
        marginMultiple = float(strategydata['marginMultiple'])
        init_amount = orderAmount * (startIndex - 1) + orderAmount * (
                1 - marginMultiple ** (len(coverRatio) + 2 - startIndex)) / (1 - marginMultiple)
    elif incrementalType == 2:
        startIndex = int(strategydata['startIndex'])
        increment = float(strategydata['increment'])
        init_amount = orderAmount * (len(coverRatio) + 1) + increment * (len(coverRatio) + 2 - startIndex) * (
                len(coverRatio) + 1 - startIndex) / 2
    currentprice = get_currentprice1(platform, symbol)
    if amount != 0:
        entry_price_avg = strategydata['buy_value'] / strategydata['buy_num']
    else:
        entry_price_avg = strategydata['entry_price']
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        # 下卖单
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # 折价卖出
        # sellprice = round(currentprice - premiumdict[symbol] / 2, pricelimit[symbol][platform])
        sellprice = round(currentprice * 0.99, pricelimit[symbol][platform])
        i = '用户{}策略{}开始强制卖出，价格{}'.format(userUuid, strategyId, sellprice)
        print(i)
        logger.info(i)
        sellparams = {"direction": 2, "amount": amount, "symbol": symbol, "platform": platform, "price": sellprice,
                      "userUuid": userUuid, "apiAccountId": apiAccountId, "source": 11, "strategyId": strategyId,

                      }
        print(sellparams)
        # 将下单参数传给跟单策略
        r5.hset('order_param_13', strategyId, json.dumps(sellparams))
        res = requests.post(Trade_url, data=sellparams)
        resdict = json.loads(res.content.decode())
        print(resdict)
        orderId = resdict["response"]["orderid"]  # 获取订单id
        sellinsertsql = "INSERT INTO coverlist(strategyId,userUuid,apiAccountId,platform,symbol,direction," \
                        "orderid,order_amount,order_price,order_time," \
                        "status,uniqueId,tradetype,coverlevel) VALUES(%s, %s, %s, %s, %s, %s,%s,%s,%s, %s, %s, " \
                        "%s,%s,%s) "
        cur.execute(sellinsertsql, (
            strategyId, userUuid, apiAccountId, platform, symbol, 2, orderId, amount, sellprice, ordertime, 0, 11, 1,0))
        conn.commit()
        print("强制卖出订单已插入数据库")
        time.sleep(3)
        # 3秒后查询卖单
        queryparams = {"direction": 2, "symbol": symbol, "platform": platform, "orderId": orderId,
                       "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11,
                       "strategyId": strategyId}
        queryres = requests.post(Queryorder_url, data=queryparams)
        querydict = json.loads(queryres.content.decode())
        print(querydict)
        status = querydict['response']['status']
        if status == 'closed':
            numberDeal = float(querydict["response"]["numberDeal"])
            fee = Fee[platform]['sellfee']
            queryparams = {"platform": platform, "symbol": symbol, "orderId": orderId, "apiId": apiAccountId,
                           "userUuid": userUuid}
            res = requests.post(Query_tradeprice_url, data=queryparams)
            queryresdict = json.loads(res.content.decode())
            try:
                tradeprice = queryresdict['response']['avgPrice']
                tradetime = queryresdict['response']['createdDate']
                sellfee = round(numberDeal * tradeprice * fee, 8)
            except:
                tradeprice = sellprice
                tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                sellfee = round(numberDeal * tradeprice * fee, 8)
            profit = round((tradeprice - entry_price_avg - (tradeprice + entry_price_avg) * fee) * numberDeal, 8)
            profitRate = round(profit / (entry_price_avg * numberDeal), 8)
            updatesql = "update coverlist set trade_amount=%s,trade_price=%s,trade_time=%s,profit=%s,profitRate=%s," \
                        "status=%s,fee=%s where " \
                        "strategyId=%s and orderid=%s "
            cur.execute(updatesql,
                        (numberDeal, tradeprice, tradetime, profit, profitRate, 1, sellfee, strategyId, orderId))
            conn.commit()
            print('强制卖出订单已成交，数据已插入数据库')
            totalprofit, totalprofitRate = sumProfit(userUuid, strategyId, init_amount)
            i = "用户{}补仓策略{}，买入均价{}，在价位{}时强制平仓，盈利{}，盈利率{}".format(userUuid, strategyId,
                                                                 entry_price_avg,
                                                                 currentprice,
                                                                 totalprofit,
                                                                 totalprofitRate)
            print(i)
            logger.info(i)
        elif status == 'open':
            cancel_order(userUuid, apiAccountId, strategyId, platform, symbol, orderId, 2)
    except Exception as e:
        i = "用户{}补仓策略{}清仓下卖单时出错{}".format(userUuid, strategyId, e)
        print(i)
        logger.error(i)
        # 调用java停止策略接口
        # updateparams = {'strategyId': strategyId, "status": 4}
        # res1 = requests.post(updateCover_url, data=updateparams)
    finally:
        # 从redis删除该策略缓存
        r2.hdel("Cover_strategy", strategyId)
        cover_ratio = strategydata['coverRatio'].split("-")
        for i in range(len(cover_ratio)):
            r2.hdel('coverlevel', '{}-{}'.format(strategyId, i))
        conn.commit()
        cur.close()
        conn.close()
        return totalprofit, totalprofitRate


# 执行止盈卖单
def traceSell(strategyId):
    strategydata = json.loads(r2.hget('Cover_strategy', strategyId))
    if strategydata['flag'] == 0:
        return
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    symbol = strategydata['symbol']
    platform = strategydata["platform"]
    profitStopRatio = strategydata['profitStopRatio']
    callbackRatio = strategydata['callbackRatio']
    amount = round(strategydata['buy_num'], amountlimit[symbol][platform])
    orderAmount = strategydata['orderAmount']
    coverRatio = strategydata['coverRatio'].split("-")
    incrementalType = int(strategydata['incrementalType'])
    if incrementalType == 0:
        init_amount = orderAmount * (len(coverRatio) + 1)
    elif incrementalType == 1:
        startIndex = int(strategydata['startIndex'])
        marginMultiple = float(strategydata['marginMultiple'])
        init_amount = orderAmount * (startIndex - 1) + orderAmount * (1 - marginMultiple ** (len(coverRatio) + 2 - startIndex)) / (1 - marginMultiple)
    elif incrementalType == 2:
        startIndex = int(strategydata['startIndex'])
        increment = float(strategydata['increment'])
        init_amount = orderAmount * (len(coverRatio) + 1) + increment * (len(coverRatio) + 2 - startIndex) * (len(coverRatio) + 1 - startIndex) / 2
    currentprice = get_currentprice1(platform, symbol)
    if not currentprice:
        return
    # print('用户{}策略{}开始追踪止盈，当前行情价{}'.format(userUuid, strategyId, currentprice))
    if amount != 0:
        entry_price_avg = strategydata['buy_value'] / strategydata['buy_num']
    else:
        entry_price_avg = strategydata['entry_price']
    if strategydata['touchtag'] == 1 and currentprice > strategydata['mostprice']:
        print("当前行情价{}更新最高价与止盈价".format(currentprice))
        strategydata['mostprice'] = currentprice
        strategydata['stopprice'] = currentprice * (1 - callbackRatio)
        r2.hset('Cover_strategy', strategyId, json.dumps(strategydata))
    print("追踪卖出,当前价{}".format(currentprice))
    if currentprice >= entry_price_avg * (1 + profitStopRatio) and strategydata['touchtag'] == 0:
        print('当前行情价{}触发激活价，作标记'.format(currentprice))
        strategydata['touchtag'] = 1
        r2.hset('Cover_strategy', strategyId, json.dumps(strategydata))
    # 当价格触碰了激活价并回落到止盈价时
    if strategydata["stopprice"] is not None and strategydata["touchtag"] == 1 and strategydata[
        "stopprice"] >= currentprice > entry_price_avg:
        conn = POOL.connection()
        cur = conn.cursor()
        try:
            # 下卖单
            ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # 折价卖出
            # sellprice = round(currentprice - premiumdict[symbol] / 2, pricelimit[symbol][platform])
            sellprice = round(currentprice * 0.99, pricelimit[symbol][platform])
            sellparams = {"direction": 2, "amount": amount, "symbol": symbol, "platform": platform, "price": sellprice,
                          "userUuid": userUuid, "apiAccountId": apiAccountId, "source": 11, "strategyId": strategyId,
                          'tradetype': 1}
            # 将下单参数传给跟单策略
            r5.hset('order_param_13', strategyId, json.dumps(sellparams))
            i = '用户{}策略{}当前价格{}触碰了激活价并回落到止盈价,开始止盈卖出,卖出价{}'.format(userUuid, strategyId, currentprice, sellprice)
            print(i)
            logger.info(i)
            res = requests.post(Trade_url, data=sellparams)
            resdict = json.loads(res.content.decode())
            print(resdict)
            logger.info(resdict)
            orderId = resdict["response"]["orderid"]  # 获取订单id
            sellinsertsql = "INSERT INTO coverlist(strategyId,userUuid,apiAccountId,platform,symbol,direction," \
                            "orderid,order_amount,order_price,order_time," \
                            "status,uniqueId,tradetype,coverlevel) VALUES(%s, %s, %s, %s, %s, %s,%s,%s,%s, %s, %s, " \
                            "%s,%s,%s) "
            cur.execute(sellinsertsql, (strategyId, userUuid, apiAccountId, platform, symbol, 2, orderId, amount, sellprice, ordertime, 0, 11,1, 0))
            conn.commit()
            time.sleep(3)
            # 3秒后查询卖单
            queryparams = {"direction": 2, "symbol": symbol, "platform": platform, "orderId": orderId,
                           "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11,
                           "strategyId": strategyId}
            queryres = requests.post(Queryorder_url, data=queryparams)
            querydict = json.loads(queryres.content.decode())
            status = querydict['response']['status']
            if status == 'closed':
                numberDeal = float(querydict["response"]["numberDeal"])
                fee = Fee[platform]['sellfee']
                queryparams = {"platform": platform, "symbol": symbol, "orderId": orderId, "apiId": apiAccountId,
                               "userUuid": userUuid}
                res = requests.post(Query_tradeprice_url, data=queryparams)
                queryresdict = json.loads(res.content.decode())
                logger.info(querydict)
                try:
                    tradeprice = queryresdict['response']['avgPrice']
                    tradetime = queryresdict['response']['createdDate']
                    sellfee = round(numberDeal * tradeprice * fee, 8)
                except:
                    tradeprice = sellprice
                    tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    sellfee = round(numberDeal * tradeprice * fee, 8)
                profit = round(
                    (tradeprice - entry_price_avg - (tradeprice + entry_price_avg) * fee) * amount, 8)
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
                cur.execute(updatesql,
                            (numberDeal, tradeprice, tradetime, profit, profitRate, 1, sellfee, strategyId, orderId))
                conn.commit()
                strategydata['flag'] = 0
                r2.hset('Cover_strategy', strategyId, json.dumps(strategydata))
                totalprofit, totalprofitRate = sumProfit(userUuid, strategyId, init_amount)
                params = {'strategyId': strategyId, 'profit': totalprofit, 'profitRate': totalprofitRate}
                res = requests.post(updateCover_url, data=params)
                resdict = json.loads(res.content.decode())
                print(resdict)
            elif status == "open":
                cancel_order(userUuid, apiAccountId, strategyId, platform, symbol, orderId, 2)
        except Exception as e:
            i = "用户{}补仓策略{}止盈下卖单时出错{}".format(userUuid, strategyId, e)
            logger.error(i)
            # 调用java停止策略接口
            # updateparams = {'strategyId': strategyId, "status": 4, }
            # res1 = requests.post(updateCover_url, data=updateparams)
        finally:
            conn.commit()
            cur.close()
            conn.close()


def traceBuy(strategydata, index):
    strategyId = strategydata['strategyId']
    cover_label = r2.hget('coverlevel', '{0}-{1}'.format(strategyId, index))
    if strategydata['flag'] == 0 or (not cover_label):
        return
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
    incrementalType = int(strategydata['incrementalType'])
    currentprice = get_currentprice1(platform, symbol)
    if not currentprice:
        return
    if incrementalType == 1:
        startIndex = int(strategydata['startIndex'])
        marginMultiple = float(strategydata['marginMultiple'])
    elif incrementalType == 2:
        startIndex = int(strategydata['startIndex'])
        increment = float(strategydata['increment'])
    # print('用户{}策略{}开始追踪补仓，当前行情价{}'.format(userUuid, strategyId, currentprice))
    # 当价格在此档位区间，并触碰到了最低价
    print("追踪买入,当前价{}".format(currentprice))
    if (cover_label['covertraceprice'] > currentprice > next_cover_price) and (
            cover_label['coverprice'] > currentprice):
        print('当前行情价{}更新最低价与抄底价'.format(currentprice))
        cover_label['covertraceprice'] = currentprice
        cover_label['stopprice'] = currentprice * (1 + covercallbackratio)
        r2.hset("coverlevel", '{0}-{1}'.format(strategyId, index), json.dumps(cover_label))
    # 当价格触碰到了激活价
    if currentprice <= cover_label['coverprice'] and cover_label['touchtag'] == 0:
        print('当前行情价{}触发激活价，作标记'.format(currentprice))
        cover_label['touchtag'] = 1
        r2.hset('coverlevel', '{0}-{1}'.format(strategyId, index), json.dumps(cover_label))
    # 当价格触碰了激活价并回升到抄底价时
    if cover_label["touchtag"] == 1 and cover_label["stopprice"] < currentprice < cover_label['coverprice']:
        conn = POOL.connection()
        cur = conn.cursor()
        try:
            # 下买单
            ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # 溢价买入
            # buyprice = round(currentprice + premiumdict[symbol] / 2, pricelimit[symbol][platform])
            buyprice = round(currentprice * 1.01, pricelimit[symbol][platform])
            if index + 1 >= startIndex:
                if incrementalType == 1:  # 是否开启倍投
                    count = count * (marginMultiple ** (index + 2 - startIndex))
                elif incrementalType == 2:  # 是否等差额增仓
                    count += increment * (index + 2 - startIndex)
            amount = count / buyprice
            x, y = str(amount).split('.')
            amount = float(x + '.' + y[0:amountlimit[symbol][platform]])
            buyparams = {"direction": 1, "amount": amount, "symbol": symbol, "platform": platform, "price": buyprice,
                         "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11, "strategyId": strategyId,
                         'tradetype': 1}
            # 将下单参数传给跟单策略
            r5.hset('order_param_13', strategyId, json.dumps(buyparams))
            i = '用户{}策略{}当前价格{}触碰了补仓{}档并回升到抄底价，补仓买入价格{}'.format(userUuid, strategyId, currentprice, index + 1, buyprice)
            print(i)
            logger.info(i)
            res = requests.post(Trade_url, data=buyparams)
            resdict = json.loads(res.content.decode())
            logger.info(resdict)
            orderid = resdict["response"]["orderid"]  # 获取订单id
            insertsql = "INSERT INTO coverlist(userUuid,apiAccountId,strategyId,platform,symbol,direction,orderid," \
                        "order_amount,order_price,order_time,status,uniqueId,tradetype,coverlevel) VALUES(%s, %s, %s, " \
                        "%s, %s,%s,%s, %s, %s, %s," \
                        " %s,%s,%s,%s) "
            insertdata = (
                userUuid, apiAccountId, strategyId, platform, symbol, 1, orderid, amount, buyprice, ordertime, 0,
                11, 1, index + 1)
            cur.execute(insertsql, insertdata)
            conn.commit()
            time.sleep(3)
            # 3s后查询买单
            queryparams = {"direction": 1, "symbol": symbol, "platform": platform, "orderId": orderid,
                           "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11,
                           "strategyId": strategyId}
            queryres = requests.post(Queryorder_url, data=queryparams)
            querydict = json.loads(queryres.content.decode())
            logger.info("用户{}策略{}平台{}查询买单{}".format(userUuid, strategyId, platform, querydict))
            status = querydict['response']['status']
            if status == 'closed':
                numberDeal = round(float(querydict["response"]["numberDeal"]) * 0.998,8)
                fee = Fee[platform]['buyfee']
                queryparams = {"platform": platform, "symbol": symbol, "orderId": orderid, "apiId": apiAccountId,
                               "userUuid": userUuid}
                res = requests.post(Query_tradeprice_url, data=queryparams)
                queryresdict = json.loads(res.content.decode())
                logger.info(queryresdict)
                try:
                    tradeprice = queryresdict["response"]["avgPrice"]
                    tradetime = queryresdict['response']['createdDate']
                    buyfee = round(tradeprice * numberDeal * fee, 8)
                except:
                    tradeprice = buyprice
                    tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    buyfee = round(tradeprice * numberDeal * fee, 8)
                # 存入数据库
                updatesql = "update coverlist set trade_amount=%s,trade_price=%s,trade_time=%s,status=%s,fee=%s where " \
                            "strategyId=%s and orderid=%s"
                cur.execute(updatesql, (numberDeal, tradeprice, tradetime, 1, buyfee, strategyId, orderid))
                conn.commit()
                strategydata['buy_value'] += numberDeal * tradeprice
                strategydata['buy_num'] += numberDeal
                r2.hset('Cover_strategy', strategyId, json.dumps(strategydata))
                r2.hdel('coverlevel', '{0}-{1}'.format(strategyId, index))
                # 补仓后修改买入均价
                params = {'strategyId': strategyId, 'averagePrice': strategydata['buy_value'] / strategydata['buy_num']}
                res = requests.post(updateCover_url, data=params)
                resdict = json.loads(res.content.decode())
                print(resdict)
            elif status == 'open':
                cancel_order(userUuid, apiAccountId, strategyId, platform, symbol, orderid, 1)
        except Exception as e:
            i = "用户{}补仓策略{}补仓时出错{}".format(userUuid, strategyId, e)
            logger.error(i)
            # 调用java停止策略接口
            # updateparams = {'strategyId': strategyId, "status": 4, }
            # res1 = requests.post(updateCover_url, data=updateparams)
        finally:
            conn.commit()
            cur.close()
            conn.close()


def run(strategydata):
    startBuy(strategydata)
    for index in range(len(strategydata['coverRatio'].split('-'))):
        traceBuy(strategydata, index)
    traceSell(strategydata['strategyId'])


if __name__ == '__main__':
    while True:
        try:
            strategy_list = r2.hvals("Cover_strategy")
            strategy_list = [json.loads(i) for i in strategy_list]
            T = []
            for strategy_info in strategy_list:
                T.append(Thread(target=run, args=(strategy_info,)))
            for t in T:
                t.start()
            for t in T:
                t.join()
        except Exception as e:
            print(e)
        finally:
            time.sleep(1)

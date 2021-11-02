# -*- coding: utf-8 -*-
# @Time : 2020/4/15 16:43
import json
import sys
import time
import requests
from threading import Thread
sys.path.append("..")
from loggerConfig import logger
from tools.Config import amountlimit, Trade_url, Queryorder_url, Query_tradeprice_url, updateTraceStrategy, \
    pricelimit
from tools.databasePool import *
from tools.get_market_info import get_currentprice1


# 追踪卖单
def traceSellStrategy(strategydata):
    strategyId = strategydata["strategyId"]
    print("（卖）追踪策略{}在运行".format(strategyId))
    userUuid = strategydata["userUuid"]  # 用户id
    apiAccountId = strategydata["apiAccountId"]  # 子账户id
    platform = strategydata["platform"]
    symbol = strategydata["symbol"]
    basePrice = strategydata["basePrice"]
    targetPrice = strategydata["targetPrice"]
    callbackRate = strategydata["callbackRate"]
    strategyType = strategydata["strategyType"]
    if strategyType == 1:
        count = strategydata["count"]
        x, y = str(count).split('.')
        count = float(x + '.' + y[0:amountlimit[symbol][platform]])  # 下单数量小数位精度限制
        # try:
        #     current_price = [i["buyprice"] for i in datalist if i["platform"] == platform and i["symbol"] == symbol][0]
        # except:
        #     current_price = get_market_depth(platform, symbol)["buyprice"]
        current_price = get_currentprice1(platform, symbol)
        print("{}-{}最新价{}".format(platform, symbol, current_price))
        # 更新最高价和止盈价
        if current_price > strategydata["mostPrice"]:  # 更新最高价和止盈价
            strategydata["mostPrice"] = current_price
            strategydata["stopPrice"] = current_price * (1 - callbackRate)
            r2.hset("tracestrategysell", strategyId, json.dumps(strategydata))  # 记录到redis
            print("更新最高价{},止盈价{}".format(strategydata["mostPrice"], strategydata["stopPrice"]))
        # 当价格触碰到激活价时，做一个标记
        if current_price >= targetPrice and (strategydata["touchTags"] is None or strategydata["touchTags"] == 0):
            strategydata["touchTags"] = 1
            r2.hset("tracestrategysell", strategyId, json.dumps(strategydata))  # 记录到redis
            i = "用户{}长线追盈策略{}触发激活价标记".format(userUuid, strategyId)
            print(i)
            logger.info(i)
        # 当价格触碰了激活价并回落到止盈价时
        if strategydata["stopPrice"] is not None and strategydata["touchTags"] == 1 and strategydata[
            "stopPrice"] >= current_price > basePrice:
            conn = POOL.connection()
            cur = conn.cursor()
            try:
                # 下卖单
                sellordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                # sellprice = current_price - premiumdict[symbol] / 2  # 溢价处理
                sellprice = round(current_price * 0.999, pricelimit[symbol][platform])
                sellparams = {"direction": 2, "amount": count, "symbol": symbol, "platform": platform,
                              "price": sellprice, "userUuid": userUuid, "apiAccountId": apiAccountId, "source": 5,
                              "strategyId": strategyId, 'tradetype': 1}
                # 将下单参数传给跟单策略
                new_sellparams = sellparams.copy()
                new_sellparams['baseprice'] = basePrice
                r5.hset('order_param_4', strategyId, json.dumps(new_sellparams))
                res = requests.post(Trade_url, data=sellparams)
                resdict = json.loads(res.content.decode())
                orderid = resdict["response"]["orderid"]  # 获取订单id
                # 记录到数据库
                insertsql = "INSERT INTO tracelist(userUuid,apiAccountId,strategyId,platform,symbol,direction," \
                            "orderid,order_count,order_price,order_time,status) VALUES(%s, %s, %s, %s, %s,%s,%s, %s, " \
                            "%s, %s, %s) "
                insertdata = (
                    userUuid, apiAccountId, strategyId, platform, symbol, 2, orderid, count, sellprice, sellordertime,
                    0)
                cur.execute(insertsql, insertdata)
                i = "用户{}子账户{}智能追踪策略{}止盈下卖单成功，已插入数据库".format(userUuid, apiAccountId, strategyId)
                print(i)
                logger.info(i)
                time.sleep(4)
                # 4秒后查询卖单
                queryparams = {"direction": 2, "symbol": symbol, "platform": platform, "orderId": orderid,
                               "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 5,
                               "strategyId": strategyId}
                queryres = requests.post(Queryorder_url, data=queryparams)
                querydict = json.loads(queryres.content.decode())
                logger.info("用户{}策略{}平台{}查询卖单{}".format(userUuid, strategyId, platform, querydict))
                status = querydict['response']['status']
                # 假如卖单全部成交
                if status == 'closed':
                    numberDeal = float(querydict["response"]["numberDeal"])
                    queryparams1 = {"platform": platform, "symbol": symbol, "orderId": orderid, "apiId": apiAccountId,
                                    "userUuid": userUuid, "strategyId": strategyId}
                    res = requests.post(Query_tradeprice_url, data=queryparams1)
                    queryresdict = json.loads(res.content.decode())
                    try:
                        tradeprice = queryresdict["response"]["avgPrice"]
                        selltradetime = queryresdict["response"]["createdDate"]
                    except:
                        tradeprice = sellprice
                        selltradetime = sellordertime
                    updatesql = "update tracelist set trade_count=%s,trade_price=%s,trade_time=%s,status=%s where " \
                                "strategyId=%s and orderid=%s "
                    cur.execute(updatesql, (numberDeal, tradeprice, selltradetime, 1, strategyId, orderid))
                    profit = round((tradeprice - basePrice) * count, 8)
                    profitRate = round(profit / (basePrice * count), 8)
                    i = "用户{}长线追盈策略{}，基准价{}，价格涨到最高价{}后,又回调{}%至{},追踪止盈，盈利{}，盈利率{}".format(userUuid, strategyId,
                                                                                         basePrice,
                                                                                         strategydata["mostPrice"],
                                                                                         callbackRate * 100,
                                                                                         strategydata["stopPrice"],
                                                                                         profit, profitRate)
                    logger.info(i)
                    # 返回收益给java
                    updateprofit = {'strategyId': strategyId, "status": 2, "profit": str(profit),
                                    "profitRate": str(profitRate)}
                    res = requests.post(updateTraceStrategy, data=updateprofit)
                    resdict = json.loads(res.content.decode())
                    logger.info("返回收益给java端{}".format(resdict))
            except Exception as e:
                i = "长线追盈策略{}下卖单时出错{}".format(strategyId, e)
                logger.error(i)
                # 调用java停止策略接口
                updateparams = {'strategyId': strategyId, "status": 4, "profit": 0, "profitRate": 0}
                res1 = requests.post(updateTraceStrategy, data=updateparams)
            finally:
                conn.commit()
                cur.close()
                conn.close()
                #  从redis中删除策略，不再运行
                time.sleep(5)
                r2.hdel("tracestrategysell", strategyId)
                logger.info("长线追盈策略{}已止盈成功，删除该策略".format(strategyId))

    # if strategyType == 3:
    #     count = strategydata["count"] / 2  # 此处是交易币数量,成交一半
    #     x, y = str(count).split('.')
    #     count = float(x + '.' + y[0:amountlimit[symbol][platform]])  # 下单数量小数位精度限制
    #     try:
    #         current_price = [i["buyprice"] for i in datalist if i["platform"] == platform and i["symbol"] == symbol][0]
    #     except:
    #         current_price = get_market_depth(platform, symbol)["buyprice"]
    #     print("{}-{}最新价{}".format(platform, symbol, current_price))
    #
    #     # 更新最高价和止盈价
    #     if current_price > strategydata["mostPrice"]:  # 更新最高价和止盈价
    #         strategydata["mostPrice"] = current_price
    #         strategydata["stopPrice"] = current_price * (1 - callbackRate)
    #         r2.hset("tracestrategysell", strategyId, json.dumps(strategydata))  # 记录到redis
    #         print("更新最高价{},止盈价{}".format(strategydata["mostPrice"], strategydata["stopPrice"]))
    #     # 当价格触碰到激活价时，做一个标记
    #     if current_price >= targetPrice and (strategydata["touchTags"] == None or strategydata["touchTags"] == 0):
    #         strategydata["touchTags"] = 1
    #         r2.hset("tracestrategysell", strategyId, json.dumps(strategydata))  # 记录到redis
    #         i = "用户{}长线追盈策略{}触发激活价标记".format(userUuid, strategyId)
    #         print(i)
    #         logger.info(i)
    #     # 当价格触碰了激活价并回落到止盈价时
    #     if strategydata["stopPrice"] != None and strategydata["touchTags"] == 1 and current_price <= strategydata[
    #         "stopPrice"] and current_price > basePrice:
    #         conn = POOL.connection()
    #         cur = conn.cursor()
    #         try:
    #             # 下卖单
    #             sellordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    #             sellprice = current_price - premiumdict[symbol]  # 溢价处理
    #             sellparams = {"direction": 2, "amount": count, "symbol": symbol, "platform": platform, "price": sellprice,
    #                           "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 4, "icebergId": strategyId, "uniqueId": 2}
    #             res = requests.post(Trade_url, data=sellparams)
    #             resdict = json.loads(res.content.decode())
    #             orderid = resdict["response"]["orderid"]  # 获取订单id
    #             time.sleep(3)
    #             # 3秒后查询卖单
    #             queryparams = {"direction": 2, "symbol": symbol, "platform": platform, "orderId": orderid,
    #                            "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 4, "icebergId": strategyId}
    #             queryres = requests.post(Queryorder_url, data=queryparams)
    #             querydict = json.loads(queryres.content.decode())
    #             logger.info("用户{}策略{}平台{}查询卖单{}".format(userUuid, strategyId, platform, querydict))
    #             numberDeal = float(querydict["response"]["numberDeal"])
    #             # 假如卖单全部成交
    #             if numberDeal == count:
    #                 tradeprice = sellprice
    #                 selltradetime = sellordertime
    #                 try:
    #                     # 2s后查询实际成交价
    #                     time.sleep(2)
    #                     queryparams = {"platform": platform, "symbol": symbol, "orderId": orderid, "apiId": apiAccountId, "userUuid": userUuid}
    #                     res = requests.post(Query_tradeprice_url, data=queryparams)
    #                     queryresdict = json.loads(res.content.decode())
    #                     if queryresdict["response"]["avgPrice"] != None:
    #                         tradeprice = queryresdict["response"]["avgPrice"]
    #                     if queryresdict["response"]["createdDate"] != None:
    #                         selltradetime = queryresdict["response"]["createdDate"]
    #                 except Exception as e:
    #                     logger.error("策略{}查询订单{}实际成交均价失败报错{}".format(strategyId, orderid, e))
    #                 insertsql = "INSERT INTO gridlist(userUuid,apiAccountId,strategyId,platform,symbol,sellprice,sellcount,sellorderid,sellstatus,sellordertime,selltradetime,uniqueId) VALUES( %s, %s,%s, %s, %s, %s,%s,%s, %s, %s, %s,%s)"
    #                 insertdata = (
    #                     userUuid, apiAccountId, strategyId, platform, symbol, tradeprice, numberDeal, orderid, 1, sellordertime, selltradetime, 2)
    #                 cur.execute(insertsql, insertdata)
    #                 # 停止长线策略
    #                 r2.hdel("tracestrategysell", strategyId)
    #                 # 开启网格策略
    #                 minTradeQuantity = count / limitamount[symbol]
    #                 resdata = {"userUuid": userUuid, "apiAccountId": apiAccountId, "strategyId": strategyId, "platform": platform, "symbol": symbol,
    #                            "entryPrice": tradeprice, "initialTradeCoin": count,
    #                            "initialValCoin": numberDeal * sellprice, "minTradeQuantity": minTradeQuantity, "status": 1}
    #                 res = requests.post(updateStrategy3_url, data=resdata)
    #                 logger.info("组合策略{}长线完成，开启网格{}".format(strategyId, res.text))
    #         except Exception as e:
    #             # 删除长线策略
    #             r2.hdel("tracestrategysell", strategyId)
    #             # 防错停止，通知java端
    #             requests.post(stopStrategy3_url, data={"userUuid": userUuid, "apiAccountId": apiAccountId, "strategyId": strategyId, "status": 4,"stopType":1})
    #         finally:
    #             conn.commit()
    #             cur.close()
    #             conn.close()


# 追踪买单
def traceBuyStrategy(strategydata):
    strategyId = strategydata["strategyId"]
    print("（买）追踪策略{}在运行".format(strategyId))
    userUuid = strategydata["userUuid"]  # 用户id
    apiAccountId = strategydata["apiAccountId"]  # 子账户id
    platform = strategydata["platform"]
    symbol = strategydata["symbol"]
    basePrice = strategydata["basePrice"]
    targetPrice = strategydata["targetPrice"]
    callbackRate = strategydata["callbackRate"]
    strategyType = strategydata["strategyType"]
    if strategyType == 1:
        count = strategydata["count"]  # 这里是usdt的数量
        # try:
        #     current_price = [i["sellprice"] for i in datalist if i["platform"] == platform and i["symbol"] == symbol][0]
        # except:
        #     current_price = get_market_depth(platform, symbol)["sellprice"]
        current_price = get_currentprice1(platform, symbol)
        print("{}-{}最新价{}".format(platform, symbol, current_price))
        # 更新最低价和止盈价
        if current_price < strategydata["mostPrice"] and current_price != 0:  # 更新最低价和抄底价
            strategydata["mostPrice"] = current_price
            strategydata["stopPrice"] = current_price * (1 + callbackRate)
            r2.hset("tracestrategybuy", strategyId, json.dumps(strategydata))
            print("追踪策略{}更新最低价{},抄底价{}".format(strategyId, strategydata["mostPrice"], strategydata["stopPrice"]))
        # 假如价格触发激活价，做一个标记
        if current_price <= targetPrice and (
                strategydata["touchTags"] is None or strategydata["touchTags"] == 0) and current_price != 0:
            strategydata["touchTags"] = 1
            r2.hset("tracestrategybuy", strategyId, json.dumps(strategydata))
            i = "用户{}长线追盈策略{}触发激活价标记".format(userUuid, strategyId)
            print(i)
            logger.info(i)
        # 当价格触发了激活价并且大于抄底价时
        if strategydata["stopPrice"] is not None and current_price >= strategydata["stopPrice"] and strategydata[
            "touchTags"] == 1 and current_price < basePrice:
            conn = POOL.connection()
            cur = conn.cursor()
            try:
                # 下买单
                buyordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                # buyprice = current_price + premiumdict[symbol] / 2  # 溢价处理
                buyprice = round(current_price * 1.001, pricelimit[symbol][platform])
                amount = count / buyprice  # 下单数量 = usdt数量/价格,保留四位小数四舍五不入
                x, y = str(amount).split('.')
                amount = float(x + '.' + y[0:amountlimit[symbol][platform]])
                buyparams = {"direction": 1, "amount": amount, "symbol": symbol, "platform": platform,
                             "price": buyprice, "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 5,
                             "strategyId": strategyId}
                # 将下单参数传给跟单策略
                new_buyparams = buyparams.copy()
                new_buyparams['base_price'] = basePrice
                r5.hset('order_param_4', strategyId, json.dumps(new_buyparams))
                res = requests.post(Trade_url, data=buyparams)
                resdict = json.loads(res.content.decode())
                logger.info("用户{}策略{}平台{}下买单{}".format(userUuid, strategyId, platform, resdict))
                orderid = resdict["response"]["orderid"]  # 获取订单id
                # 记录到数据库
                insertsql = "INSERT INTO tracelist(userUuid,apiAccountId,strategyId,platform,symbol,direction,orderid,order_count,order_price,order_time,status) VALUES(%s, %s, %s, %s, %s,%s,%s, %s, %s, %s, %s)"
                insertdata = (
                    userUuid, apiAccountId, strategyId, platform, symbol, 1, orderid, amount, buyprice, buyordertime, 0)
                cur.execute(insertsql, insertdata)
                i = "用户{}子账户{}智能追踪策略{}下买单成功，已插入数据库".format(userUuid, apiAccountId, strategyId)
                print(i)
                logger.info(i)
                time.sleep(3)
                # 查询买单
                queryparams = {"direction": 1, "symbol": symbol, "platform": platform, "orderId": orderid,
                               "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 5,
                               "startegyId": strategyId,
                               'tradetype': 1}
                queryres = requests.post(Queryorder_url, data=queryparams)
                querydict = json.loads(queryres.content.decode())
                logger.info("用户{}策略{}平台{}查询买单{}".format(userUuid, strategyId, platform, querydict))
                status = querydict['response']['status']
                # 假如卖单全部成交
                if status == 'closed':
                    numberDeal = float(querydict["response"]["numberDeal"])
                    queryparams1 = {"platform": platform, "symbol": symbol, "orderId": orderid, "apiId": apiAccountId,
                                    "userUuid": userUuid, "strategyId": strategyId}
                    res = requests.post(Query_tradeprice_url, data=queryparams1)
                    queryresdict = json.loads(res.content.decode())
                    try:
                        tradeprice = queryresdict["response"]["avgPrice"]
                        buytradetime = queryresdict["response"]["createdDate"]
                    except:
                        tradeprice = buyprice
                        buytradetime = buyordertime
                    # 计算预期收益
                    expectprofit = round((basePrice - tradeprice) * amount, 8)
                    expectprofitRate = round(expectprofit / (basePrice * amount), 8)
                    i = "用户{}追踪委托策略（买）{}，基准价{}，价格跌到最低价{}后,又回调{}%至{},追踪抄底,预期盈利{}，预期盈利率{}".format(userUuid, strategyId,
                                                                                                basePrice,
                                                                                                strategydata[
                                                                                                    "mostPrice"],
                                                                                                callbackRate * 100,
                                                                                                strategydata[
                                                                                                    "stopPrice"],
                                                                                                expectprofit,
                                                                                                expectprofitRate)
                    logger.info(i)
                    # 记录到数据库
                    updatesql = "update tracelist set trade_count=%s,trade_price=%s,trade_time=%s,status=%s where " \
                                "strategyId=%s and orderid=%s "
                    cur.execute(updatesql, (numberDeal, tradeprice, buytradetime, 1, strategyId, orderid))
                    # 返回预期收益给java端
                    updateprofit = {'strategyId': strategyId, "status": 2,
                                    "expectprofit": str(expectprofit), "expectprofitRate": str(expectprofitRate)}
                    res = requests.post(updateTraceStrategy, data=updateprofit)
                    resdict = json.loads(res.content.decode())
            except Exception as e:
                i = "长线追盈策略{}下买单时出错{}".format(strategyId, e)
                logger.error(i)
                # 调用java停止接口
                updateparams = {'strategyId': strategyId, "status": 4, "expectprofit": 0, "expectprofitRate": 0}
                res1 = requests.post(updateTraceStrategy, data=updateparams)
            finally:
                conn.commit()
                cur.close()
                conn.close()
                time.sleep(5)
                print("追踪策略买单完成，返回收益给java端", res.url, resdict)
                # 从redis中删除,不再运行
                r2.hdel("tracestrategybuy", strategyId)

    # if strategyType == 3:
    #     count = strategydata["count"] / 2  # 这里是usdt的数量
    #     try:
    #         current_price = [i["sellprice"] for i in datalist if i["platform"] == platform and i["symbol"] == symbol][0]
    #     except:
    #         current_price = get_market_depth(platform, symbol)["sellprice"]
    #     print("{}-{}最新价{}".format(platform, symbol, current_price))
    #     # 更新最低价和止盈价
    #     if current_price < strategydata["mostPrice"] and current_price != 0:  # 更新最低价和抄底价
    #         strategydata["mostPrice"] = current_price
    #         strategydata["stopPrice"] = current_price * (1 + callbackRate)
    #         r2.hset("tracestrategybuy", strategyId, json.dumps(strategydata))
    #         print("追踪策略{}更新最低价{},抄底价{}".format(strategyId, strategydata["mostPrice"], strategydata["stopPrice"]))
    #     # 假如价格触发激活价，做一个标记
    #     if current_price <= targetPrice and (strategydata["touchTags"] == None or strategydata["touchTags"] == 0) and current_price != 0:
    #         strategydata["touchTags"] = 1
    #         r2.hset("tracestrategybuy", strategyId, json.dumps(strategydata))
    #         i = "用户{}长线追盈策略{}触发激活价标记".format(userUuid, strategyId)
    #         print(i)
    #         logger.info(i)
    #     # 当价格触发了激活价并且大于抄底价时
    #     if strategydata["stopPrice"] != None and current_price >= strategydata["stopPrice"] and strategydata[
    #         "touchTags"] == 1 and current_price < basePrice:
    #         conn = POOL.connection()
    #         cur = conn.cursor()
    #         try:
    #             # 下买单
    #             buyordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    #             buyprice = current_price + premiumdict[symbol] / 2  # 溢价处理
    #             amount = count / buyprice  # 下单数量 = usdt数量/价格,保留四位小数四舍五不入
    #             x, y = str(amount).split('.')
    #             amount = float(x + '.' + y[0:amountlimit[symbol][platform]])
    #             buyparams = {"direction": 1, "amount": amount, "symbol": symbol, "platform": platform, "price": buyprice,
    #                          "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 4, "icebergId": strategyId, "uniqueId": 2}
    #             res = requests.post(Trade_url, data=buyparams)
    #             resdict = json.loads(res.content.decode())
    #             logger.info("用户{}策略{}平台{}下买单{}".format(userUuid, strategyId, platform, resdict))
    #             orderid = resdict["response"]["orderid"]  # 获取订单id
    #             time.sleep(3)
    #             # 查询买单
    #             queryparams = {"direction": 1, "symbol": symbol, "platform": platform, "orderId": orderid,
    #                            "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 4, "icebergId": strategyId}
    #             queryres = requests.post(Queryorder_url, data=queryparams)
    #             querydict = json.loads(queryres.content.decode())
    #             logger.info("用户{}策略{}平台{}查询买单{}".format(userUuid, strategyId, platform, querydict))
    #             numberDeal = float(querydict["response"]["numberDeal"])
    #             # 假如买单全部成交
    #             if numberDeal == amount:
    #                 tradeprice = buyprice
    #                 buytradetime = buyordertime
    #                 try:
    #                     # 2s后查询实际成交价
    #                     time.sleep(2)
    #                     queryparams = {"platform": platform, "symbol": symbol, "orderId": orderid, "apiId": apiAccountId, "userUuid": userUuid}
    #                     res = requests.post(Query_tradeprice_url, data=queryparams)
    #                     queryresdict = json.loads(res.content.decode())
    #                     if queryresdict["response"]["avgPrice"] != None:
    #                         tradeprice = queryresdict["response"]["avgPrice"]
    #                     if queryresdict["response"]["createdDate"] != None:
    #                         buytradetime = queryresdict["response"]["createdDate"]
    #                 except Exception as e:
    #                     logger.error("策略{}查询订单{}实际成交均价失败报错{}".format(strategyId, orderid, e))
    #                 # 记录到数据库
    #                 insertsql = "INSERT INTO gridlist(userUuid,apiAccountId,strategyId,platform,symbol,buyprice,buycount,buyorderid,buystatus,buyordertime,buytradetime,uniqueId) VALUES(%s,%s, %s, %s, %s, %s, %s,%s,%s, %s, %s, %s)"
    #                 insertdata = (
    #                 userUuid, apiAccountId, strategyId, platform, symbol, tradeprice, numberDeal, orderid, 1, buyordertime, buytradetime, 2)
    #                 cur.execute(insertsql, insertdata)
    #                 # 停止长线策略
    #                 r2.hdel("tracestrategybuy", strategyId)
    #                 # 开启网格策略
    #                 minTradeQuantity = numberDeal / limitamount[symbol]
    #                 resdata = {"userUuid": userUuid, "apiAccountId": apiAccountId, "strategyId": strategyId, "platform": platform, "symbol": symbol,
    #                            "entryPrice": tradeprice, "initialTradeCoin": numberDeal, "initialValCoin": count,
    #                            "minTradeQuantity": minTradeQuantity, "status": 1}
    #                 res = requests.post(updateStrategy3_url, data=resdata)
    #                 logger.info("组合策略{}长线完成，开启网格{}".format(strategyId, res.text))
    #         except Exception as e:
    #             # 删除长线策略
    #             r2.hdel("tracestrategybuy", strategyId)
    #             # 防错停止，通知java端
    #             requests.post(stopStrategy3_url, data={"userUuid": userUuid, "apiAccountId": apiAccountId, "strategyId": strategyId, "status": 4,"stopType":1})
    #         finally:
    #             conn.commit()
    #             cur.close()
    #             conn.close()


def goDoTraceStrategy():
    num = 0
    while True:
        try:
            # 获取各交易所买一卖一价
            # datalist = get_all_market_depth()
            # 追踪卖策略
            if num % 2 == 1:
                selltracestrategydata = [json.loads(i) for i in r2.hvals("tracestrategysell")]
                selltracelist = []
                for strategydata in selltracestrategydata:
                    selltracelist.append(Thread(target=traceSellStrategy, args=(strategydata,)))
                for t in selltracelist:
                    t.start()
                for t in selltracelist:
                    t.join()
            if num % 2 == 0:
                buytracestrategydata = [json.loads(i) for i in r2.hvals("tracestrategybuy")]
                buytracelist = []
                for strategydata in buytracestrategydata:
                    buytracelist.append(Thread(target=traceBuyStrategy, args=(strategydata,)))
                for t in buytracelist:
                    t.start()
                for t in buytracelist:
                    t.join()
        except Exception as e:
            i = "长线追盈多线程报错{}".format(e)
            # logger.error(i)
        finally:
            num += 1
            if num == 10:
                num = 0
            time.sleep(1)


# 策略执行方法
if __name__ == '__main__':
    goDoTraceStrategy()

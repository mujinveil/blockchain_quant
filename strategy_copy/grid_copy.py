# encode='utf-8'
import json
import time
from threading import Thread
import requests
from loggerConfig import logger
from tools.Config import Trade_url, Cancel_url
from tools.databasePool import r2, r4, r5, POOL


# 撤销网格单
def cancelcopyorders(userUuid, apiAccountId, strategyId, platform, symbol, orderid, sellorderlist, buyorderlist):
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        if orderid in sellorderlist:
            res = requests.post(Cancel_url, data={"direction": 2, "symbol": symbol, "platform": platform,
                                                  "orderId": orderid, "apiAccountId": apiAccountId,
                                                  "userUuid": userUuid,
                                                  "source": 4, "strategyId": strategyId, "icebergId": strategyId})
            cur.execute("update t_gridtrade_copy set sellstatus=2 where strategyId=%s and sellorderid=%s",
                        (strategyId, orderid))
        if orderid in buyorderlist:
            res = requests.post(Cancel_url, data={"direction": 1, "symbol": symbol, "platform": platform,
                                                  "orderId": orderid, "apiAccountId": apiAccountId,
                                                  "userUuid": userUuid,
                                                  "source": 4, "strategyId": strategyId, "icebergId": strategyId})
            cur.execute("update t_gridtrade_copy set buystatus=2 where strategyId=%s and buyorderid=%s",
                        (strategyId, orderid))
    except Exception as e:
        i = "系统正在为用户{}跟单策略{}撤销{}平台订单{}出错{}".format(userUuid, strategyId, platform, orderid, e)
        logger.error(i)
    finally:
        conn.commit()
        cur.close()
        conn.close()


def grid_strategy_copy(tracer_info, order_param):
    try:
        userUuid = tracer_info['userUuid']
        apiAccountId = tracer_info["apiAccountId"]
        platform = tracer_info["platform"]
        symbol = tracer_info["symbol"]
        counterCoinName = symbol.split("_")[0]
        valueCoinName = symbol.split("_")[1]
        strategyId = tracer_info['strategyId']
        followstrategyId = tracer_info['followStrategyId']
        entryPrice = tracer_info['entryPrice']
        createTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        flag = order_param['flag']
        amount = order_param['amount']
        sell_price = order_param['sell_price']
        buy_price = order_param['buy_price']
        gap = order_param['gap']
        makerFee = order_param['makerFee']
        # 初始网格跟踪部署,基本难以跟单
        if flag == 0:
            conn = POOL.connection()
            cur = conn.cursor()
            try:
                print("=========开始往上部署一个卖单=========")
                sell_dict = {"direction": 2, "amount": amount, "symbol": symbol, "platform": platform,
                             "price": sell_price, "apiAccountId": apiAccountId, "userUuid": userUuid,
                             "source": 4, "strategyId": strategyId}
                res_sell = requests.post(Trade_url, data=sell_dict)
                trade_sell_dict = json.loads(res_sell.content.decode())
                sellorderid = trade_sell_dict["response"]["orderid"]  # 获取订单id
                info2 = "网格{}初始化部署卖单委托成功，交易平台：{}，价格：{}，数量{}".format(strategyId, platform, sell_price, amount)
                print(info2)
                logger.info(info2)
                # 插入数据库
                profit = gap * amount
                net_profit = profit - (entryPrice + sell_price) * amount * makerFee
                sellordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                sellinsertsql = "insert into t_gridtrade_copy (userUuid,apiAccountId,strategyId,followstrategyId,platform,symbol,sellprice,sellcount,sellorderid,sellstatus,sellordertime,profit,netprofit,uniqueId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                cur.execute(sellinsertsql,
                            (userUuid, apiAccountId, strategyId, followstrategyId, platform, symbol, sell_price, amount,
                             sellorderid, 0, sellordertime, profit, net_profit, 1))

                print("========开始往下部署一个买单============")
                buy_dict = {"direction": 1, "amount": amount, "symbol": symbol, "platform": platform,
                            "price": buy_price,
                            "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 4, "strategyId": strategyId, }
                res_buy = requests.post(Trade_url, data=buy_dict)
                trade_buy_dict = json.loads(res_buy.content.decode())
                # logger.info("网格{}初始化部署买单{}".format(strategyId, trade_buy_dict))
                buyorderid = trade_buy_dict["response"]["orderid"]  # 获取订单id
                info3 = "网格{}初始化部署买单委托成功，交易平台{}，价格：{}，数量{}".format(strategyId, platform, buy_price, amount)
                print(info3)
                logger.info(info3)
                buyordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                buyinsertsql = "insert into t_gridtrade_copy(userUuid,apiAccountId,strategyId,followstrategyId," \
                               "platform,symbol,buyprice,buycount,buyorderid,buystatus,buyordertime,uniqueId) values " \
                               "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s); "
                cur.execute(buyinsertsql, (
                    userUuid, apiAccountId, strategyId, followstrategyId, platform, symbol, buy_price, amount,
                    buyorderid, 0, buyordertime, 1))
            except Exception as e:
                i = "用户{}，初步部署网格策略{}，报错信息{}".format(userUuid, strategyId, e)
                print(i)
                logger.error(i)
            finally:
                conn.commit()
                cur.close()
                conn.close()

        # 卖单全部成交
        elif flag == 1:
            conn = POOL.connection()
            cur = conn.cursor()
            try:
                try:
                    # 如果之前没有跟踪部署买卖单，此处不需要改变卖单状态与撤买单
                    # 1.改变卖单状态
                    selltradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    cur.execute(
                        'update t_gridtrade_copy set sellstatus=%s,selltradetime=%s where strategyId=%s and '
                        'followstrategyId=%s and  sellstatus=0',
                        (1, selltradetime, strategyId, followstrategyId))
                    # 2、撤销对应的买单
                    cur.execute(
                        'select buyorderid from t_gridtrade_copy where strategyId=%s and followstrategyId=%s and '
                        'buystatus=0',
                        (strategyId, followstrategyId))
                    buyorderid = cur.fetchone()[0]
                    cancel_param = {"direction": 1, "symbol": symbol, "platform": platform,
                                    "orderId": buyorderid, "apiAccountId": apiAccountId,
                                    "userUuid": userUuid, "source": 4, "strategyId": strategyId}
                    cancelres = requests.post(Cancel_url, data=cancel_param)
                    cancelinfo = "用户{}跟单策略{}平台{}撤销买单{}，返回结果{}".format(userUuid, strategyId, platform,
                                                                      buyorderid, cancelres.text)
                    # logger.info(cancelinfo)
                    print(cancelinfo)
                    deletebuysql = "delete from t_gridtrade_copy where strategyId=%s and buyorderid=%s"
                    cur.execute(deletebuysql, (strategyId, buyorderid))
                except Exception as e:
                    print("用户{}跟踪{}策略{}撤买单与修改卖单状态失败{}".format(userUuid, followstrategyId, strategyId, e))
                # 3、新挂一个卖单（比最大的卖单大一个网格)
                newsellordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                newsellparams = {"direction": 2, "amount": amount, "symbol": symbol, "platform": platform,
                                 "price": sell_price, "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 4,
                                 "strategyId": strategyId}
                res2 = requests.post(Trade_url, data=newsellparams)
                dict2 = json.loads(res2.content.decode())
                sellcode = dict2['code']
                if sellcode != 1:
                    sellerrorinfo = "您的{}网格策略{}跟单资金不足或者交易所接口不通，导致网格策略下卖单失败，可在调整资金后再重新创建开启".format(strategyId,
                                                                                                  counterCoinName)
                    print(sellerrorinfo)
                    logger.info(sellerrorinfo)
                    # 资产不足信息储存到redis
                    remainerrordata = {"strategyId": strategyId, 'followstrategyId': followstrategyId,
                                       "apiAccountId": apiAccountId, "coin": counterCoinName,
                                       "userUuid": userUuid, "platform": platform, "marktime": int(time.time() * 1000)}
                    r2.hset("errormark", strategyId, json.dumps(remainerrordata))
                elif sellcode == 1:
                    i = "用户{}网格策略跟单{}平台{}新挂卖单{}".format(userUuid, strategyId, platform, dict2)
                    print(i)
                    logger.info(i)
                    newsellorderid = dict2["response"]["orderid"]
                    cur.execute("select * from t_gridtrade_copy where strategyId=%s and sellprice=%s and sellstatus=2",
                                (strategyId, sell_price))
                    selectres = cur.fetchall()
                    if len(selectres) == 1:
                        updatesql2 = "update t_gridtrade_copy set sellorderid=%s,sellstatus=%s,sellordertime=%s where " \
                                     "strategyId=%s and sellprice=%s and sellstatus=2 "
                        cur.execute(updatesql2, (newsellorderid, 0, newsellordertime, strategyId, sell_price))
                    elif len(selectres) == 0:
                        profit = round((sell_price - entryPrice) * amount, 8)
                        netprofit = round(profit - (entryPrice + sell_price) * amount * makerFee, 8)
                        if profit <= 0:
                            profit = -1 * profit
                        if netprofit <= 0:
                            netprofit = -1 * netprofit
                        sellinsertsql = "insert into t_gridtrade_copy(userUuid,apiAccountId,strategyId," \
                                        "followstrategyId,platform,symbol,buyprice,buycount,buystatus,buyordertime," \
                                        "buytradetime,sellprice,sellcount,sellorderid,sellstatus,sellordertime," \
                                        "profit,netprofit,uniqueId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                                        "%s,%s,%s,%s,%s,%s); "
                        cur.execute(sellinsertsql, (
                            userUuid, apiAccountId, strategyId, followstrategyId, platform, symbol, entryPrice, amount,
                            1, createTime, createTime, sell_price, amount, newsellorderid, 0, newsellordertime, profit,
                            netprofit, 1))
                # 4、新挂一个买单（比最新成交价小一个网格间距)
                newbuyordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                newbuyparams = {"direction": 1, "amount": amount, "symbol": symbol, "platform": platform,
                                "price": buy_price, "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 4,
                                "strategyId": strategyId}
                res1 = requests.post(Trade_url, data=newbuyparams)
                dict1 = json.loads(res1.content.decode())
                buycode = dict1["code"]
                if buycode != 1:
                    buyerrorinfo = "您的{}网格策略{}跟单资金不足或者交易所接口不通，导致网格策略跟踪下买单失败，请停止当前策略，可在调整资金后再重新创建策略".format(
                        strategyId, valueCoinName)
                    print(buyerrorinfo)
                    # logger.info(buyerrorinfo)
                    # 资产不足信息储存到redis
                    remainerrordata = {"strategyId": strategyId, 'followstrategyId': followstrategyId,
                                       "userUuid": userUuid, "apiAccountId": apiAccountId, "platform": platform,
                                       "coin": valueCoinName, "marktime": int(time.time() * 1000)}
                    r2.hset("errormark", strategyId, json.dumps(remainerrordata))
                elif buycode == 1:
                    i = "用户{}策略{}平台{}新挂买单{}".format(userUuid, strategyId, platform, dict1)
                    # logger.info(i)
                    print(i)
                    newtradeinfo = "量化策略{}跟踪部署新的网格订单，卖单价{}，买单价{}".format(strategyId, sell_price, buy_price)
                    print(newtradeinfo)
                    logger.info(newtradeinfo)
                    newbuyorderid = dict1["response"]["orderid"]
                    buyinsertsql = "insert into t_gridtrade_copy (userUuid,apiAccountId,strategyId,followstrategyId," \
                                   "platform,symbol,buyprice," \
                                   "buycount,buyorderid,buystatus,buyordertime,uniqueId) values (%s,%s,%s,%s,%s,%s," \
                                   "%s,%s,%s,%s,%s," \
                                   "%s); "
                    cur.execute(buyinsertsql, (
                        userUuid, apiAccountId, strategyId, followstrategyId, platform, symbol, buy_price, amount,
                        newbuyorderid, 0, newbuyordertime, 1))
            except Exception as e:
                i = "用户{}跟踪{}网格策略{},网格单成交后重新部署出错{}".format(userUuid, followstrategyId, strategyId, e)
                print(i)
                logger.error(i)
            finally:
                conn.commit()
                cur.close()
                conn.close()

        # 买单全部成交
        elif flag == 2:
            conn = POOL.connection()
            cur = conn.cursor()
            try:
                buytradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                buyorderid = ""
                try:
                    cur.execute(
                        'select buyorderid from t_gridtrade_copy where strategyId=%s and followstrategyId=%s and buystatus=0',
                        (strategyId, followstrategyId))
                    buyorderid = cur.fetchone()[0]
                    # 1、撤销卖单
                    cur.execute(
                        'select sellorderid from t_gridtrade_copy where strategyId=%s and followstrategyId=%s and sellstatus=0',
                        (strategyId, followstrategyId))
                    sellorderid = cur.fetchone()[0]
                    cancel_param = {"direction": 2, "symbol": symbol, "platform": platform,
                                    "orderId": sellorderid, "apiAccountId": apiAccountId,
                                    "userUuid": userUuid, "source": 4, "strategyId": strategyId, }
                    cancelres1 = requests.post(Cancel_url, data=cancel_param)
                    cancelinfo1 = "用户{}策略{}平台{}撤销卖单{}，返回结果{}".format(userUuid, strategyId, platform,
                                                                     sellorderid, cancelres1.text)
                    # logger.info(cancelinfo1)
                    print(cancelinfo1)
                    updatasql3 = "update t_gridtrade_copy set sellstatus=2 where strategyId=%s and sellorderid=%s"
                    cur.execute(updatasql3, (strategyId, sellorderid))
                except Exception as e:
                    print("用户{}跟踪{}网格策略{}撤卖单与修改买单状态失败{}".format(userUuid, followstrategyId, strategyId, e))
                # 2、新挂一个卖单，比最新成交价高一个网格间距，加到数据库对应的买单后边
                newsellordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                newsellparams = {"direction": 2, "amount": amount, "symbol": symbol, "platform": platform,
                                 "price": sell_price, "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 4,
                                 "strategyId": strategyId, }
                res2 = requests.post(Trade_url, data=newsellparams)
                dict2 = json.loads(res2.content.decode())
                sellcode = dict2["code"]  # 卖单状态
                if sellcode != 1:
                    sellerrorinfo = "您的{}网格策略{}跟单资金不足或者交易所接口不通，导致网格策略下卖单失败，可在调整资金后再重新创建开启".format(strategyId,
                                                                                                  counterCoinName)
                    print(sellerrorinfo)
                    # 资产不足信息储存到redis
                    remainerrordata = {"strategyId": strategyId, "followstrategyId": followstrategyId,
                                       "apiAccountId": apiAccountId, "userUuid": userUuid, "platform": platform,
                                       "coin": counterCoinName, "marktime": int(time.time() * 1000)}
                    r2.hset("errormark", strategyId, json.dumps(remainerrordata))
                elif sellcode == 1:
                    i = "用户{}策略{}平台{}新挂卖单{}".format(userUuid, strategyId, platform, dict2)
                    logger.info(i)
                    print(i)
                    newsellorderid = dict2["response"]["orderid"]
                    profit = round(gap * amount, 8)
                    netprofit = round(profit - (sell_price - gap) * amount * makerFee - sell_price * amount * makerFee,
                                      8)
                    if buyorderid:
                        updatesql1 = "update t_gridtrade_copy set buystatus=%s,buytradetime=%s,sellprice=%s," \
                                     "sellcount=%s,sellorderid=%s,sellstatus=%s,sellordertime=%s,profit=%s," \
                                     "netprofit=%s where strategyId=%s and buyorderid=%s "
                        cur.execute(updatesql1,
                                    (1, buytradetime, sell_price, amount, newsellorderid, 0, newsellordertime, profit,
                                     netprofit, strategyId, buyorderid))
                    else:
                        sellinsertsql = "insert into t_gridtrade_copy (userUuid,apiAccountId,strategyId," \
                                        "followstrategyId,platform,symbol,sellprice,sellcount,sellorderid,sellstatus," \
                                        "sellordertime,profit,netprofit,uniqueId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                                        "%s,%s,%s,%s,%s); "
                        cur.execute(sellinsertsql,
                                    (userUuid, apiAccountId, strategyId, followstrategyId, platform, symbol, sell_price,
                                     amount, newsellorderid, 0, newsellordertime, profit, netprofit, 1))
                # 3、新挂一个买单（比之前的买单小一个网格）
                newbuyordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                newbuyparams = {"direction": 1, "amount": amount, "symbol": symbol, "platform": platform,
                                "price": buy_price, "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 4,
                                "strategyId": strategyId}
                res1 = requests.post(Trade_url, data=newbuyparams)
                dict1 = json.loads(res1.content.decode())
                buycode = dict1["code"]
                if buycode != 1:
                    buyerrorinfo = "您的{}网格策略{}跟单资金不足或者交易所接口不通，导致网格策略下买单失败，请停止当前策略，可在调整资金后再重新创建开启".format(strategyId,
                                                                                                         valueCoinName)
                    print(buyerrorinfo)
                    # logger.info(buyerrorinfo)
                    # 资产不足信息储存到redis
                    remainerrordata = {"strategyId": strategyId, 'followstrategyId': followstrategyId,
                                       "platform": platform, "coin": valueCoinName,
                                       "marktime": int(time.time() * 1000), "apiAccountId": apiAccountId,
                                       "userUuid": userUuid}
                    r2.hset("errormark", strategyId, json.dumps(remainerrordata))
                elif buycode == 1:
                    i = "用户{}策略{}平台{}新挂买单{}".format(userUuid, strategyId, platform, dict1)
                    logger.info(i)
                    print(i)
                    newtradeinfo = "量化策略{}跟踪部署新的网格订单，卖单价{}，买单价{}".format(strategyId, sell_price, buy_price)
                    # r.lpush("gridinfo:{}".format(userId), newtradeinfo)  # 信息存储到redis
                    newbuyorderid = dict1["response"]["orderid"]
                    buyinsertsql = "insert into t_gridtrade_copy (userUuid,apiAccountId,strategyId,followstrategyId," \
                                   "platform,symbol,buyprice,buycount,buyorderid,buystatus,buyordertime," \
                                   "uniqueId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s); "
                    cur.execute(buyinsertsql, (
                        userUuid, apiAccountId, strategyId, followstrategyId, platform, symbol, buy_price, amount,
                        newbuyorderid, 0, newbuyordertime, 1))
            except Exception as e:
                i = "用户{}跟踪{}网格策略{},网格单成交后重新部署出错{}".format(userUuid, followstrategyId, strategyId, e)
                print(i)
                logger.error(i)
            finally:
                conn.commit()
                cur.close()
                conn.close()
    except Exception as e:
        i = "网格跟踪{}策略{}运行时出错{}".format(followstrategyId, strategyId, e)
        logger.info(i)
        # updateparams = {'strategyId': strategyId, "status": 4}
        # requests.post(updateCopy_url, data=updateparams)


def grid_stop(tracer_info):
    strategyId = tracer_info['strategyId']
    followstrategyId = tracer_info['followStrategyId']
    initialCoin = tracer_info['initialCoin']
    userUuid = tracer_info['userUuid']
    apiAccountId = tracer_info['apiAccountId']
    platform = tracer_info['platform']
    symbol = tracer_info['symbol']
    conn = POOL.connection()
    cur = conn.cursor()
    profit = 0
    profitrate = 0
    netprofit = 0
    netprofitrate = 0
    try:
        cur.execute(
            "select sellorderid from t_gridtrade_copy where strategyId=%s and followstrategyId=%s and sellstatus=0",
            (strategyId, followstrategyId))
        selectres1 = cur.fetchall()  # 选出未成交的卖单id
        cur.execute(
            "select buyorderid from t_gridtrade_copy where strategyId=%s and followstrategyId=%s and buystatus=0",
            (strategyId, followstrategyId))
        selectres2 = cur.fetchall()  # 选出未成交的买单id
        sellorderlist = [i[0] for i in selectres1]  # 未成交卖单
        buyorderlist = [i[0] for i in selectres2]  # 未成交买单
        cur.execute(
            "select sum(profit),sum(netprofit) from t_gridtrade_copy where strategyId=%s and followstrategyId=%s and sellstatus=1 and ("
            "buystatus=1 or buystatus is NULL)",
            (strategyId, followstrategyId))  # 计算总利润
        profitres = cur.fetchone()
        if profitres != (None, None):
            profit = round(float(profitres[0]), 4)  # 网格收益
            netprofit = round(float(profitres[1]), 4)
            netprofitrate = round(netprofit / initialCoin, 6)  # 网格净收益（扣除手续费)
        # 查询策略未成交单是否成交,没有成交则撤单
        tlist = []
        for orderid in sellorderlist + buyorderlist:
            tlist.append(Thread(target=cancelcopyorders,
                                args=(userUuid, apiAccountId, strategyId, platform, symbol, orderid,
                                      sellorderlist, buyorderlist)))
        for t in tlist:
            t.start()
        for t in tlist:
            t.join()
        profitinfo = "手动停止或者防错停止网格策略{}，统计收益：网格收益{}，净收益{}".format(strategyId, profit, netprofit)
        print(profitinfo)
    except Exception as e:
        print("用户{}跟单策略{}撤销网格单失败{}".format(userUuid, strategyId, e))
    finally:
        cur.close()
        conn.close()
        return netprofit, netprofitrate


if __name__ == "__main__":
    while True:
        strategyIdList = r4.keys()
        for followstrategyId in strategyIdList:
            tracer_list = r4.hvals(followstrategyId)
            tracer_list = [json.loads(i) for i in tracer_list]
            strategytype = tracer_list[0]['strategyType']
            order_param = r5.hget('order_param_{}'.format(strategytype), followstrategyId)
            if not tracer_list:
                continue
            T = []
            for tracer_info in tracer_list:
                strategyId = tracer_info['strategyId']
                followstrategyId = tracer_info['followStrategyId']
                if not order_param:
                    continue
                if isinstance(order_param, str):
                    order_param = json.loads(order_param)
                if strategytype == 1:  # 网格策略
                    T.append(Thread(target=grid_strategy_copy, args=(tracer_info, order_param,)))
            for t in T:
                t.start()
            for t in T:
                t.join()
            r5.hdel('order_param_{}'.format(strategytype), followstrategyId)
        time.sleep(1)

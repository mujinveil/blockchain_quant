# encoding=utf-8
from threading import Thread
import sys
sys.path.append("..")
from loggerConfig import logger
from tools.Config import Cancel_url, amountlimit, premiumdict, Trade_url, Queryorder_url, Query_tradeprice_url, \
    pricelimit, updateGridRate
from tools.databasePool import r2, r5, POOL_grid
from tools.get_market_info import *


# 撤销网格单
def cancelgridorders(userUuid, apiAccountId, strategyId, platform, symbol, orderid, sellorderlist, buyorderlist):
    conn = POOL_grid.connection()
    cur = conn.cursor()
    try:
        if orderid in sellorderlist:
            res = requests.post(Cancel_url, data={"direction": 2, "symbol": symbol, "platform": platform,
                                                  "orderId": orderid, "apiAccountId": apiAccountId,
                                                  "userUuid": userUuid,
                                                  "source": 4, "strategyId": strategyId, "icebergId": strategyId})
            cur.execute("update t_gridtrade set sellstatus=2 where strategyId=%s and sellorderid=%s",
                        (strategyId, orderid))
        if orderid in buyorderlist:
            res = requests.post(Cancel_url, data={"direction": 1, "symbol": symbol, "platform": platform,
                                                  "orderId": orderid, "apiAccountId": apiAccountId,
                                                  "userUuid": userUuid,
                                                  "source": 4, "strategyId": strategyId, "icebergId": strategyId})
            cur.execute("update t_gridtrade set buystatus=2 where strategyId=%s and buyorderid=%s",
                        (strategyId, orderid))
    except Exception as e:
        i = "系统正在为用户{}撤销{}平台订单{}出错{}".format(userUuid, platform, orderid, e)
        logger.error(i)
    finally:
        conn.commit()
        cur.close()
        conn.close()


# 平仓处理
def clear_grid_remain(userUuid, apiAccountId, strategyId, platform, symbol, amount):
    try:
        print("==============================下单======================================")
        amount = amount
        try:
            x, y = str(amount).split('.')
            amount = float(x + '.' + y[0:amountlimit[symbol][platform]])
        except Exception as e:
            info = "单笔下单量为整数，无需截取小数位"
            print(info)
        current_price = get_currentprice1(platform, symbol)
        price = current_price - premiumdict[symbol]
        tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        tradeparams = {"direction": 2, "amount": amount, "symbol": symbol, "platform": platform, "price": price,
                       "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 4, "strategyId": strategyId,
                       "uniqueId": 2, "icebergId": strategyId}
        traderes = requests.post(Trade_url, data=tradeparams)
        trade_dict = json.loads(traderes.content.decode())
        orderid = trade_dict["response"]["orderid"]  # 获取订单id

        print("=================================查询===================================")
        queryparams = {"direction": 2, "symbol": symbol, "platform": platform, "orderId": orderid,
                       "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 4, "strategyId": strategyId,
                       "icebergId": strategyId}
        res = requests.post(Queryorder_url, data=queryparams)
        queryresdict = json.loads(res.content.decode())
        numberDeal = float(queryresdict["response"]["numberDeal"])
        try:
            queryparams = {"platform": platform, "symbol": symbol, "orderId": orderid, "apiId": apiAccountId,
                           "userUuid": userUuid}
            res = requests.post(Query_tradeprice_url, data=queryparams)
            queryresdict = json.loads(res.content.decode())
            if queryresdict["response"]["avgPrice"] != None:
                price = queryresdict["response"]["avgPrice"]
            if queryresdict["response"]["createdDate"] != None:
                tradetime = queryresdict["response"]["createdDate"]
        except Exception as e:
            pass
        print("================================撤单====================================")
        cancelparams = {"direction": 2, "symbol": symbol, "platform": platform, "orderId": orderid,
                        "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 4, "strategyId": strategyId,
                        "icebergId": strategyId}
        requests.post(Cancel_url, data=cancelparams)

        conn = POOL_grid.connection()
        cur = conn.cursor()
        sellinsertsql = "insert into t_gridtrade (userUuid,apiAccountId,strategyId,platform,symbol,sellprice,sellcount,sellorderid,sellstatus,sellordertime,uniqueId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        cur.execute(sellinsertsql,
                    (userUuid, apiAccountId, strategyId, platform, symbol, price, numberDeal, orderid, 1, tradetime, 3))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        errorinfo = "策略{}停止平仓时出错{}".format(strategyId, e)
        logger.error(errorinfo)


#  量化策略2网格部署（无密度单）
def gridbegin2(griddata, userUuid, strategyId):
    info1 = "量化策略{}正在部署初始网格单...".format(strategyId)
    print(info1)
    apiAccountId = griddata["apiAccountId"]
    platform = griddata["platform"]
    symbol = griddata["symbol"]
    costBuyprice = griddata["entryPrice"]  # 买入成本价
    gap = griddata["gridSpacing"]  # 网格间距
    makerFee = griddata["makerFee"]  # 挂单手续费
    count = griddata["minTradeQuantity"]  # 每次交易量
    # 开始部署网格
    try:
        conn = POOL_grid.connection()
        cur = conn.cursor()
        sellprice = round((costBuyprice + gap), pricelimit[symbol][platform])
        buyprice = round((costBuyprice - gap), pricelimit[symbol][platform])
        # 将初始网格买卖单参数传给跟单策略
        order_param = {'amount': count, 'sell_price': sellprice, 'buy_price': buyprice, 'gap': gap,
                       'makerFee': makerFee, 'flag': 0}
        r5.hset('order_param_1', strategyId, json.dumps(order_param))
        # 1、往上部署一个卖单
        sell_dict = {"direction": 2, "amount": count, "symbol": symbol, "platform": platform,
                     "price": sellprice, "apiAccountId": apiAccountId, "userUuid": userUuid,
                     "source": 4, "strategyId": strategyId, "icebergId": strategyId}
        print(sell_dict)
        res_sell = requests.post(Trade_url, data=sell_dict)
        trade_sell_dict = json.loads(res_sell.content.decode())
        print(trade_sell_dict)
        # trade_sell_info = "网格{}初始化部署卖单{}".format(strategyId, trade_sell_dict)
        # print(trade_sell_info)
        sellorderid = trade_sell_dict["response"]["orderid"]  # 获取订单id
        info2 = "网格{}初始化部署卖单委托成功，交易平台：{}，价格：{}，数量{}".format(strategyId, platform, sellprice, count)
        print(info2)
        logger.info(info2)
        profit = round(gap * count, 8)
        netprofit = round(profit - costBuyprice * count * makerFee - sellprice * count * makerFee, 8)
        sellordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        sellinsertsql = "insert into t_gridtrade (userUuid,apiAccountId,strategyId,platform,symbol,sellprice,sellcount,sellorderid,sellstatus,sellordertime,profit,netprofit,uniqueId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        cur.execute(sellinsertsql,
                    (userUuid, apiAccountId, strategyId, platform, symbol, sellprice, count, sellorderid, 0,
                     sellordertime, profit, netprofit, 1))
        selldata = {"userUuid": userUuid, "apiAccountId": apiAccountId, "strategyId": strategyId, "platform": platform,
                    "symbol": symbol, "count": count, "sellprice": sellprice, "sellorderid": sellorderid}
        key_sell = "grid2:sell:" + str(strategyId)
        r2.set(key_sell, json.dumps(selldata))  # 存入redis数据库
        # 2、往下部署一个买单
        buy_dict = {"direction": 1, "amount": count, "symbol": symbol, "platform": platform, "price": buyprice,
                    "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 4, "strategyId": strategyId,
                    "icebergId": strategyId}
        res_buy = requests.post(Trade_url, data=buy_dict)
        trade_buy_dict = json.loads(res_buy.content.decode())
        print(trade_buy_dict)
        logger.info("网格{}初始化部署买单{}".format(strategyId, trade_buy_dict))
        buyorderid = trade_buy_dict["response"]["orderid"]  # 获取订单id
        info3 = "网格{}初始化部署买单委托成功，交易平台：{}，价格：{}，数量{}".format(strategyId, platform, buyprice, count)
        print(info3)
        logger.info(info3)
        # r.lpush("gridinfo:{}".format(userId), info3)  # 信息存储到redis
        buyordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        buyinsertsql = "insert into t_gridtrade (userUuid,apiAccountId,strategyId,platform,symbol,buyprice,buycount,buyorderid,buystatus,buyordertime,uniqueId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        cur.execute(buyinsertsql, (
            userUuid, apiAccountId, strategyId, platform, symbol, buyprice, count, buyorderid, 0, buyordertime, 1))
        buydata = {"userUuid": userUuid, "apiAccountId": apiAccountId, "strategyId": strategyId, "platform": platform,
                   "symbol": symbol, "count": count, "buyprice": buyprice, "buyorderid": buyorderid}
        key_buy = "grid2:buy:" + str(strategyId)
        r2.set(key_buy, json.dumps(buydata))
        conn.commit()
        cur.close()
        conn.close()
        return 1
    except Exception as e:
        i = "用户{}，初步部署网格策略{}，报错信息{}".format(userUuid, strategyId, e)
        print(i)
        logger.error(i)
        return 0


# 网格策略
def gridStrategy(griddata):
    try:
        strategyType = griddata["strategyType"]
        # 量化2的逻辑
        if strategyType == 2:
            userUuid = griddata["userUuid"]  # 用户id
            apiAccountId = griddata["apiAccountId"]  # 子账户id
            strategyId = griddata["strategyId"]
            platform = griddata["platform"]  # 平台
            symbol = griddata["symbol"]  # 交易对
            entryPrice = griddata["entryPrice"]  # 入场价格
            counterCoinName = symbol.split("_")[0]  # 交易币
            valueCoinName = symbol.split("_")[1]  # 计价币
            # initialTradeCoin = griddata["initialTradeCoin"]  # 用户用于网格策略的交易币数量
            # initialValCoin = griddata["initialValCoin"]  # 用于运行策略的计价币数量
            initialCoin = griddata["existingUsdt"]  # 折合成计价币
            gap = griddata["gridSpacing"]  # 网格间距
            count = griddata["minTradeQuantity"]  # 每次下单量
            highprice = griddata["profitCeiling"]  # 止盈位
            lowprice = griddata["stopLossPrice"]  # 支撑位，止损
            makerFee = griddata["makerFee"]  # 挂单手续费
            createTime = griddata["createTime"]  # 策略开启时间
            # currentprice = \
            #     [i["currentprice"] for i in currentpricelist if i["platform"] == platform and i["symbol"] == symbol][0]
            info = "量化策略{}正在执行中...".format(strategyId)
            print(info)
            # if currentprice == 0:  # 如果当前价为0，则取其他平台均价，如果其他平台也为0，则再取单个平台当前价
            #     currentpricelistnew = [i["currentprice"] for i in currentpricelist if
            #                            i["symbol"] == symbol and i["currentprice"] != 0]
            #     if len(currentpricelistnew) != 0:
            #         currentprice = round(sum(currentpricelistnew) / len(currentpricelistnew),
            #                              pricelimit[symbol][platform])
            #     else:
            currentprice = get_currentprice1(platform, symbol)
            print("{}平台{}交易对当前价格：{}".format(platform, symbol, currentprice))
            # 一、如果当前价小于等于止损价，止损出场
            if currentprice != 0 and currentprice <= lowprice + gap:  # 防止继续下单导致资金不足
                info1 = "行情到达止损价，暂停量化策略{}，您可以选择手动停止该策略，或者等待行情回调至网格区间继续运行。".format(strategyId)
                print(info1)
                logger.info(info1)
                # pausetags = r2.hget("gridpausetags", strategyId)
                # if pausetags != "1":  # 如果之前标记不为1，则发送短信提示，并将状态改为1
                #     # res = requests.get(sendmessage_url, params={"userUuid": userUuid, "apiAccountId": apiAccountId, "strategyName": "量化", "runType": ""})
                #     r2.hset("gridpausetags", strategyId, 1)
                # updateparams = {'strategyId': strategyId, "status": 2}
                # res1 = requests.post(updateGridStatus, data=updateparams)
                # print(json.loads(res1.content.decode()))
            elif currentprice >= highprice - gap:  # 防止继续下单导致资金不足
                info2 = "行情上涨，已超出您的资金范围，暂停量化策略{}，您可以选择手动停止该策略，或者等待行情回调至网格区间继续运行。".format(strategyId)
                print(info2)
                logger.info(info2)
                # pausetags = r2.hget("gridpausetags", strategyId)
                # if pausetags != "2":  # 如果之前标记不为2，则发送短信提示，并将状态改为2
                #     # res = requests.get(sendmessage_url, params={"userUuid": userUuid, "apiAccountId": apiAccountId, "strategyName": "量化", "runType": "", "clientSource": 1,
                #     #                                             "version": version, "client": "python", "ticket": ticket})
                #     r2.hset("gridpausetags", strategyId, 2)
                # updateparams = {'strategyId': strategyId, "status": 2}
                # res1 = requests.post(updateGridStatus, data=updateparams)
                # print(json.loads(res1.content.decode()))
            # 二、查询卖单和买单，并根据成交情况重新部署
            else:
                # 获取网格交易卖单和买单列表
                sellnum = 0
                buynum = 0
                grid_sell_order = json.loads(r2.get("grid2:sell:" + str(strategyId)))
                print("网格{}卖单".format(strategyId), grid_sell_order)
                grid_buy_order = json.loads(r2.get("grid2:buy:" + str(strategyId)))
                print("网格{}买单".format(strategyId), grid_buy_order)
                sellquerydict = {}
                try:
                    sell_query = {"direction": 2, "symbol": symbol, "platform": platform,
                                  "orderId": grid_sell_order["sellorderid"],
                                  "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 4,
                                  "strategyId": strategyId, "icebergId": strategyId}
                    sellqueryres = requests.post(Queryorder_url, data=sell_query)
                    sellquerydict = json.loads(sellqueryres.content.decode())
                    print(sellquerydict)
                    sellnum = float(sellquerydict["response"]["numberDeal"])
                    sellinfo = "用户{}交易平台{},最小卖单订单号{},价格{},已成交量{}".format(userUuid, platform,
                                                                         grid_sell_order["sellorderid"],
                                                                         grid_sell_order["sellprice"], sellnum)
                    print(sellinfo)
                except Exception as e:
                    i = "用户{}网格策略{}交易平台{}查询订单{}出错{}，{}".format(userUuid, strategyId, platform,
                                                               grid_sell_order["sellorderid"], e, sellquerydict)
                    print(i)
                    logger.error(i)
                if sellnum != count:  # 如果卖单没有完全成交，则查询买单是否成交，否则不查询
                    buyquerydict = {}
                    try:
                        buy_query = {"direction": 1, "symbol": symbol, "platform": platform,
                                     "orderId": grid_buy_order["buyorderid"],
                                     "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 4,
                                     "strategyId": strategyId, "icebergId": strategyId}
                        buyqueryres = requests.post(Queryorder_url, data=buy_query)
                        buyquerydict = json.loads(buyqueryres.content.decode())
                        print(buyquerydict)
                        buynum = float(buyquerydict["response"]["numberDeal"])
                        buyinfo = "用户{}交易平台{},最大买单订单号{},价格{},已成交量{}".format(userUuid, platform,
                                                                            grid_buy_order["buyorderid"],
                                                                            grid_buy_order["buyprice"], buynum)
                        print(buyinfo)
                    except Exception as e:
                        i = "用户{}网格策略{}交易平台{}查询订单{}出错{},{}".format(userUuid, strategyId, platform,
                                                                   grid_buy_order["buyorderid"], e, buyquerydict)
                        print(i)
                        logger.error(i)

                # 如果卖单成交
                if sellnum == count:
                    conn = POOL_grid.connection()
                    cur = conn.cursor()
                    try:
                        finishsellprice = grid_sell_order["sellprice"]  # 已经成交单的价格
                        newsellprice = round((finishsellprice + gap), pricelimit[symbol][platform])
                        newbuyprice = round((finishsellprice - gap), pricelimit[symbol][platform])
                        # 将下单参数传给跟单策略
                        order_param = {"flag": 1, "makerFee": makerFee, "gap": gap, "sell_price": newsellprice,
                                       "buy_price": newbuyprice, "amount": count}
                        r5.hset('order_param_1', strategyId, json.dumps(order_param))
                        info2 = "量化策略{}已成交一个卖单，交易平台{}，成交价{}，成交数量{}，正在为您部署新的网格...".format(strategyId, platform,
                                                                                         finishsellprice, sellnum)
                        print(info2)
                        logger.info(info2)
                        # 0.更新策略利润，返回给java端
                        cur.execute(
                            "select sum(profit),sum(netprofit) from t_gridtrade where strategyId=%s and sellstatus=1 "
                            "and (buystatus=1 or buystatus is NULL)", (strategyId,))
                        profitres = cur.fetchone()
                        profit = netprofit = netprofitrate = 0
                        if profitres != (None, None):
                            profit = round(float(profitres[0]), 4)  # 网格收益
                            netprofit = round(float(profitres[1]), 4)
                            netprofitrate = round(netprofit / initialCoin, 6)  # 网格净收益（扣除手续费)
                        params = {'strategyId': strategyId, 'netprofit': netprofit, 'netprofitRate': netprofitrate}
                        res = requests.post(updateGridRate, data=params)
                        resdict = json.loads(res.content.decode())
                        print(resdict)
                        # 1、改变卖单状态
                        selltradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        cur.execute(
                            'update t_gridtrade set sellstatus=%s,selltradetime=%s where strategyId=%s and sellorderid=%s',
                            (1, selltradetime, strategyId, grid_sell_order["sellorderid"]))
                        # 2、撤销对应的买单
                        cancelbuyordrinfo = json.loads(r2.get("grid2:buy:" + str(strategyId)))
                        cancelres = requests.post(Cancel_url,
                                                  data={"direction": 1, "symbol": symbol, "platform": platform,
                                                        "orderId": cancelbuyordrinfo["buyorderid"],
                                                        "apiAccountId": apiAccountId,
                                                        "userUuid": userUuid, "source": 4, "strategyId": strategyId,
                                                        "icebergId": strategyId})
                        cancelinfo = "用户{}策略{}平台{}撤销买单{}，返回结果{}".format(userUuid, strategyId, platform,
                                                                        cancelbuyordrinfo, cancelres.text)
                        # logger.info(cancelinfo)
                        print(cancelinfo)
                        deletebuysql = "delete from t_gridtrade where strategyId=%s and buyorderid=%s"
                        cur.execute(deletebuysql, (strategyId, cancelbuyordrinfo["buyorderid"]))
                        # 3、新挂一个卖单（比最大的卖单大一个网格，此时要注意余额是否足够)
                        newsellordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        newsellparams = {"direction": 2, "amount": count, "symbol": symbol, "platform": platform,
                                         "price": newsellprice, "apiAccountId": apiAccountId, "userUuid": userUuid,
                                         "source": 4, "strategyId": strategyId, "icebergId": strategyId}
                        res2 = requests.post(Trade_url, data=newsellparams)
                        dict2 = json.loads(res2.content.decode())
                        i = "用户{}策略{}平台{}新挂卖单{}".format(userUuid, strategyId, platform, dict2)
                        logger.info(i)
                        print(i)
                        sellcode = dict2["code"]  # 卖单状态
                        if sellcode != 1:
                            sellerrorinfo = "您的{}平台{}资金不足或者交易所接口不通，导致网格策略下卖单失败，可在调整资金后再重新创建开启".format(platform,
                                                                                                      counterCoinName)
                            print(sellerrorinfo)
                            logger.info(sellerrorinfo)
                            # 资产不足信息储存到redis
                            remainerrordata = {"strategyId": strategyId, "apiAccountId": apiAccountId,
                                               "userUuid": userUuid, "platform": platform,
                                               "coin": counterCoinName, "marktime": int(time.time() * 1000)}
                            r2.hset("errormark", strategyId, json.dumps(remainerrordata))
                        elif sellcode == 1:
                            newsellorderid = dict2["response"]["orderid"]
                            cur.execute(
                                "select * from t_gridtrade where strategyId=%s and sellprice=%s and sellstatus=2",
                                (strategyId, newsellprice))
                            selectres = cur.fetchall()
                            if len(selectres) == 1:
                                updatesql2 = "update t_gridtrade set sellorderid=%s,sellstatus=%s,sellordertime=%s " \
                                             "where strategyId=%s and sellprice=%s and sellstatus=2 "
                                cur.execute(updatesql2, (
                                    newsellorderid, 0, newsellordertime, strategyId, newsellprice))
                            elif len(selectres) == 0:
                                profit = round((newsellprice - entryPrice) * count, 8)
                                netprofit = round(profit - entryPrice * count * makerFee - newsellprice * count * \
                                                  makerFee, 8)
                                if profit <= 0:
                                    profit = -1 * profit
                                if netprofit <= 0:
                                    netprofit = -1 * netprofit
                                # sellinsertsql = "insert into t_gridtrade (userUuid,apiAccountId,strategyId,platform,symbol,sellprice,sellcount,sellorderid,sellstatus,sellordertime,profit,netprofit,uniqueId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                                # cur.execute(sellinsertsql, (
                                #     userUuid, apiAccountId, strategyId, platform, symbol, newsellprice, count, newsellorderid, 0,
                                #     newsellordertime, profit, netprofit, 1))
                                # 这种情形与卖单对应的是入场价的买单
                                sellinsertsql = "insert into t_gridtrade (userUuid,apiAccountId,strategyId,platform," \
                                                "symbol,buyprice,buycount,buystatus,buyordertime,buytradetime," \
                                                "sellprice,sellcount,sellorderid,sellstatus,sellordertime,profit," \
                                                "netprofit,uniqueId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                                                "%s,%s,%s,%s,%s); "
                                cur.execute(sellinsertsql, (
                                    userUuid, apiAccountId, strategyId, platform, symbol, entryPrice, count, 1,
                                    createTime, createTime, newsellprice, count, newsellorderid, 0, newsellordertime,
                                    profit, netprofit, 1))
                            # 4、新挂一个买单（比最新成交价小一个网格间距)
                            newbuyordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                            newbuyparams = {"direction": 1, "amount": count, "symbol": symbol, "platform": platform,
                                            "price": newbuyprice,
                                            "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 4,
                                            "strategyId": strategyId, "icebergId": strategyId}
                            res1 = requests.post(Trade_url, data=newbuyparams)
                            dict1 = json.loads(res1.content.decode())
                            i = "用户{}策略{}平台{}新挂买单{}".format(userUuid, strategyId, platform, dict1)
                            # logger.info(i)
                            print(i)
                            buycode = dict1["code"]
                            if buycode != 1:
                                buyerrorinfo = "您的{}平台{}资金不足或者交易所接口不通，导致网格策略下买单失败，请停止当前策略，可在调整资金后再重新创建开启".format(
                                    platform,
                                    valueCoinName)
                                print(buyerrorinfo)
                                logger.info(buyerrorinfo)
                                # 资产不足信息储存到redis
                                remainerrordata = {"strategyId": strategyId, "apiAccountId": apiAccountId,
                                                   "userUuid": userUuid, "platform": platform,
                                                   "coin": valueCoinName, "marktime": int(time.time() * 1000)}
                                r2.hset("errormark", strategyId, json.dumps(remainerrordata))
                            elif buycode == 1:
                                newtradeinfo = "量化策略{}部署新的网格订单，卖单价{}，买单价{}".format(strategyId, newsellprice,
                                                                                   newbuyprice)
                                print(newtradeinfo)
                                logger.info(newtradeinfo)
                                newbuyorderid = dict1["response"]["orderid"]
                                buyinsertsql = "insert into t_gridtrade (userUuid,apiAccountId,strategyId,platform,symbol,buyprice,buycount,buyorderid,buystatus,buyordertime,uniqueId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                                cur.execute(buyinsertsql, (
                                    userUuid, apiAccountId, strategyId, platform, symbol, newbuyprice, count,
                                    newbuyorderid, 0, newbuyordertime, 1))
                                # 5、修改内存中订单信息
                                selldata = {"userUuid": userUuid, "apiAccountId": apiAccountId,
                                            "strategyId": strategyId, "platform": platform,
                                            "symbol": symbol, "count": count, "sellprice": newsellprice,
                                            "sellorderid": newsellorderid}
                                buydata = {"userUuid": userUuid, "apiAccountId": apiAccountId, "strategyId": strategyId,
                                           "platform": platform,
                                           "symbol": symbol, "count": count, "buyprice": newbuyprice,
                                           "buyorderid": newbuyorderid}
                                key_sell = "grid2:sell:" + str(strategyId)
                                key_buy = "grid2:buy:" + str(strategyId)
                                r2.set(key_sell, json.dumps(selldata))
                                r2.set(key_buy, json.dumps(buydata))
                                print("修改redis缓存中的订单")
                    except Exception as e:
                        i = "用户{}策略{}网格单成交后重新部署出错{}".format(userUuid, strategyId, e)
                        print(i)
                        logger.error(i)
                    finally:
                        conn.commit()
                        cur.close()
                        conn.close()

                # 如果买单成交(此处还要判断卖单是否成交，极端行情短时间内买卖单都会成交 )
                if buynum == count:
                    conn = POOL_grid.connection()
                    cur = conn.cursor()
                    try:
                        finishbuyprice = grid_buy_order["buyprice"]
                        newsellprice = round((finishbuyprice + gap), pricelimit[symbol][platform])
                        newbuyprice = round((finishbuyprice - gap), pricelimit[symbol][platform])
                        # 将下单参数传给跟单策略
                        order_param = {"flag": 2, "makerFee": makerFee, "gap": gap, "sell_price": newsellprice,
                                       "buy_price": newbuyprice, "amount": count}
                        r5.hset('order_param_1', strategyId, json.dumps(order_param))
                        info3 = "量化策略{}已成交一个买单，交易平台{}，成交价{}，成交数量{}，正在为您部署新的网格...".format(strategyId, platform,
                                                                                         finishbuyprice, buynum)
                        logger.info(info3)
                        print(info3)
                        buytradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        # 1、撤销卖单
                        cancelsellorderinfo = json.loads(r2.get("grid2:sell:" + str(strategyId)))
                        cancelres1 = requests.post(Cancel_url,
                                                   data={"direction": 2, "symbol": symbol, "platform": platform,
                                                         "orderId": cancelsellorderinfo["sellorderid"],
                                                         "apiAccountId": apiAccountId,
                                                         "userUuid": userUuid, "source": 4, "strategyId": strategyId,
                                                         "icebergId": strategyId})
                        cancelinfo1 = "用户{}策略{}平台{}撤销卖单{}，返回结果{}".format(userUuid, strategyId, platform,
                                                                         cancelsellorderinfo, cancelres1.text)
                        # logger.info(cancelinfo1)
                        print(cancelinfo1)
                        updatasql3 = "update t_gridtrade set sellstatus=2 where strategyId=%s and sellorderid=%s"
                        cur.execute(updatasql3, (strategyId, cancelsellorderinfo["sellorderid"]))
                        # 2、新挂一个卖单，比最新成交价高一个网格间距，加到数据库对应的买单后边
                        newsellprice = round((finishbuyprice + gap), pricelimit[symbol][platform])
                        newsellordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        newsellparams = {"direction": 2, "amount": count, "symbol": symbol, "platform": platform,
                                         "price": newsellprice,
                                         "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 4,
                                         "strategyId": strategyId, "icebergId": strategyId}
                        res2 = requests.post(Trade_url, data=newsellparams)
                        dict2 = json.loads(res2.content.decode())
                        i = "用户{}策略{}平台{}新挂卖单{}".format(userUuid, strategyId, platform, dict2)
                        logger.info(i)
                        print(i)
                        sellcode = dict2["code"]  # 卖单状态
                        if sellcode != 1:
                            sellerrorinfo = "您的{}平台{}资金不足或者交易所接口不通，导致网格策略下卖单失败，请停止当前策略，可在调整资金后再重新创建开启".format(platform,
                                                                                                              counterCoinName)
                            print(sellerrorinfo)
                            # 资产不足信息储存到redis
                            remainerrordata = {"strategyId": strategyId, "apiAccountId": apiAccountId,
                                               "userUuid": userUuid, "platform": platform,
                                               "coin": counterCoinName,
                                               "marktime": int(time.time() * 1000)}
                            r2.hset("errormark", strategyId, json.dumps(remainerrordata))
                        elif sellcode == 1:
                            newsellorderid = dict2["response"]["orderid"]
                            profit = round(gap * count, 8)
                            netprofit = round(profit - (newsellprice - gap) * count * makerFee - newsellprice * count * \
                                              makerFee, 8)
                            updatesql1 = "update t_gridtrade set buystatus=%s,buytradetime=%s,sellprice=%s,sellcount=%s,sellorderid=%s,sellstatus=%s,sellordertime=%s,profit=%s,netprofit=%s where strategyId=%s and buyorderid=%s "
                            cur.execute(updatesql1, (
                                1, buytradetime, newsellprice, count, newsellorderid, 0, newsellordertime, profit,
                                netprofit,
                                strategyId, grid_buy_order["buyorderid"]))
                            # 3、新挂一个买单（比之前的买单小一个网格）
                            newbuyprice = round((finishbuyprice - gap), pricelimit[symbol][platform])
                            newbuyordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                            newbuyparams = {"direction": 1, "amount": count, "symbol": symbol, "platform": platform,
                                            "price": newbuyprice,
                                            "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 4,
                                            "strategyId": strategyId, "icebergId": strategyId}
                            res1 = requests.post(Trade_url, data=newbuyparams)
                            dict1 = json.loads(res1.content.decode())
                            i = "用户{}策略{}平台{}新挂买单{}".format(userUuid, strategyId, platform, dict1)
                            logger.info(i)
                            print(i)
                            buycode = dict1["code"]
                            if buycode != 1:
                                buyerrorinfo = "您的{}平台{}资金不足或者交易所接口不通，导致网格策略下买单失败，请停止当前策略，可在调整资金后再重新创建开启".format(
                                    platform,
                                    valueCoinName)
                                print(buyerrorinfo)
                                logger.info(buyerrorinfo)
                                # r.lpush("gridinfo:{}".format(userId), buyerrorinfo)  # 信息存储到redis
                                # 资产不足信息储存到redis
                                remainerrordata = {"strategyId": strategyId, "apiAccountId": apiAccountId,
                                                   "userUuid": userUuid, "platform": platform,
                                                   "coin": valueCoinName,
                                                   "marktime": int(time.time() * 1000)}
                                r2.hset("errormark", strategyId, json.dumps(remainerrordata))
                            elif buycode == 1:
                                newtradeinfo = "量化策略{}部署新的网格订单，卖单价{}，买单价{}".format(strategyId, newsellprice,
                                                                                   newbuyprice)
                                # r.lpush("gridinfo:{}".format(userId), newtradeinfo)  # 信息存储到redis
                                newbuyorderid = dict1["response"]["orderid"]
                                buyinsertsql = "insert into t_gridtrade (userUuid,apiAccountId,strategyId,platform,symbol,buyprice,buycount,buyorderid,buystatus,buyordertime,uniqueId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                                cur.execute(buyinsertsql, (
                                    userUuid, apiAccountId, strategyId, platform, symbol, newbuyprice, count,
                                    newbuyorderid, 0, newbuyordertime, 1))
                                # 4、修改内存中订单信息
                                selldata = {"userUuid": userUuid, "apiAccountId": apiAccountId,
                                            "strategyId": strategyId, "platform": platform,
                                            "symbol": symbol,
                                            "count": count, "sellprice": newsellprice, "sellorderid": newsellorderid}
                                buydata = {"userUuid": userUuid, "apiAccountId": apiAccountId, "strategyId": strategyId,
                                           "platform": platform,
                                           "symbol": symbol,
                                           "count": count, "buyprice": newbuyprice, "buyorderid": newbuyorderid}
                                key_sell = "grid2:sell:" + str(strategyId)
                                key_buy = "grid2:buy:" + str(strategyId)
                                r2.set(key_sell, json.dumps(selldata))
                                r2.set(key_buy, json.dumps(buydata))
                                print("修改redis缓存中的订单")
                    except Exception as e:
                        i = "用户{}策略{}网格单成交后重新部署出错{}".format(userUuid, strategyId, e)
                        print(i)
                        logger.error(i)
                    finally:
                        conn.commit()
                        cur.close()
                        conn.close()
    except Exception as e:
        i = "网格策略{}运行时出错{}".format(griddata["strategyId"], e)
        print(i)
        # logger.info(i)


def goDoGridStrategy():
    gridnum = 0
    while True:
        try:
            print("*************************************************************")
            print("网格策略第{}次运行".format(gridnum))
            print("*************************************************************")
            allgridstrategyId = r2.hkeys("gridstrategy")
            strategyIdlist = []
            for strategyId in allgridstrategyId:
                if strategyId[-1] == str(gridnum):
                    strategyIdlist.append(strategyId)
            if not strategyIdlist:
                i = "没有符合条件的策略"
                print(i)
                time.sleep(1)
            else:
                gridThreads = []
                # currentpricelist = get_all_currentprice()  # 获取所有平台所有交易对最新价
                gridstrategydatalist = r2.hmget("gridstrategy", strategyIdlist)
                for griddata in gridstrategydatalist:
                    gridThreads.append(Thread(target=gridStrategy, args=(json.loads(griddata),)))
                for t in gridThreads:
                    t.start()
                for t in gridThreads:
                    t.join()
                del gridThreads
                # del currentpricelist
            del allgridstrategyId
            del strategyIdlist
        except Exception as e:
            info = "网格多线程报错{}".format(e)
            print(info)
            logger.error(info)
        finally:
            time.sleep(1)
            gridnum += 1
            if gridnum == 10:
                gridnum = 0


if __name__ == '__main__':
    goDoGridStrategy()

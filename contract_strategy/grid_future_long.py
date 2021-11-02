# encoding="utf-8"
import json
import time
from threading import Thread
import requests
import sys
sys.path.append("..")
from loggerConfig import logger
from tools.Config import futurepricelimit, contract_size_dict, future_remain_url, future_strategy_status_update, \
    future_makerFee
from tools.databasePool import POOL_grid, r0
from tools.future_trade import contract_usdt_trade, query_contract_usdt_order, cancel_contract_usdt_order
from tools.get_future_market_info import get_perpetualprice


def cancel_future_gridorder(userUuid, apiAccountId, strategyId, symbol,platform, orderid, sellorderlist, buyorderlist):
    conn = POOL_grid.connection()
    cur = conn.cursor()
    try:
        if orderid in sellorderlist:
            cancelres = cancel_contract_usdt_order(userUuid, apiAccountId, symbol,platform, orderid)
            if cancelres['success']:
                cur.execute("update t_contractgrid set sellstatus=2 where strategyId=%s and sellorderid=%s",
                            (strategyId, orderid))
        if orderid in buyorderlist:
            cancelres = cancel_contract_usdt_order(userUuid, apiAccountId,symbol, platform, orderid)
            if cancelres['success']:
                cur.execute("update t_contractgrid set buystatus=2 where strategyId=%s and buyorderid=%s",
                            (strategyId, orderid))
    except Exception as e:
        i = "系统正在为用户{}撤销{}平台订单{}出错{}".format(userUuid, platform, orderid, e)
        print(i)
        logger.error(i)
    finally:
        conn.commit()
        cur.close()
        conn.close()


def clear_future_grid_remain(userUuid, apiAccountId, strategyId, platform, symbol, amount, leverage, direction):
    # 平仓处理
    conn = POOL_grid.connection()
    cur = conn.cursor()
    try:
        current_price = get_perpetualprice(platform, symbol)
        contract_code = "{}-usdt".format(symbol).upper()
        contract_size = contract_size_dict[symbol][platform]
        if direction == "buy":
            sellprice = round(current_price * 0.99, futurepricelimit[symbol][platform])
            sellordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            res1 = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, amount, sellprice, 2, 2, 4, leverage)
            orderId = res1['response']['orderId'].replace('"', "")
            # 清仓下单记录插入数据库
            insertsql = "INSERT INTO t_contractgrid(userUuid,apiAccountId,strategyId,platform,contract_code,contract_size,direction," \
                        "leverage,sellprice,sellcount,sellorderid,sellstatus,sellordertime,uniqueId) VALUES(" \
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            insertdata = (
                userUuid, apiAccountId, strategyId, platform, contract_code, contract_size, direction, leverage,
                sellprice, amount, orderId, 0, sellordertime, 1)
            cur.execute(insertsql, insertdata)
            i="用户{}子账户{}合约网格做多策略{}开始强制平仓，下单记录插入数据库".format(userUuid,apiAccountId,strategyId)
            print(i)
            logger.info(i)
            # 3s后查询订单情况
            time.sleep(3)
            res = query_contract_usdt_order(userUuid, apiAccountId, platform, orderId, symbol)['response']
            order_status = res['status']
            if order_status == "COMPLETED":
                trade_avg_price = float(res['detail'][0]['price'])  # 成交均价
                tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                updatesql = "update t_contractgrid set sellprice=%s,sellstatus=%s,selltradetime=%s where " \
                            "strategyId=%s and sellorderid=%s"
                cur.execute(updatesql, (trade_avg_price, 1, tradetime, strategyId, orderId))
                i = "用户{}子账户{}合约网格做多策略{}清仓成功，成交记录插入数据库".format(userUuid,apiAccountId,strategyId)
                print(i)
                logger.info(i)
            elif order_status == "TRADING":
                res = cancel_contract_usdt_order(userUuid, apiAccountId,symbol,platform, orderId)
                if res['success']:
                    # 取消订单，将数据库订单状态改为2
                    cancel_sql = 'update t_contractgrid set sellstatus=2 where strategyId=%s and sellorderid=%s'
                    cur.execute(cancel_sql, (strategyId, orderId))
                    i="用户{}子账户{}合约网格做多策略{}清仓订单未完全成交，已撤单".format(userUuid,apiAccountId,strategyId)
                    print(i)
                    logger.info(i)
        elif direction == "sell":
            buyprice = round(current_price * 1.01, futurepricelimit[symbol][platform])
            buyordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            res1 = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, amount, buyprice, 1, 2, 2, leverage)
            orderId = res1['response']['orderId'].replace('"', "")
            # 清仓下单记录插入数据库
            insertsql = "INSERT INTO t_contractgrid(userUuid,apiAccountId,strategyId,platform,contract_code,contract_size,direction," \
                        "leverage,buyprice,buycount,buyorderid,buystatus,buyordertime,uniqueId) VALUES(" \
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            insertdata = (
                userUuid, apiAccountId, strategyId, platform, contract_code, contract_size, direction, leverage,
                buyprice, amount,
                orderId, 0, buyordertime, 1)
            cur.execute(insertsql, insertdata)
            i="用户{}子账户{}合约网格做空策略{}开始强制平仓，下单记录插入数据库".format(userUuid,apiAccountId,strategyId)
            print(i)
            logger.info(i)
            # 3s后查询订单情况
            time.sleep(3)
            res = query_contract_usdt_order(userUuid, apiAccountId, platform, orderId, symbol)['response']
            order_status = res['status']
            if order_status == "COMPLETED":
                trade_avg_price = float(res['detail'][0]['price'])  # 成交均价
                tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                updatesql = "update t_contractgrid set buyprice=%s,buystatus=%s,buytradetime=%s where " \
                            "strategyId=%s and buyorderid=%s"
                cur.execute(updatesql, (trade_avg_price, 1, tradetime, strategyId, orderId))
            elif order_status == "TRADING":
                res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol,platform, orderId)
                if res['success']:
                    # 取消订单，将数据库订单状态改为2
                    cancel_sql = 'update t_contractgrid set buystatus=2 where strategyId=%s and buyorderid=%s'
                    cur.execute(cancel_sql, (strategyId, orderId))
    except Exception as e:
        errorinfo = "策略{}停止平仓时出错{}".format(strategyId, e)
        print(errorinfo)
        logger.error(errorinfo)
    finally:
        conn.commit()
        cur.close()
        conn.close()


def openBuyPosition(griddata):
    userUuid = griddata['userUuid']
    apiAccountId = griddata['apiAccountId']
    strategyId = griddata['strategyId']
    platform = griddata['platform']
    symbol = griddata['symbol']
    leverage = griddata['leverage']
    gap = griddata['gap']
    sheets = griddata['sheets']
    high_price = griddata['highprice']
    contract_code = "{}-usdt".format(symbol).upper()
    contract_size = contract_size_dict[symbol][platform]
    current_price = get_perpetualprice(platform, symbol)
    first_sheets = (high_price - current_price) // gap * sheets
    conn = POOL_grid.connection()
    cur = conn.cursor()
    try:
        order_price = round(current_price * 1.01, futurepricelimit[symbol][platform])
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        i = "用户{}子账户{}开启合约网格做多策略{}，开仓下买单".format(userUuid, apiAccountId, strategyId)
        print(i)
        logger.info(i)
        resdict = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, first_sheets, order_price, 1, 2, 1,
                                      leverage)
        orderId = resdict['response']['orderId'].replace('"', "")
        #  将下单信息插入数据库
        insertsql = "INSERT INTO t_contractgrid(userUuid,apiAccountId,strategyId,platform,contract_code,contract_size,direction," \
                    "leverage,buyprice,buycount,buyorderid,buystatus,buyordertime,uniqueId) VALUES(" \
                    "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        insertdata = (
            userUuid, apiAccountId, strategyId, platform, contract_code, contract_size, "buy", leverage, order_price,
            first_sheets, orderId, 0, ordertime, 1)
        cur.execute(insertsql, insertdata)
        # 3s后查询订单情况
        time.sleep(3)
        res = query_contract_usdt_order(userUuid, apiAccountId, platform, orderId, symbol)['response']
        order_status = res['status']
        if order_status == "COMPLETED":
            trade_avg_price = float(res['detail'][0]['price'])  # 成交均价
            griddata['entryPrice'] = trade_avg_price
            r0.hset("longgridstrategy", strategyId, json.dumps(griddata))
            tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            updatesql = "update t_contractgrid set buyprice=%s,buystatus=%s,buytradetime=%s where " \
                        "strategyId=%s and buyorderid=%s"
            cur.execute(updatesql, (trade_avg_price, 1, tradetime, strategyId, orderId))
        elif order_status == "TRADING":
            res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol,platform, orderId)
            if res['success']:
                # 取消订单，将数据库订单状态改为2
                cancel_sql = 'update t_contractgrid set buystatus=2 where strategyId=%s and buyorderid=%s'
                cur.execute(cancel_sql, (strategyId, orderId))
        conn.commit()
        cur.close()
        conn.close()
        return trade_avg_price
    except Exception as e:
        errorinfo = "合约网格做多策略{}开仓时出错{}".format(strategyId, e)
        print(errorinfo)
        logger.error(errorinfo)
        return 0


def longgridbegin(griddata):
    strategyId = griddata['strategyId']
    userUuid = griddata['userUuid']
    apiAccountId = griddata['apiAccountId']
    platform = griddata['platform']
    symbol = griddata['symbol']
    contract_code = "{}-usdt".format(symbol).upper()
    gap = griddata['gap']  # 网格间距
    makerFee = future_makerFee[platform]['buyfee']  # 挂单手续费
    sheets = griddata['sheets']  # 每个网格挂单张数
    leverage = griddata['leverage']  # 杠杆倍数
    contract_size = float(contract_size_dict[symbol][platform])  # 合约面值
    conn = POOL_grid.connection()
    cur = conn.cursor()
    try:
        entryPrice = openBuyPosition(griddata)
        if not entryPrice:
            return 0
        # 1、往上部署一个卖单,由于是挂单不考虑是否会立即成交，所以不需要查询订单记录
        sellPrice = round((entryPrice + gap), futurepricelimit[symbol][platform])
        sellordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        resdict = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, sheets, sellPrice, 2, 1, 4, leverage)
        sellorderId = resdict['response']['orderId'].replace('"', "")
        info1 = "合约网格{}初始化部署卖单委托成功,交易合约{}-usdt,交易平台：{}，价格：{}，数量{}".format(strategyId, symbol, platform, sellPrice, sheets)
        print(info1)
        logger.info(info1)
        # 利润计算,（平仓 - 开仓)*成交合约张数 * 合约面值
        if platform == "binance":
            profit = round(gap * sheets, 8)
            netprofit = round(profit - (sellPrice + entryPrice) * sheets * makerFee, 8)
        else:
            profit = round(gap * sheets * contract_size, 8)  # 利润计算
            # 手续费 成交价*成交合约张数*合约面值*费率
            netprofit = round(profit - (sellPrice + entryPrice) * sheets * contract_size * makerFee, 8)  # 净利润计算
        # 插入数据库，数据库需重新设计
        sellinsertsql = "insert into t_contractgrid (userUuid,apiAccountId,strategyId,platform,contract_code," \
                        "contract_size,direction,leverage,sellprice,sellcount,sellorderid,sellstatus,sellordertime," \
                        "profit,netprofit,uniqueId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s); "
        cur.execute(sellinsertsql,
                    (userUuid, apiAccountId, strategyId, platform, contract_code, contract_size, "buy", leverage,
                     sellPrice, sheets, sellorderId, 0, sellordertime, profit, netprofit, 1))
        selldata = {"userUuid": userUuid, "apiAccountId": apiAccountId, "strategyId": strategyId, "platform": platform,
                    "symbol": symbol, "count": sheets, "sellprice": sellPrice, "sellorderid": sellorderId, }
        key_sell = "grid2:sell:" + str(strategyId)
        r0.set(key_sell, json.dumps(selldata))  # 存入redis数据库
        # 2、往下部署一个买单,由于是挂单不考虑是否会立即成交，所以不需要查询订单记录
        buyPrice = round((entryPrice - gap), futurepricelimit[symbol][platform])
        buyordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        resdict = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, sheets, buyPrice, 1, 1, 1, leverage)
        buyorderId = resdict['response']['orderId'].replace('"', '')
        info2 = "合约网格{}初始化部署买单委托成功,交易合约{}-usdt,交易平台：{}，价格：{}，数量{}".format(strategyId, symbol, platform, buyPrice, sheets)
        print(info2)
        logger.info(info2)
        # 买单不计算利润，插入数据库即可
        buyinsertsql = "insert into t_contractgrid (userUuid,apiAccountId,strategyId,platform,contract_code," \
                       "contract_size,direction,leverage,buyprice,buycount,buyorderid,buystatus,buyordertime," \
                       "uniqueId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s); "
        cur.execute(buyinsertsql,
                    (userUuid, apiAccountId, strategyId, platform, contract_code, contract_size, "buy", leverage,
                     buyPrice, sheets, buyorderId, 0, buyordertime, 1))
        buydata = {"userUuid": userUuid, "apiAccountId": apiAccountId, "strategyId": strategyId, "platform": platform,
                   "symbol": symbol, "count": sheets, "buyprice": buyPrice, "buyorderid": buyorderId}
        key_buy = "grid2:buy:" + str(strategyId)
        r0.set(key_buy, json.dumps(buydata))  # 存入redis数据库
        conn.commit()
        cur.close()
        conn.close()
        return 1
    except Exception as e:
        i = "用户{}，初步部署合约网格策略{}，报错信息{}".format(userUuid, strategyId, e)
        print(i)
        logger.error(i)
        return 0


def future_grid_strategy(griddata):
    try:
        userUuid = griddata["userUuid"]  # 用户id
        apiAccountId = griddata["apiAccountId"]  # 子账户id
        strategyId = griddata["strategyId"]  # 策略id
        platform = griddata["platform"]  # 平台
        symbol = griddata["symbol"]  # 交易对
        entryPrice = griddata["entryPrice"]  # 入场价格
        gap = griddata["gap"]  # 网格间距
        makerFee = future_makerFee[platform]['buyfee']
        sheets = griddata["sheets"]  # 每次下单量
        leverage = griddata['leverage']  # 杠杆倍数
        contract_code = "{}-usdt".format(symbol).upper()
        contract_size = float(contract_size_dict[symbol][platform])  # 合约面值
        initialValCoin = griddata["initialValCoin"]  # 用于运行策略的计价币数量
        createTime = griddata["createTime"]  # 策略开启时间
        highprice = griddata["highprice"]  # 止盈位
        lowprice = griddata["lowprice"]  # 止损位
        currentprice = get_perpetualprice(platform, symbol)
        if currentprice > highprice:
            print("强制止盈出场")
            profit, profitRate = future_grid_stop(griddata)
            data = {'strategyId': strategyId, "profit": profit, "profitRate": profitRate, "status": 2}
            res = requests.post(future_strategy_status_update, data=data)
            if res['success']:
                i = "用户{}子账户{}合约做多策略{}强制止盈出场,成功".format(userUuid, apiAccountId.strategyId)
                print(i)
                logger.info(i)
        elif currentprice < lowprice:
            print("强制止损出场")
            profit, profitRate = future_grid_stop(griddata)
            data = {'strategyId': strategyId, "profit": profit, "profitRate": profitRate, "status": 2}
            res = requests.post(future_strategy_status_update, data=data)
            if res['success']:
                i = "用户{}子账户{}合约做空策略{}强制止盈出场,成功".format(userUuid, apiAccountId.strategyId)
                print(i)
                logger.info(i)
        else:
            # 从redis中读取订单数据，获取网格交易卖单和买单列表
            sellnum = 0
            buynum = 0
            grid_sell_order = json.loads(r0.get("grid2:sell:" + str(strategyId)))
            print("网格{}卖单".format(strategyId), grid_sell_order)
            grid_buy_order = json.loads(r0.get("grid2:buy:" + str(strategyId)))
            print("网格{}买单".format(strategyId), grid_buy_order)
            sell_orderid = grid_sell_order["sellorderid"]
            buy_orderid = grid_buy_order['buyorderid']
            sellquery = query_contract_usdt_order(userUuid, apiAccountId, platform, sell_orderid, symbol)
            sellquerydict = sellquery['response']
            sellorder_status = sellquerydict['status']
            if sellorder_status == "COMPLETED":
                sellnum = sheets
            if sellnum != sheets:  # 如果卖单没有完全成交，则查询买单是否成交，否则不查询
                buyquery = query_contract_usdt_order(userUuid, apiAccountId, platform, buy_orderid, symbol)
                buyquerydict = buyquery['response']
                buyorder_status = buyquerydict['status']
                if buyorder_status == "COMPLETED":
                    buynum = sheets
            # 如果卖单成交
            if sellnum == sheets:
                conn = POOL_grid.connection()
                cur = conn.cursor()
                try:
                    finishsellprice = grid_sell_order["sellprice"]  # 已经成交单的价格
                    info2 = "量化策略{}已成交一个卖单，交易平台{}，交易合约{}-usdt，成交价{}，成交数量{}，正在为您部署新的网格...".format(strategyId, platform,
                                                                                                 symbol,
                                                                                                 finishsellprice,
                                                                                                 sellnum)
                    print(info2)
                    logger.info(info2)
                    # 0.更新策略利润，返回给java端
                    cur.execute(
                        "select sum(profit),sum(netprofit) from t_contractgrid where strategyId=%s and sellstatus=1 "
                        "and (buystatus=1 or buystatus is NULL)", (strategyId,))
                    profitres = cur.fetchone()
                    profit = netprofit = netprofitrate = 0
                    if profitres != (None, None):
                        profit = round(float(profitres[0]), 4)  # 网格收益
                        netprofit = round(float(profitres[1]), 4)
                        netprofitrate = round(netprofit / initialValCoin, 6)  # 网格净收益（扣除手续费)
                    params = {'strategyId': strategyId, 'profit': netprofit, 'profitRate': netprofitrate}
                    res = requests.post(future_strategy_status_update, data=params)
                    resdict = json.loads(res.content.decode())
                    # 1、改变卖单状态
                    selltradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    cur.execute(
                        'update t_contractgrid set sellstatus=%s,selltradetime=%s where strategyId=%s and '
                        'sellorderid=%s', (1, selltradetime, strategyId, sell_orderid))
                    # 2、撤销对应的买单
                    cancelbuyordrinfo = json.loads(r0.get("grid2:buy:" + str(strategyId)))
                    res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol,platform, buy_orderid)
                    cancelinfo = "用户{}策略{}平台{}撤销买单{}，返回结果{}".format(userUuid, strategyId, platform,
                                                                    cancelbuyordrinfo, res)
                    logger.info(cancelinfo)
                    print(cancelinfo)
                    deletebuysql = "delete from t_contractgrid where strategyId=%s and buyorderid=%s"
                    cur.execute(deletebuysql, (strategyId, buy_orderid))
                    # 新挂一个卖单
                    newsellordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    newsellprice = round((finishsellprice + gap), futurepricelimit[symbol][platform])
                    res2 = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, sheets, newsellprice, 2, 1, 4,
                                               leverage)
                    i = "用户{}策略{}平台{}交易合约{}-usdt新挂卖单{}".format(userUuid, strategyId, platform, symbol, res2)
                    print(i)
                    logger.info(i)
                    newsellorderid = res2["response"]["orderId"].replace('"', "")
                    cur.execute(
                        "select * from t_contractgrid where strategyId=%s and sellprice=%s and sellstatus=2",
                        (strategyId, newsellprice))
                    selectres = cur.fetchall()
                    if len(selectres) == 1:
                        updatesql2 = "update t_contractgrid set sellorderid=%s,sellstatus=%s,sellordertime=%s where " \
                                     "strategyId=%s and sellprice=%s and sellstatus=2 "
                        cur.execute(updatesql2, (newsellorderid, 0, newsellordertime, strategyId, newsellprice))
                    elif len(selectres) == 0:
                        if platform == "binance":
                            profit = round((newsellprice - entryPrice) * sheets, 8)
                            netprofit = round(profit - (entryPrice + newsellprice) * sheets * makerFee, 8)
                        else:
                            profit = round((newsellprice - entryPrice) * sheets * contract_size, 8)
                            netprofit = round(profit - (entryPrice + newsellprice) * sheets * makerFee * contract_size,
                                              8)
                        profit = -1 * profit if profit <= 0 else profit
                        netprofit = -1 * netprofit if netprofit <= 0 else netprofit
                        sellinsertsql = "insert into t_contractgrid(userUuid,apiAccountId,strategyId,platform," \
                                        "contract_code,contract_size,direction,leverage,buyprice,buycount,buystatus," \
                                        "buyordertime,buytradetime,sellprice,sellcount,sellorderid,sellstatus," \
                                        "sellordertime,profit,netprofit,uniqueId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s); "
                        cur.execute(sellinsertsql, (
                            userUuid, apiAccountId, strategyId, platform, contract_code, contract_size, "buy", leverage,
                            entryPrice, sheets,
                            1, createTime, createTime, newsellprice, sheets, newsellorderid, 0, newsellordertime,
                            profit, netprofit, 1))
                        # 4、新挂一个买单（比最新成交价小一个网格间距)
                        newbuyordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        newbuyprice = round((finishsellprice - gap), futurepricelimit[symbol][platform])
                        res1 = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, sheets, newbuyprice, 1, 1,
                                                   1, leverage)
                        newtradeinfo = "量化策略{}部署新的网格订单，交易合约{}-usdt,卖单价{}，买单价{}".format(strategyId, symbol, newsellprice,
                                                                                       newbuyprice)
                        print(newtradeinfo)
                        logger.info(newtradeinfo)
                        newbuyorderid = res1["response"]["orderId"].replace('"', "")
                        buyinsertsql = "insert into t_contractgrid(userUuid,apiAccountId,strategyId,platform," \
                                       "contract_code,contract_size,direction,leverage,buyprice,buycount,buyorderid," \
                                       "buystatus,buyordertime,uniqueId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                                       "%s,%s); "
                        cur.execute(buyinsertsql, (
                            userUuid, apiAccountId, strategyId, platform, contract_code, contract_size, "buy", leverage,
                            newbuyprice, sheets, newbuyorderid, 0, newbuyordertime, 1))
                        # 5、修改内存中订单信息
                        selldata = {"userUuid": userUuid, "apiAccountId": apiAccountId,
                                    "strategyId": strategyId, "platform": platform,
                                    "symbol": symbol, "count": sheets, "sellprice": newsellprice,
                                    "sellorderid": newsellorderid}
                        buydata = {"userUuid": userUuid, "apiAccountId": apiAccountId, "strategyId": strategyId,
                                   "platform": platform,
                                   "symbol": symbol, "count": sheets, "buyprice": newbuyprice,
                                   "buyorderid": newbuyorderid}
                        key_sell = "grid2:sell:" + str(strategyId)
                        key_buy = "grid2:buy:" + str(strategyId)
                        r0.set(key_sell, json.dumps(selldata))
                        r0.set(key_buy, json.dumps(buydata))
                        print("修改redis缓存中的订单")
                except Exception as e:
                    i = "用户{}策略{}合约网格单成交后重新部署出错{}".format(userUuid, strategyId, e)
                    print(i)
                    logger.error(i)
                finally:
                    conn.commit()
                    cur.close()
                    conn.close()

            # 如果买单成交
            if buynum == sheets:
                conn = POOL_grid.connection()
                cur = conn.cursor()
                try:
                    finishbuyprice = grid_buy_order["buyprice"]
                    info3 = "量化策略{}已成交一个买单，交易平台{}，交易合约{}-usdt,成交价{}，成交数量{}，正在为您部署新的网格...".format(strategyId, platform,
                                                                                                 symbol, finishbuyprice,
                                                                                                 buynum)
                    print(info3)
                    logger.info(info3)
                    # 1、撤销卖单
                    cancelsellordrinfo = json.loads(r0.get("grid2:sell:" + str(strategyId)))
                    res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol,platform, sell_orderid)
                    cancelinfo = "用户{}合约网格策略{}平台{}撤销卖单{}，返回结果{}".format(userUuid, strategyId, platform,
                                                                    cancelsellordrinfo, res)
                    print(cancelinfo)
                    logger.info(cancelinfo)
                    updatasql3 = "update t_contractgrid set sellstatus=2 where strategyId=%s and sellorderid=%s"
                    cur.execute(updatasql3, (strategyId, sell_orderid))
                    # 2、新挂一个卖单，比最新成交价高一个网格间距，加到数据库对应的买单后边
                    buytradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    newsellprice = round((finishbuyprice + gap), futurepricelimit[symbol][platform])
                    newsellordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    res2 = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, sheets, newsellprice, 2, 1, 4,
                                               leverage)
                    i = "用户{}策略{}平台{}新挂卖单{}".format(userUuid, strategyId, platform, res2)
                    print(i)
                    logger.info(i)
                    newsellorderid = res2["response"]["orderId"].replace('"', "")
                    if platform == "binance":
                        profit = round(gap * sheets, 8)
                        netprofit = round(profit - (2 * newsellprice - gap) * sheets * makerFee, 8)
                    else:
                        profit = round(gap * sheets * contract_size, 8)
                        netprofit = round(profit - (2 * newsellprice - gap) * sheets * contract_size * makerFee, 8)
                    updatesql1 = "update t_contractgrid set buystatus=%s,buytradetime=%s,sellprice=%s,sellcount=%s," \
                                 "sellorderid=%s,sellstatus=%s,sellordertime=%s,profit=%s,netprofit=%s where " \
                                 "strategyId=%s and buyorderid=%s "
                    cur.execute(updatesql1, (
                        1, buytradetime, newsellprice, sheets, newsellorderid, 0, newsellordertime, profit,
                        netprofit, strategyId, buy_orderid))
                    # 3、新挂一个买单（比之前的买单小一个网格）
                    newbuyprice = round((finishbuyprice - gap), futurepricelimit[symbol][platform])
                    newbuyordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    res1 = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, sheets, newsellprice, 1, 1, 1,
                                               leverage)
                    newtradeinfo = "量化策略{}部署新的网格订单，卖单价{}，买单价{}".format(strategyId, newsellprice,
                                                                       newbuyprice)
                    print(newtradeinfo)
                    logger.info(newtradeinfo)
                    newbuyorderid = res1["response"]["orderId"].replace('"', "")
                    buyinsertsql = "insert into t_contractgrid (userUuid,apiAccountId,strategyId,platform," \
                                   "contract_code,contract_size,direction,leverage,buyprice,buycount,buyorderid," \
                                   "buystatus,buyordertime,uniqueId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                                   "%s); "
                    cur.execute(buyinsertsql, (
                        userUuid, apiAccountId, strategyId, platform, contract_code, contract_size, "buy",
                        leverage, newbuyprice, sheets, newbuyorderid, 0, newbuyordertime, 1))
                    # 4、修改内存中订单信息
                    selldata = {"userUuid": userUuid, "apiAccountId": apiAccountId,
                                "strategyId": strategyId, "platform": platform,
                                "symbol": symbol,
                                "count": sheets, "sellprice": newsellprice, "sellorderid": newsellorderid}
                    buydata = {"userUuid": userUuid, "apiAccountId": apiAccountId, "strategyId": strategyId,
                               "platform": platform,
                               "symbol": symbol,
                               "count": sheets, "buyprice": newbuyprice, "buyorderid": newbuyorderid}
                    key_sell = "grid2:sell:" + str(strategyId)
                    key_buy = "grid2:buy:" + str(strategyId)
                    r0.set(key_sell, json.dumps(selldata))
                    r0.set(key_buy, json.dumps(buydata))
                    print("修改redis缓存中的订单")
                except Exception as e:
                    i = "用户{}合约策略{}网格单成交后重新部署出错{}".format(userUuid, strategyId, e)
                    print(i)
                    logger.error(i)
                finally:
                    conn.commit()
                    cur.close()
                    conn.close()
    except Exception as e:
        i = "合约网格策略{}运行时出错{}".format(griddata["strategyId"], e)
        print(i)
        logger.info(i)


def future_grid_stop(griddata):
    strategyId = griddata['strategyId']
    platform = griddata['platform']
    userUuid = griddata['userUuid']
    apiAccountId = griddata['apiAccountId']
    symbol = griddata['symbol']
    leverage = griddata['leverage']
    direction = griddata['direction']
    contract_size = float(contract_size_dict[symbol][platform])  # 合约面值
    entryPrice = griddata["entryPrice"]  # 入场价格
    initialValCoin = griddata["initialValCoin"]  # 用于运行策略的计价币数量
    try:
        if direction == "buy":
            r0.hdel("longgridstrategy", strategyId)  # 从redis删除该策略
        else:
            r0.hdel("shortgridstrategy", strategyId)
        r0.delete("grid2:buy:{}".format(strategyId))
        r0.delete("grid2:sell:{}".format(strategyId))
    except Exception as e:  # 防止redis撤销出问题
        errorinfo = "策略{}撤单时redis出问题{}".format(strategyId, e)
        print(errorinfo)
        logger.error(errorinfo)
    conn = POOL_grid.connection()
    cur = conn.cursor()
    cur.execute("select sellorderid from t_contractgrid where strategyId=%s and sellstatus=0", (strategyId,))
    selectres1 = cur.fetchall()
    cur.execute("select buyorderid from t_contractgrid where strategyId=%s and buystatus=0", (strategyId,))
    selectres2 = cur.fetchall()
    if direction == "buy":
        cur.execute(
            "select sum(profit),sum(netprofit) from t_contractgrid where strategyId=%s and sellstatus=1 and ("
            "buystatus=1 or buystatus is NULL)", (strategyId,))
    else:
        cur.execute(
            "select sum(profit),sum(netprofit) from t_contractgrid where strategyId=%s and buystatus=1 and ("
            "sellstatus=1  or sellstatus is NULL)", (strategyId,))
    profitres = cur.fetchone()
    cur.close()
    conn.close()
    profit = netprofit = netprofitrate = 0
    if profitres != (None, None):
        profit = round(float(profitres[0]), 4)  # 网格收益
        netprofit = round(float(profitres[1]), 4)
        netprofitrate = round(netprofit / initialValCoin, 6)  # 网格净收益（扣除手续费)
    info = "手动停止合约网格策略{}，系统正在为你撤销{}平台所有的委托单".format(strategyId, platform)
    print(info)
    logger.info(info)
    sellorderlist = [i[0] for i in selectres1]  # 未成交卖单
    buyorderlist = [i[0] for i in selectres2]  # 未成交买单
    # 查询策略未成交单是否成交,没有成交则撤单
    tlist = []
    for orderid in sellorderlist + buyorderlist:
        tlist.append(Thread(target=cancel_future_gridorder,
                            args=(userUuid, apiAccountId, strategyId, symbol,platform, orderid, sellorderlist, buyorderlist)))
    for t in tlist:
        t.start()
    for t in tlist:
        t.join()
    profitinfo = "手动停止合约网格策略{}，统计收益：网格收益{}，净收益{}".format(strategyId, profit, netprofit)
    print(profitinfo)
    print("合约网格策略{}停止时平仓处理".format(strategyId))
    # 该合约持仓情况
    position_param = {"userUuid": userUuid, "apiAccountId": apiAccountId, "platform": platform, "symbol": symbol}
    remainres = requests.get(future_remain_url, params=position_param)
    remaindict = remainres.json()
    if remaindict['success']:
        TradeCoin_amount = abs(remaindict['response']['available'])
        clear_future_grid_remain(userUuid, apiAccountId, strategyId, platform, symbol, TradeCoin_amount, leverage,
                                 direction)
    else:
        print("网格策略查询剩余资产失败")
    return netprofit, netprofitrate


def goLongGridStrategy():
    gridnum = 0
    while True:
        try:
            print("*************************************************************")
            print("网格策略第{}次运行".format(gridnum))
            print("*************************************************************")
            allgridstrategyId = r0.hkeys("longgridstrategy")
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
                gridstrategydatalist = r0.hmget("longgridstrategy", strategyIdlist)
                for griddata in gridstrategydatalist:
                    gridThreads.append(Thread(target=future_grid_strategy, args=(json.loads(griddata),)))
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
    goLongGridStrategy()
    # griddata = {'userUuid': '379', 'strategyId': 219, 'apiAccountId': 10179, 'platform': 'T8ex', 'symbol': 'eth',
    #             'entryPrice': 2100, 'gap': 20, 'makerFee': 0.0002, 'sheets': 1, 'leverage': 10,
    #             "createTime": "2021-07-09 10:00:00","direction":"buy",
    #             "highprice": 2300, "lowprice": 1900}
    # longgridbegin(griddata)

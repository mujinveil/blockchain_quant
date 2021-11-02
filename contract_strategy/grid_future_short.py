# encoding="utf-8"
import json
import time
from threading import Thread
import requests
import sys
sys.path.append("..")
from loggerConfig import logger
from contract_strategy.grid_future_long import future_grid_stop
from tools.Config import futurepricelimit, contract_size_dict, future_makerFee, future_strategy_status_update
from tools.databasePool import POOL_grid, r0
from tools.future_trade import contract_usdt_trade, query_contract_usdt_order, cancel_contract_usdt_order
from tools.get_future_market_info import get_perpetualprice


def openSellPosition(griddata):
    userUuid = griddata['userUuid']
    apiAccountId = griddata['apiAccountId']
    strategyId = griddata['strategyId']
    platform = griddata['platform']
    symbol = griddata['symbol']
    leverage = griddata['leverage']
    gap = griddata['gap']
    sheets =griddata['sheets']
    low_price = griddata['lowprice']
    contract_code = "{}-usdt".format(symbol).upper()
    contract_size = contract_size_dict[symbol][platform]
    current_price = get_perpetualprice(platform, symbol)
    first_sheets = (current_price - low_price) // gap *sheets
    conn = POOL_grid.connection()
    cur = conn.cursor()
    try:
        order_price = round(current_price * 0.99, futurepricelimit[symbol][platform])
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        i="用户{}子账户{}开启合约网格做空策略{}，开仓下卖单".format(userUuid, apiAccountId, strategyId)
        print(i)
        logger.info(i)
        resdict = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, first_sheets, order_price, 2, 2, 3,leverage)
        orderId = resdict['response']['orderId'].replace('"', "")
        #  将下单信息插入数据库
        insertsql = "INSERT INTO t_contractgrid(userUuid,apiAccountId,strategyId,platform,contract_code,contract_size,direction," \
                    "leverage,sellprice,sellcount,sellorderid,sellstatus,sellordertime,uniqueId) VALUES(" \
                    "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        insertdata = (
            userUuid, apiAccountId, strategyId, platform, contract_code, contract_size, "sell", leverage, order_price,
            first_sheets, orderId, 0, ordertime, 1)
        cur.execute(insertsql, insertdata)
        # 3s后查询订单情况
        time.sleep(3)
        res = query_contract_usdt_order(userUuid, apiAccountId, platform, orderId, symbol)['response']
        order_status = res['status']
        if order_status == "COMPLETED":
            trade_avg_price = float(res['detail'][0]['price'])  # 成交均价
            griddata['entryPrice'] = trade_avg_price
            r0.hset("shortgridstrategy", strategyId, json.dumps(griddata))
            tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            updatesql = "update t_contractgrid set sellprice=%s,sellstatus=%s,selltradetime=%s where " \
                        "strategyId=%s and sellorderid=%s"
            cur.execute(updatesql, (trade_avg_price, 1, tradetime, strategyId, orderId))
        elif order_status == "TRADING":
            res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol,platform, orderId)
            if res['success']:
                # 取消订单，将数据库订单状态改为2
                cancel_sql = 'update t_contractgrid set sellstatus=2 where strategyId=%s and sellorderid=%s'
                cur.execute(cancel_sql, (strategyId, orderId))
        conn.commit()
        cur.close()
        conn.close()
        return trade_avg_price
    except Exception as e:
        errorinfo = "合约网格做空策略{}开仓时出错{}".format(strategyId,e)
        print(errorinfo)
        logger.error(errorinfo)
        return 0


def shortgridbegin(griddata):
    strategyId = griddata['strategyId']
    userUuid = griddata['userUuid']
    apiAccountId = griddata['apiAccountId']
    platform = griddata['platform']
    symbol = griddata['symbol']
    contract_code = "{}-usdt".format(symbol).upper()
    gap = griddata['gap']  # 网格间距
    makerFee = future_makerFee[platform]['sellfee']  # 挂单手续费
    sheets = griddata['sheets']  # 每个网格挂单张数
    leverage = griddata['leverage']  # 杠杆倍数
    contract_size = float(contract_size_dict[symbol][platform])  # 合约面值
    conn = POOL_grid.connection()
    cur = conn.cursor()
    try:
        entryPrice= openSellPosition(griddata)
        if not entryPrice:
            return 0
        # 2、往上部署一个卖单,由于是挂单不考虑是否会立即成交，所以不需要查询订单记录
        sellPrice = round((entryPrice + gap), futurepricelimit[symbol][platform])
        sellordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        resdict = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, sheets, sellPrice, 2, 1, 3, leverage)
        sellorderId = resdict['response']['orderId'].replace('"', '')
        info2 = "网格{}初始化部署卖单委托成功,交易合约{}-usdt,交易平台：{}，价格：{}，数量{}".format(strategyId, symbol, platform, sellPrice, sheets)
        print(info2)
        logger.info(info2)
        # 卖单不计算利润，插入数据库即可
        sellinsertsql = "insert into t_contractgrid (userUuid,apiAccountId,strategyId,platform,contract_code,contract_size,direction,leverage,sellprice," \
                        "sellcount,sellorderid,sellstatus,sellordertime,uniqueId) values (%s,%s,%s," \
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s); "
        cur.execute(sellinsertsql,
                    (userUuid, apiAccountId, strategyId, platform, contract_code, contract_size, "sell", leverage,
                     sellPrice, sheets, sellorderId, 0, sellordertime, 1))
        selldata = {"userUuid": userUuid, "apiAccountId": apiAccountId, "strategyId": strategyId, "platform": platform,
                    "symbol": symbol, "count": sheets, "sellprice": sellPrice, "sellorderid": sellorderId}
        key_buy = "grid2:sell:" + str(strategyId)
        r0.set(key_buy, json.dumps(selldata))  # 存入redis数据库
        # 1、往下部署一个买单,由于是挂单不考虑是否会立即成交，所以不需要查询订单记录
        buyPrice = round((entryPrice - gap), futurepricelimit[symbol][platform])
        buyordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        resdict = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, sheets, sellPrice, 1, 1, 2, leverage)
        buyorderId = resdict['response']['orderId'].replace('"', "")
        info1 = "网格{}初始化部署买单委托成功,交易合约{}-usdt,交易平台：{}，价格：{}，数量{}".format(strategyId, symbol, platform, buyPrice, sheets)
        print(info1)
        logger.info(info1)
        # 利润计算（平仓 - 开仓)*成交合约张数 * 合约面值
        if platform == "binance":
            profit = round(gap * sheets, 8)
            netprofit = round(profit - (buyPrice + entryPrice) * sheets * makerFee, 8)
        else:
            profit = round(gap * sheets * contract_size, 8)  # 利润计算
            # 手续费 成交价*成交合约张数*合约面值*费率
            netprofit = round(profit - (buyPrice + entryPrice) * sheets * contract_size * makerFee, 8)  # 净利润计算
        # 插入数据库，数据库需重新设计
        buyinsertsql = "insert into t_contractgrid (userUuid,apiAccountId,strategyId,platform,contract_code,contract_size,direction,leverage,buyprice," \
                       "buycount,buyorderid,buystatus,buyordertime,profit,netprofit,uniqueId) values (%s,%s,%s," \
                       "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s); "
        cur.execute(buyinsertsql,
                    (userUuid, apiAccountId, strategyId, platform, contract_code, contract_size, "sell", leverage,
                     buyPrice, sheets, buyorderId, 0, buyordertime, profit, netprofit, 1))
        buydata = {"userUuid": userUuid, "apiAccountId": apiAccountId, "strategyId": strategyId, "platform": platform,
                   "symbol": symbol, "count": sheets, "buyprice": buyPrice, "buyorderid": buyorderId}
        key_buy = "grid2:buy:" + str(strategyId)
        r0.set(key_buy, json.dumps(buydata))  # 存入redis数据库
        conn.commit()
        cur.close()
        conn.close()
        return 1
    except Exception as e:
        i = "用户{}，初步部署网格策略{}，报错信息{}".format(userUuid, strategyId, e)
        print(i)
        logger.error(i)
        return 0


def gridStrategy(griddata):
    try:
        userUuid = griddata["userUuid"]  # 用户id
        apiAccountId = griddata["apiAccountId"]  # 子账户id
        strategyId = griddata["strategyId"]
        platform = griddata["platform"]  # 平台
        symbol = griddata["symbol"]  # 交易对
        entryPrice = griddata["entryPrice"]  # 入场价格
        gap = griddata["gap"]  # 网格间距
        makerFee = future_makerFee[platform]['sellfee']  # 挂单手续费
        sheets = griddata["sheets"]  # 每次下单量
        leverage = griddata['leverage']  # 杠杆倍数
        contract_code = "{}-usdt".format(symbol).upper()
        contract_size = float(contract_size_dict[symbol][platform])  # 合约面值
        initialValCoin = griddata["initialValCoin"]  # 用于运行策略的计价币数量
        createTime = griddata["createTime"]  # 策略开启时间
        highprice = griddata["highprice"]  # 止损位
        lowprice = griddata["lowprice"]  # 止盈位
        currentprice = get_perpetualprice(platform, symbol)
        if currentprice > highprice:
            print("强制止损出场")
            profit, profitRate = future_grid_stop(griddata)
            data = {'strategyId': strategyId, "profit": profit, "profitRate": profitRate, "status": 2}
            res = requests.post(future_strategy_status_update, data=data)
            if res['success']:
                i="用户{}子账户{}合约做空策略{}强制止损出场,成功".format(userUuid, apiAccountId, strategyId)
                print(i)
                logger.info(i)
        elif currentprice < lowprice:
            print("强制止盈出场")
            profit, profitRate = future_grid_stop(griddata)
            data = {'strategyId': strategyId, "profit": profit, "profitRate": profitRate, "status": 2}
            res = requests.post(future_strategy_status_update, data=data)
            if res['success']:
                i="用户{}子账户{}合约做空策略{}强制止盈出场,成功".format(userUuid, apiAccountId, strategyId)
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
            buyquerydict = query_contract_usdt_order(userUuid, apiAccountId, platform, buy_orderid, symbol)[
                'response']
            buyorder_status = buyquerydict['status']
            if buyorder_status == "COMPLETED":
                buynum = sheets
            if buynum != sheets:  # 如果买单没有完全成交，则查询卖单是否成交，否则不查询
                sellquerydict = query_contract_usdt_order(userUuid, apiAccountId, platform, sell_orderid, symbol)[
                    'response']
                sellorder_status = sellquerydict['status']
                if sellorder_status == "COMPLETED":
                    sellnum = sheets
            # 如果买单成交
            if buynum == sheets:
                conn = POOL_grid.connection()
                cur = conn.cursor()
                try:
                    finishbuyprice = grid_buy_order["buyprice"]  # 已经成交单的价格
                    info2 = "量化策略{}已成交一个买单，交易平台{}，交易合约{}-usdt，成交价{}，成交数量{}，正在为您部署新的网格...".format(strategyId, platform,
                                                                                                 contract_code,
                                                                                                 finishbuyprice, buynum)
                    print(info2)
                    logger.info(info2)
                    # 0.更新策略利润，返回给java端
                    cur.execute(
                        "select sum(profit),sum(netprofit) from t_contractgrid where strategyId=%s and buystatus=1 "
                        "and (sellstatus=1 or sellstatus is NULL)", (strategyId,))
                    profitres = cur.fetchone()
                    profit = netprofit = netprofitrate = 0
                    if profitres != (None, None):
                        profit = round(float(profitres[0]), 4)  # 网格收益
                        netprofit = round(float(profitres[1]), 4)
                        netprofitrate = round(netprofit / initialValCoin, 6)  # 网格净收益（扣除手续费)
                    params = {'strategyId': strategyId, 'profit': netprofit, 'profitRate': netprofitrate}
                    res = requests.post(future_strategy_status_update, data=params)
                    resdict = json.loads(res.content.decode())
                    # 1、改变买单状态
                    buytradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    cur.execute(
                        'update t_contractgrid set buystatus=%s,buytradetime=%s where strategyId=%s and buyorderid=%s',
                        (1, buytradetime, strategyId, buy_orderid))
                    # 2、撤销对应的卖单
                    cancelsellordrinfo = json.loads(r0.get("grid2:sell:" + str(strategyId)))
                    res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol,platform, sell_orderid)
                    cancelinfo = "用户{}策略{}平台{}撤销卖单{}，返回结果{}".format(userUuid, strategyId, platform, cancelsellordrinfo,
                                                                    res)
                    print(cancelinfo)
                    logger.info(cancelinfo)
                    deletesellsql = "delete from t_contractgrid where strategyId=%s and sellorderid=%s"
                    cur.execute(deletesellsql, (strategyId, sell_orderid))
                    # 新挂一个买单
                    newbuyordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    newbuyprice = round((finishbuyprice - gap), futurepricelimit[symbol][platform])
                    res2 = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, sheets, newbuyprice, 1, 1, 2,leverage)
                    i = "用户{}策略{}平台{}交易合约{}-usdt新挂买单{}".format(userUuid, strategyId, platform, symbol, res2)
                    logger.info(i)
                    print(i)
                    newbuyorderid = res2["response"]["orderId"].replace('"', "")
                    cur.execute(
                        "select * from t_contractgrid where strategyId=%s and buyprice=%s and buystatus=2",
                        (strategyId, newbuyprice))
                    selectres = cur.fetchall()
                    if len(selectres) == 1:
                        updatesql2 = "update t_contractgrid set buyorderid=%s,buystatus=%s,buyordertime=%s where " \
                                     "strategyId=%s and buyprice=%s and buystatus=2 "
                        cur.execute(updatesql2, (newbuyorderid, 0, newbuyordertime, strategyId, newbuyprice))
                    elif len(selectres) == 0:
                        if platform == "binance":
                            profit = round((entryPrice - newbuyprice) * sheets, 8)
                            netprofit = round((entryPrice - newbuyprice) * sheets, 8)
                        else:
                            profit = round((entryPrice - newbuyprice) * sheets * contract_size, 8)
                            netprofit = round(profit - (entryPrice + newbuyprice) * sheets * makerFee * contract_size,
                                              8)
                        profit = -1 * profit if profit <= 0 else profit
                        netprofit = -1 * netprofit if netprofit <= 0 else netprofit
                        buyinsertsql = "insert into t_contractgrid(userUuid,apiAccountId,strategyId,platform," \
                                       "contract_code,contract_size,direction,leverage,buyprice,buycount,buystatus," \
                                       "buyordertime,sellprice,sellcount,sellstatus,sellordertime,selltradetime,profit,netprofit," \
                                       "uniqueId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                        cur.execute(buyinsertsql, (
                            userUuid, apiAccountId, strategyId, platform, contract_code, contract_size, "sell",
                            leverage, newbuyprice, sheets,
                            0, newbuyordertime, entryPrice, sheets, 1, createTime, createTime, profit, netprofit, 1))
                        # 4、新挂一个卖单（比最新成交价大一个网格间距)
                        newsellordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        newsellprice = round((finishbuyprice + gap), futurepricelimit[symbol][platform])
                        res1 = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, sheets, newsellprice, 2, 1,
                                                   3, leverage)
                        newtradeinfo = "量化策略{}部署新的网格订单，交易合约{}-usdt,卖单价{}，买单价{}".format(strategyId, symbol, newsellprice,
                                                                                       newbuyprice)
                        print(newtradeinfo)
                        logger.info(newtradeinfo)
                        newsellorderid = res1["response"]["orderId"].replace('"', "")
                        sellinsertsql = "insert into t_contractgrid(userUuid,apiAccountId,strategyId,platform," \
                                        "contract_code,contract_size,direction,leverage,sellprice,sellcount," \
                                        "sellorderid,sellstatus,sellordertime,uniqueId) values (%s,%s,%s,%s,%s,%s,%s," \
                                        "%s,%s,%s,%s,%s,%s,%s); "
                        cur.execute(sellinsertsql, (
                            userUuid, apiAccountId, strategyId, platform, contract_code, contract_size, "sell",
                            leverage,
                            newsellprice, sheets, newsellorderid, 0, newsellordertime, 1))
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
                    i = "用户{}策略{}网格单成交后重新部署出错{}".format(userUuid, strategyId, e)
                    print(i)
                    logger.error(i)
                finally:
                    conn.commit()
                    cur.close()
                    conn.close()

            # 如果卖单成交
            if sellnum == sheets:
                conn = POOL_grid.connection()
                cur = conn.cursor()
                try:
                    finishsellprice = grid_sell_order["sellprice"]
                    info3 = "量化策略{}已成交一个卖单，交易平台{}，交易合约{}-usdt,成交价{}，成交数量{}，正在为您部署新的网格...".format(strategyId, platform,
                                                                                                 symbol,
                                                                                                 finishsellprice,
                                                                                                 sellnum)
                    print(info3)
                    logger.info(info3)
                    # 1、撤销买单
                    cancelbuyordrinfo = json.loads(r0.get("grid2:buy:" + str(strategyId)))
                    res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol,platform, buy_orderid)
                    cancelinfo = "用户{}策略{}平台{}撤销买单{}，返回结果{}".format(userUuid, strategyId, platform,
                                                                    cancelbuyordrinfo, res)
                    print(cancelinfo)
                    logger.info(cancelinfo)
                    updatasql3 = "update t_contractgrid set buystatus=2 where strategyId=%s and buyorderid=%s"
                    cur.execute(updatasql3, (strategyId, buy_orderid))
                    # 2、新挂一个买单，比最新成交价低一个网格间距，加到数据库对应的卖单前边
                    selltradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    newbuyprice = round((finishsellprice - gap), futurepricelimit[symbol][platform])
                    newbuyordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    res2 = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, sheets, newbuyprice, 1, 1, 2,
                                               leverage)
                    i = "用户{}策略{}平台{}新挂买单{}".format(userUuid, strategyId, platform, res2)
                    print(i)
                    logger.info(i)
                    newbuyorderid = res2["response"]["orderId"].replace('"', "")
                    if platform == "binance":
                        profit = round(gap * sheets, 8)
                        netprofit = round(profit - (2 * newbuyprice + gap) * sheets * makerFee, 8)
                    else:
                        profit = round(gap * sheets * contract_size, 8)
                        netprofit = round(profit - (2 * newbuyprice + gap) * sheets * contract_size * makerFee, 8)
                    updatesql1 = "update t_contractgrid set sellstatus=%s,selltradetime=%s,buyprice=%s,buycount=%s," \
                                 "buyorderid=%s,buystatus=%s,buyordertime=%s,profit=%s,netprofit=%s where " \
                                 "strategyId=%s and sellorderid=%s "
                    cur.execute(updatesql1, (
                        1, selltradetime, newbuyprice, sheets, newbuyorderid, 0, newbuyordertime, profit,
                        netprofit, strategyId, sell_orderid))
                    # 3、新挂一个卖单（比之前的卖单大一个网格）
                    newsellprice = round((finishsellprice + gap), futurepricelimit[symbol][platform])
                    newsellordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    res1 = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, sheets, newsellprice, 2, 1, 3,
                                               leverage)
                    newtradeinfo = "量化策略{}部署新的网格订单，卖单价{}，买单价{}".format(strategyId, newsellprice,
                                                                       newbuyprice)
                    print(newtradeinfo)
                    logger.info(newtradeinfo)
                    newsellorderid = res1["response"]["orderId"].replace('"', "")
                    sellinsertsql = "insert into t_contractgrid (userUuid,apiAccountId,strategyId,platform," \
                                    "contract_code,contract_size,direction,leverage,sellprice,sellcount,sellorderid," \
                                    "sellstatus,sellordertime,uniqueId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                                    "%s,%s); "
                    cur.execute(sellinsertsql, (
                        userUuid, apiAccountId, strategyId, platform, contract_code, contract_size, "sell",
                        leverage, newsellprice, sheets, newsellorderid, 0, newsellordertime, 1))
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
        logger.info(i)


def goShortGridStrategy():
    gridnum = 0
    while True:
        try:
            print("*************************************************************")
            print("网格策略第{}次运行".format(gridnum))
            print("*************************************************************")
            allgridstrategyId = r0.hkeys("shortgridstrategy")
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
                gridstrategydatalist = r0.hmget("shortgridstrategy", strategyIdlist)
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
            # time.sleep(1)
            gridnum += 1
            if gridnum == 10:
                gridnum = 0


if __name__ == '__main__':
    goShortGridStrategy()
    # griddata = {"userUuid":"398051ac70ef4da9aafd33ce0b95195f","strategyId": 423,"apiAccountId": 10153,"platform":"T8ex",
    #             "symbol":"eth","entryPrice": 2100,"gap": 20, "makerFee": 0.0002, "sheets": 1, "leverage": 10,
    #             "createTime": "2021-07-09 10:00:00","direction":"sell","highprice": 2300, "lowprice": 1900}
    # shortgridbegin(griddata)

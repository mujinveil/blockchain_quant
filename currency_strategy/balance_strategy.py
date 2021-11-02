# encoding=utf-8
import json
import time
from threading import Thread
import requests
import sys
sys.path.append("..")
from loggerConfig import logger
from tools.Config import Cancel_url, amountlimit, premiumdict, Trade_url, Queryorder_url, Query_tradeprice_url, Fee, \
    pricelimit, stopBalanceStrategy_url, Remain_url, minamountdict
from tools.databasePool import POOL, r2
from tools.get_market_info import get_currentprice1

# 网格结合资产均衡
"""
①比如入场价9000，初始资金2btc + 5000usdt，此时btc市值为18000，不管行情涨跌，始终维持btc市值为18000。
↑行情上涨至9100，卖出(9100*2-18000)/9100=0.021978 btc，此时持仓1.97802btc，5200usdt。
↓行情回落至9000，买入(18000-1.97802*9000)/9000=0.021978btc，此时持仓2btc，5,002.198usdt； 
↓行情跌至8900，买入(18000-8900*2)/8900=0.02247191btc，此时持仓2.02247191btc，4802.198usdt；
↑行情回涨至9000，卖出(2.02247191*9000-18000)/9000=0.0224719 btc，此时持仓2btc，5,004.45usdt。
"""


# 统计总手续费
def get_total_fee(strategyId):
    totalTradecoinFee = 0
    totalValcoinFee = 0
    try:
        conn = POOL.connection()
        cur = conn.cursor()
        cur.execute("select sum(fee) from balancelist where strategyId=%s and direction=1 and status=1", (strategyId,))
        tradefeeres = cur.fetchone()[0]
        cur.execute("select sum(fee) from balancelist where strategyId=%s and direction=2 and status=1", (strategyId,))
        valuefeeres = cur.fetchone()[0]
        if tradefeeres is not None:
            totalTradecoinFee = float(tradefeeres)
        if valuefeeres is not None:
            totalValcoinFee = float(valuefeeres)
        cur.close()
        conn.close()
    except Exception as e:
        errorinfo = "策略{}在查询总手续费时出错{}".format(strategyId, e)
        logger.error(errorinfo)
    return totalTradecoinFee, totalValcoinFee


# 均衡策略结束时撤单
def cancel_balancestrategy_orders(userUuid, apiAccountId, strategyId, platform, symbol):
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        cur.execute("select orderid from balancelist where strategyId=%s and status=0 and direction=1", (strategyId,))
        buyorderid = cur.fetchone()[0]
        cancelbuyparams = {"direction": 1, "symbol": symbol, "platform": platform, "orderId": buyorderid,
                           "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 10, "icebergId": strategyId,
                           "strategyId": strategyId}
        cancelbuyres = requests.post(Cancel_url, data=cancelbuyparams)
        resbuy = json.loads(cancelbuyres.content.decode())
        if resbuy["code"] == 1:
            cur.execute("update balancelist set status=2 where strategyId=%s and orderid=%s", (strategyId, buyorderid))
        cur.execute("select orderid from balancelist where strategyId=%s and status=0 and direction=2", (strategyId,))
        sellorderid = cur.fetchone()[0]
        cancelsellparams = {"direction": 2, "symbol": symbol, "platform": platform, "orderId": sellorderid,
                            "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 10, "icebergId": strategyId,
                            "strategyId": strategyId}
        cancelsellres = requests.post(Cancel_url, data=cancelsellparams)
        ressell = json.loads(cancelsellres.content.decode())
        print(ressell)
        if ressell["code"] == 1:
            cur.execute("update balancelist set status=2 where strategyId=%s and orderid=%s", (strategyId, sellorderid))
    except Exception as e:
        i = "用户{}策略{}撤销{}平台订单出错{}".format(userUuid, strategyId, platform, e)
        logger.error(i)
    finally:
        conn.commit()
        cur.close()
        conn.close()


# 平仓处理
def clear_tradecoin_remains(userUuid, apiAccountId, strategyId, platform, symbol, amount):
    conn = POOL.connection()
    cur = conn.cursor()
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
                       "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 10, "icebergId": strategyId,
                       "strategyId": strategyId}
        traderes = requests.post(Trade_url, data=tradeparams)
        trade_dict = json.loads(traderes.content.decode())
        print(trade_dict)
        orderid = trade_dict["response"]["orderid"]  # 获取订单id
        print("=================================查询===================================")
        queryparams = {"direction": 2, "symbol": symbol, "platform": platform, "orderId": orderid,
                       "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 10, "icebergId": strategyId,
                       "strategyId": strategyId}
        res = requests.post(Queryorder_url, data=queryparams)
        queryresdict = json.loads(res.content.decode())
        print(queryresdict)
        numberDeal = float(queryresdict["response"]["numberDeal"])
        try:
            queryparams = {"platform": platform, "symbol": symbol, "orderId": orderid, "apiId": apiAccountId,
                           "userUuid": userUuid}
            res = requests.post(Query_tradeprice_url, data=queryparams)
            queryresdict = json.loads(res.content.decode())
            print(queryresdict)
            if queryresdict["response"]["avgPrice"] is not None:
                price = queryresdict["response"]["avgPrice"]
            if queryresdict["response"]["createdDate"] is not None:
                tradetime = queryresdict["response"]["createdDate"]
        except:
            pass
        print("================================撤单====================================")
        cancelparams = {"direction": 2, "symbol": symbol, "platform": platform, "orderId": orderid,
                        "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 10, "icebergId": strategyId,
                        "strategyId": strategyId}
        requests.post(Cancel_url, data=cancelparams)
        sellfee = round(price * numberDeal * Fee[platform]["sellfee"], 8)
        sellinsertsql = "INSERT INTO balancelist(strategyId,userUuid,apiAccountId,platform,symbol,direction,orderid," \
                        "order_amount,order_price,order_time,trade_amount,trade_price,trade_time,status,fee," \
                        "uniqueId) VALUES(%s, %s, %s, %s, %s, %s,%s,%s,%s, %s, %s, %s,%s, %s, %s, %s) "
        cur.execute(sellinsertsql, (
            strategyId, userUuid, apiAccountId, platform, symbol, 2, orderid, amount, price, tradetime, numberDeal,
            price, tradetime, 1, sellfee, 2))
    except Exception as e:
        errorinfo = "策略{}停止平仓时出错{}".format(strategyId, e)
        logger.error(errorinfo)
    finally:
        conn.commit()
        cur.close()
        conn.close()


# 1、根据初始仓位，买入65%份额的交易币
def banlancestrategy_begin_0(userUuid, apiAccountId, strategyId, platform, symbol, entryPrice, initialValCoin):
    conn = POOL.connection()
    cur = conn.cursor()
    # 1、根据初始仓位，买入65%份额的交易币
    init_buy_price = round(entryPrice * 1.01, pricelimit[symbol][platform])
    init_buy_amount = round(initialValCoin / init_buy_price * 0.65, amountlimit[symbol][platform])
    if init_buy_amount > 0:
        init_buy_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        init_buy_dict = {"direction": 1, "amount": init_buy_amount, "symbol": symbol, "platform": platform,
                         "price": init_buy_price, "apiAccountId": apiAccountId, "userUuid": userUuid,
                         "source": 10, "icebergId": strategyId, "strategyId": strategyId, 'tradetype': 1}
        init_buy_res = requests.post(Trade_url, data=init_buy_dict)
        init_trade_buy_dict = json.loads(init_buy_res.content.decode())
        logger.info("网格{}第一笔买单{}".format(strategyId, init_trade_buy_dict))
        print("网格{}第一笔买单{}".format(strategyId, init_trade_buy_dict))
        init_buyorderid = init_trade_buy_dict["response"]["orderid"]  # 获取订单id
        buyinsertsql = "INSERT INTO balancelist(strategyId,userUuid,apiAccountId,platform,symbol,direction," \
                       "orderid,order_amount,order_price,order_time,status,uniqueId) VALUES(%s, %s, %s, %s, %s, %s," \
                       "%s,%s, %s, %s, %s,%s) "
        cur.execute(buyinsertsql, (
            strategyId, userUuid, apiAccountId, platform, symbol, 1, init_buyorderid, init_buy_amount,
            init_buy_price, init_buy_time, 1, 1))
        conn.commit()
        i = "用户{}子账户{}动态平衡策略{},交易对{}初始订单已插入数据库".format(userUuid, apiAccountId, strategyId, symbol)
        print(i)
        time.sleep(3)
        # 查询成交数量
        init_buy_query = {"direction": 1, "symbol": symbol, "platform": platform,
                          "orderId": init_buyorderid, "apiAccountId": apiAccountId, "userUuid": userUuid,
                          "source": 10, "strategyId": strategyId}
        init_buyqueryres = requests.post(Queryorder_url, data=init_buy_query)
        init_buyquerydict = json.loads(init_buyqueryres.content.decode())
        tradenum = float(init_buyquerydict["response"]["numberDeal"])
        trade_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print("查询网格{}第一笔买单{}".format(strategyId, init_buyquerydict))
        # 查询实际成交价
        queryparams = {"platform": platform, "symbol": symbol, "orderId": init_buyorderid, "apiId": apiAccountId,
                       "userUuid": userUuid,
                       "strategyId": strategyId, "icebergId": strategyId}
        res = requests.post(Query_tradeprice_url, data=queryparams)
        queryresdict = json.loads(res.content.decode())
        tradeprice = queryresdict["response"]["avgPrice"]
        if tradeprice is None:
            tradeprice = init_buy_price
        print("查询网格{}第一笔买单成交均价{}".format(strategyId, queryresdict))
        if tradenum == init_buy_amount:
            updatesql = "update balancelist set trade_amount=%s,trade_price=%s,trade_time=%s,status=1 where " \
                        "strategyId=%s and orderid=%s "
            cur.execute(updatesql, (tradenum, tradeprice, trade_time, strategyId, init_buyorderid))
            i = "用户{}子账户{}动态平衡策略{},交易对{}初始订单已全部成交并插入数据库".format(userUuid, apiAccountId, strategyId, symbol)
            print(i)
            conn.commit()
        cur.close()
        conn.close()
    # 查询此时的资产
    remainres = requests.get(Remain_url, params={"userUuid": userUuid, "apiAccountId": apiAccountId})
    remaindict = json.loads(remainres.content.decode())
    tradeCoin = symbol.split("_")[0]  # 交易币
    valCoin = symbol.split("_")[1]  # 计价币
    TradeCoin_amount = [i["remains"] for i in remaindict["response"] if i["coin"] == tradeCoin][0]
    ValCoin_amount = [i["remains"] for i in remaindict["response"] if i["coin"] == valCoin][0]
    print("查询资产{}-{}，{}-{}".format(tradeCoin, TradeCoin_amount, valCoin, ValCoin_amount))
    return TradeCoin_amount, ValCoin_amount


#  均衡策略挂初始单
def balancestrategy_begin(userUuid, apiAccountId, strategyId, platform, symbol, entryPrice, initialTradeCoin,
                          initialValCoin, spacingRatio):
    try:
        conn = POOL.connection()
        cur = conn.cursor()
        # 1、往上部署一个卖单
        sellprice = round(entryPrice * (1 + spacingRatio), pricelimit[symbol][platform])  # 卖单价
        sellamount = round((sellprice * initialTradeCoin - entryPrice * initialTradeCoin) / sellprice,
                           amountlimit[symbol][platform])
        buyprice = round(entryPrice * (1 - spacingRatio), pricelimit[symbol][platform])  # 买单价格
        buyamount = round(
            (entryPrice * initialTradeCoin - buyprice * initialTradeCoin) / buyprice * (1 + Fee[platform]["buyfee"]),
            amountlimit[symbol][platform])  # 买单数量(因考虑手续费，所以多买一点)
        # 卖单数量
        print(sellprice, sellamount)
        if sellamount < minamountdict[symbol][platform]:
            return 2
        if buyamount < minamountdict[symbol][platform]:
            return 2
        sell_dict = {"direction": 2, "amount": sellamount, "symbol": symbol, "platform": platform,
                     "price": sellprice, "apiAccountId": apiAccountId, "userUuid": userUuid,
                     "source": 10, "strategyId": strategyId}
        res_sell = requests.post(Trade_url, data=sell_dict)
        trade_sell_dict = json.loads(res_sell.content.decode())
        logger.info("平衡策略{}初始化部署卖单{}".format(strategyId, trade_sell_dict))
        sellorderid = trade_sell_dict["response"]["orderid"]  # 获取订单id
        info2 = "卖单1委托成功，交易平台：{}，价格：{}，数量{}".format(platform, sellprice, sellamount)
        print(info2)
        sellordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        sellinsertsql = "INSERT INTO balancelist(strategyId,userUuid,apiAccountId,platform,symbol,direction,orderid," \
                        "order_amount,order_price,order_time,status,uniqueId) VALUES(%s, %s, %s, %s, %s, %s,%s,%s,%s," \
                        " %s, %s, %s) "
        cur.execute(sellinsertsql, (
            strategyId, userUuid, apiAccountId, platform, symbol, 2, sellorderid, sellamount, sellprice, sellordertime,
            0, 1))
        conn.commit()
        selldata = {"strategyId": strategyId, "userUuid": userUuid, "apiAccountId": apiAccountId, "platform": platform,
                    "symbol": symbol,
                    "sellamount": sellamount, "sellprice": sellprice, "sellorderid": sellorderid,
                    "icebergId": strategyId}
        key_sell = "balance:sell:" + str(strategyId)
        r2.set(key_sell, json.dumps(selldata))  # 存入redis数据库
        # 2、往下部署一个买单
        buy_dict = {"direction": 1, "amount": buyamount, "symbol": symbol, "platform": platform,
                    "price": buyprice, "apiAccountId": apiAccountId, "userUuid": userUuid,
                    "source": 10, "icebergId": strategyId, "strategyId": strategyId}
        res_buy = requests.post(Trade_url, data=buy_dict)
        trade_buy_dict = json.loads(res_buy.content.decode())
        logger.info("网格{}初始化部署买单{}".format(strategyId, trade_buy_dict))
        buyorderid = trade_buy_dict["response"]["orderid"]  # 获取订单id
        info3 = "买单1委托成功，交易平台{}，价格：{}，数量{}".format(platform, buyprice, buyamount)
        print(info3)
        buyordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        buyinsertsql = "INSERT INTO balancelist(strategyId,userUuid,apiAccountId,platform,symbol,direction,orderid,order_amount,order_price,order_time,status,uniqueId) VALUES(%s, %s, %s, %s, %s, %s,%s,%s, %s, %s, %s,%s)"
        cur.execute(buyinsertsql, (
            strategyId, userUuid, apiAccountId, platform, symbol, 1, buyorderid, buyamount, buyprice, buyordertime, 0,
            1))
        conn.commit()
        buydata = {"strategyId": strategyId, "userUuid": userUuid, "apiAccountId": apiAccountId, "platform": platform,
                   "symbol": symbol,
                   "buyamount": buyamount, "buyprice": buyprice, "buyorderid": buyorderid, "icebergId": strategyId}
        key_buy = "balance:buy:" + str(strategyId)
        r2.set(key_buy, json.dumps(buydata))  # 存入redis数据库
        cur.close()
        conn.close()
        return 1
    except Exception as e:
        i = "用户{}，初步部署网格策略{}，报错信息{}".format(userUuid, strategyId, e)
        print(i)
        logger.error(i)
        return 0


def balancestrategy_main(balancestrategydata):
    userUuid = balancestrategydata["userUuid"]  # 用户号
    apiAccountId = balancestrategydata["apiAccountId"]  # 子账户号
    strategyId = balancestrategydata["strategyId"]
    platform = balancestrategydata["platform"]
    symbol = balancestrategydata["symbol"]
    tradeCoin = symbol.split("_")[0]  # 交易币
    valCoin = symbol.split("_")[1]  # 计价币
    entryPrice = balancestrategydata["entryPrice"]  # 买入成本价
    initialTradeCoin = balancestrategydata["initialTradeCoin"]  # 初始交易币数量
    initialValCoin = balancestrategydata["initialValCoin"]  # 初始计价币数量
    spacingRatio = balancestrategydata["spacingRatio"]  # 网格间距
    stoplossRate = balancestrategydata["stoplossRate"]  # 止损比例
    init_trade = balancestrategydata["init_trade"]  # 第一笔买单是否进行
    currentprice = get_currentprice1(platform, symbol)
    if init_trade == 0:
        TradeCoin_amount, ValCoin_amount = banlancestrategy_begin_0(userUuid, apiAccountId, strategyId, platform,
                                                                    symbol, entryPrice, initialValCoin)
        balancestrategydata["initialTradeCoin"] = TradeCoin_amount
        balancestrategydata["initialValCoin"] = ValCoin_amount
        balancestrategydata["init_trade"] = 1
        r2.hset("balance_strategy", strategyId, json.dumps(balancestrategydata))
        balancestrategy_begin(userUuid, apiAccountId, strategyId, platform, symbol, entryPrice, TradeCoin_amount,
                              ValCoin_amount, spacingRatio)
    else:
        if currentprice != 0 and entryPrice - currentprice >= entryPrice * stoplossRate:
            print("平衡策略{}止损停止".format(strategyId))
            # 调用止损停止接口
            stopres = requests.post(stopBalanceStrategy_url,
                                    data={"strategyId": strategyId, "userUuid": userUuid, "status": 6})
            logger.info(stopres.text)
        try:
            sellnum = 0
            buynum = 0
            sell_order = json.loads(r2.get("balance:sell:{}".format(strategyId)))
            print("平衡策略{}卖单{}".format(strategyId, sell_order))
            sellamount = sell_order["sellamount"]
            sellprice = sell_order["sellprice"]
            sellorderid = sell_order["sellorderid"]
            buy_order = json.loads(r2.get("balance:buy:{}".format(strategyId)))
            print("平衡策略{}买单{}".format(strategyId, buy_order))
            buyamount = buy_order["buyamount"]
            buyprice = buy_order["buyprice"]
            buyorderid = buy_order["buyorderid"]
            sellquerydict = {}
            try:
                sell_query = {"direction": 2, "symbol": symbol, "platform": platform,
                              "orderId": sellorderid, "apiAccountId": apiAccountId, "userUuid": userUuid,
                              "source": 10, "icebergId": strategyId, "strategyId": strategyId}
                sellqueryres = requests.post(Queryorder_url, data=sell_query)
                sellquerydict = json.loads(sellqueryres.content.decode())
                sellnum = float(sellquerydict["response"]["numberDeal"])
                sellinfo = "用户{}交易平台{},卖单订单号{},价格{},已成交量{}".format(userUuid, platform, sellorderid, sellprice, sellnum)
                print(sellinfo)
            except Exception as e:
                i = "用户{}平衡策略{}交易平台{}查询订单{}出错{}，{}".format(userUuid, strategyId, platform, "sellorderid", e,
                                                           sellquerydict)
                print(i)
                logger.error(i)
            if sellnum != sellamount:  # 如果卖单没有成交或者没有完全成交，则查询买单是否成交，否则不查询
                buyquerydict = {}
                try:
                    buy_query = {"direction": 1, "symbol": symbol, "platform": platform,
                                 "orderId": buyorderid, "apiAccountId": apiAccountId, "userUuid": userUuid,
                                 "source": 10, "icebergId": strategyId, "strategyId": strategyId}
                    buyqueryres = requests.post(Queryorder_url, data=buy_query)
                    buyquerydict = json.loads(buyqueryres.content.decode())
                    buynum = float(buyquerydict["response"]["numberDeal"])
                    buyinfo = "用户{}交易平台{},买单订单号{},价格{},已成交量{}".format(userUuid, platform, buyorderid, buyprice, buynum)
                    print(buyinfo)
                except Exception as e:
                    i = "用户{}平衡策略{}交易平台{}查询订单{}出错{},{}".format(userUuid, strategyId, platform, buyorderid, e,
                                                               buyquerydict)
                    print(i)
                    logger.error(i)

            # 如果卖单成交
            if sellnum == sellamount:
                try:
                    conn = POOL.connection()
                    cur = conn.cursor()
                    info2 = "平衡策略{}已成交一个卖单，交易平台{}，成交价{}，成交数量{}，正在为您部署新的网格...".format(strategyId, platform, sellprice,
                                                                                     sellnum)
                    logger.info(info2)
                    # 1、修改卖单状态
                    sellfee = sellprice * sellnum * Fee[platform]["sellfee"]  # 手续费(卖单扣的是usdt)
                    selltradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    cur.execute(
                        'update balancelist set trade_amount=%s,trade_price=%s,trade_time=%s,status=%s,fee=%s where '
                        'strategyId=%s and orderid=%s',
                        (sellnum, sellprice, selltradetime, 1, sellfee, strategyId, sellorderid))
                    conn.commit()
                    # 2、撤销买单并修改状态
                    cancelres = requests.post(Cancel_url,
                                              params={"direction": 1, "symbol": symbol, "platform": platform,
                                                      "orderId": buyorderid, "apiAccountId": apiAccountId,
                                                      "userUuid": userUuid, "source": 10,
                                                      "icebergId": strategyId, "strategyId": strategyId})
                    resdict = json.loads(cancelres.content.decode())
                    cancelinfo = "用户{}策略{}平台{}撤销买单{}，返回结果{}".format(userUuid, strategyId, platform, buyorderid, resdict)
                    logger.info(cancelinfo)
                    cur.execute('update balancelist set status=%s where strategyId=%s and orderid=%s',
                                (2, strategyId, buyorderid))
                    conn.commit()
                    cur.close()
                    conn.close()
                    # 3、以最新成交价为入场价，以最新资产为入场资金，新挂买卖单
                    try:
                        remainres = requests.get(Remain_url,params={"userUuid": userUuid, "apiAccountId": apiAccountId})
                        remaindict = json.loads(remainres.content.decode())
                        TradeCoin_amount = [i["remains"] for i in remaindict["response"] if i["coin"] == tradeCoin][0]
                        ValCoin_amount = [i["remains"] for i in remaindict["response"] if i["coin"] == valCoin][0]
                        balancestrategy_begin(userUuid, apiAccountId, strategyId, platform, symbol, sellprice,
                                              TradeCoin_amount, ValCoin_amount,
                                              spacingRatio)
                    except Exception as e:
                        logger.error(e)
                except Exception as e:
                    errorinfo = "平衡策略{}成交卖单{}，部署新的订单时出错{}".format(strategyId, sellorderid, e)
                    logger.error(errorinfo)

            # 如果买单成交
            if buynum == buyamount:
                try:
                    conn = POOL.connection()
                    cur = conn.cursor()
                    info2 = "平衡策略{}已成交一个买单，交易平台{}，成交价{}，成交数量{}，正在为您部署新的网格...".format(strategyId, platform, buyprice,
                                                                                     buynum)
                    logger.info(info2)
                    # 1、修改买单状态
                    buytradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    buyfee = buynum * Fee[platform]["buyfee"]  # 手续费(买单扣的是交易币)
                    cur.execute(
                        'update balancelist set trade_amount=%s,trade_price=%s,trade_time=%s,status=%s,fee=%s where '
                        'strategyId=%s and orderid=%s',
                        (buynum, buyprice, buytradetime, 1, buyfee, strategyId, buyorderid))
                    conn.commit()
                    # 2、撤销卖单并修改状态
                    cancelres = requests.post(Cancel_url,
                                              params={"direction": 2, "symbol": symbol, "platform": platform,
                                                      "orderId": sellorderid,
                                                      "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 10,
                                                      "icebergId": strategyId, "strategyId": strategyId})
                    cancelinfo = "用户{}策略{}平台{}撤销卖单{}，返回结果{}".format(userUuid, strategyId, platform, sellorderid,
                                                                    cancelres.text)
                    logger.info(cancelinfo)
                    cur.execute('update balancelist set status=%s where strategyId=%s and orderid=%s',
                                (2, strategyId, sellorderid))
                    conn.commit()
                    cur.close()
                    conn.close()
                    # 3、以最新成交价为入场价，以最新资产为入场资金，新挂买卖单
                    try:
                        remainres = requests.get(Remain_url,
                                                 params={"userUuid": userUuid, "apiAccountId": apiAccountId})
                        remaindict = json.loads(remainres.content.decode())
                        TradeCoin_amount = [i["remains"] for i in remaindict["response"] if i["coin"] == tradeCoin][0]
                        ValCoin_amount = [i["remains"] for i in remaindict["response"] if i["coin"] == valCoin][0]
                        balancestrategy_begin(userUuid, apiAccountId, strategyId, platform, symbol, buyprice,
                                              TradeCoin_amount, ValCoin_amount,
                                              spacingRatio)
                    except Exception as e:
                        logger.error(e)
                except Exception as e:
                    errorinfo = "平衡策略{}成交买单{}，部署新的订单时出错{}".format(strategyId, buyorderid, e)
                    logger.error(errorinfo)

        except Exception as e:
            pass


def goDoBalanceStrategy():
    num = 0
    while True:
        try:
            print("*************************************************************")
            print("平衡策略第{}次运行".format(num))
            print("*************************************************************")
            # currentpricelist = get_all_currentprice()  # 获取所有平台所有交易对最新价
            allstrategyId = r2.hkeys("balance_strategy")
            strategyIdlist = []
            for strategyId in allstrategyId:
                if strategyId[-1] == str(num):
                    strategyIdlist.append(strategyId)
            if not strategyIdlist:
                i = "没有符合条件的策略"
                print(i)
            else:
                balanceThreads = []
                balancestrategydatalist = r2.hmget("balance_strategy", strategyIdlist)
                for balancedata in balancestrategydatalist:
                    print(balancedata)
                    balanceThreads.append(
                        Thread(target=balancestrategy_main, args=(json.loads(balancedata),)))
                for t in balanceThreads:
                    t.start()
                for t in balanceThreads:
                    t.join()
                del balanceThreads
            # del currentpricelist
            del allstrategyId
            del strategyIdlist
        except Exception as e:
            info = "平衡策略多线程报错{}".format(e)
            print(info)
            logger.error(info)
        finally:
            time.sleep(1)
            num += 1
            if num == 10:
                num = 0


if __name__ == '__main__':
    goDoBalanceStrategy()

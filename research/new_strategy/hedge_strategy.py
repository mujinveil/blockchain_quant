# -*- coding: utf-8 -*-
# @Time : 2020/4/1 11:22
import time
from threading import Thread
from tools.Config import hedge_place_order, hedge_query_order, hedge_cancel_order, Query_tradeprice_url, pricelimit, \
    hedge_syncOrder_url
import requests, json
from tools.databasePool import POOL, r2
from tools.get_market_info import MyThread, get_all_market_depth, get_market_depth


# userUuid = "5bf7a0d6ea7e45a9acab6fbc36937dcd"
# apiAccountId = 10056
# strategyId = 666
# platform = "befinx"
# amount = 0.001
# symbol = "btc_usdt"
# price = 6300
# direction = 1

# buydata = {"userUuid": userUuid,
#             "apiAccountId": apiAccountId,
#             "strategyId": strategyId,
#             "platform": platform,
#             "amount": amount,
#             "symbol": symbol,
#             "price": 6300,
#             "direction": 1,
#             "source": 1}
#
#
# selldata = {"userUuid": userUuid,
#             "apiAccountId": apiAccountId,
#             "strategyId": strategyId,
#             "platform": platform,
#             "amount": amount,
#             "symbol": symbol,
#             "price": 6500,
#             "direction": 2,
#             "source": 1}
#


def place_order(data):
    # 这里是伪代码，假设okex的订单成功
    # if data["platform"] != "befinx":
    #     data["orderId"] = int(time.time())
    # else:
    res = requests.post(hedge_place_order, data=data)
    resdict = json.loads(res.content.decode())
    data["orderId"] = resdict["response"]["orderId"]
    return data


def place_all_orders(buydata, selldata):
    result_list = []
    T = []
    for data in [buydata, selldata]:
        t = MyThread(place_order, (data,))
        T.append(t)
        t.start()
    for t in T:
        t.join()
        result_list.append(t.get_result())
    return result_list


# 查询对冲订单
def query_hedge_order(orderinfo):
    orderinfo["numberDeal"] = 0
    try:
        # if orderinfo["platform"] != "befinx":
        #     orderinfo["numberDeal"] = orderinfo["amount"]
        # else:
        res = requests.post(hedge_query_order, data=orderinfo)
        resdict = json.loads(res.content.decode())
        numberDeal = float(resdict["response"]["numberDeal"])
        orderinfo["numberDeal"] = numberDeal
        return orderinfo
    except Exception as e:
        info = "查询对冲订单出错{}".format(e)
        print(info)
        return orderinfo


# 查询所有订单
def query_all_order(buyorderinfo, sellorderinfo):
    result_list = []
    T = []
    for data in [buyorderinfo, sellorderinfo]:
        t = MyThread(query_hedge_order, (data,))
        T.append(t)
        t.start()
    for t in T:
        t.join()
        result_list.append(t.get_result())
    return result_list


# 取消订单
def cancel_hedge_order(orderinfo):
    num = 0
    try:
        while num <= 3:
            # if orderinfo["platform"] != "befinx":
            #     break
            # else:
            cancelres = requests.post(hedge_cancel_order, data=orderinfo)
            resdict = json.loads(cancelres.content.decode())
            print("{}平台撤单".format(orderinfo["platfrom"]), resdict)
            code = resdict["code"]
            if code == 1:
                break
    except Exception as e:
        num += 1
        info = "取消订单出错{}".format(e)


# 取消所有订单
def cancel_all_order(buyorderinfo, sellorderinfo):
    T = []
    for data in [buyorderinfo, sellorderinfo]:
        t = MyThread(cancel_hedge_order, (data,))
        T.append(t)
        t.start()
    for t in T:
        t.join()


def hedgeStrategy(hedgedata, allmarketdepthdata):
    userUuid = hedgedata["userUuid"]
    strategyId = hedgedata["strategyId"]
    apiAccountId1 = hedgedata["apiAccountId1"]
    apiAccountId2 = hedgedata["apiAccountId2"]
    platform1 = hedgedata["platform1"]
    platform2 = hedgedata["platform2"]
    takerFee1 = hedgedata["takerFee1"]
    takerFee2 = hedgedata["takerFee2"]
    symbol = hedgedata["symbol"]
    # 获取两个交易所的买一卖一价
    platform1_info = [i for i in allmarketdepthdata if i["platform"] == platform1 and i["symbol"] == symbol][0]
    platform2_info = [i for i in allmarketdepthdata if i["platform"] == platform2 and i["symbol"] == symbol][0]
    print(platform1_info)
    print(platform2_info)
    # 如果交易所1的买一价，大于交易所2的卖一价，并且在扣除手续费后，还有盈利空间，那么在交易所1下卖单同时交易所2下买单，高卖低买
    print(platform1_info["buyprice"] * (1 - takerFee1) - platform2_info["sellprice"] * (1 + takerFee2))
    if platform1_info["buyprice"] * (1 - takerFee1) - platform2_info["sellprice"] * (1 + takerFee2) > 0:
        # 1、同步下买卖单
        amount = min(platform1_info["buyquantity"], platform2_info["sellquantity"])
        selldata = {"userUuid": userUuid,
                    "apiAccountId": apiAccountId1,
                    "strategyId": strategyId,
                    "platform": platform1,
                    "amount": amount,
                    "symbol": symbol,
                    "price": platform1_info["buyprice"],
                    "direction": 2,
                    "source": 1}
        buydata = {"userUuid": userUuid,
                   "apiAccountId": apiAccountId2,
                   "strategyId": strategyId,
                   "platform": platform2,
                   "amount": amount,
                   "symbol": symbol,
                   "price": platform2_info["sellprice"],
                   "direction": 1,
                   "source": 1}
        orderlist = place_all_orders(selldata, buydata)
        sellorder = [i for i in orderlist if i["direction"] == 2][0]
        buyorder = [i for i in orderlist if i["direction"] == 1][0]
        print("买单", buyorder)
        print("卖单", sellorder)
        # 2、查询订单
        time.sleep(1)
        orderlist = query_all_order(buyorderinfo=buyorder, sellorderinfo=sellorder)
        sellorder = [i for i in orderlist if i["direction"] == 2][0]
        buyorder = [i for i in orderlist if i["direction"] == 1][0]
        print("查询买单", buyorder)
        print("查询卖单", sellorder)
        # 如果两边都成交，则存入数据库，进入下一轮循环
        if sellorder["numberDeal"] == amount and buyorder["numberDeal"] == amount:
            conn = POOL.connection()
            cur = conn.cursor()
            insertsql = "INSERT INTO hedgelist (strategyId,userUuid,apiAccountId,platform,symbol,direction,orderid,amount,price,time,status,profit,uniqueId)  \
                        VALUES(%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s)"
            timestr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            profit = (sellorder["price"] * (1 - takerFee1) - buyorder["price"] * (1 + takerFee2)) * amount
            uniqueId = int(time.time() * 1000)
            buyinsertdata = [buyorder["strategyId"], buyorder["userUuid"], buyorder["apiAccountId"],
                             buyorder["platform"], buyorder["symbol"], buyorder["direction"],
                             buyorder["orderId"], buyorder["amount"], buyorder["price"],
                             timestr, 1, profit, uniqueId]
            sellinsertdata = [sellorder["strategyId"], sellorder["userUuid"], sellorder["apiAccountId"],
                              sellorder["platform"], sellorder["symbol"], sellorder["direction"],
                              sellorder["orderId"], sellorder["amount"], sellorder["price"],
                              timestr, 1, profit, uniqueId]
            cur.execute(insertsql, buyinsertdata)
            cur.execute(insertsql, sellinsertdata)
            conn.commit()
            cur.close()
            conn.close()
            # 同步至java
            res = requests.post(hedge_syncOrder_url, data={"listJson": json.dumps(
                [
                    {
                        "amount": amount,
                        "apiAccountId": buyorder["apiAccountId"],
                        "addDate": timestr,
                        "direction": 1,
                        "id": strategyId,
                        "orderid": buyorder["orderId"],
                        "platform": buyorder["platform"],
                        "price": buyorder["price"],
                        "profit": profit,
                        "status": 1,
                        "strategyId": strategyId,
                        "symbol": symbol,
                        "uniqueId": uniqueId,
                        "userUuid": buyorder["userUuid"]
                    },
                    {
                        "amount": amount,
                        "apiAccountId": sellorder["apiAccountId"],
                        "addDate": timestr,
                        "direction": 2,
                        "id": strategyId,
                        "orderid": sellorder["orderId"],
                        "platform": sellorder["platform"],
                        "price": sellorder["price"],
                        "profit": profit,
                        "status": 1,
                        "strategyId": strategyId,
                        "symbol": symbol,
                        "uniqueId": uniqueId,
                        "userUuid": sellorder["userUuid"]
                    }
                ])})
        # 如果两边没有完全成交，则采取补单的方式，此时可能要放弃利润
        else:
            # 1、先同步撤单(包括两边都未成交的情况)
            cancel_all_order(buyorderinfo=buyorder, sellorderinfo=sellorder)
            # 2、计算已成交数量的差值
            if sellorder["numberDeal"] > buyorder["numberDeal"]:  # 如果卖单成交较多，则补齐买单
                # 先获取买方交易所的卖一价，然后溢价买入
                newbuyprice = round(get_market_depth(platform2, symbol)["sellprice"] * (1 + 0.001),
                                    pricelimit[symbol][platform2])
                newbuyamount = sellorder["numberDeal"] - buyorder["numberDeal"]
                newbuydata = {"userUuid": userUuid,
                              "apiAccountId": apiAccountId2,
                              "strategyId": strategyId,
                              "platform": platform2,
                              "amount": newbuyamount,
                              "symbol": symbol,
                              "price": newbuyprice,
                              "direction": 1,
                              "source": 1}
                newbuyorder = place_order(data=newbuydata)  # 对冲买入接口
                tradeprice = newbuyprice
                try:
                    # 查询实际成交均价
                    res = requests.post(Query_tradeprice_url, data=newbuyorder)
                    queryresdict = json.loads(res.content.decode())
                    if queryresdict["response"]["avgPrice"] != None:
                        tradeprice = queryresdict["response"]["avgPrice"]
                except:
                    pass
                conn = POOL.connection()
                cur = conn.cursor()
                insertsql = "INSERT INTO hedgelist (strategyId,userUuid,apiAccountId,platform,symbol,direction,orderid,amount,price,time,status,profit,uniqueId)  \
                                        VALUES(%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s)"
                timestr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                avg_buyprice = (buyorder["numberDeal"] * buyorder["price"] + tradeprice * newbuyamount) / sellorder[
                    "numberDeal"]
                profit = (sellorder["price"] * (1 - takerFee1) - avg_buyprice * (1 + takerFee2)) * sellorder[
                    "numberDeal"]
                uniqueId = int(time.time() * 1000)
                buyinsertdata = [buyorder["strategyId"], buyorder["userUuid"], buyorder["apiAccountId"],
                                 buyorder["platform"], buyorder["symbol"], buyorder["direction"],
                                 buyorder["orderId"], sellorder["numberDeal"], avg_buyprice,
                                 timestr, 1, profit, uniqueId]
                sellinsertdata = [sellorder["strategyId"], sellorder["userUuid"], sellorder["apiAccountId"],
                                  sellorder["platform"], sellorder["symbol"], sellorder["direction"],
                                  sellorder["orderId"], sellorder["numberDeal"], sellorder["price"],
                                  timestr, 1, profit, uniqueId]
                cur.execute(insertsql, buyinsertdata)
                cur.execute(insertsql, sellinsertdata)
                conn.commit()
                cur.close()
                conn.close()
                # 同步至java
                res = requests.post(hedge_syncOrder_url, data={"listJson": json.dumps(
                    [
                        {
                            "amount": sellorder["numberDeal"],
                            "apiAccountId": buyorder["apiAccountId"],
                            "addDate": timestr,
                            "direction": 1,
                            "id": strategyId,
                            "orderid": buyorder["orderId"],
                            "platform": buyorder["platform"],
                            "price": avg_buyprice,
                            "profit": profit,
                            "status": 1,
                            "strategyId": strategyId,
                            "symbol": symbol,
                            "uniqueId": uniqueId,
                            "userUuid": buyorder["userUuid"]
                        },
                        {
                            "amount": sellorder["numberDeal"],
                            "apiAccountId": sellorder["apiAccountId"],
                            "addDate": timestr,
                            "direction": 2,
                            "id": strategyId,
                            "orderid": sellorder["orderId"],
                            "platform": sellorder["platform"],
                            "price": sellorder["price"],
                            "profit": profit,
                            "status": 1,
                            "strategyId": strategyId,
                            "symbol": symbol,
                            "uniqueId": uniqueId,
                            "userUuid": sellorder["userUuid"]
                        }
                    ])})

            if sellorder["numberDeal"] < buyorder["numberDeal"]:  # 如果买单成交较多，则补齐卖单
                # 先获取卖方交易所的买一价，然后溢价卖出
                newsellprice = round(get_market_depth(platform1, symbol)["buyprice"] * (1 - 0.001),
                                     pricelimit[symbol][platform1])
                newsellamount = buyorder["numberDeal"] - sellorder["numberDeal"]
                newselldata = {"userUuid": userUuid,
                               "apiAccountId": apiAccountId1,
                               "strategyId": strategyId,
                               "platform": platform1,
                               "amount": newsellamount,
                               "symbol": symbol,
                               "price": newsellprice,
                               "direction": 2,
                               "source": 1}
                newsellorder = place_order(data=newselldata)
                tradeprice = newsellprice
                try:
                    # 查询实际成交均价
                    res = requests.post(Query_tradeprice_url, data=newsellorder)
                    queryresdict = json.loads(res.content.decode())
                    if queryresdict["response"]["avgPrice"] != None:
                        tradeprice = queryresdict["response"]["avgPrice"]
                except:
                    pass
                conn = POOL.connection()
                cur = conn.cursor()
                insertsql = "INSERT INTO hedgelist (strategyId,userUuid,apiAccountId,platform,symbol,direction,orderid,amount,price,time,status,profit,uniqueId)  \
                                                        VALUES(%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s)"
                timestr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                avg_sellprice = (sellorder["numberDeal"] * sellorder["price"] + tradeprice * newsellamount) / buyorder[
                    "numberDeal"]
                profit = (avg_sellprice * (1 - takerFee1) - buyorder["price"] * (1 + takerFee2)) * buyorder[
                    "numberDeal"]
                uniqueId = int(time.time() * 1000)
                buyinsertdata = [buyorder["strategyId"], buyorder["userUuid"], buyorder["apiAccountId"],
                                 buyorder["platform"], buyorder["symbol"], buyorder["direction"],
                                 buyorder["orderId"], buyorder["numberDeal"], buyorder["price"],
                                 timestr, 1, profit, uniqueId]
                sellinsertdata = [sellorder["strategyId"], sellorder["userUuid"], sellorder["apiAccountId"],
                                  sellorder["platform"], sellorder["symbol"], sellorder["direction"],
                                  sellorder["orderId"], buyorder["numberDeal"], avg_sellprice,
                                  timestr, 1, profit, uniqueId]
                cur.execute(insertsql, buyinsertdata)
                cur.execute(insertsql, sellinsertdata)
                conn.commit()
                cur.close()
                conn.close()
                # 同步至java
                res = requests.post(hedge_syncOrder_url, data={"listJson": json.dumps(
                    [
                        {
                            "amount": buyorder["numberDeal"],
                            "apiAccountId": buyorder["apiAccountId"],
                            "addDate": timestr,
                            "direction": 1,
                            "id": strategyId,
                            "orderid": buyorder["orderId"],
                            "platform": buyorder["platform"],
                            "price": buyorder["price"],
                            "profit": profit,
                            "status": 1,
                            "strategyId": strategyId,
                            "symbol": symbol,
                            "uniqueId": uniqueId,
                            "userUuid": buyorder["userUuid"]
                        },
                        {
                            "amount": buyorder["numberDeal"],
                            "apiAccountId": sellorder["apiAccountId"],
                            "addDate": timestr,
                            "direction": 2,
                            "id": strategyId,
                            "orderid": sellorder["orderId"],
                            "platform": sellorder["platform"],
                            "price": avg_sellprice,
                            "profit": profit,
                            "status": 1,
                            "strategyId": strategyId,
                            "symbol": symbol,
                            "uniqueId": uniqueId,
                            "userUuid": sellorder["userUuid"]
                        }
                    ])})

    # 如果交易所2的买一价，大于交易所1的卖一价，并且在扣除手续费后，还有盈利空间，那么在交易所2下卖单同时交易所1下买单，高卖低买
    print(platform2_info["buyprice"] * (1 - takerFee2) - platform1_info["sellprice"] * (1 + takerFee1))
    if platform2_info["buyprice"] * (1 - takerFee2) - platform1_info["sellprice"] * (1 + takerFee1) > 0:
        # 同时下买卖单
        amount = min(platform2_info["buyquantity"], platform1_info["sellquantity"])
        selldata = {"userUuid": userUuid,
                    "apiAccountId": apiAccountId2,
                    "strategyId": strategyId,
                    "platform": platform2,
                    "amount": amount,
                    "symbol": symbol,
                    "price": platform2_info["buyprice"],
                    "direction": 2,
                    "source": 1}
        buydata = {"userUuid": userUuid,
                   "apiAccountId": apiAccountId1,
                   "strategyId": strategyId,
                   "platform": platform1,
                   "amount": amount,
                   "symbol": symbol,
                   "price": platform1_info["sellprice"],
                   "direction": 1,
                   "source": 1}
        orderlist = place_all_orders(selldata, buydata)
        sellorder = [i for i in orderlist if i["direction"] == 2][0]
        buyorder = [i for i in orderlist if i["direction"] == 1][0]
        print("买单", buyorder)
        print("卖单", sellorder)
        # 2、查询订单
        time.sleep(1)
        orderlist = query_all_order(buyorderinfo=buyorder, sellorderinfo=sellorder)
        sellorder = [i for i in orderlist if i["direction"] == 2][0]
        buyorder = [i for i in orderlist if i["direction"] == 1][0]
        print("查询买单", buyorder)
        print("查询卖单", sellorder)
        if sellorder["numberDeal"] == amount and buyorder["numberDeal"] == amount:
            conn = POOL.connection()
            cur = conn.cursor()
            insertsql = "INSERT INTO hedgelist (strategyId,userUuid,apiAccountId,platform,symbol,direction,orderid,amount,price,time,status,profit,uniqueId)  \
                        VALUES(%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s)"
            timestr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            status = 1
            profit = (sellorder["price"] * (1 - takerFee2) - buyorder["price"] * (1 + takerFee1)) * amount
            uniqueId = int(time.time() * 1000)
            buyinsertdata = [buyorder["strategyId"], buyorder["userUuid"], buyorder["apiAccountId"],
                             buyorder["platform"], buyorder["symbol"], buyorder["direction"],
                             buyorder["orderId"], buyorder["numberDeal"], buyorder["price"],
                             timestr, status, profit, uniqueId]
            sellinsertdata = [sellorder["strategyId"], sellorder["userUuid"], sellorder["apiAccountId"],
                              sellorder["platform"], sellorder["symbol"], sellorder["direction"],
                              sellorder["orderId"], sellorder["numberDeal"], sellorder["price"],
                              timestr, status, profit, uniqueId]
            cur.execute(insertsql, buyinsertdata)
            cur.execute(insertsql, sellinsertdata)
            conn.commit()
            cur.close()
            conn.close()
            # 同步至java
            res = requests.post(hedge_syncOrder_url, data={"listJson": json.dumps(
                [
                    {
                        "amount": buyorder["numberDeal"],
                        "apiAccountId": buyorder["apiAccountId"],
                        "addDate": timestr,
                        "direction": 1,
                        "id": strategyId,
                        "orderid": buyorder["orderId"],
                        "platform": buyorder["platform"],
                        "price": buyorder["price"],
                        "profit": profit,
                        "status": 1,
                        "strategyId": strategyId,
                        "symbol": symbol,
                        "uniqueId": uniqueId,
                        "userUuid": buyorder["userUuid"]
                    },
                    {
                        "amount": sellorder["numberDeal"],
                        "apiAccountId": sellorder["apiAccountId"],
                        "addDate": timestr,
                        "direction": 2,
                        "id": strategyId,
                        "orderid": sellorder["orderId"],
                        "platform": sellorder["platform"],
                        "price": sellorder["price"],
                        "profit": profit,
                        "status": 1,
                        "strategyId": strategyId,
                        "symbol": symbol,
                        "uniqueId": uniqueId,
                        "userUuid": sellorder["userUuid"]
                    }
                ])})
        else:
            # 1、先同步撤单(包括两边都未成交的情况)
            cancel_all_order(buyorderinfo=buyorder, sellorderinfo=sellorder)
            # 2、计算已成交数量的差值
            if sellorder["numberDeal"] > buyorder["numberDeal"]:  # 如果卖单成交较多，则补齐买单
                # 先获取买方交易所的卖一价，然后溢价买入
                newbuyprice = round(get_market_depth(platform1, symbol)["sellprice"] * (1 + 0.001),
                                    pricelimit[symbol][platform1])
                newbuyamount = sellorder["numberDeal"] - buyorder["numberDeal"]
                newbuydata = {"userUuid": userUuid,
                              "apiAccountId": apiAccountId1,
                              "strategyId": strategyId,
                              "platform": platform1,
                              "amount": newbuyamount,
                              "symbol": symbol,
                              "price": newbuyprice,
                              "direction": 1,
                              "source": 1}
                newbuyorder = place_order(data=newbuydata)
                tradeprice = newbuyprice
                try:
                    # 查询实际成交均价
                    res = requests.post(Query_tradeprice_url, data=newbuyorder)
                    queryresdict = json.loads(res.content.decode())
                    if queryresdict["response"]["avgPrice"] != None:
                        tradeprice = queryresdict["response"]["avgPrice"]
                except:
                    pass
                conn = POOL.connection()
                cur = conn.cursor()
                insertsql = "INSERT INTO hedgelist (strategyId,userUuid,apiAccountId,platform,symbol,direction,orderid,amount,price,time,status,profit,uniqueId)  \
                                                    VALUES(%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s)"
                timestr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                avg_buyprice = (buyorder["numberDeal"] * buyorder["price"] + tradeprice * newbuyamount) / sellorder[
                    "numberDeal"]
                profit = (sellorder["price"] * (1 - takerFee2) - avg_buyprice * (1 + takerFee1)) * sellorder[
                    "numberDeal"]
                uniqueId = int(time.time() * 1000)
                buyinsertdata = [buyorder["strategyId"], buyorder["userUuid"], buyorder["apiAccountId"],
                                 buyorder["platform"], buyorder["symbol"], buyorder["direction"],
                                 buyorder["orderId"], sellorder["numberDeal"], avg_buyprice,
                                 timestr, 1, profit, uniqueId]
                sellinsertdata = [sellorder["strategyId"], sellorder["userUuid"], sellorder["apiAccountId"],
                                  sellorder["platform"], sellorder["symbol"], sellorder["direction"],
                                  sellorder["orderId"], sellorder["numberDeal"], sellorder["price"],
                                  timestr, 1, profit, uniqueId]
                cur.execute(insertsql, buyinsertdata)
                cur.execute(insertsql, sellinsertdata)
                conn.commit()
                cur.close()
                conn.close()
                # 同步至java
                res = requests.post(hedge_syncOrder_url, data={"listJson": json.dumps(
                    [
                        {
                            "amount": sellorder["numberDeal"],
                            "apiAccountId": buyorder["apiAccountId"],
                            "addDate": timestr,
                            "direction": 1,
                            "id": strategyId,
                            "orderid": buyorder["orderId"],
                            "platform": buyorder["platform"],
                            "price": avg_buyprice,
                            "profit": profit,
                            "status": 1,
                            "strategyId": strategyId,
                            "symbol": symbol,
                            "uniqueId": uniqueId,
                            "userUuid": buyorder["userUuid"]
                        },
                        {
                            "amount": sellorder["numberDeal"],
                            "apiAccountId": sellorder["apiAccountId"],
                            "addDate": timestr,
                            "direction": 2,
                            "id": strategyId,
                            "orderid": sellorder["orderId"],
                            "platform": sellorder["platform"],
                            "price": sellorder["price"],
                            "profit": profit,
                            "status": 1,
                            "strategyId": strategyId,
                            "symbol": symbol,
                            "uniqueId": uniqueId,
                            "userUuid": sellorder["userUuid"]
                        }
                    ])})

            if sellorder["numberDeal"] < buyorder["numberDeal"]:  # 如果买单成交较多，则补齐卖单
                # 先获取卖方交易所的买一价，然后溢价卖出
                newsellprice = round(get_market_depth(platform2, symbol)["buyprice"] * (1 - 0.001),
                                     pricelimit[symbol][platform2])
                newsellamount = buyorder["numberDeal"] - sellorder["numberDeal"]
                newselldata = {"userUuid": userUuid,
                               "apiAccountId": apiAccountId2,
                               "strategyId": strategyId,
                               "platform": platform2,
                               "amount": newsellamount,
                               "symbol": symbol,
                               "price": newsellprice,
                               "direction": 2,
                               "source": 1}
                newsellorder = place_order(data=newselldata)
                tradeprice = newsellprice
                try:
                    # 查询实际成交均价
                    res = requests.post(Query_tradeprice_url, data=newsellorder)
                    queryresdict = json.loads(res.content.decode())
                    if queryresdict["response"]["avgPrice"] is not None:
                        tradeprice = queryresdict["response"]["avgPrice"]
                except:
                    pass
                conn = POOL.connection()
                cur = conn.cursor()
                insertsql = "INSERT INTO hedgelist (strategyId,userUuid,apiAccountId,platform,symbol,direction,orderid,amount,price,time,status,profit,uniqueId)  \
                                                                    VALUES(%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s)"
                timestr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                avg_sellprice = (sellorder["numberDeal"] * sellorder["price"] + tradeprice * newsellamount) / buyorder[
                    "numberDeal"]
                profit = (avg_sellprice * (1 - takerFee2) - buyorder["price"] * (1 + takerFee1)) * buyorder[
                    "numberDeal"]
                uniqueId = int(time.time() * 1000)
                buyinsertdata = [buyorder["strategyId"], buyorder["userUuid"], buyorder["apiAccountId"],
                                 buyorder["platform"], buyorder["symbol"], buyorder["direction"],
                                 buyorder["orderId"], buyorder["numberDeal"], buyorder["price"],
                                 timestr, 1, profit, uniqueId]
                sellinsertdata = [sellorder["strategyId"], sellorder["userUuid"], sellorder["apiAccountId"],
                                  sellorder["platform"], sellorder["symbol"], sellorder["direction"],
                                  sellorder["orderId"], buyorder["numberDeal"], avg_sellprice,
                                  timestr, 1, profit, uniqueId]
                cur.execute(insertsql, buyinsertdata)
                cur.execute(insertsql, sellinsertdata)
                conn.commit()
                cur.close()
                conn.close()
                # 同步至java
                res = requests.post(hedge_syncOrder_url, data={"listJson": json.dumps(
                    [
                        {
                            "amount": buyorder["numberDeal"],
                            "apiAccountId": buyorder["apiAccountId"],
                            "addDate": timestr,
                            "direction": 1,
                            "id": strategyId,
                            "orderid": buyorder["orderId"],
                            "platform": buyorder["platform"],
                            "price": buyorder["price"],
                            "profit": profit,
                            "status": 1,
                            "strategyId": strategyId,
                            "symbol": symbol,
                            "uniqueId": uniqueId,
                            "userUuid": buyorder["userUuid"]
                        },
                        {
                            "amount": buyorder["numberDeal"],
                            "apiAccountId": sellorder["apiAccountId"],
                            "addDate": timestr,
                            "direction": 2,
                            "id": strategyId,
                            "orderid": sellorder["orderId"],
                            "platform": sellorder["platform"],
                            "price": avg_sellprice,
                            "profit": profit,
                            "status": 1,
                            "strategyId": strategyId,
                            "symbol": symbol,
                            "uniqueId": uniqueId,
                            "userUuid": sellorder["userUuid"]
                        }
                    ])})

    else:
        print("没有盈利空间，进入下一轮对冲")


while True:
    try:
        hedgedatalist = r2.hvals("Hedge_Strategy")
        hedgedatalist = [json.loads(i) for i in hedgedatalist]
        if hedgedatalist == []:
            time.sleep(2)
        else:
            allmarketdepthdata = get_all_market_depth()
            # print(allmarketdepthdata)
            T = []
            for hedgedata in hedgedatalist:
                t = Thread(target=hedgeStrategy, args=(hedgedata, allmarketdepthdata))
                T.append(t)
                t.start()
            for t in T:
                t.join()
        time.sleep(0.5)
    except:
        pass

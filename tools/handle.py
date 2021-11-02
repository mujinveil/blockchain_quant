# encoding=utf-8
import json
import time
from threading import Thread
import pymysql
import requests
from tools.Config import Trade_url, Queryorder_url, Cancel_url, Bulk_trade_url, Bulk_cancel_url
from tools.databasePool import r1, r2, POOL


# 根据数量和价格下买单(用于刷单)
def buy(userUuid, apiAccountId, strategyId, platform, symbol, amount, price):
    try:
        data = {"userUuid": userUuid,
                "apiAccountId": apiAccountId,
                "strategyId": strategyId,
                "platform": platform,
                "amount": amount,
                "symbol": symbol,
                "price": price,
                "direction": 1,
                "source": 7}
        res = requests.post(Trade_url, data=data)
        resdict = json.loads(res.content.decode())
        if resdict["code"] == 1:
            print("用户{}-{}下买单成功,订单id{},数量{}，金额{}".format(userUuid, apiAccountId, resdict["response"]["orderid"], amount,
                                                         price))
        else:
            print("用户{}-{}下买单失败,交易对{},数量{}，金额{},response{}".format(userUuid, apiAccountId, symbol, amount, price,
                                                                   resdict))
        return resdict["response"]["orderid"]
    except Exception as e:
        print("用户{}-{}下买单失败，交易对{},数量{}，金额{},报错信息{}".format(userUuid, apiAccountId, symbol, amount, price, e))


# 根据数量和价格下卖单(用于刷单)
def sell(userUuid, apiAccountId, strategyId, platform, symbol, amount, price):
    try:
        data = {"userUuid": userUuid, "apiAccountId": apiAccountId, "strategyId": strategyId, "platform": platform,
                "amount": amount, "symbol": symbol, "price": price, "direction": 2, "source": 7}
        res = requests.post(Trade_url, data=data)
        resdict = json.loads(res.content.decode())
        if resdict["code"] == 1:
            print("用户{}-{}下卖单成功,订单id{},数量{}，金额{}".format(userUuid, apiAccountId, resdict["response"]["orderid"], amount,
                                                         price))
        else:
            print("用户{}-{}下卖单失败,交易对{},数量{}，金额{},response{}".format(userUuid, apiAccountId, symbol, amount, price,
                                                                   resdict))
        return resdict["response"]["orderid"]
    except Exception as e:
        print("用户{}-{}下卖单失败,交易对{},数量{}，金额{},报错信息{}".format(userUuid, apiAccountId, symbol, amount, price, e))


# 查询订单
def query(userUuid, apiAccountId, strategyId, platform, symbol, orderId):
    try:
        data = {"userUuid": userUuid,
                "apiAccountId": apiAccountId,
                "strategyId": strategyId,
                "platform": platform,
                "symbol": symbol,
                "orderId": orderId,
                "source": 7}
        res = requests.post(Queryorder_url, data=data)
        resdict = json.loads(res.content.decode())
        # status = resdict["response"]["status"]
        numberDeal = float(resdict["response"]["numberDeal"])
        print("用户{}-{}，订单{}，成交数量{}".format(userUuid, apiAccountId, orderId, numberDeal))
        return numberDeal
    except Exception as e:
        print("用户{}-{}查询订单{}失败，报错信息{}".format(userUuid, apiAccountId, orderId, e))


# 撤销订单（用于刷单)
def cancel(userUuid, apiAccountId, strategyId, platform, symbol, orderId):
    try:
        data = {"userUuid": userUuid,
                "apiAccountId": apiAccountId,
                "strategyId": strategyId,
                "platform": platform,
                "symbol": symbol,
                "orderId": orderId,
                "source": 7}
        res = requests.post(Cancel_url, data=data)
        resdict = json.loads(res.content.decode())
        print("用户{}-{}，撤销订单{},返回{}".format(userUuid, apiAccountId, orderId, resdict))
    except Exception as e:
        print("用户{}-{}撤销交易对{}订单{}失败，报错信息{}".format(userUuid, apiAccountId, symbol, orderId, e))


# 根据数量和价格下买单(用于盘口订单)
def buy_1(userUuid, apiAccountId, strategyId, platform, symbol, amount, price):
    try:
        data = {"userUuid": userUuid,
                "apiAccountId": apiAccountId,
                "strategyId": strategyId,
                "platform": platform,
                "amount": amount,
                "symbol": symbol,
                "price": price,
                "direction": 1,
                "source": 8}
        res = requests.post(Trade_url, data=data)
        resdict = json.loads(res.content.decode())
        if resdict["code"] == 1:
            print("用户{}-{}下买单成功，数量{}，金额{}".format(userUuid, apiAccountId, amount, price))
            orderId = resdict["response"]["orderid"]
            if orderId is not None:
                data["orderId"] = orderId
                r1.hset("T8ex_{}_buyorders".format(symbol), orderId, json.dumps(data))
        else:
            print(
                "用户{}-{}下买单失败，交易对{},数量{}，金额{}，reponse{}".format(userUuid, apiAccountId, symbol, amount, price, resdict))
    except Exception as e:
        print("用户{}-{}下买单失败，交易对{},数量{}，金额{},报错信息{}".format(userUuid, apiAccountId, symbol, amount, price, e))


# 根据数量和价格下卖单(用于盘口订单)
def sell_1(userUuid, apiAccountId, strategyId, platform, symbol, amount, price):
    try:
        data = {"userUuid": userUuid,
                "apiAccountId": apiAccountId,
                "strategyId": strategyId,
                "platform": platform,
                "amount": amount,
                "symbol": symbol,
                "price": price,
                "direction": 2,
                "source": 8}
        res = requests.post(Trade_url, data=data)
        resdict = json.loads(res.content.decode())
        if resdict["code"] == 1:
            print("用户{}-{}下卖单成功，数量{}，金额{}".format(userUuid, apiAccountId, amount, price))
            orderId = resdict["response"]["orderid"]
            if orderId is not None:
                data["orderId"] = orderId
                r1.hset("T8ex_{}_sellorders".format(symbol), orderId, json.dumps(data))
        else:
            print("用户{}-{}下卖单失败，交易对{},数量{}，金额{},response{}".format(userUuid, apiAccountId, symbol, amount, price,
                                                                   resdict))
    except Exception as e:
        print("用户{}-{}下卖单失败，交易对{},数量{}，金额{},报错信息{}".format(userUuid, apiAccountId, symbol, amount, price, e))


# 撤销订单（用于盘口订单）
def cancel_1(userUuid, apiAccountId, strategyId, platform, symbol, orderId, direction):
    try:
        data = {"userUuid": userUuid,
                "apiAccountId": apiAccountId,
                "strategyId": strategyId,
                "platform": platform,
                "symbol": symbol,
                "orderId": orderId,
                "source": 8}
        res = requests.post(Cancel_url, data=data)
        resdict = json.loads(res.content.decode())
        if resdict["code"] == 1:
            if direction == 1:
                r1.hdel("T8ex_{}_buyorders".format(symbol), orderId)
                print("用户{}-{}，撤销买单{}，返回{}".format(userUuid, apiAccountId, orderId, resdict))
            if direction == 2:
                r1.hdel("T8ex_{}_sellorders".format(symbol), orderId)
                print("用户{}-{}，撤销卖单{}，返回{}".format(userUuid, apiAccountId, orderId, resdict))
    except Exception as e:
        print("用户{}-{}撤销订单{}失败，报错信息{}".format(userUuid, apiAccountId, orderId, e))


# 查询订单
def query_1(userUuid, apiAccountId, strategyId, platform, symbol, orderId, direction):
    try:
        data = {"userUuid": userUuid,
                "apiAccountId": apiAccountId,
                "strategyId": strategyId,
                "platform": platform,
                "symbol": symbol,
                "orderId": orderId,
                "direction": direction,
                "source": 8}
        res = requests.post(Queryorder_url, data=data)
        resdict = json.loads(res.content.decode())
        if resdict['message'] == "订单查询未果":
            if direction == 1:
                r1.hdel("T8ex_{}_buyorders".format(symbol), orderId)
            if direction == 2:
                r1.hdel("T8ex_{}_sellorders".format(symbol), orderId)
        else:
            status = resdict["response"]["status"]
            print("用户{}-{}，订单{}，状态{}".format(userUuid, apiAccountId, orderId, status))
            if status == "closed" or status == "cancelled":  # 状态 open挂单中 closed已完成 cancelled撤单 part部分交易
                if direction == 1:
                    r1.hdel("T8ex_{}_buyorders".format(symbol), orderId)
                if direction == 2:
                    r1.hdel("T8ex_{}_sellorders".format(symbol), orderId)
    except Exception as e:
        print("用户{}-{}查询订单{}失败，报错信息{}".format(userUuid, apiAccountId, orderId, e))


# 多线程批量挂单,传入订单列表[{"amount":0.01,"price":5201.2},{},{}...]
def place_all_orders(buylist, selllist):
    try:
        T = []
        for i in buylist:
            T.append(Thread(target=buy_1, args=(
                i["userUuid"], i["apiAccountId"], i["strategyId"], i["platform"], i["symbol"], i["amount"],
                i["price"])))
        for j in selllist:
            T.append(Thread(target=sell_1, args=(
                j["userUuid"], j["apiAccountId"], j["strategyId"], j["platform"], j["symbol"], j["amount"],
                j["price"])))
        for t in T:
            t.start()
            time.sleep(0.2)  # 假多线程……因为交易所限制了下单频次
        for t in T:
            t.join()
    except:
        pass


# 多线程批量挂买单,传入订单列表[{"amount":0.01,"price":5201.2},{},{}...]
def place_buy_orders(buylist):
    try:
        T = []
        for i in buylist:
            T.append(Thread(target=buy_1, args=(
                i["userUuid"], i["apiAccountId"], i["strategyId"], i["platform"], i["symbol"], i["amount"],
                i["price"])))
        for t in T:
            t.start()
            time.sleep(0.2)
        for t in T:
            t.join()
    except:
        pass


# 多线程批量挂卖单,传入订单列表[{"amount":0.01,"price":5201.2},{},{}...]
def place_sell_orders(selllist):
    try:
        T = []
        for j in selllist:
            T.append(Thread(target=sell_1, args=(
                j["userUuid"], j["apiAccountId"], j["strategyId"], j["platform"], j["symbol"], j["amount"],
                j["price"])))
        for t in T:
            t.start()
            time.sleep(0.2)
        for t in T:
            t.join()
    except:
        pass


def bulk_currency_buy_orders(buylist):
    try:
        if not buylist:
            return
        res = requests.post(Bulk_trade_url, data={"orderInfos": json.dumps(buylist)},stream = True)
        resdict = json.loads(res.content.decode())
        if resdict["code"] == 1:
            res = resdict["response"]
            if res is not None:
                for i in res:
                    userUuid = i['userUuid']
                    apiAccountId = i['apiAccountId']
                    strategyId=i['icebergId']
                    symbol = i['symbol']
                    orderId = i['orderid']
                    price=i['price']
                    platform=i['platform']
                    data = {'userUuid': userUuid, "apiAccountId": apiAccountId,"price":price,"direction":1,
                            "strategyId":strategyId,"symbol": symbol, "orderId": orderId,"platform":platform}
                    r1.hset("T8ex_{}_buyorders".format(symbol), orderId, json.dumps(data))
    except Exception as e:
        print("用户{}-{}批量下买单失败,报错信息{}".format(userUuid, apiAccountId, e))


def bulk_currency_sell_orders(selllist):
    try:
        if not selllist:
            return
        res = requests.post(Bulk_trade_url, data={"orderInfos": json.dumps(selllist)},stream = True)
        resdict = json.loads(res.content.decode())
        if resdict["code"] == 1:
            res = resdict["response"]
            if res is not None:
                for i in res:
                    userUuid = i['userUuid']
                    apiAccountId = i['apiAccountId']
                    strategyId = i['icebergId']
                    symbol = i['symbol']
                    platform =i['platform']
                    price = i['price']
                    orderId = i['orderid']
                    data = {'userUuid': userUuid, "apiAccountId": apiAccountId,"price":price,"direction":2,
                            "strategyId":strategyId,"symbol": symbol, "orderId": orderId,"platform":platform}
                    r1.hset("T8ex_{}_sellorders".format(symbol), orderId, json.dumps(data))
    except Exception as e:
        print("用户{}-{}批量下卖单失败,报错信息{}".format(userUuid, apiAccountId, e))


def bulk_currency_cancel_orders(orderlist):
    try:
        if not orderlist:
            return
        cancelorderlist = []
        for i in orderlist:
            data = {"userUuid": i['userUuid'],
                    "apiAccountId": i['apiAccountId'],
                    "strategyId": i['strategyId'],
                    "platform": i['platform'],
                    "symbol": i['symbol'],
                    "orderId": i['orderId'],
                    "source": 8}
            cancelorderlist.append(data)
        res = requests.post(Bulk_cancel_url, data={'orderIds': json.dumps(cancelorderlist)})
        resdict = json.loads(res.content.decode())
        userUuid = orderlist[0]['userUuid']
        apiAccountId = orderlist[0]['apiAccountId']
        symbol = orderlist[0]['symbol']
        if resdict["code"] == 1:
            for i in range(len(orderlist)):
                direction = orderlist[i]['direction']
                orderId = orderlist[i]['orderId']
                if direction == 1:
                    r1.hdel("T8ex_{}_buyorders".format(symbol), orderId)
                    print("用户{}-{}，撤销买单{}，返回{}".format(userUuid, apiAccountId, orderId, resdict))
                if direction == 2:
                    r1.hdel("T8ex_{}_sellorders".format(symbol), orderId)
                    print("用户{}-{}，撤销卖单{}，返回{}".format(userUuid, apiAccountId, orderId, resdict))
    except Exception as e:
        print("用户{}-{}批量撤销订单失败，报错信息{}".format(userUuid, apiAccountId, e))


# 获取网格订单买卖单对比列表
def get_grid_orderlist(strategyId):
    try:
        data = {"buy_count": 0, "buy_total": 0,
                "sell_count": 0, "sell_total": 0,
                "profitCeiling": "", "stopLossPrice": "",
                "profit_total": 0, "profit_rate": 0,
                "netprofit_total": 0, "netprofit_rate": 0,
                "orderlist": []}
        strategy_info = r2.hget("gridstrategy", strategyId)
        strategy_info = json.loads(strategy_info)
        data["profitCeiling"] = strategy_info["profitCeiling"]
        data["stopLossPrice"] = strategy_info["stopLossPrice"]
        existingUsdt = strategy_info["existingUsdt"]
        conn = POOL.connection()
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cur.execute("select * from gridlist where strategyId=%s", (strategyId,))
        res = cur.fetchall()
        data["buy_count"] = len([i for i in res if i["buystatus"] == 1])
        data["sell_count"] = len([i for i in res if i["sellstatus"] == 1])
        buy_total = float(sum([i["buycount"] for i in res if i["buystatus"] == 1]))
        sell_total = float(sum([i["sellcount"] for i in res if i["sellstatus"] == 1]))
        data["buy_total"] = buy_total
        data["sell_total"] = sell_total
        profit_total = float(
            sum([i["profit"] for i in res if i["sellstatus"] == 1 and (i["buystatus"] == 1 or i["buystatus"] == None)]))
        netprofit_total = float(sum(
            [i["netprofit"] for i in res if i["sellstatus"] == 1 and (i["buystatus"] == 1 or i["buystatus"] == None)]))
        data["profit_total"] = profit_total
        data["netprofit_total"] = netprofit_total
        data["profit_rate"] = round(profit_total / existingUsdt, 8)
        data["netprofit_rate"] = round(netprofit_total / existingUsdt, 8)
        for i in res:
            item = {}
            if i["buystatus"] == 1 or i["sellstatus"] == 1:
                item["buyprice"] = float(i["buyprice"]) if i["buyprice"] is not None else i["buyprice"]
                item["buycount"] = float(i["buycount"]) if i["buycount"] is not None else i["buyprice"]
                item["buystatus"] = i["buystatus"]
                item["buytradetime"] = i["buytradetime"]
                item["sellprice"] = float(i["sellprice"]) if i["sellprice"] is not None else i["buyprice"]
                item["sellcount"] = float(i["sellcount"]) if i["sellcount"] is not None else i["buyprice"]
                item["sellstatus"] = i["sellstatus"]
                item["selltradetime"] = i["selltradetime"]
                if i["sellstatus"] == 1 and (i["buystatus"] == 1 or i["buystatus"] is None):
                    item["profit"] = round(float(i["profit"]), 8)
                    item["netprofit"] = round(float(i["netprofit"]), 8)
                else:
                    item["profit"] = 0
                    item["netprofit"] = 0
                data["orderlist"].append(item)
        cur.close()
        conn.close()
        return data
    except Exception as e:
        print(e)
        return {}


# 同步网格买卖单
def synchronize_grid_orderlist(id):
    try:
        conn = POOL.connection()
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cur.execute("select * from gridlist where id > %s", (id,))
        res = cur.fetchall()
        res1 = []
        for i in res:
            if i["buyprice"] is not None:
                i["buyprice"] = float(i["buyprice"])
            if i["buycount"] is not None:
                i["buycount"] = float(i["buycount"])
            if i["sellprice"] is not None:
                i["sellprice"] = float(i["sellprice"])
            if i["sellcount"] is not None:
                i["sellcount"] = float(i["sellcount"])
            if i["profit"] is not None:
                i["profit"] = float(i["profit"])
            if i["netprofit"] is not None:
                i["netprofit"] = float(i["netprofit"])
            res1.append(i)
        return res1
    except:
        return []


if __name__ == "__main__":
    # selllist = [
    #     {'userUuid': '398051ac70ef4da9aafd33ce0b95195f', 'apiAccountId': 10210, 'strategyId': 102, 'platform': 'T8ex',
    #      'symbol': 'eth_usdt', 'amount': 0.057269, 'price': 3279.259275, 'direction': 2, 'source': 8},
    #     {'userUuid': '398051ac70ef4da9aafd33ce0b95195f', 'apiAccountId': 10210, 'strategyId': 102, 'platform': 'T8ex',
    #      'symbol': 'eth_usdt', 'amount': 0.024792, 'price': 3282.724126, 'direction': 2, 'source': 8},
    # ]
    # bulk_currency_sell_orders(selllist)
    orderlist=[{
              "userUuid": "398051ac70ef4da9aafd33ce0b95195f",
               "apiAccountId":  10209,
               "price":  3258.554,
               "direction":  1,
               "strategyId":  102,
               "symbol":  "eth_usdt",
               "orderId":  "BX1629101417220174",
               "platform":  "T8ex"
            },
             {
                "userUuid": "398051ac70ef4da9aafd33ce0b95195f",
                "apiAccountId": 10209,
                "price": 3260.1694,
                "direction": 1,
                "strategyId": 102,
                "symbol": "eth_usdt",
                "orderId": "BX1629101400967634",
                "platform": "T8ex"
            }]
    bulk_currency_cancel_orders(orderlist)

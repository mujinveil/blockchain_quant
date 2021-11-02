# encoding=utf-8
import json
import time
from threading import Thread
import requests
from tools.Config import future_trade_url, future_cancel_order_url, future_query_order_url, future_bulk_trade_url, \
    future_bulk_cancel_url
from tools.databasePool import r6


# 根据数量和价格下买单(用于刷单)
def buy(userUuid, apiAccountId, symbol, platform, amount, price, direction, orderPriceType, offset, leverRate):
    try:
        data = {"userUuid": userUuid,
                "apiAccountId": apiAccountId,
                "symbol": symbol,
                "platform": platform,
                "amount": amount,
                "price": price,
                "direction": 1,
                "orderPriceType": orderPriceType,
                "offset": offset,
                "leverRate": leverRate,
                }
        res = requests.post(future_trade_url, data=data)
        resdict = json.loads(res.content.decode())
        if resdict["code"] == 1:
            print("用户{}-{}下买单成功,订单id{},数量{}，金额{}".format(userUuid, apiAccountId, resdict["response"]["orderId"], amount,
                                                         price))
        else:
            print("用户{}-{}下买单失败,交易对{},数量{}，金额{},response{}".format(userUuid, apiAccountId, symbol, amount, price,
                                                                   resdict))
        return resdict['response']['orderId'].replace('"', "")
    except Exception as e:
        print("用户{}-{}下买单失败，交易对{},数量{}，金额{},报错信息{}".format(userUuid, apiAccountId, symbol, amount, price, e))


# 根据数量和价格下卖单(用于刷单)
def sell(userUuid, apiAccountId, symbol, platform, amount, price, direction, orderPriceType, offset, leverRate):
    try:
        data = {"userUuid": userUuid,
                "apiAccountId": apiAccountId,
                "symbol": symbol,
                "platform": platform,
                "amount": amount,
                "price": price,
                "direction": 2,
                "orderPriceType": orderPriceType,
                "offset": offset,
                "leverRate": leverRate,
                }
        res = requests.post(future_trade_url, data=data)
        resdict = json.loads(res.content.decode())
        if resdict["code"] == 1:
            print("用户{}-{}下卖单成功,订单id{},数量{}，金额{}".format(userUuid, apiAccountId, resdict["response"]["orderId"], amount,
                                                         price))
        else:
            print("用户{}-{}下卖单失败,交易对{},数量{}，金额{},response{}".format(userUuid, apiAccountId, symbol, amount, price,
                                                                   resdict))
        return resdict['response']['orderId'].replace('"', "")
    except Exception as e:
        print("用户{}-{}下卖单失败，交易对{},数量{}，金额{},报错信息{}".format(userUuid, apiAccountId, symbol, amount, price, e))


# 撤销订单（用于刷单)
def cancel(userUuid, apiAccountId, platform, orderId):
    try:
        data = {"userUuid": userUuid,
                "apiAccountId": apiAccountId,
                "platform": platform,
                "orderId": orderId,
                }
        res = requests.post(future_cancel_order_url, data=data)
        resdict = json.loads(res.content.decode())
        print("用户{}-{}，撤销订单{},返回{}".format(userUuid, apiAccountId, orderId, resdict))
    except Exception as e:
        print("用户{}-{}撤销订单{}失败，报错信息{}".format(userUuid, apiAccountId, orderId, e))


# 根据数量和价格下买单(用于盘口订单)
def buy_1(userUuid, apiAccountId, symbol, platform, amount, price, direction, orderPriceType, offset, leverRate):
    try:
        data = {"userUuid": userUuid,
                "apiAccountId": apiAccountId,
                "symbol": symbol,
                "platform": platform,
                "amount": amount,
                "price": price,
                "direction": direction,
                "orderPriceType": orderPriceType,
                "offset": offset,
                "leverRate": leverRate,
                }
        res = requests.post(future_trade_url, data=data)
        resdict = json.loads(res.content.decode())
        if resdict["code"] == 1:
            print("用户{}-{}下买单成功，数量{}，金额{}".format(userUuid, apiAccountId, amount, price))
            orderId = resdict["response"]["orderId"].replace('"', "")
            if orderId is not None:
                data["orderId"] = orderId
                r6.hset("T8ex_{}_buyorders".format(symbol), orderId, json.dumps(data))
        else:
            print("用户{}-{}下买单失败，数量{}，金额{}，reponse{}".format(userUuid, apiAccountId, amount, price, resdict))
    except Exception as e:
        print("用户{}-{}下买单失败，数量{}，金额{},报错信息{}".format(userUuid, apiAccountId, amount, price, e))


# 根据数量和价格下买单(用于盘口订单)
def sell_1(userUuid, apiAccountId, symbol, platform, amount, price, direction, orderPriceType, offset, leverRate):
    try:
        data = {"userUuid": userUuid,
                "apiAccountId": apiAccountId,
                "symbol": symbol,
                "platform": platform,
                "amount": amount,
                "price": price,
                "direction": 2,
                "orderPriceType": orderPriceType,
                "offset": offset,
                "leverRate": leverRate,
                }
        res = requests.post(future_trade_url, data=data)
        resdict = json.loads(res.content.decode())
        if resdict["code"] == 1:
            print("用户{}-{}下卖单成功，数量{}，金额{}".format(userUuid, apiAccountId, amount, price))
            orderId = resdict["response"]["orderId"].replace('"', '')
            if orderId is not None:
                data["orderId"] = orderId
                r6.hset("T8ex_{}_sellorders".format(symbol), orderId, json.dumps(data))
        else:
            print("用户{}-{}下卖单失败，数量{}，金额{}，reponse{}".format(userUuid, apiAccountId, amount, price, resdict))
    except Exception as e:
        print("用户{}-{}下卖单失败，数量{}，金额{},报错信息{}".format(userUuid, apiAccountId, amount, price, e))


# 撤销订单（用于盘口订单）
def cancel_1(userUuid, apiAccountId, platform, orderId, direction, symbol):
    try:
        data = {"userUuid": userUuid,
                "apiAccountId": apiAccountId,
                "platform": platform,
                "orderId": orderId,
                }
        res = requests.post(future_cancel_order_url, data=data)
        resdict = json.loads(res.content.decode())
        if "订单已完成或不存在" in resdict['message'] or resdict["code"] == 1:
            if direction == 1:
                r6.hdel("T8ex_{}_buyorders".format(symbol), orderId)
                print("用户{}-{},买单过多,撤销买单{}，返回{}".format(userUuid, apiAccountId, orderId, resdict))
            if direction == 2:
                r6.hdel("T8ex_{}_sellorders".format(symbol), orderId)
                print("用户{}-{},卖单过多,撤销卖单{}，返回{}".format(userUuid, apiAccountId, orderId, resdict))
    except Exception as e:
        print("用户{}-{}撤销订单{}失败，报错信息{}".format(userUuid, apiAccountId, orderId, e))


# 查询订单(用于盘口订单)
def query_1(userUuid, apiAccountId, platform, orderId, direction, symbol):
    try:
        data = {"userUuid": userUuid,
                "apiAccountId": apiAccountId,
                "platform": platform,
                "orderId": orderId,
                }
        res = requests.post(future_query_order_url, data=data)
        resdict = json.loads(res.content.decode())
        if resdict['message'] == "成功":
            status = resdict["response"]["status"]
            print("用户{}-{}，订单{}，状态{}".format(userUuid, apiAccountId, orderId, status))
            if status == "COMPLETED" or status == "CANCELED":  # 状态 交易中 TRADING，完成 COMPLETED，取消 CANCELED
                if direction == 1:
                    r6.hdel("T8ex_{}_buyorders".format(symbol), orderId)
                    print("用户{}-{}，撤销买单{}，返回{}".format(userUuid, apiAccountId, orderId, resdict))
                if direction == 2:
                    r6.hdel("T8ex_{}_buyorders".format(symbol), orderId)
                    print("用户{}-{}，撤销卖单{}，返回{}".format(userUuid, apiAccountId, orderId, resdict))
        if resdict['message'] == "订单未知":
            if direction == 1:
                r6.hdel("T8ex_{}_buyorders".format(symbol), orderId)
                print("用户{}-{}，撤销买单{}，返回{}".format(userUuid, apiAccountId, orderId, resdict))
            if direction == 2:
                r6.hdel("T8ex_{}_sellorders".format(symbol), orderId)
                print("用户{}-{}，撤销卖单{}，返回{}".format(userUuid, apiAccountId, orderId, resdict))
    except Exception as e:
        print("用户{}-{}查询订单{}失败，报错信息{}".format(userUuid, apiAccountId, orderId, e))


# 多线程批量挂买单,传入订单列表[{"amount":0.01,"price":5201.2},{},{}...](用于盘口订单)
def place_buy_orders(buylist):
    try:
        T = []
        for i in buylist:
            T.append(Thread(target=buy_1, args=(
                i["userUuid"], i["apiAccountId"], i["symbol"], i['platform'], i["amount"], i["price"], i["direction"],
                i["orderPriceType"], i['offset'], i['leverRate'])))
        for t in T:
            t.start()
            time.sleep(0.2)
        for t in T:
            t.join()
    except Exception as e:
        print(e)


# 多线程批量挂卖单,传入订单列表[{"amount":0.01,"price":5201.2},{},{}...](用于盘口订单)
def place_sell_orders(selllist):
    try:
        T = []
        for j in selllist:
            T.append(Thread(target=sell_1, args=(
                j["userUuid"], j["apiAccountId"], j["symbol"], j['platform'], j["amount"], j["price"], j["direction"],
                j["orderPriceType"], j['offset'], j['leverRate'])))
        for t in T:
            t.start()
            time.sleep(0.2)
        for t in T:
            t.join()
    except:
        pass


# 多线程批量撤单,传入订单列表[{'userUuid':"","apiAccountId":""}](用于盘口订单)
def place_cancel_orders(orderlist):
    try:
        T = []
        for i in orderlist:
            T.append(Thread(target=cancel_1, args=(i['userUuid'], i['apiAccountId'], i['platform'], i['orderId'], i['direction'], i['symbol'])))
        for t in T:
            t.start()
            time.sleep(0.2)
        for t in T:
            t.join()
    except:
        pass


# 请求一次,批量挂买单(用于盘口订单)
def bulk_buy_orders(buylist):
    try:
        if not buylist:
            return
        res = requests.post(future_bulk_trade_url, data={"orderInfos": json.dumps(buylist)})
        resdict = json.loads(res.content.decode())
        userUuid = buylist[0]['userUuid']
        apiAccountId = buylist[0]['apiAccountId']
        symbol = buylist[0]['symbol']
        if resdict["code"] == 1:
            orderIdlist = resdict["response"]["orderIds"]
            if orderIdlist is not None:
                for i in range(len(buylist)):
                    data = buylist[i]
                    orderId = orderIdlist[i]
                    data["orderId"] = orderId
                    print("用户{}-{}下买单成功,订单编号{}".format(userUuid, apiAccountId, orderId))
                    r6.hset("T8ex_{}_buyorders".format(symbol), orderId, json.dumps(data))
        else:
            print("用户{}-{}批量下买单失败，reponse{}".format(userUuid, apiAccountId, resdict))
    except Exception as e:
        print("用户{}-{}批量下买单失败,报错信息{}".format(userUuid, apiAccountId, e))


# 请求一次,批量挂卖单(用于盘口订单)
def bulk_sell_orders(selllist):
    try:
        if not selllist:
            return
        res = requests.post(future_bulk_trade_url, data={"orderInfos": json.dumps(selllist)})
        resdict = json.loads(res.content.decode())
        userUuid = selllist[0]['userUuid']
        apiAccountId = selllist[0]['apiAccountId']
        symbol = selllist[0]['symbol']
        res = []
        if resdict["code"] == 1:
            orderIdlist = resdict["response"]["orderIds"]
            if orderIdlist is not None:
                for i in range(len(selllist)):
                    data = selllist[i]
                    orderId = orderIdlist[i]
                    data["orderId"] = orderId
                    res.append(data)
                    r6.hset("T8ex_{}_sellorders".format(symbol), orderId, json.dumps(data))
                    print("用户{}-{}下卖单成功,订单编号{}".format(userUuid, apiAccountId, orderId))
        else:
            print("用户{}-{}批量下卖单失败，reponse{}".format(userUuid, apiAccountId, resdict))
    except Exception as e:
        print("用户{}-{}批量下卖单失败,报错信息{}".format(userUuid, apiAccountId, e))


# 请求一次，批量撤单(用于盘口撤单)
def bulk_cancel_orders(orderlist):
    try:
        cancelorderlist = []
        for i in orderlist:
            data = {"userUuid": i['userUuid'],
                    "apiAccountId": i['apiAccountId'],
                    "platform": i['platform'],
                    "orderId": i['orderId'],
                    }
            print(data)
            cancelorderlist.append(data)
        res = requests.post(future_bulk_cancel_url, data={'orderIds': json.dumps(cancelorderlist)})
        resdict = json.loads(res.content.decode())
        print(resdict)
        userUuid = orderlist[0]['userUuid']
        apiAccountId = orderlist[0]['apiAccountId']
        symbol = orderlist[0]['symbol']
        if resdict["code"] == 1:
            for i in range(len(orderlist)):
                direction = orderlist[i]['direction']
                orderId = orderlist[i]['orderId']
                if direction == 1:
                    r6.hdel("T8ex_{}_buyorders".format(symbol), orderId)
                    print("用户{}-{}，撤销买单{}，返回{}".format(userUuid, apiAccountId, orderId, resdict))
                if direction == 2:
                    r6.hdel("T8ex_{}_sellorders".format(symbol), orderId)
                    print("用户{}-{}，撤销卖单{}，返回{}".format(userUuid, apiAccountId, orderId, resdict))
    except Exception as e:
        print("用户{}-{}撤销订单失败，报错信息{}".format(userUuid, apiAccountId, e))


if __name__ == "__main__":
    # res1 = r6.hgetall("T8ex_{}_buyorders".format("btc"))
    # res1 = [json.loads(i) for i in res1.values()]
    # place_cancel_orders(res1)
    selllist = [
        {'userUuid': '398051ac70ef4da9aafd33ce0b95195f', 'apiAccountId': 10210, 'symbol': 'eth', 'platform': 'T8ex',
         'amount': 5, 'price': 3271.43, 'direction': 2, 'orderPriceType': 1, 'offset': 3, 'leverRate': 2},
        {'userUuid': '398051ac70ef4da9aafd33ce0b95195f', 'apiAccountId': 10210, 'symbol': 'eth', 'platform': 'T8ex',
         'amount': 2, 'price': 3271.43, 'direction': 2, 'orderPriceType': 1, 'offset': 3, 'leverRate': 2},
        {'userUuid': '398051ac70ef4da9aafd33ce0b95195f', 'apiAccountId': 10210, 'symbol': 'eth', 'platform': 'T8ex',
         'amount': 9, 'price': 3271.43, 'direction': 2, 'orderPriceType': 1, 'offset': 3, 'leverRate': 2},
    ]
    bulk_sell_orders(selllist)
    # orderlist = [
    #     {'userUuid': '398051ac70ef4da9aafd33ce0b95195f', 'apiAccountId': 10210, 'symbol': 'eth', 'platform': 'T8ex',
    #      'amount': 4, 'price': 3233.92, 'direction': 2, 'orderPriceType': 1, 'offset': 3, 'leverRate': 2,
    #      'orderId': 'CX1629076481796656'},
    #     {'userUuid': '398051ac70ef4da9aafd33ce0b95195f', 'apiAccountId': 10210, 'symbol': 'eth', 'platform': 'T8ex',
    #      'amount': 2, 'price': 3238.16, 'direction': 2, 'orderPriceType': 1, 'offset': 3, 'leverRate': 2,
    #      'orderId': 'CX1629076481796121'}]
    #
    # bulk_cancel_orders(orderlist)

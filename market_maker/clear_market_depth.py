import json
import time

import requests

from tools.Config import befinx_base_url, Trade_url
from market_maker.bitget_marketmaker import buy, sell
from tools.get_market_info import get_currentprice0

# #  taker买卖单
# userUuid = "4c39e2dfa2844548be76dbd7b6183c47"
# apiAccountId1 = 10129
# apiAccountId2 = 10129
# platform = "befinx"
# symbol = "gech_sgdt"
# strategyId = 888
#
# url = "{}/open-api/open/trade_plate".format(befinx_base_url)
# res = requests.post(url, data={"symbol": symbol.replace("_", "/").upper(), "size": 200})
# dict = json.loads(res.content.decode())
# befinxdict = {"platform": "befinx", "symbol": symbol}
# ask = dict["data"]["ask"]
# bid = dict["data"]["bid"]
# print(ask)
# print(bid)
# for i in ask:
#     amount = i["amount"]
#     price = i["price"]
#     buy(userUuid, apiAccountId1, strategyId, platform, symbol, amount, price)
#     time.sleep(0.2)
# for i in bid:
#     amount = i["amount"]
#     price = i["price"]
#     sell(userUuid, apiAccountId2, strategyId, platform, symbol, amount, price)
#     time.sleep(0.2)


# 撤销数据库订单

# symbol = "btc_usdt"
# res1 = r1.hgetall("befinx_{}_buyorders".format(symbol))
# res1 = [json.loads(i) for i in res1.values()]
# res2 = r1.hgetall("befinx_{}_sellorders".format(symbol))
# res2 = [json.loads(i) for i in res2.values()]
#
# print(res1)
# print(res2)
# for item in res1+res2:
#     print(item)
#     if item["userUuid"] == "4c39e2dfa2844548be76dbd7b6183c47":
#         cancel_1(item["userUuid"], item["apiAccountId"], item["strategyId"], item["platform"], item["symbol"], item["orderId"], item["direction"])
#         print("****************")
#     time.sleep(1)


# 查询redis中的订单，成交的话就删除
# symbol = "mac_sgdt"
# symbol = "gech_sgdt"
# # symbol = "eth_usdt"
# res1 = r1.hgetall("befinx_{}_buyorders".format(symbol))
# res1 = [json.loads(i) for i in res1.values()]
# res2 = r1.hgetall("befinx_{}_sellorders".format(symbol))
# res2 = [json.loads(i) for i in res2.values()]
#
# print(res1)
# print(res2)


# for item in res1+res2:
#     print(item)
#     userUuid = item["userUuid"]
#     apiAccountId = item["apiAccountId"]
#     strategyId = item["strategyId"]
#     platform = item["platform"]
#     orderId = item["orderId"]
#     direction = item["direction"]
#     try:
#         data = {"userUuid": userUuid,
#                 "apiAccountId": apiAccountId,
#                 "strategyId": strategyId,
#                 "platform": platform,
#                 "symbol": symbol,
#                 "orderId": orderId,
#                 "direction": direction,
#                 "source": 8}
#         res = requests.post(Queryorder_url, data=data)
#         resdict = json.loads(res.content.decode())
#         print(resdict)
#         status = resdict["response"]["status"]
#         print("用户{}-{}，订单{}，状态{}".format(userUuid, apiAccountId, orderId, status))
#         if status == "closed" or status == "cancelled":  # 状态 open挂单中 closed已完成 cancelled撤单 part部分交易
#             if direction == 1:
#                 r1.hdel("befinx_{}_buyorders".format(symbol), orderId)
#             if direction == 2:
#                 r1.hdel("befinx_{}_sellorders".format(symbol), orderId)
#     except Exception as e:
#         print("用户{}-{}查询订单{}失败，报错信息{}".format(userUuid, apiAccountId, orderId, e))
#         if direction == 1:
#             r1.hdel("befinx_{}_buyorders".format(symbol), orderId)
#         if direction == 2:
#             r1.hdel("befinx_{}_sellorders".format(symbol), orderId)
#     time.sleep(1)
#
# for item in res1+res2:
#     print(item)
#     userUuid = item["userUuid"]
#     apiAccountId = item["apiAccountId"]
#     strategyId = item["strategyId"]
#     platform = item["platform"]
#     orderId = item["orderId"]
#     direction = item["direction"]
#     try:
#         data = {"userUuid": userUuid,
#                 "apiAccountId": apiAccountId,
#                 "strategyId": strategyId,
#                 "platform": platform,
#                 "symbol": symbol,
#                 "orderId": orderId,
#                 "source": 8}
#         res = requests.post(Cancel_url, data=data)
#         resdict = json.loads(res.content.decode())
#         print(resdict)
#         if resdict["code"] == 1:
#             if direction == 1:
#                 r1.hdel("befinx_{}_buyorders".format(symbol), orderId)
#                 print("用户{}-{}，撤销买单{}，返回{}".format(userUuid, apiAccountId, orderId, resdict))
#             if direction == 2:
#                 r1.hdel("befinx_{}_sellorders".format(symbol), orderId)
#                 print("用户{}-{}，撤销卖单{}，返回{}".format(userUuid, apiAccountId, orderId, resdict))
#     except Exception as e:
#         print("用户{}-{}撤销订单{}失败，报错信息{}".format(userUuid, apiAccountId, orderId, e))
#     time.sleep(1)


# url = "{}/open-api/open/trade_plate".format(befinx_base_url)
# res = requests.post(url, data={"symbol": symbol.replace("_", "/").upper(), "size": 200})
# dict = json.loads(res.content.decode())
# befinxdict = {"platform": "befinx", "symbol": symbol}
# ask = dict["data"]["ask"]
# bid = dict["data"]["bid"]
# print(ask)
# print(bid)

# from databasePool import POOL_grid
# direction = 2
# conn = POOL_grid.connection()
# cur = conn.cursor()
# cur.execute("SELECT orderid FROM `t_balance_order_record`")
# res = cur.fetchall()
# print(res)
#
# n = 1
# for i in res:
#     orderId = i[0]
#     print(n,orderId)
#     print("================================撤单====================================", int(time.time()))
#     cancelparams = {"direction": direction, "symbol": symbol, "platform": platform, "orderId": orderId,
#                     "apiAccountId": apiAccountId, "userUuid": userUuid, "source": source, "strategyId": strategyId}
#     cancelres = requests.post(Cancel_url, data=cancelparams)
#     print(cancelres.url)
#     resdict = json.loads(cancelres.content.decode())
#     print(resdict)
#     n+=1


# 测试

while True:
    try:
        # userUuid = "398051ac70ef4da9aafd33ce0b95195f"
        # apiAccountId = 10152   # "btc 总余额122.73294，其中可用122.73294，锁定0.0,usdt总余额487713.34，其中可用482713.3锁定5000.039"
        userUuid = "6e7c88272f554956a35d8ed2cf833201"
        apiAccountId = 10155
        strategyId = 88888888888
        platform = "befinx"
        symbol = "btc_usdt"
        amount = 0.0001
        url = "{}/open-api/open/trade_plate".format(befinx_base_url)
        res = requests.post(url, data={"symbol": symbol.replace("_", "/").upper(), "size": 200})
        dict = json.loads(res.content.decode())
        try:
            ask = [i for i in dict["data"]["ask"] if i["price"] >= 10000]
            bid = [i for i in dict["data"]["bid"] if i["price"] >= 10000]
        except:
            ask = []
            bid = []
        current_price = get_currentprice0("okex", "btc_usdt")
        if ask == [] and bid == []:
            # 刷单(参考okex的价格)
            if current_price != 0:
                buyparams = {"direction": 1, "amount": amount, "symbol": symbol, "platform": platform,
                             "price": current_price,
                             "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 7, "strategyId": strategyId}
                buytrade = requests.post(Trade_url, data=buyparams)
                sellparams = {"direction": 2, "amount": amount, "symbol": symbol, "platform": platform,
                              "price": current_price,
                              "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 7,
                              "strategyId": strategyId}
                selltrade = requests.post(Trade_url, data=sellparams)
        else:
            for i in ask:
                amount = i["amount"]
                price = i["price"]
                if abs(price - current_price) / current_price < 0.0005:
                    buy(userUuid, apiAccountId, strategyId, platform, symbol, amount, price)
                    time.sleep(1)
            for i in bid:
                amount = i["amount"]
                price = i["price"]
                if abs(price - current_price) / current_price < 0.0005:
                    sell(userUuid, apiAccountId, strategyId, platform, symbol, amount, price)
                    time.sleep(1)
    except Exception as e:
        print(e)
    finally:
        time.sleep(1)

# # # -*- coding: utf-8 -*-
# # # @Time : 2020/3/23 16:49
import json
import time

import requests
from tools.Config import Remain_url

# from databasePool import POOL, r1, r2, POOL_grid
# from handle import buy, query, cancel, sell

# 测试
# userUuid = "398051ac70ef4da9aafd33ce0b95195f"
# # apiAccountId = 10152   # "btc 总余额122.73294，其中可用122.73294，锁定0.0,usdt总余额487713.34，其中可用482713.3锁定5000.039"
# apiAccountId = 10158
# strategyId = 88888888888
# platform = "befinx"
# symbol = "btc_usdt"
# amount = 0.001197
# price = 49214
# direction = 2
# source = 4

# APP正式
userUuid = "6e7c88272f554956a35d8ed2cf833201"
apiAccountId = 10156
strategyId = 1111111111
platform = "befinx"
symbol = "btc_usdt"
amount = 0.02
direction = 1
source = 7


# 无界通正式
# userUuid = "666101af496e4d24a326f118feceb25b"
# apiAccountId = 10141
# strategyId = 1234560000
# platform = "huobi"
# symbol = "eth_usdt"
# amount = 1
# price = 500
# direction = 2
# source = 7


# print("=============================当前行情======================================", int(time.time()))
# current_price = get_currentprice0(platform, symbol)
# print(current_price)
# price = current_price*1.2
#
# print("==============================下单======================================", int(time.time()))
# print("下单总金额{}".format(amount * price))
# tradeparams = {"direction": direction, "amount": amount, "symbol": symbol, "platform": platform, "price": price,
#                "apiAccountId": apiAccountId, "userUuid": userUuid, "source": source, "strategyId": strategyId}
#
# traderes = requests.post(Trade_url, data=tradeparams)
# print(traderes.url)
# trade_dict = json.loads(traderes.content.decode())
# print(trade_dict)
# code = trade_dict["code"]  # 获取下单状态
# orderId = trade_dict["response"]["orderid"]  # 获取订单id
# print(code, orderId)
#
# time.sleep(1)
# print("=================================查询订单是否成交===================================", int(time.time()))
# queryparams = {"direction": direction, "symbol": symbol, "platform": platform, "orderId": orderId,
#                "apiAccountId": apiAccountId, "userUuid": userUuid, "source": source, "strategyId": strategyId}
# res = requests.post(Queryorder_url, data=queryparams)
# queryresdict = json.loads(res.content.decode())
# print(queryresdict)
# numberDeal = float(queryresdict["response"]["numberDeal"])
# print(res.url)
# print(queryresdict)
# print("已成交{}".format(numberDeal))
#
# print("=================================查询实际成交价===================================", int(time.time()))
# queryparams = {"platform": platform, "symbol": symbol, "orderId": orderId, "apiId": apiAccountId, "userUuid": userUuid,
#                "strategyId": strategyId}
# res = requests.post(Query_tradeprice_url, data=queryparams)
# queryresdict = json.loads(res.content.decode())
# tradeprice = queryresdict["response"]["avgPrice"]
# print(queryresdict)
# print("实际成交均价{}".format(tradeprice))
#
# time.sleep(1)
#
# print("================================撤单====================================", int(time.time()))
# cancelparams = {"direction": direction, "symbol": symbol, "platform": platform, "orderId": orderId,
#                 "apiAccountId": apiAccountId, "userUuid": userUuid, "source": source, "strategyId": strategyId}
# cancelres = requests.post(Cancel_url, data=cancelparams)
# print(cancelres.url)
# resdict = json.loads(cancelres.content.decode())
# print(resdict)

print("================================查询资产====================================", int(time.time()))
remainres = requests.get(Remain_url, params={"userUuid": userUuid, "apiAccountId": apiAccountId})
remaindict = json.loads(remainres.content.decode())
print(remaindict)
print(remainres.url)
tradeCoin = symbol.split("_")[0]  # 交易币
valCoin = symbol.split("_")[1]  # 计价币
TradeCoin_amount = [i["remains"] for i in remaindict["response"] if i["coin"] == tradeCoin][0]
TradeCoin_amount_lock = [i["lock"] for i in remaindict["response"] if i["coin"] == tradeCoin][0]
TradeCoin_amount_over = [i["over"] for i in remaindict["response"] if i["coin"] == tradeCoin][0]
ValCoin_amount = [i["remains"] for i in remaindict["response"] if i["coin"] == valCoin][0]
ValCoin_amount_lock = [i["lock"] for i in remaindict["response"] if i["coin"] == valCoin][0]
ValCoin_amount_over = [i["over"] for i in remaindict["response"] if i["coin"] == valCoin][0]
print("子账号{}:\n{} 总余额{}，其中可用{}，锁定{}\n{}总余额{}，其中可用{}锁定{}".format(apiAccountId, tradeCoin, TradeCoin_amount,
                                                                TradeCoin_amount_over, TradeCoin_amount_lock, valCoin,
                                                                ValCoin_amount, ValCoin_amount_over,
                                                                ValCoin_amount_lock))

if __name__ == '__main__':
    res = requests.get("https://api.huobi.pro/market/tickers")
    pricelimit0 = {}
    pricelimit = {}
    amountlimit = {}
    minamountdict = {}
    premiumdict = {}
    for i in res.json()["data"]:
        for symbol in symbols:
            if i["symbol"] == symbol.replace("_", ""):
                print(i)
                price_list = [i['open'], i['close'], i['high'], i['low']]
                price_float_list = [str(a).split(".")[1] for a in price_list]
                price_float_len = max([len(b) for b in price_float_list])
                price_float_min = float("0." + "0" * (price_float_len - 1) + "1")
                minamountdict[symbol] = {"huobi": price_float_min, "okex": price_float_min, "binance": price_float_min,
                                         "befinx": price_float_min}
                premiumdict[symbol] = price_float_min*5
                # pricelimit0[symbol] = price_float_len
                # pricelimit[symbol] = {"huobi": price_float_len, "okex": price_float_len, "binance": price_float_len,
                #                       "befinx": price_float_len}
                # amount_list = [i["bidSize"], i["askSize"]]
                # amount_float_list = [str(a).split(".")[1] for a in amount_list]
                # amount_float_len = max([len(c) for c in amount_float_list])
                # amountlimit[symbol] = {"huobi": amount_float_len, "okex": amount_float_len, "binance": amount_float_len,
                #                        "befinx": amount_float_len}
    print(premiumdict)

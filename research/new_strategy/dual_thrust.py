import pandas as pd
import requests
import json,time

from tools.Config import Queryorder_url, Trade_url, Query_tradeprice_url, Remain_url, pricelimit, amountlimit
from tools.get_market_info import get_currentprice0


def Dual_Thrust_Range(klinedata,platform,symbol):
    dataframe = dict({'time': [i[0] for i in klinedata],
                      "close": [float(i[4]) for i in klinedata],
                      "low": [float(i[3]) for i in klinedata],
                      "high": [float(i[2]) for i in klinedata]})
    data = pd.DataFrame(dataframe)
    data['HH'] = data['high'].rolling(2).max()
    data['HC'] = data['close'].rolling(2).max()
    data['LC'] = data['close'].rolling(2).min()
    data['LL'] = data['low'].rolling(2).min()
    data['range'] = max((data['HH'] - data['LC']), (data['HC'] - data['LL']))
    current_price = get_currentprice0(platform, symbol)
    dual_thrust_sign = 0
    if current_price > data['open'][-1] + 0.7 * data['range'][-2]:
        dual_thrust_sign = 1
    if current_price < data['open'][-1] - 0.7 * data['range'][-2]:
        dual_thrust_sign = -1
    return dual_thrust_sign


# 执行买单
def buy(strategy_info):
    userUuid = strategy_info["userUuid"]
    apiAccountId = strategy_info["apiAccountId"]
    strategyId = strategy_info["strategyId"]
    platform = strategy_info["platform"]  # 交易平台
    symbol = strategy_info["symbol"]  # 交易对
    entryPrice = strategy_info["entryPrice"]  # 入场价
    tradeCoin = symbol.split("_")[0]
    valCoin = symbol.split("_")[1]
    initialValCoin = strategy_info["initialValCoin"]  # 用于策略的初始计价币数量
    # 1、查询资产（可用的计价币usdt数量），跟初始资金做对比，如果大于初始资金，则以初始资金为准
    remainres = requests.get(Remain_url, params={"userUuid": userUuid, "apiAccountId": apiAccountId})
    remaindict = json.loads(remainres.content.decode())
    ValCoin_amount_over = [i["over"] for i in remaindict["response"] if i["coin"] == valCoin][0]
    total_amount = min([initialValCoin, ValCoin_amount_over])
    # 2、查询行情
    current_price = get_currentprice0(platform, symbol)
    # 3、计算下单金额（溢价买入）
    buy_price = round(current_price * 1.01, pricelimit[symbol][platform])
    buy_amount = total_amount / buy_price
    try:
        x, y = str(buy_amount).split('.')
        buy_amount = float(x + '.' + y[0:amountlimit[symbol][platform]])
    except:
        pass
    tradeparams = {"direction": 1, "amount": buy_amount, "symbol": symbol, "platform": platform, "price": buy_price,
                   "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 8, "strategyId": strategyId}
    traderes = requests.post(Trade_url, data=tradeparams)
    trade_dict = json.loads(traderes.content.decode())
    code = trade_dict["code"]  # 获取下单状态
    orderId = trade_dict["response"]["orderid"]  # 获取订单id
    # 4、2秒后查询订单是否成交
    time.sleep(2)
    queryparams = {"direction": 1, "symbol": symbol, "platform": platform, "orderId": orderId,
                   "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 8, "strategyId": strategyId}
    res = requests.post(Queryorder_url, data=queryparams)
    queryresdict = json.loads(res.content.decode())
    numberDeal = float(queryresdict["response"]["numberDeal"])
    # 5、查询成交均价
    time.sleep(1)
    queryparams1 = {"platform": platform, "symbol": symbol, "orderId": orderId, "apiId": apiAccountId,
                    "userUuid": userUuid,
                    "strategyId": strategyId}
    res = requests.post(Query_tradeprice_url, data=queryparams1)
    queryresdict = json.loads(res.content.decode())
    try:
        tradeprice = queryresdict["response"]["avgPrice"]
        tradetime = queryresdict["response"]["createdDate"]
        sellfee = numberDeal * tradeprice * 0.002
    except:
        tradeprice = buy_price
        tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        sellfee = numberDeal * tradeprice * 0.002
    # 6、存入数据库
    conn = POOL.connection()
    cur = conn.cursor()
    sellinsertsql = "INSERT INTO orderlist(strategyId,userUuid,apiAccountId,platform,symbol,direction,orderid,order_amount,order_price,order_time,trade_amount,trade_price,trade_time,status,fee,uniqueId) VALUES(%s, %s, %s, %s, %s, %s,%s,%s,%s, %s, %s, %s,%s, %s, %s, %s)"
    cur.execute(sellinsertsql, (
        strategyId, userUuid, apiAccountId, platform, symbol, 1, orderId, buy_amount, buy_price, tradetime, numberDeal,
        tradeprice, tradetime, 1, sellfee, 9))
    conn.commit()
    cur.close()
    conn.close()




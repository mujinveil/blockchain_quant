# encoding='utf-8
import json
import threading
from threading import Thread
import pandas as pd
import requests

from tools.Config import huobifuture_api_url
from tools.databasePool import r0
from tools.future_trade import contract_trade


class MyThread(threading.Thread):
    def __init__(self, func, args=()):
        super(MyThread, self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result  # 如果子线程不使用join方法，此处可能会报没有self.result的错误
        except Exception:
            return None


# 获得三合约的盘口买一卖一价
# 获取永续合约行情深度数据
# def get_perpetualdepth(symbol):
#     url = huobifuture_api_url+"/linear-swap-ex/market/depth?contract_code={}-USDT&type=step0".format(symbol.upper())
#     res = requests.get(url)
#     resdict = json.loads(res.content.decode())
#     sell1 = resdict['tick']['asks'][0][0]
#     buy1 = resdict['tick']['bids'][0][0]
#     return [sell1, buy1]


# 获取当周、当季合约行情深度数据
# def get_futuredepth(symbol, contract_type):
#     contract_type_dict = {'this_week': 'CW', 'next_week': 'NW', 'quarter': 'CQ'}
#     contract_type = contract_type_dict[contract_type]
#     url = huobifuture_api_url + "/market/depth?symbol={}_{}&type=step6".format(symbol.upper(), contract_type)
#     res = requests.get(url)
#     resdict = json.loads(res.content.decode())
#     sell1 = resdict['tick']['asks'][0][0]
#     buy1 = resdict['tick']['bids'][0][0]
#     return [sell1, buy1]


# 获取三个合约的现价
# 获取当季、当周、次周价格
def get_futureprice(symbol, contract_type):
    contract_type_dict = {'this_week': 'CW', 'next_week': 'NW', 'quarter': 'CQ', }
    url = huobifuture_api_url + '/market/history/kline?period=5min&size=1000&symbol={}_{}'.format(symbol.upper(),
                                                                                                  contract_type_dict[
                                                                                                      contract_type])
    res = requests.get(url)
    resdict = json.loads(res.content.decode())
    close_array = [i['close'] for i in resdict['data']]
    return close_array


# 获取永续合约价格
# def get_perpetualprice(symbol):
#     now = int(time.time())
#     start_time = now - 599700
#     url = huobifuture_api_url + '/linear-swap-ex/market/history/kline?contract_code={}&period=5min&from={}&to={}'.format(
#         symbol, start_time, now)
#     res = requests.get(url)
#     resdict = json.loads(res.content.decode())
#     close_array = [i['close'] for i in resdict['data']]
#     return close_array


# 多线程获取合约现价
def get_all_contract_price(symbol):
    t1 = MyThread(get_futureprice, args=(symbol, 'next_week',))
    t2 = MyThread(get_futureprice, args=(symbol, 'quarter',))
    t3 = MyThread(get_futureprice, args=(symbol, 'this_week',))
    t1.start()
    t2.start()
    t3.start()
    t1.join(timeout=10)
    t2.join(timeout=10)
    t3.join(timeout=10)
    close_array1 = t1.get_result()
    close_array2 = t2.get_result()
    close_array3 = t3.get_result()
    return close_array1, close_array2, close_array3

# def get_contractCode(symbol):  # 获得当季，当周合约对应的合约代码
#     url = huobifuture_api_url + '/api/v1/contract_contract_info'
#     data = {'symbol': symbol.upper()}
#     res = requests.get(url, params=data)
#     if res.status_code == 200:
#         resdict = json.loads(res.content.decode())['data']
#         thisweek_code = resdict[0]['contract_code']
#         quarter_code = resdict[2]['contract_code']
#         return thisweek_code, quarter_code


# 多线程获取合约买一卖一盘口价
# def get_all_contract_price(symbol):
#     t1 = MyThread(get_futuredepth, args=(symbol, 'next_week',))
#     t2 = MyThread(get_futuredepth, args=(symbol, 'quarter',))
#     t3 = MyThread(get_futuredepth, args=(symbol, 'this_week',))
#     t1.start()
#     t2.start()
#     t3.start()
#     t1.join(timeout=10)
#     t2.join(timeout=10)
#     t3.join(timeout=10)
#     depth_array1 = t1.get_result()  # 当周
#     depth_array2 = t2.get_result()  # 当季
#     depth_array3 = t3.get_result()  # 次周
#     return depth_array1, depth_array2, depth_array3


def trade_info(symbol, takerfee):
    df = pd.DataFrame()
    close_array1, close_array2, close_array3 = get_all_contract_price(symbol)
    if close_array1 and close_array2 and close_array3:
        df['this_week'] = close_array1  # 当周合约
        df['quarter'] = close_array2  # 当季合约
        df['next_week'] = close_array3  # 次周合约
        diff = df['quarter'] + df['this_week'] - 2 * df['next_week']
        diff_mean = diff.ewm(alpha=0.001).mean()
        print('差价均值{},当前差价{}'.format(diff_mean.iloc[-1], diff.iloc[-1]))
        aim_amount = -(round((diff.iloc[-1] - diff_mean.iloc[-1]) / (15 * close_array1[-1] * takerfee), 1))
        return aim_amount
    else:
        return None


# def tick_trade_info(symbol, takerfee):
#     Alpha = 0.000001
#     try:
#         depth_array1, depth_array2, depth_array3 = get_all_contract_price(symbol)
#         Grid = depth_array2[0] * takerfee * 15
#         diff_buy = depth_array2[1] + depth_array3[1] - 2 * depth_array1[0]
#         diff_sell = depth_array2[0] + depth_array3[0] - 2 * depth_array1[1]
#         global diff_mean
#         if not diff_mean:
#             diff_mean = (diff_buy + diff_sell) / 2
#         else:
#             diff_mean = diff_mean * (1 - Alpha) + Alpha * (diff_buy + diff_sell) / 2
#         aim_buy_amount = round(-(diff_buy - diff_mean) / Grid, 2)
#         aim_sell_amount = round(-(diff_sell - diff_mean) / Grid, 2)
#         print(aim_buy_amount, aim_sell_amount, diff_mean)
#     except Exception as e:
#         print(e)
#         return None, None
#     else:
#         return aim_buy_amount, aim_sell_amount



def trade(strategydata):
    # userUuid = strategydata['userUuid']
    # apiAccountId = strategydata['apiAccountId']
    strategyId = strategydata['strategyId']
    # platform = strategydata['platform']
    symbol = strategydata['symbol']
    leverRate = strategydata['leverRate']
    now_amount = json.loads(r0.hget('barbitrage_amount', strategyId))
    taker_fee = strategydata['takerfee']
    aim_amount = trade_info(symbol, taker_fee)
    if aim_amount is not None and float(aim_amount) - float(now_amount) < -1:
        print(aim_amount)
        trade_amount = 1  # 当周开多,当季永续开空
        print('当周开多,当季次周开空')
        if now_amount >= 1:
            contract_trade(symbol, "quarter", "next_week", "this_week", trade_amount, "sell", "close", leverRate,
                           "opponent", 2)
        else:
            contract_trade(symbol, "quarter", "next_week", "this_week", trade_amount, "sell", "open", leverRate,
                           "opponent", 2)
        r0.hset('barbitrage_amount', strategyId, json.dumps(now_amount - 1))
    elif aim_amount is not None and float(aim_amount) - float(now_amount) > 1:
        print(aim_amount)
        trade_amount = 1  # 当周开空,当季永续开多
        print('当周开空,当季次周开多')
        if now_amount <= -1:
            contract_trade(symbol, "quarter", "next_week", "this_week", trade_amount, "buy", "close", leverRate,
                           "opponent", 2)
        else:
            contract_trade(symbol, "quarter", "next_week", "this_week", trade_amount, "buy", "open", leverRate,
                           "opponent", 2)
        r0.hset('barbitrage_amount', strategyId, json.dumps(now_amount + 1))


if __name__ == "__main__":
    diff_mean = 0
    while True:
        try:
            strategy_list = r0.hvals("butterfly_arbitrage")
            strategy_list = [json.loads(i) for i in strategy_list]
            T = []
            for strategy_info in strategy_list:
                T.append(Thread(target=trade, args=(strategy_info,)))
            for t in T:
                t.start()
            for t in T:
                t.join()
        except Exception as e:
            print(e)
        # finally:
        #     time.sleep(1)

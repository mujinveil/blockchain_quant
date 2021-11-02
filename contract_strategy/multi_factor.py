# encoding="utf-8"
import json
import sys
import time
from threading import Thread
import numpy as np
import pandas as pd
import requests
sys.path.append("..")
from tools.Config import future_takerFee, contract_size_dict
from tools.Config import futurepricelimit, future_strategy_status_update, futureamountlimit
from tools.databasePool import POOL, r0
from tools.future_trade import contract_usdt_trade, query_contract_usdt_order, cancel_contract_usdt_order
from tools.get_future_market_info import get_perpetualprice, get_future_klinedata0
from tools.tool import get_future_profit


# 获取k线数据（现货）
def get_klinedata(platform, symbol, granularity):
    df = pd.DataFrame()
    if platform == "huobi":
        # 50条数据，时间粒度1min, 5min, 15min, 30min, 60min, 4hour, 1day, 1mon, 1week, 1year
        huobi_granularity_dict = {60: "1min", 300: "5min", 900: "15min", 1800: "30min", 3600: "60min",
                                  14400: "4hour", 86400: "1day", 604800: "1week", 2592000: "mon",
                                  946080000: "1year"}
        for _ in range(3):
            try:
                response = requests.get(
                    "https://api.huobi.pro/market/history/kline?period={}&size=50&symbol={}".format(
                        huobi_granularity_dict[granularity], symbol.replace("_", "")), timeout=3)
                if response.status_code == 200:
                    res = response.json()['data'][::-1]
                    df['close'] = [i['close'] for i in res]
                    break
            except Exception as e:
                print(e)
    if platform == "binance":
        for _ in range(3):
            try:
                # 50条数据，时间粒度1m, 5m, 15m, 30m, 1h, 4h, 1d
                binance_granularity_dict = {60: "1m", 300: "5m", 900: "15m", 1800: "30m", 3600: "1h",
                                            14400: "4h", 86400: "1d"}
                response = requests.get("https://www.binancezh.cc/api/v3/klines?symbol={}&interval={}&limit=50".format(
                    symbol.upper().replace("_", ""), binance_granularity_dict[granularity]), timeout=1)
                if response.status_code == 200:
                    data = response.json()
                    df['close'] = [float(i[4]) for i in data]
                    break
            except Exception as e:
                print(e)
    return df


# 获取永续合约持仓
def future_position(platform, symbol):
    if platform == "binance":
        url = 'https://dapi.binance.com/futures/data/openInterestHist'
        # now = int(time.time())-86400 *29
        data = {'symbol': symbol, 'period': '1d', "pair": "{}USD".format(symbol)}
        df = pd.DataFrame()
        for _ in range(3):
            try:
                response = requests.get(url, params=data)
                if response.status_code == 200:
                    res = response.json()
                    df[symbol] = [float(i['sumOpenInterest']) for i in res]
                    # df['time']=[time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(i["timestamp"]/1000)) for i in res]
                    break
            except Exception as e:
                print(e)
        return df
    elif platform == "huobi":
        url = 'https://api.btcgateway.pro/linear-swap-api/v1/swap_his_open_interest'
        data = {'contract_code': '{}-usdt'.format(symbol).upper(), 'period': '1day', "amount_type": 1}
        df = pd.DataFrame()
        for _ in range(3):
            try:
                response = requests.get(url, params=data)
                if response.status_code == 200:
                    res = response.json()['data']['tick']
                    df[symbol] = [float(i['value']) for i in res]
                    break
            except Exception as e:
                print(e)
        return df


# 获取多个币种永续合约的50日收盘价
def get_contracts_klinedata(platform, symbols):
    contracts_klinedata = None
    for symbol in symbols:
        df = get_future_klinedata0(platform, symbol)
        df.rename(columns={'close': symbol}, inplace=True)
        contracts_klinedata = df if contracts_klinedata is None else pd.concat([contracts_klinedata, df], axis=1)
    return contracts_klinedata


# 获得多个币种近30日的持仓详情
def get_all_position(symbols):
    all_position = None
    for symbol in symbols:
        df = future_position("huobi", symbol)
        all_position = df if all_position is None else pd.concat([all_position, df], axis=1)
    return all_position


# 复合动量因子
def compound_momentum(symbols, df):
    judge_list = None
    for symbol in symbols:
        df['return'] = np.log(df[symbol] / df[symbol].shift())
        up_days = np.sum(df['return'].values > 0)
        down_days = np.sum(df['return'].values > 0)
        return_all = df['return'].sum()
        judge = pd.DataFrame({symbol: [up_days / (up_days + down_days) * return_all]})
        # judge=pd.DataFrame({symbol:[1*return_all]})
        judge_list = judge.T if judge_list is None else pd.concat([judge_list, judge.T], axis=0)
    judge_list.columns = ['return']
    judge_list.sort_values(by=['return'], ascending=False, inplace=True)
    return judge_list


# 收益率偏度因子
def skew_factor(symbols, df):
    skew_list = None
    for symbol in symbols:
        ret = np.log(df[symbol] / df[symbol].shift())
        ret.columns = [symbol]
        ret_skew = pd.DataFrame({symbol: [ret.skew()]})  # 计算偏度后转置，index为【future】,即期货种类代码
        skew_list = ret_skew.T if skew_list is None else pd.concat([skew_list, ret_skew.T], axis=0)
    skew_list.columns = ['skew']
    skew_list.sort_values(by=['skew'], ascending=False, inplace=True)
    return skew_list


# # 基差动量因子
# def basic_momentum(symbols, df, df_index):
#     index_ret_all = None
#     contract_ret_all = None
#     for symbol in symbols:
#         index_ret = (df_index[symbol].iloc[-1] - df_index[symbol].iloc[0]) / df_index[symbol].iloc[0]
#         index_ret = pd.DataFrame({symbol: [index_ret]})
#         index_ret_all = index_ret.T if index_ret is None else pd.concat([index_ret_all, index_ret.T], axis=0)
#     for symbol in symbols:
#         contract_ret = (df[symbol].iloc[-1] - df[symbol].iloc[0]) / df[symbol].iloc[0]
#         contract_ret = pd.DataFrame({symbol: [contract_ret]})
#         contract_ret_all = contract_ret.T if contract_ret is None else pd.concat([contract_ret_all, contract_ret.T],
#                                                                                  axis=0)
#     factor_momentum = contract_ret_all - index_ret_all
#     factor_momentum.columns = ['basic_return']
#     factor_momentum.sort_values(by=['basic_return'], ascending=False, inplace=True)
#     factor_momentum = factor_momentum.dropna()
#     return factor_momentum


# 仓单因子
def ware_house_factor(symbols, df):
    ware_house_list = None
    for symbol in symbols:
        ware_house_pct = pd.DataFrame(
            {symbol: [(float(df[symbol].iloc[-1]) - float(df[symbol].iloc[0])) / float(df[symbol].iloc[0])]})
        ware_house_list = ware_house_pct.T if ware_house_list is None else pd.concat(
            [ware_house_list, ware_house_pct.T], axis=0)
    ware_house_list.columns = ['position_change']
    ware_house_list.sort_values(by=['position_change'], ascending=True, inplace=True)
    ware_house_list.dropna(inplace=True)
    return ware_house_list


def weighted_factor_score(factor, factor_name, weight):
    # 给定每只期货合约的因子数据及权重，返回加权后的打分结果
    # weight是因子的权重，是该因子多空组合夏普比率，计算过程省了
    factor.columns = [factor_name]
    factor.iloc[:2] = 1
    factor.iloc[2:-2] = 0
    factor.iloc[-2:] = -1
    factor = factor * weight
    return factor


def multi_factor_signal(symbols, df, df_positions):
    # 由于排序经常变动，调低基差动量权重已减少调仓频率
    ware_house = weighted_factor_score(ware_house_factor(symbols, df_positions), '仓单因子', 1.8)  # 1
    judge_list = weighted_factor_score(compound_momentum(symbols, df), '复合动量', 0.5)  # 3.49
    skew_list = weighted_factor_score(skew_factor(symbols, df), '偏度', 1.37)  # 1.37
    result = pd.concat([ware_house, judge_list, skew_list], axis=1)
    result = result.sum(axis=1).sort_values(ascending=False)
    result.iloc[:2] = 1
    result.iloc[2:-2] = 0
    result.iloc[-2:] = -1
    return result


def trade(strategydata, symbol, direction, amount, off_set):
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    strategyId = strategydata['strategyId']
    platform = strategydata['platform']
    contract_code = "{}-usdt".format(symbol).upper()
    leverRate = strategydata['leverage']
    contract_fee = float(future_takerFee[platform]['sellfee'])  # 合约费率
    contract_size = float(contract_size_dict[symbol][platform])  # 合约面值
    close = get_perpetualprice(platform, symbol)
    init_amount = amount * 4
    if off_set == 1:
        # 先计算需下单的张数，币安与其他平台分开计算
        if direction == "buy":
            if platform == "binance":
                count = round(amount * leverRate / (close * 1.01), futureamountlimit[symbol][platform])
            else:
                count = int(amount * leverRate / (close * 1.01 * contract_size))
        if direction == "sell":
            if platform == "binance":
                count = round(amount * leverRate / (close * 0.99), futureamountlimit[symbol][platform])
            else:
                count = int(amount * leverRate / (close * 0.99 * contract_size))

        # 买入做多
        if direction == "buy":
            conn = POOL.connection()
            cur = conn.cursor()
            try:
                ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                order_price = round(close * 1.01, futurepricelimit[symbol][platform])
                i = "用户{}子账户{}多因子策略{}买入{}做多，行情价{}".format(userUuid, apiAccountId, strategyId, symbol, close)
                print(i)
                # logger.info(i)
                res = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, count, order_price, 1, 2, 1,
                                          leverRate)
                print(symbol, res)
                orderId = res['response']['orderId'].replace('"', "")
                # 将下单信息插入数据库
                insertsql = "INSERT INTO multi_factorlist(userUuid,apiAccountId,strategyId,platform,contract_code,direction," \
                            "offset,leverage,orderid,order_amount,order_price,order_time,status,uniqueId," \
                            "contract_size,tradetype) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                insertdata = (
                    userUuid, apiAccountId, strategyId, platform, contract_code, 1, "open", leverRate, orderId, count,
                    order_price, ordertime, 0, 11, contract_size, 2)
                cur.execute(insertsql, insertdata)
                conn.commit()
                # 3s后查询订单状态
                time.sleep(3)
                res = query_contract_usdt_order(userUuid, apiAccountId, platform, orderId, symbol)['response']
                print(res)
                order_status = res['status']
                if order_status == "COMPLETED":
                    trade_volume = count  # 成交数量
                    if platform == "binance":
                        trade_amount = res['detail'][0]['tradeBalance']
                        fee = trade_amount * contract_fee
                    else:
                        fee = res['detail'][0]['fee']  # 手续费
                    trade_avg_price = res['detail'][0]['price']  # 成交均价
                    strategydata['flag'][symbol] = 1
                    strategydata['entryPrice'][symbol] = trade_avg_price
                    strategydata['trade_amount'][symbol] = trade_volume
                    tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    # 将成交记录更新到数据库
                    updatesql = "update multi_factorlist set trade_amount=%s,trade_price=%s,trade_time=%s,status=%s," \
                                "fee=%s where strategyId=%s and orderid=%s "
                    cur.execute(updatesql, (trade_volume, trade_avg_price, tradetime, 1, fee, strategyId, orderId))
                    conn.commit()
                    i = "用户{}子账户{}多因子策略{}买入做多订单插入数据库".format(userUuid, apiAccountId, strategyId)
                    print(i)
                    # logger.info(i)
                    r0.hset('multi_factor_strategy', strategyId, json.dumps(strategydata))
                elif order_status == "TRADING":
                    res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol, platform, orderId)
                    if res['success']:
                        # 取消订单，将数据库订单状态改为2
                        cancel_sql = 'update multi_factorlist set status=2 where strategyId=%s and orderid=%s'
                        cur.execute(cancel_sql, (strategyId, orderId))
                        conn.commit()
            except Exception as e:
                i = "用户{}子账户{}多因子策略{}买入{}失败{}".format(userUuid, apiAccountId, strategyId, symbol, e)
                print(i)
                # logger.info(i)
                # 调用java停止策略接口
                # updateparams = {'strategyId': strategyId, "status": 4}
                # res1 = requests.post(future_strategy_status_update, data=updateparams)
                # print(json.loads(res1.content.decode()))
            finally:
                cur.close()
                conn.close()

        # 卖出做空
        elif direction == "sell":
            conn = POOL.connection()
            cur = conn.cursor()
            try:
                ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                order_price = round(close * 0.99, futurepricelimit[symbol][platform])
                i = "用户{}子账户{}多因子策略{}卖出{}做空，行情价{}".format(userUuid, apiAccountId, strategyId, symbol, close)
                print(i)
                # logger.info(i)
                res = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, count, order_price, 2, 2, 3,
                                          leverRate)
                print(symbol, res)
                orderId = res['response']['orderId'].replace('"', "")
                # 将下单信息插入数据库
                insertsql = "INSERT INTO multi_factorlist(userUuid,apiAccountId,strategyId,platform,contract_code," \
                            "direction,offset,leverage,orderid,order_amount,order_price,order_time,status,uniqueId," \
                            "contract_size,tradetype) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                insertdata = (
                    userUuid, apiAccountId, strategyId, platform, contract_code, 2, "open", leverRate, orderId, count,
                    order_price, ordertime, 0, 11, contract_size, 2)
                cur.execute(insertsql, insertdata)
                conn.commit()
                # 3s后查询订单状态
                time.sleep(3)
                res = query_contract_usdt_order(userUuid, apiAccountId, platform, orderId, symbol)['response']
                order_status = res['status']
                if order_status == "COMPLETED":
                    trade_volume = count  # 成交数量
                    if platform == "binance":
                        trade_amount = res['detail'][0]['tradeBalance']
                        fee = trade_amount * contract_fee
                    else:
                        fee = res['detail'][0]['fee']  # 手续费
                    trade_avg_price = res['detail'][0]['price']  # 成交均价
                    strategydata['flag'][symbol] = -1
                    strategydata['entryPrice'][symbol] = trade_avg_price
                    strategydata['trade_amount'][symbol] = trade_volume
                    tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    # 将成交记录更新到数据库
                    updatesql = "update multi_factorlist set trade_amount=%s,trade_price=%s,trade_time=%s,status=%s," \
                                "fee=%s where strategyId=%s and orderid=%s "
                    cur.execute(updatesql, (trade_volume, trade_avg_price, tradetime, 1, fee, strategyId, orderId))
                    conn.commit()
                    i = "用户{}子账户{}多因子策略{}卖出做空订单插入数据库".format(userUuid, apiAccountId, strategyId)
                    print(i)
                    # logger.info(i)
                    r0.hset('multi_factor_strategy', strategyId, json.dumps(strategydata))
                elif order_status == "TRADING":
                    res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol, platform, orderId)
                    if res['success']:
                        # 取消订单，将数据库订单状态改为2
                        cancel_sql = 'update keltnerlist set status=2 where strategyId=%s and orderid=%s'
                        cur.execute(cancel_sql, (strategyId, orderId))
                        conn.commit()
            except Exception as e:
                i = "用户{}子账户{}多因子策略{}卖出做空{}失败{}".format(userUuid, apiAccountId, strategyId, symbol, e)
                print(i)
                # logger.info(i)
                # 调用java停止策略接口
                # updateparams = {'strategyId': strategyId, "status": 4}
                # res1 = requests.post(future_strategy_status_update, data=updateparams)
                # print(json.loads(res1.content.decode()))
            finally:
                cur.close()
                conn.close()

    # 买入平空
    if off_set == 2 and direction == "buy":
        conn = POOL.connection()
        cur = conn.cursor()
        try:
            # 买入平仓
            ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            order_price = round(close * 1.01, futurepricelimit[symbol][platform])
            print("用户{}子账户{}多因子策略{}买入{}平空，行情价".format(userUuid, apiAccountId, strategyId, symbol, close))
            res = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, amount, order_price, 1, 2, 2, leverRate)
            print(res)
            orderId = res['response']['orderId'].replace('"', "")
            #  将下单信息插入数据库
            insertsql = "INSERT INTO multi_factorlist(userUuid,apiAccountId,strategyId,platform,contract_code,direction," \
                        "offset,leverage,orderid,order_amount,order_price,order_time,status,uniqueId,contract_size,tradetype) VALUES(" \
                        "%s, %s, %s,%s,%s,%s,%s, %s, %s, %s,%s,%s,%s,%s,%s,%s) "
            insertdata = (
                userUuid, apiAccountId, strategyId, platform, contract_code, 1, "close", leverRate, orderId, amount,
                order_price, ordertime, 0, 11, contract_size, 2)
            cur.execute(insertsql, insertdata)
            conn.commit()
            # 3s后查询订单状态
            time.sleep(3)
            res = query_contract_usdt_order(userUuid, apiAccountId, platform, orderId, symbol)['response']
            order_status = res['status']
            if order_status == "COMPLETED":
                trade_volume = amount  # 成交数量
                if platform == 'binance':
                    trade_amount = res['detail'][0]['tradeBalance']
                    fee = trade_amount * contract_fee
                    trade_avg_price = float(res['detail'][0]['price'])  # 成交均价
                    # 开空成交均价
                    entry_price = float(strategydata['entryPrice'][symbol])
                    # 计算利润（平仓 - 开仓)*成交合约张数 * 合约面值
                    profit = (entry_price - trade_avg_price) * trade_volume
                    # 手续费 成交价*成交合约张数*合约面值*费率
                    total_fee = (trade_avg_price + entry_price) * trade_volume * contract_fee
                    profit = profit - total_fee
                    profitRate = round(profit / (entry_price * trade_volume / leverRate), 8)
                else:
                    fee = res['detail'][0]['fee']  # 手续费
                    trade_avg_price = float(res['detail'][0]['price'])  # 成交均价
                    # 开空成交均价
                    entry_price = float(strategydata['entryPrice'][symbol])
                    # 计算利润,(平仓 - 开仓)*成交合约张数 * 合约面值
                    profit = (entry_price - trade_avg_price) * trade_volume * contract_size
                    # 手续费 成交价*成交合约张数*合约面值*费率
                    total_fee = (trade_avg_price + entry_price) * trade_volume * contract_size * contract_fee
                    profit = profit - total_fee
                    profitRate = round(profit / (entry_price * trade_volume * contract_size / leverRate), 8)
                print("用户{}子账户{}多因子策略{}止盈买入{}平空仓,此轮利润{}".format(userUuid, apiAccountId, strategyId, symbol, profit))
                tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                # 将成交记录更新到数据库
                updatesql = "update multi_factorlist set trade_amount=%s,trade_price=%s,trade_time=%s,profit=%s," \
                            "profitRate=%s,status=%s,fee=%s where strategyId=%s and orderid=%s"
                cur.execute(updatesql, (
                    trade_volume, trade_avg_price, tradetime, profit, profitRate, 1, fee, strategyId, orderId))
                conn.commit()
                print("用户{}子账户{}多因子策略{}买入平空订单插入数据库".format(userUuid, apiAccountId, strategyId))
                del strategydata['flag'][symbol]
                del strategydata['entryPrice'][symbol]
                del strategydata['trade_amount'][symbol]
                r0.hset('multi_factor_strategy', strategyId, json.dumps(strategydata))
                totalprofit, totalprofitRate = get_future_profit(userUuid, strategyId, init_amount, "multi_factorlist")
                params = {'strategyId': strategyId, 'profit': totalprofit, 'profitRate': totalprofitRate}
                res = requests.post(future_strategy_status_update, data=params)
                resdict = json.loads(res.content.decode())
                print(resdict)
            elif order_status == "TRADING":
                res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol, platform, orderId)
                if res['success']:
                    # 取消订单，将数据库订单状态改为2
                    cancel_sql = 'update multi_factorlist set status=2 where strategyId=%s and orderid=%s'
                    cur.execute(cancel_sql, (strategyId, orderId))
                    conn.commit()
        except Exception as e:
            i = "用户{}子账户{}多因子策略{}买入{}失败{}".format(userUuid, apiAccountId, strategyId, symbol, e)
            print(i)
            # logger.info(i)
            # 调用java停止策略接口
            # updateparams = {'strategyId': strategyId, "status": 4}
            # res1 = requests.post(future_strategy_status_update, data=updateparams)
            # print(json.loads(res1.content.decode()))
        finally:
            cur.close()
            conn.close()

    # 卖出平多
    if off_set == 2 and direction == "sell":
        conn = POOL.connection()
        cur = conn.cursor()
        try:
            # 卖出平仓
            ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            order_price = round(close * 0.99, futurepricelimit[symbol][platform])
            i = "用户{}子账户{}多因子策略{},卖出{}平仓，行情价{}".format(userUuid, apiAccountId, strategyId, symbol, close)
            print(i)
            # logger.info(i)
            res = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, amount, order_price, 2, 2, 4,
                                      leverRate)
            print(res)
            orderId = res['response']['orderId'].replace('"', "")
            #  将下单信息插入数据库
            insertsql = "INSERT INTO multi_factorlist(userUuid,apiAccountId,strategyId,platform,contract_code," \
                        "direction,offset,leverage,orderid,order_amount,order_price,order_time,status,uniqueId," \
                        "contract_size,tradetype) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            insertdata = (
                userUuid, apiAccountId, strategyId, platform, contract_code, 2, "close", leverRate, orderId, amount,
                order_price, ordertime, 0, 11, contract_size, 2)
            cur.execute(insertsql, insertdata)
            conn.commit()
            # 3s后查询订单状态
            time.sleep(3)
            res = query_contract_usdt_order(userUuid, apiAccountId, platform, orderId, symbol)['response']
            print(res)
            order_status = res['status']
            if order_status == "COMPLETED":
                trade_volume = amount  # 成交数量
                if platform == "binance":
                    trade_amount = res['detail'][0]['tradeBalance']
                    fee = trade_amount * contract_fee
                    trade_avg_price = float(res['detail'][0]['price'])  # 成交均价
                    # 开多成交均价
                    entry_price = float(strategydata['entryPrice'][symbol])
                    # 计算利润,（平仓 - 开仓)*成交合约张数 * 合约面值
                    profit = (trade_avg_price - entry_price) * trade_volume
                    # 手续费 成交价*成交合约张数*合约面值*费率
                    total_fee = (trade_avg_price + entry_price) * trade_volume * contract_fee
                    profit = profit - total_fee
                    profitRate = round(profit / (entry_price * trade_volume / leverRate), 8)
                else:
                    fee = res['detail'][0]['fee']  # 手续费
                    trade_avg_price = float(res['detail'][0]['price'])  # 成交均价
                    # 开多成交均价
                    entry_price = float(strategydata['entryPrice'][symbol])
                    # 计算利润,（平仓 - 开仓)*成交合约张数 * 合约面值
                    profit = (trade_avg_price - entry_price) * trade_volume * contract_size
                    # 手续费 成交价*成交合约张数*合约面值*费率
                    total_fee = (trade_avg_price + entry_price) * trade_volume * contract_size * contract_fee
                    profit = profit - total_fee
                    profitRate = round(profit / (entry_price * trade_volume * contract_size / leverRate), 8)
                i = "用户{}子账户{}多因子策略{}止盈卖出{}平仓，此轮利润{}".format(userUuid, apiAccountId, strategyId, symbol, profit)
                print(i)
                # logger.info(i)
                # 成交记录存入数据库
                tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                updatesql = "update multi_factorlist set trade_amount=%s,trade_price=%s,trade_time=%s,profit=%s," \
                            "profitRate=%s,status=%s,fee=%s where strategyId=%s and orderid=%s"
                cur.execute(updatesql, (
                    trade_volume, trade_avg_price, tradetime, profit, profitRate, 1, fee, strategyId, orderId))
                conn.commit()
                i = "用户{}子账户{}多因子策略{}卖出平多订单插入数据库".format(userUuid, apiAccountId, strategyId)
                print(i)
                # logger.info(i)
                del strategydata['flag'][symbol]
                del strategydata['entryPrice'][symbol]
                del strategydata['trade_amount'][symbol]
                r0.hset('multi_factor_strategy', strategyId, json.dumps(strategydata))
                totalprofit, totalprofitRate = get_future_profit(userUuid, strategyId, init_amount, "multi_factorlist")
                params = {'strategyId': strategyId, 'profit': totalprofit, 'profitRate': totalprofitRate}
                res = requests.post(future_strategy_status_update, data=params)
                resdict = json.loads(res.content.decode())
                print(resdict)
            elif order_status == "TRADING":
                res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol, platform, orderId)
                if res['success']:
                    # 取消订单，将数据库订单状态改为2
                    cancel_sql = 'update multi_factorlist set status=2 where strategyId=%s and orderid=%s'
                    cur.execute(cancel_sql, (strategyId, orderId))
                    conn.commit()
        except Exception as e:
            i = "用户{}子账户{}多因子策略{}卖出失败{}".format(userUuid, apiAccountId, strategyId, e)
            print(i)
            # logger.info(i)
            # 调用java停止策略接口
            # updateparams = {'strategyId': strategyId, "status": 4}
            # res1 = requests.post(future_strategy_status_update, data=updateparams)
            # print(json.loads(res1.content.decode()))
        finally:
            cur.close()
            conn.close()


def multi_thread_trade(strategydata, buy_list, sell_list):
    T = []
    for i in buy_list + sell_list:
        symbol = i['symbol']
        amount = i['amount']
        direction = i['direction']
        off_set = i['off_set']
        T.append(Thread(target=trade, args=(strategydata, symbol, direction, amount, off_set)))
    for t in T:
        t.start()
    for t in T:
        t.join()


# 策略清仓
def multi_factor_stopout(strategydata):
    userUuid = strategydata['userUuid']
    strategyId = strategydata['strategyId']
    amount = strategydata['amount']
    init_amount = amount * 4
    buy_list = []
    sell_list = []
    symbol_list = strategydata['flag']
    for symbol in symbol_list.keys():
        flag = symbol_list[symbol]
        trade_volume = strategydata['trade_amount'][symbol]
        if flag == 1:
            sell_list.append({"strategydata": strategydata, "symbol": symbol, "direction": "sell",
                              "amount": trade_volume, "off_set": 2})
        if flag == -1:
            buy_list.append({"strategydata": strategydata, "symbol": symbol, "direction": "buy",
                             "amount": trade_volume, "off_set": 2})
    multi_thread_trade(strategydata, buy_list, sell_list)
    totalprofit, totalprofitRate = get_future_profit(userUuid, strategyId, init_amount, "multi_factorlist")
    return totalprofit, totalprofitRate


def main(strategydata):
    localtime = time.localtime(time.time())
    print(localtime)
    if localtime.tm_hour != 10 or localtime.tm_min != 10 or localtime.tm_sec != 10:
        return
    # if localtime.tm_min % 2 !=0:
    #     return
    df = get_contracts_klinedata("huobi", symbols)
    df_positions = get_all_position(symbols)
    res = multi_factor_signal(symbols, df, df_positions)
    buy_pool = list(res.index[:2])
    sell_pool = list(res.index[-2:])
    amount = strategydata['amount']
    # 先判断是否需要变更仓位
    symbol_list = strategydata['flag']
    relocate = 0  # 是否需要调仓换股
    # 无持仓，开仓
    if len(symbol_list.keys()) == 0:
        relocate = 1
    # 有持仓，判断是否需要调仓
    else:
        for symbol in symbol_list.keys():
            flag = symbol_list[symbol]
            if flag == 1 and (symbol not in buy_pool):
                # 先清仓
                multi_factor_stopout(strategydata)
                time.sleep(3)
                relocate = 1
                break
            elif flag == -1 and (symbol not in sell_pool):
                # 先清仓
                multi_factor_stopout(strategydata)
                time.sleep(3)
                relocate = 1
                break
        if len(symbol_list.keys()) < len(buy_pool + sell_pool):
            multi_factor_stopout(strategydata)
            time.sleep(3)
            relocate = 1
    # 需调仓
    if relocate == 1:
        buy_list = []
        sell_list = []
        # 再开仓
        for symbol in buy_pool:
            buy_list.append({"symbol": symbol, "direction": "buy",
                             "amount": amount, "off_set": 1})
        for symbol in sell_pool:
            sell_list.append({"symbol": symbol, "direction": "sell",
                              "amount": amount, "off_set": 1})
        multi_thread_trade(strategydata, buy_list, sell_list)


if __name__ == "__main__":
    symbols = ['btc', 'eth', 'link', 'eos', 'fil', 'ltc', 'dot', 'doge']
    while True:
        try:
            strategy_list = r0.hvals("multi_factor_strategy")
            strategy_list = [json.loads(i) for i in strategy_list]
            T = []
            for strategy_info in strategy_list:
                T.append(Thread(target=main, args=(strategy_info,)))
            for t in T:
                t.start()
            for t in T:
                t.join()
        except Exception as e:
            print(e)
        finally:
            time.sleep(1)

    # strategydata = {
    #     "userUuid": "425",
    #     "apiAccountId": 10212,
    #     "strategyId": 1260,
    #     "flag": {
    #     },
    #     "platform": "T8ex",
    #     "leverage": 2,
    #     "amount": 50,
    #     "entryPrice": {
    #     },
    #     "trade_amount": {
    #     }
    # }

    # trade(strategydata, "fil", "buy", 10, 2)
    # main(strategydata)
    # totalprofit, totalprofitRate = multi_factor_stopout(strategydata)
    # print(totalprofit, totalprofitRate)

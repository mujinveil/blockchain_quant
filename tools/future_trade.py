# encoding=utf-8
import json
import time
import numpy as np
import requests
from loggerConfig import logger
from tools.Config import future_trade_url, future_cancel_order_url, future_query_order_url, future_switchlever_url, \
    future_takerFee, contract_size_dict, futurepricelimit, future_strategy_status_update, strategydict, \
    futureamountlimit
from tools.databasePool import POOL, r0
from tools.tool import get_future_profit

np.set_printoptions(suppress=True)  # 取消科学计数法


# # 当季、当周合约下单，火币平台
# def contract_trade(symbol, contract_type, contract_type_2, contract_type_3, volume, direction, offset, lever_rate,
#                    order_price_type, tradetype):
#     URL = huobifuture_api_url
#     ACCESS_KEY = 'ed2htwf5tf-ae6710c9-63ee5618-fff66'
#     SECRET_KEY = ''
#     dm = HuobiDM(URL, ACCESS_KEY, SECRET_KEY)
#     if tradetype == 1:  # 单个合约下单
#         dm.send_contract_order(symbol=symbol, contract_type=contract_type, contract_code="", client_order_id="",
#                                price="", volume=volume, direction=direction, offset=offset, lever_rate=lever_rate,
#                                order_price_type=order_price_type)
#     if tradetype == 2:  # 合约批量下单
#         '''
#         orders_data: example:
#         orders_data = {'orders_data': [
#                {'symbol': 'BTC', 'contract_type': 'quarter',
#                 'contract_code':'BTC181228',  'client_order_id':'',
#                 'price':1, 'volume':1, 'direction':'buy', 'offset':'open',
#                 'leverRate':20, 'orderPriceType':'limit'},
#                {'symbol': 'BTC','contract_type': 'quarter',
#                 'contract_code':'BTC181228', 'client_order_id':'',
#                 'price':2, 'volume':2, 'direction':'buy', 'offset':'open',
#                 'leverRate':20, 'orderPriceType':'limit'}]}
#         '''
#         direction2 = 'sell' if direction == 'buy' else 'buy'
#         order_data = {'orders_data': [
#             {'symbol': symbol, 'contract_type': contract_type,
#              'contract_code': '', 'client_order_id': '',
#              'price': '', 'volume': volume, 'direction': direction,
#              'offset': offset, 'leverRate': lever_rate,
#              'orderPriceType': order_price_type},
#             {'symbol': symbol, 'contract_type': contract_type_2,
#              'contract_code': '', 'client_order_id': '',
#              'price': '', 'volume': 2 * volume, 'direction': direction2,
#              'offset': offset, 'leverRate': lever_rate,
#              'orderPriceType': order_price_type},
#             {'symbol': symbol, 'contract_type': contract_type_3,
#              'contract_code': '', 'client_order_id': '',
#              'price': '', 'volume': volume, 'direction': direction,
#              'offset': offset, 'leverRate': lever_rate,
#              'orderPriceType': order_price_type},
#
#         ]}
#         dm.send_contract_batchorder(order_data)
#
#
# # USDT永续合约订单信息查询，火币平台
# def swap_order_info(order_id, contract_code):
#     url = huobifuture_api_url
#     request_path = '/linear-swap-api/v1/swap_order_info'
#     ACCESS_KEY = 'ed2htwf5tf-ae6710c9-63ee5618-fff66'
#     SECRET_KEY = ''
#     params = {'order_id': order_id,
#               'contract_code': contract_code}
#     res = api_key_post(url, request_path, params, ACCESS_KEY, SECRET_KEY)
#     return res
#
#
# # USDT永续合约下单，火币平台
# def swap_trade(contract_code, volume, direction, offset, lever_rate, order_price_type):
#     url = huobifuture_api_url
#     request_path = '/linear-swap-api/v1/swap_order'
#     ACCESS_KEY = 'ed2htwf5tf-ae6710c9-63ee5618-fff66'
#     SECRET_KEY = ''
#     params = {'volume': volume, 'direction': direction, 'offset': offset,
#               'lever_rate': lever_rate, 'order_price_type': order_price_type,
#               'contract_code': contract_code}
#     res = api_key_post(url, request_path, params, ACCESS_KEY, SECRET_KEY)
#     return res
#
#
# # USDT永续合约订单撤单，火币平台
# def swap_order_cancel(order_id, contract_code):
#     url = huobifuture_api_url
#     request_path = '/linear-swap-api/v1/swap_cancel'
#     ACCESS_KEY = 'ed2htwf5tf-ae6710c9-63ee5618-fff66'
#     SECRET_KEY = ''
#     params = {'order_id': order_id,
#               'contract_code': contract_code}
#     res = api_key_post(url, request_path, params, ACCESS_KEY, SECRET_KEY)
#     return res
#
#
# # USDT永续合约闪电平仓，火币平台
# def swap_lightning_close(contract_code, volume, direction):
#     url = huobifuture_api_url
#     request_path = '/linear-swap-api/v1/swap_cross_lightning_close_position'
#     ACCESS_KEY = 'ed2htwf5tf-ae6710c9-63ee5618-fff66'
#     SECRET_KEY = ''
#     params = {'contract_code': contract_code,
#               'volume': volume,
#               'direction': direction}
#     res = api_key_post(url, request_path, params, ACCESS_KEY, SECRET_KEY)
#     return res


# USDT永续合约下单，IFS平台
def contract_usdt_trade(userUuid, apiAccountId, symbol, platform, amount, price, direction, orderPriceType, offset,
                        leverRate):
    order_params = {'userUuid': userUuid, 'apiAccountId': apiAccountId, 'symbol': symbol, 'platform': platform,
                    'amount': amount, 'price': price, 'direction': direction, 'orderPriceType': orderPriceType,
                    'offset': offset, 'leverRate': leverRate}
    print(order_params)
    try:
        response = requests.post(future_trade_url, data=order_params)
        if response.status_code == 200:
            return response.json()
        else:
            return response.json()
    except Exception as e:
        print("httpPost failed, detail is:%s" % e)
        return {"response": "fail", "msg": "%s" % e}


# USDT永续合约订单信息查询，IFS平台
def query_contract_usdt_order(userUuid, apiAccountId, platform, orderId, symbol):
    query_order_param = {'userUuid': userUuid, 'apiAccountId': apiAccountId,
                         'platform': platform, "orderId": orderId, "symbol": symbol}
    try:
        response = requests.post(future_query_order_url, data=query_order_param)
        if response.status_code == 200:
            return response.json()
        else:
            return response.json()
    except Exception as e:
        print("httpPost failed, detail is:%s" % e)
        return {"response": "fail", "msg": "%s" % e}


# USDT永续合约订单撤单，IFS平台
def cancel_contract_usdt_order(userUuid, apiAccountId, symbol, platform, orderId):
    cancel_order_param = {'userUuid': userUuid, 'apiAccountId': apiAccountId,
                          'platform': platform, "orderId": orderId, "symbol": symbol}
    try:
        response = requests.post(future_cancel_order_url, data=cancel_order_param)
        if response.status_code == 200:
            return response.json()
        else:
            return response.json()
    except Exception as e:
        print("httpPost failed, detail is:%s" % e)
        return {"response": "fail", "msg": "%s" % e}


#  更改合约杠杆倍数
def switch_lever_rate(userUuid, apiAccountId, platform, leverRate, symbol):
    switch_data = {'userUuid': userUuid, 'apiAccountId': apiAccountId, 'platform': platform,
                   'leverRate': leverRate, 'symbol': symbol}
    try:
        res = requests.post(future_switchlever_url, data=switch_data)
        print(res.json())
        if res.json()['success']:
            print("用户{}子账户{}合约切换杠杆成功".format(userUuid, apiAccountId))
            return {"success": "合约切换杠杆成功"}
        else:
            return {"fail": res.json()['message']}
    except Exception as e:
        print("用户{}子账户{}合约切换杠杆失败,原因{}".format(userUuid, apiAccountId, e))
        return {"fail": "交易所接口不通"}


# 开多仓,IFS平台
def buy_open(strategydata, close):
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    strategyId = strategydata['strategyId']
    symbol = strategydata['symbol']
    contract_code = "{}-usdt".format(symbol).upper()
    platform = strategydata['platform']
    leverRate = strategydata['leverage']
    strategytype = strategydata['strategyType']
    strategyname = strategydict[strategytype][0]
    tablename = strategydict[strategytype][1]
    cachename = strategydict[strategytype][2]
    amount = strategydata['firstSheets']
    if platform == "binance":
        amount = round(amount, futureamountlimit[symbol][platform])
    contract_fee = float(future_takerFee[platform]['buyfee'])  # 合约费率
    contract_size = float(contract_size_dict[symbol][platform])  # 合约面值
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        # 买入做多
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        order_price = round(close * 1.01, futurepricelimit[symbol][platform])
        i = "用户{}子账户{}{}策略{}买入做多{},行情价{}".format(userUuid, apiAccountId, strategyname, strategyId, symbol, close)
        print(i)
        logger.info(i)
        res = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, amount, order_price, 1, 2, 1, leverRate)
        print(res)
        orderId = res['response']['orderId'].replace('"', "")
        # 将下单信息插入数据库
        insertsql = "INSERT INTO {0}(userUuid,apiAccountId,strategyId,platform,contract_code,direction," \
                    "offset,leverage,orderid,order_amount,order_price,order_time,status,uniqueId," \
                    "contract_size,tradetype) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ".format(
            tablename)
        insertdata = (
            userUuid, apiAccountId, strategyId, platform, contract_code, 1, "open", leverRate, orderId, amount,
            order_price, ordertime, 0, 11, contract_size, 2)
        cur.execute(insertsql, insertdata)
        conn.commit()
        # 3s后查询订单状态
        time.sleep(3)
        res = query_contract_usdt_order(userUuid, apiAccountId, platform, orderId, symbol)['response']
        order_status = res['status']
        if order_status == "COMPLETED":
            trade_volume = amount  # 成交数量
            if platform == "binance":
                trade_amount = res['detail'][0]['tradeBalance']
                fee = trade_amount * contract_fee
            else:
                fee = res['detail'][0]['fee']  # 手续费
            trade_avg_price = res['detail'][0]['price']  # 成交均价
            strategydata['flag'] = 1
            strategydata['entryPrice'] = trade_avg_price
            tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # 将成交记录更新到数据库
            updatesql = "update {0} set trade_amount=%s,trade_price=%s,trade_time=%s,status=%s," \
                        "fee=%s where strategyId=%s and orderid=%s ".format(tablename)
            cur.execute(updatesql, (trade_volume, trade_avg_price, tradetime, 1, fee, strategyId, orderId))
            conn.commit()
            i = "用户{}子账户{}{}策略{}买入做多订单插入数据库".format(userUuid, apiAccountId, strategyname, strategyId)
            print(i)
            logger.info(i)
            r0.hset(cachename, strategyId, json.dumps(strategydata))
        elif order_status == "TRADING":
            res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol, platform, orderId)
            if res['success']:
                # 取消订单，将数据库订单状态改为2
                cancel_sql = 'update {} set status=2 where strategyId=%s and orderid=%s'.format(tablename)
                cur.execute(cancel_sql, (strategyId, orderId))
                conn.commit()
    except Exception as e:
        i = "用户{}子账户{}{}策略{}买入做多失败{}".format(userUuid, apiAccountId, strategyname, strategyId, e)
        print(i)
        logger.info(i)
        # 调用java停止策略接口
        # updateparams = {'strategyId': strategyId, "status": 4}
        # res1 = requests.post(future_strategy_status_update, data=updateparams)
        # print(json.loads(res1.content.decode()))
    finally:
        cur.close()
        conn.close()


# 开空仓,IFS平台
def sell_open(strategydata, close):
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    strategyId = strategydata['strategyId']
    symbol = strategydata['symbol']
    contract_code = "{}-usdt".format(symbol).upper()
    platform = strategydata['platform']
    leverRate = strategydata['leverage']
    strategytype = strategydata['strategyType']
    strategyname = strategydict[strategytype][0]
    tablename = strategydict[strategytype][1]
    cachename = strategydict[strategytype][2]
    amount = strategydata['firstSheets']
    if platform == "binance":
        amount = round(amount, futureamountlimit[symbol][platform])
    contract_fee = float(future_takerFee[platform]['sellfee'])  # 合约费率
    contract_size = float(contract_size_dict[symbol][platform])  # 合约面值
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        # 卖出做空
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        order_price = round(close * 0.99, futurepricelimit[symbol][platform])
        i = "用户{}子账户{}{}策略{}卖出做空{},行情价{}".format(userUuid, apiAccountId, strategyname, strategyId, symbol, close)
        print(i)
        logger.info(i)
        res = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, amount, order_price, 2, 2, 3, leverRate)
        print(res)
        orderId = res['response']['orderId'].replace('"', "")
        # 将下单信息插入数据库
        insertsql = "INSERT INTO {}(userUuid,apiAccountId,strategyId,platform,contract_code,direction," \
                    "offset,leverage,orderid,order_amount,order_price,order_time,status,uniqueId," \
                    "contract_size,tradetype) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ".format(
            tablename)
        insertdata = (
            userUuid, apiAccountId, strategyId, platform, contract_code, 2, "open", leverRate, orderId, amount,
            order_price, ordertime, 0, 11, contract_size, 2)
        cur.execute(insertsql, insertdata)
        conn.commit()
        # 3s后查询订单状态
        time.sleep(3)
        res = query_contract_usdt_order(userUuid, apiAccountId, platform, orderId, symbol)['response']
        order_status = res['status']
        if order_status == "COMPLETED":
            trade_volume = amount  # 成交数量
            if platform == "binance":
                trade_amount = res['detail'][0]['tradeBalance']
                fee = trade_amount * contract_fee
            else:
                fee = res['detail'][0]['fee']  # 手续费
            trade_avg_price = res['detail'][0]['price']  # 成交均价
            strategydata['flag'] = -1
            strategydata['entryPrice'] = trade_avg_price
            tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # 将成交记录更新到数据库
            updatesql = "update {} set trade_amount=%s,trade_price=%s,trade_time=%s,status=%s," \
                        "fee=%s where strategyId=%s and orderid=%s ".format(tablename)
            cur.execute(updatesql, (trade_volume, trade_avg_price, tradetime, 1, fee, strategyId, orderId))
            conn.commit()
            i = "用户{}子账户{}{}策略{}卖出做空订单插入数据库".format(userUuid, apiAccountId, strategyname, strategyId)
            print(i)
            logger.info(i)
            r0.hset(cachename, strategyId, json.dumps(strategydata))
        elif order_status == "TRADING":
            res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol, platform, orderId)
            if res['success']:
                # 取消订单，将数据库订单状态改为2
                cancel_sql = 'update {} set status=2 where strategyId=%s and orderid=%s'.format(tablename)
                cur.execute(cancel_sql, (strategyId, orderId))
                conn.commit()
    except Exception as e:
        i = "用户{}子账户{}{}策略{}卖出做空失败{}".format(userUuid, apiAccountId, strategyname, strategyId, e)
        print(i)
        logger.info(i)
        # 调用java停止策略接口
        # updateparams = {'strategyId': strategyId, "status": 4}
        # res1 = requests.post(future_strategy_status_update, data=updateparams)
        # print(json.loads(res1.content.decode()))
    finally:
        cur.close()
        conn.close()


# 平多仓,IFS平台
def buy_close(strategydata, close):
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    strategyId = strategydata['strategyId']
    symbol = strategydata['symbol']
    contract_code = "{}-usdt".format(symbol).upper()
    platform = strategydata['platform']
    leverRate = strategydata['leverage']
    strategytype = strategydata['strategyType']
    strategyname = strategydict[strategytype][0]
    tablename = strategydict[strategytype][1]
    cachename = strategydict[strategytype][2]
    amount = strategydata['firstSheets']
    if platform == "binance":
        amount = round(amount, futureamountlimit[symbol][platform])
    contract_fee = float(future_takerFee[platform]['sellfee'])  # 合约费率
    contract_size = float(contract_size_dict[symbol][platform])  # 合约面值
    if platform == "binance":
        init_amount = amount * close / leverRate
    else:
        init_amount = amount * close * contract_size / leverRate
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        # 卖出平仓
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        order_price = round(close * 0.99, futurepricelimit[symbol][platform])
        i = "用户{}子账户{}{}策略{}平{}多仓,行情价{}".format(userUuid, apiAccountId, strategyname, strategyId, symbol, close)
        print(i)
        logger.info(i)
        res = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, amount, order_price, 2, 2, 4, leverRate)
        print(res)
        orderId = res['response']['orderId'].replace('"', "")
        #  将下单信息插入数据库
        insertsql = "INSERT INTO {}(userUuid,apiAccountId,strategyId,platform,contract_code," \
                    "direction,offset,leverage,orderid,order_amount,order_price,order_time,status,uniqueId," \
                    "contract_size,tradetype) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ".format(
            tablename)
        insertdata = (
            userUuid, apiAccountId, strategyId, platform, contract_code, 2, "close", leverRate, orderId,
            amount, order_price, ordertime, 0, 11, contract_size, 2)
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
                entry_price = float(strategydata['entryPrice'])
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
                entry_price = float(strategydata['entryPrice'])
                # 计算利润,（平仓 - 开仓)*成交合约张数 * 合约面值
                profit = (trade_avg_price - entry_price) * trade_volume * contract_size
                # 手续费 成交价*成交合约张数*合约面值*费率
                total_fee = (trade_avg_price + entry_price) * trade_volume * contract_size * contract_fee
                profit = profit - total_fee
                profitRate = round(profit / (entry_price * trade_volume * contract_size / leverRate), 8)
            i = "用户{}子账户{}{}策略{}止盈卖出平仓，此轮利润{}".format(userUuid, apiAccountId, strategyname, strategyId, profit)
            print(i)
            logger.info(i)
            # 成交记录存入数据库
            tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            updatesql = "update {} set trade_amount=%s,trade_price=%s,trade_time=%s,profit=%s," \
                        "profitRate=%s,status=%s,fee=%s where strategyId=%s and orderid=%s".format(tablename)
            cur.execute(updatesql, (
                trade_volume, trade_avg_price, tradetime, profit, profitRate, 1, fee, strategyId, orderId))
            conn.commit()
            i = "用户{}子账户{}{}策略{}卖出平多订单插入数据库".format(userUuid, apiAccountId, strategyname, strategyId)
            print(i)
            logger.info(i)
            strategydata['flag'] = 0
            r0.hset(cachename, strategyId, json.dumps(strategydata))
            totalprofit, totalprofitRate = get_future_profit(userUuid, strategyId, init_amount, tablename)
            params = {'strategyId': strategyId, 'profit': totalprofit, 'profitRate': totalprofitRate}
            res = requests.post(future_strategy_status_update, data=params)
            resdict = json.loads(res.content.decode())
            print(resdict)
        elif order_status == "TRADING":
            res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol, platform, orderId)
            if res['success']:
                # 取消订单，将数据库订单状态改为2
                cancel_sql = 'update {} set status=2 where strategyId=%s and orderid=%s'.format(tablename)
                cur.execute(cancel_sql, (strategyId, orderId))
                conn.commit()
    except Exception as e:
        i = "用户{}子账户{}{}策略{}卖出失败{}".format(userUuid, apiAccountId, strategyname, strategyId, e)
        print(i)
        logger.info(i)
        # 调用java停止策略接口
        # updateparams = {'strategyId': strategyId, "status": 4}
        # res1 = requests.post(future_strategy_status_update, data=updateparams)
        # print(json.loads(res1.content.decode()))
    finally:
        cur.close()
        conn.close()


# 平空仓,IFS平台
def sell_close(strategydata, close):
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    strategyId = strategydata['strategyId']
    symbol = strategydata['symbol']
    contract_code = "{}-usdt".format(symbol).upper()
    platform = strategydata['platform']
    leverRate = strategydata['leverage']
    strategytype = strategydata['strategyType']
    strategyname = strategydict[strategytype][0]
    tablename = strategydict[strategytype][1]
    cachename = strategydict[strategytype][2]
    amount = strategydata['firstSheets']
    if platform == "binance":
        amount = round(amount, futureamountlimit[symbol][platform])
    contract_fee = float(future_takerFee[platform]['sellfee'])  # 合约费率
    contract_size = float(contract_size_dict[symbol][platform])  # 合约面值
    if platform == "binance":
        init_amount = amount * close / leverRate
    else:
        init_amount = amount * close * contract_size / leverRate
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        # 买入平仓
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        order_price = round(close * 1.01, futurepricelimit[symbol][platform])
        print("用户{}子账户{}{}策略{}平{}空仓,行情价{}".format(userUuid, apiAccountId, strategyname, strategyId, symbol, close))
        res = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, amount, order_price, 1, 2, 2, leverRate)
        print(res)
        orderId = res['response']['orderId'].replace('"', "")
        #  将下单信息插入数据库
        insertsql = "INSERT INTO {}(userUuid,apiAccountId,strategyId,platform,contract_code,direction," \
                    "offset,leverage,orderid,order_amount,order_price,order_time,status,uniqueId,contract_size,tradetype) VALUES(" \
                    "%s, %s, %s,%s,%s,%s,%s, %s, %s, %s,%s,%s,%s,%s,%s,%s) ".format(tablename)
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
                entry_price = float(strategydata['entryPrice'])
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
                entry_price = float(strategydata['entryPrice'])
                # 计算利润,(平仓 - 开仓)*成交合约张数 * 合约面值
                profit = (entry_price - trade_avg_price) * trade_volume * contract_size
                # 手续费 成交价*成交合约张数*合约面值*费率
                total_fee = (trade_avg_price + entry_price) * trade_volume * contract_size * contract_fee
                profit = profit - total_fee
                profitRate = round(profit / (entry_price * trade_volume * contract_size / leverRate), 8)
            print("用户{}子账户{}{}策略{}止盈买入平空仓,此轮利润{}".format(userUuid, apiAccountId, strategyname, strategyId, profit))
            tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # 将成交记录更新到数据库
            updatesql = "update {} set trade_amount=%s,trade_price=%s,trade_time=%s,profit=%s," \
                        "profitRate=%s,status=%s,fee=%s where strategyId=%s and orderid=%s".format(tablename)
            cur.execute(updatesql, (
                trade_volume, trade_avg_price, tradetime, profit, profitRate, 1, fee, strategyId, orderId))
            conn.commit()
            print("用户{}子账户{}{}策略{}买入平空订单插入数据库".format(userUuid, apiAccountId, strategyname, strategyId))
            strategydata['flag'] = 0
            r0.hset(cachename, strategyId, json.dumps(strategydata))
            totalprofit, totalprofitRate = get_future_profit(userUuid, strategyId, init_amount, tablename)
            params = {'strategyId': strategyId, 'profit': totalprofit, 'profitRate': totalprofitRate}
            res = requests.post(future_strategy_status_update, data=params)
            resdict = json.loads(res.content.decode())
            print(resdict)
        elif order_status == "TRADING":
            res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol, platform, orderId)
            if res['success']:
                # 取消订单，将数据库订单状态改为2
                cancel_sql = 'update {} set status=2 where strategyId=%s and orderid=%s'.format(tablename)
                cur.execute(cancel_sql, (strategyId, orderId))
                conn.commit()
    except Exception as e:
        i = "用户{}子账户{}{}策略{}买入失败{}".format(userUuid, apiAccountId, strategyname, strategyId, e)
        print(i)
        logger.info(i)
        # 调用java停止策略接口
        # updateparams = {'strategyId': strategyId, "status": 4}
        # res1 = requests.post(future_strategy_status_update, data=updateparams)
        # print(json.loads(res1.content.decode()))
    finally:
        cur.close()
        conn.close()


# 海龟策略买入开多
def turtle_buy_open(strategydata, close, unit):
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    strategyId = strategydata['strategyId']
    symbol = strategydata['symbol']
    contract_code = "{}-usdt".format(symbol).upper()
    platform = strategydata['platform']
    leverRate = int(strategydata['leverage'])
    contract_fee = float(future_takerFee[platform]['buyfee'])  # 合约费率
    contract_size = float(contract_size_dict[symbol][platform])  # 合约面值
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        # 买入做多
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        order_price = round(close * 1.01, futurepricelimit[symbol][platform])
        i = "用户{}子账户{}海龟策略{}买入做多{},行情价{}".format(userUuid, apiAccountId, strategyId, symbol, close)
        print(i)
        logger.info(i)
        res = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, unit, order_price, 1, 2, 1, leverRate)
        print(res)
        orderId = res['response']['orderId'].replace('"', "")
        # 将下单信息插入数据库
        insertsql = "INSERT INTO turtlelist(userUuid,apiAccountId,strategyId,platform,contract_code,direction," \
                    "offset,leverage,orderid,order_amount,order_price,order_time,status,uniqueId," \
                    "contract_size,tradetype) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        insertdata = (
            userUuid, apiAccountId, strategyId, platform, contract_code, 1, "open", leverRate, orderId, int(unit),
            order_price, ordertime, 0, 11, contract_size, 2)
        print(insertdata)
        cur.execute(insertsql, insertdata)
        conn.commit()
        # 3s后查询订单状态
        time.sleep(3)
        res = query_contract_usdt_order(userUuid, apiAccountId, platform, orderId, symbol)['response']
        print(res)
        order_status = res['status']
        if order_status == "COMPLETED":
            if platform == "binance":
                trade_amount = res['detail'][0]['tradeBalance']
                fee = trade_amount * contract_fee
            else:
                fee = res['detail'][0]['fee']  # 手续费
            trade_avg_price = float(res['detail'][0]['price'])  # 成交均价
            strategydata['flag'] = 1
            strategydata['buy_value'] += float(unit * trade_avg_price)
            strategydata['buy_num'] += unit
            strategydata['last_price'] = close
            strategydata['add_time'] += 1
            tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # 将成交记录更新到数据库
            updatesql = "update turtlelist set trade_amount=%s,trade_price=%s,trade_time=%s,status=%s," \
                        "fee=%s where strategyId=%s and orderid=%s "
            cur.execute(updatesql, (unit, trade_avg_price, tradetime, 1, fee, strategyId, orderId))
            conn.commit()
            i = "用户{}子账户{}海龟策略{}买入做多订单插入数据库".format(userUuid, apiAccountId, strategyId)
            print(i)
            logger.info(i)
            r0.hset("Turtle_strategy", strategyId, json.dumps(strategydata))
        elif order_status == "TRADING":
            res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol, platform, orderId)
            if res['success']:
                # 取消订单，将数据库订单状态改为2
                cancel_sql = 'update turtlelist set status=2 where strategyId=%s and orderid=%s'
                cur.execute(cancel_sql, (strategyId, orderId))
                conn.commit()

    except Exception as e:
        i = "用户{}子账户{}海龟策略{}买入做多失败{}".format(userUuid, apiAccountId, strategyId, e)
        print(i)
        logger.info(i)
        # 调用java停止策略接口
        # updateparams = {'strategyId': strategyId, "status": 4}
        # res1 = requests.post(future_strategy_status_update, data=updateparams)
        # print(json.loads(res1.content.decode()))
    finally:
        cur.close()
        conn.close()


# 海龟策略卖出开空
def turtle_sell_open(strategydata, close, unit):
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    strategyId = strategydata['strategyId']
    symbol = strategydata['symbol']
    contract_code = "{}-usdt".format(symbol).upper()
    platform = strategydata['platform']
    leverRate = strategydata['leverage']
    contract_fee = float(future_takerFee[platform]['sellfee'])  # 合约费率
    contract_size = float(contract_size_dict[symbol][platform])  # 合约面值
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        # 卖出做空
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        order_price = round(close * 0.99, futurepricelimit[symbol][platform])
        i = "用户{}子账户{}海龟策略{}卖出做空{},行情价{}".format(userUuid, apiAccountId, strategyId, symbol, close)
        print(i)
        logger.info(i)
        res = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, unit, order_price, 2, 2, 3, leverRate)
        print(res)
        orderId = res['response']['orderId'].replace('"', "")
        # 将下单信息插入数据库
        insertsql = "INSERT INTO turtlelist(userUuid,apiAccountId,strategyId,platform,contract_code,direction," \
                    "offset,leverage,orderid,order_amount,order_price,order_time,status,uniqueId," \
                    "contract_size,tradetype) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        insertdata = (
            userUuid, apiAccountId, strategyId, platform, contract_code, 2, "open", leverRate, orderId, unit,
            order_price, ordertime, 0, 11, contract_size, 2)
        cur.execute(insertsql, insertdata)
        conn.commit()
        # 3s后查询订单状态
        time.sleep(3)
        res = query_contract_usdt_order(userUuid, apiAccountId, platform, orderId, symbol)['response']
        order_status = res['status']
        if order_status == "COMPLETED":
            if platform == "binance":
                trade_amount = res['detail'][0]['tradeBalance']
                fee = trade_amount * contract_fee
            else:
                fee = res['detail'][0]['fee']  # 手续费
            trade_avg_price = float(res['detail'][0]['price'])  # 成交均价
            strategydata['flag'] = -1
            strategydata['sell_value'] += float(unit * trade_avg_price)
            strategydata['sell_num'] += unit
            strategydata['last_price'] = close
            strategydata['add_time'] += 1
            tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # 将成交记录更新到数据库
            updatesql = "update turtlelist set trade_amount=%s,trade_price=%s,trade_time=%s,status=%s," \
                        "fee=%s where strategyId=%s and orderid=%s "
            cur.execute(updatesql, (unit, trade_avg_price, tradetime, 1, fee, strategyId, orderId))
            conn.commit()
            i = "用户{}子账户{}海龟策略{}卖出做空订单插入数据库".format(userUuid, apiAccountId, strategyId)
            print(i)
            logger.info(i)
            r0.hset("Turtle_strategy", strategyId, json.dumps(strategydata))
        elif order_status == "TRADING":
            res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol, platform, orderId)
            if res['success']:
                # 取消订单，将数据库订单状态改为2
                cancel_sql = 'update turtlelist set status=2 where strategyId=%s and orderid=%s'
                cur.execute(cancel_sql, (strategyId, orderId))
                conn.commit()
    except Exception as e:
        i = "用户{}子账户{}海龟策略{}卖出做空失败{}".format(userUuid, apiAccountId, strategyId, e)
        print(i)
        logger.info(i)
        # 调用java停止策略接口
        # updateparams = {'strategyId': strategyId, "status": 4}
        # res1 = requests.post(future_strategy_status_update, data=updateparams)
        # print(json.loads(res1.content.decode()))
    finally:
        cur.close()
        conn.close()


# 海龟策略卖出平多
def turtle_buy_close(strategydata, close):
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    strategyId = strategydata['strategyId']
    symbol = strategydata['symbol']
    contract_code = "{}-usdt".format(symbol).upper()
    platform = strategydata['platform']
    leverRate = strategydata['leverage']
    strategytype = strategydata['strategyType']
    amount = strategydata['amount']
    unit = strategydata['buy_num']
    contract_fee = float(future_takerFee[platform]['sellfee'])  # 合约费率
    contract_size = float(contract_size_dict[symbol][platform])  # 合约面值
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        # 卖出平仓
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        order_price = round(close * 0.99, futurepricelimit[symbol][platform])
        print(order_price)
        i = "用户{}子账户{}海龟策略{}平{}多仓,行情价{}".format(userUuid, apiAccountId, strategyId, symbol, close)
        print(i)
        logger.info(i)
        res = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, unit, order_price, 2, 2, 4, leverRate)
        print(res)
        orderId = res['response']['orderId'].replace('"', "")
        #  将下单信息插入数据库
        insertsql = "INSERT INTO turtlelist(userUuid,apiAccountId,strategyId,platform,contract_code," \
                    "direction,offset,leverage,orderid,order_amount,order_price,order_time,status,uniqueId," \
                    "contract_size,tradetype) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        insertdata = (
            userUuid, apiAccountId, strategyId, platform, contract_code, 2, "close", leverRate, orderId,
            unit, order_price, ordertime, 0, 11, contract_size, 2)
        cur.execute(insertsql, insertdata)
        conn.commit()
        # 3s后查询订单状态
        time.sleep(3)
        res = query_contract_usdt_order(userUuid, apiAccountId, platform, orderId, symbol)['response']
        print(res)
        order_status = res['status']
        if order_status == "COMPLETED":
            trade_volume = unit  # 成交数量
            if platform == "binance":
                trade_amount = res['detail'][0]['tradeBalance']
                fee = trade_amount * contract_fee
                trade_avg_price = float(res['detail'][0]['price'])  # 成交均价
                # 开多成交均价
                entry_price = strategydata['buy_value'] / strategydata['buy_num']
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
                entry_price = strategydata['buy_value'] / strategydata['buy_num']
                # 计算利润,（平仓 - 开仓)*成交合约张数 * 合约面值
                profit = (trade_avg_price - entry_price) * trade_volume * contract_size
                # 手续费 成交价*成交合约张数*合约面值*费率
                total_fee = (trade_avg_price + entry_price) * trade_volume * contract_size * contract_fee
                profit = profit - total_fee
                profitRate = round(profit / (entry_price * trade_volume * contract_size / leverRate), 8)
            i = "用户{}子账户{}海龟策略{}止盈卖出平仓，此轮利润{}".format(userUuid, apiAccountId, strategyId, profit)
            print(i)
            logger.info(i)
            # 成交记录存入数据库
            tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            updatesql = "update turtlelist set trade_amount=%s,trade_price=%s,trade_time=%s,profit=%s," \
                        "profitRate=%s,status=%s,fee=%s where strategyId=%s and orderid=%s"
            cur.execute(updatesql, (
                trade_volume, trade_avg_price, tradetime, profit, profitRate, 1, fee, strategyId, orderId))
            conn.commit()
            i = "用户{}子账户{}海龟策略{}卖出平多订单插入数据库".format(userUuid, apiAccountId, strategyId)
            print(i)
            logger.info(i)
            strategydata['flag'] = 0
            strategydata['buy_value'] = 0
            strategydata['buy_num'] = 0
            strategydata['last_price'] = 0
            strategydata['add_time'] = 0
            r0.hset("Turtle_strategy", strategyId, json.dumps(strategydata))
            totalprofit, totalprofitRate = get_future_profit(userUuid, strategyId, amount, "turtlelist")
            params = {'strategyId': strategyId, 'profit': totalprofit, 'profitRate': totalprofitRate}
            res = requests.post(future_strategy_status_update, data=params)
            resdict = json.loads(res.content.decode())
            print(resdict)
        elif order_status == "TRADING":
            res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol, platform, orderId)
            if res['success']:
                # 取消订单，将数据库订单状态改为2
                cancel_sql = 'update turtlelist set status=2 where strategyId=%s and orderid=%s'
                cur.execute(cancel_sql, (strategyId, orderId))
                conn.commit()

    except Exception as e:
        i = "用户{}子账户{}海龟策略{}卖出失败{}".format(userUuid, apiAccountId, strategyId, e)
        print(i)
        logger.info(i)
        # 调用java停止策略接口
        # updateparams = {'strategyId': strategyId, "status": 4}
        # res1 = requests.post(future_strategy_status_update, data=updateparams)
        # print(json.loads(res1.content.decode()))
    finally:
        cur.close()
        conn.close()


# 海龟策略买入平空
def turtle_sell_close(strategydata, close):
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    strategyId = strategydata['strategyId']
    symbol = strategydata['symbol']
    contract_code = "{}-usdt".format(symbol).upper()
    platform = strategydata['platform']
    leverRate = strategydata['leverage']
    amount = strategydata['amount']
    strategytype = strategydata['strategyType']
    unit = strategydata['sell_num']
    contract_fee = float(future_takerFee[platform]['sellfee'])  # 合约费率
    contract_size = float(contract_size_dict[symbol][platform])  # 合约面值
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        # 买入平仓
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        order_price = round(close * 1.01, futurepricelimit[symbol][platform])
        print("用户{}子账户{}海龟策略{}平{}空仓,行情价{}".format(userUuid, apiAccountId, strategyId, symbol, close))
        res = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, unit, order_price, 1, 2, 2, leverRate)
        print(res)
        orderId = res['response']['orderId'].replace('"', "")
        #  将下单信息插入数据库
        insertsql = "INSERT INTO turtlelist(userUuid,apiAccountId,strategyId,platform,contract_code,direction," \
                    "offset,leverage,orderid,order_amount,order_price,order_time,status,uniqueId,contract_size,tradetype) VALUES(" \
                    "%s, %s, %s,%s,%s,%s,%s, %s, %s, %s,%s,%s,%s,%s,%s,%s) "
        insertdata = (
            userUuid, apiAccountId, strategyId, platform, contract_code, 1, "close", leverRate, orderId, unit,
            order_price, ordertime, 0, 11, contract_size, 2)
        cur.execute(insertsql, insertdata)
        conn.commit()
        # 3s后查询订单状态
        time.sleep(3)
        res = query_contract_usdt_order(userUuid, apiAccountId, platform, orderId, symbol)['response']
        order_status = res['status']
        if order_status == "COMPLETED":
            trade_volume = unit  # 成交数量
            if platform == 'binance':
                trade_amount = res['detail'][0]['tradeBalance']
                fee = trade_amount * contract_fee
                trade_avg_price = float(res['detail'][0]['price'])  # 成交均价
                # 开空成交均价
                entry_price = strategydata['sell_value'] / strategydata['sell_num']
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
                entry_price = strategydata['sell_value'] / strategydata['sell_num']
                # 计算利润,(平仓 - 开仓)*成交合约张数 * 合约面值
                profit = (entry_price - trade_avg_price) * trade_volume * contract_size
                # 手续费 成交价*成交合约张数*合约面值*费率
                total_fee = (trade_avg_price + entry_price) * trade_volume * contract_size * contract_fee
                profit = profit - total_fee
                profitRate = round(profit / (entry_price * trade_volume * contract_size / leverRate), 8)
            print("用户{}子账户{}海龟策略{}止盈买入平空仓,此轮利润{}".format(userUuid, apiAccountId, strategyId, profit))
            tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # 将成交记录更新到数据库
            updatesql = "update turtlelist set trade_amount=%s,trade_price=%s,trade_time=%s,profit=%s," \
                        "profitRate=%s,status=%s,fee=%s where strategyId=%s and orderid=%s"
            cur.execute(updatesql, (
                trade_volume, trade_avg_price, tradetime, profit, profitRate, 1, fee, strategyId, orderId))
            conn.commit()
            print("用户{}子账户{}海龟策略{}买入平空订单插入数据库".format(userUuid, apiAccountId, strategyId))
            strategydata['flag'] = 0
            strategydata['sell_value'] = 0
            strategydata['sell_num'] = 0
            strategydata['last_price'] = 0
            strategydata['add_time'] = 0
            r0.hset("Turtle_strategy", strategyId, json.dumps(strategydata))
            totalprofit, totalprofitRate = get_future_profit(userUuid, strategyId, amount, "turtlelist")
            params = {'strategyId': strategyId, 'profit': totalprofit, 'profitRate': totalprofitRate}
            res = requests.post(future_strategy_status_update, data=params)
            resdict = json.loads(res.content.decode())
            print(resdict)
        elif order_status == "TRADING":
            res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol, platform, orderId)
            if res['success']:
                # 取消订单，将数据库订单状态改为2
                cancel_sql = 'update turtlelist set status=2 where strategyId=%s and orderid=%s'
                cur.execute(cancel_sql, (strategyId, orderId))
                conn.commit()
    except Exception as e:
        i = "用户{}子账户{}海龟策略{}买入失败{}".format(userUuid, apiAccountId, strategyId, e)
        print(i)
        logger.info(i)
        # 调用java停止策略接口
        # updateparams = {'strategyId': strategyId, "status": 4}
        # res1 = requests.post(future_strategy_status_update, data=updateparams)
        # print(json.loads(res1.content.decode()))
    finally:
        cur.close()
        conn.close()

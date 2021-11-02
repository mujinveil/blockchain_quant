# encoding='utf-8'
import json
import time
import requests
from loggerConfig import logger
from tools.Config import Trade_url, Queryorder_url, Query_tradeprice_url, Fee
from tools.databasePool import POOL
from tools.tool import cancel_order


def kama_buy_copy(tracer_info, order_param):
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        strategyId = tracer_info['strategyId']
        userUuid = tracer_info['userUuid']
        apiAccountId = tracer_info['apiAccountId']
        platform = tracer_info['platform']
        strategytype = int(tracer_info['strategyType'])
        followstrategyId = tracer_info['followStrategyId']
        direction = order_param['direction']
        source = order_param['source']
        amount = order_param['amount']
        price = order_param['price']
        symbol = order_param['symbol']
        new_order_param = {"direction": direction, "amount": amount, "symbol": symbol, "platform": platform,
                           "price": price, "userUuid": userUuid, "apiAccountId": apiAccountId, "source": source,
                           "strategyId": strategyId, 'tradetype': 1}
        traderes = requests.post(Trade_url, data=new_order_param)
        trade_dict = json.loads(traderes.content.decode())
        print("用户{}子账户{}跟踪自适应均线策略{}订单{}".format(userUuid, apiAccountId, strategyId, trade_dict))
        orderId = trade_dict["response"]["orderid"]  # 获取订单id
        order_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        insertsql = "INSERT INTO copylist(userUuid,apiAccountId,strategyId,followstrategyId,platform,symbol," \
                    "direction,orderid,order_amount,order_price,order_time,status,uniqueId," \
                    "tradetype,strategytype) VALUES(%s, %s, %s, %s,%s, " \
                    "%s,%s,%s, %s, %s, %s, %s,%s,%s,%s)"
        cur.execute(insertsql, (
            userUuid, apiAccountId, strategyId, followstrategyId, platform, symbol, direction, orderId, amount, price,
            order_time, 0, source, 1, strategytype))
        conn.commit()
        i = '用户{}子账户{}自适应均线策略ID{}跟踪下买单成功,并将订单插入数据库'.format(userUuid, apiAccountId, strategyId)
        print(i)
        logger.info(i)
        time.sleep(3)
        # 3秒后检查订单成交状况
        query_params = {"direction": direction, "symbol": symbol, "platform": platform,
                        "orderId": orderId, "apiAccountId": apiAccountId, "userUuid": userUuid, "source": source,
                        "strategyId": strategyId}
        res = requests.post(Queryorder_url, data=query_params)
        querydict = json.loads(res.content.decode())
        logger.info("用户{}子账户{}跟踪自适应均线策略{}查询订单{}".format(userUuid, apiAccountId, strategyId, querydict))
        resp = querydict['response']
        if resp and resp['status'] == 'closed':
            queryparams = {"platform": platform, "symbol": symbol, "orderId": orderId, "apiId": apiAccountId,
                           "userUuid": userUuid}
            res = requests.post(Query_tradeprice_url, data=queryparams)
            queryresdict = json.loads(res.content.decode())
            fee = Fee[platform]['buyfee']
            try:
                tradeprice = queryresdict['response']['avgPrice']
                tradetime = queryresdict['response']['createdDate']
                totalamount = float(queryresdict['response']['totalAmount'])
                totalfees = float(queryresdict['response']['totalFees'])
                numberDeal = round((totalamount - totalfees), 8)
                buyfee = round(numberDeal * tradeprice * fee, 8)
            except:
                tradeprice = price
                tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                numberDeal = round(amount * 0.998, 8)
                buyfee = round(numberDeal * tradeprice * fee, 8)
            # 更新数据库
            updatesql = "update copylist set trade_amount=%s,trade_price=%s,trade_time=%s," \
                        "status=1,fee=%s where strategyId=%s and orderid=%s "
            cur.execute(updatesql, (amount, tradeprice, tradetime, buyfee, strategyId, orderId))
            conn.commit()
            i = '用户{}子账户{}自定义均线策略{}跟踪下买单已全部成交,并将成交记录插入数据库'.format(userUuid, apiAccountId, strategyId)
            print(i)
            logger.info(i)
        elif resp and resp['status'] == 'open':
            print("=================订单未成交，撤单=============")
            cancel_order(userUuid, apiAccountId, strategyId, platform, symbol, orderId, direction, 'copylist')
    except Exception as e:
        i = '用户{}子账户{}自适应均线策略{}跟单失败{}'.format(userUuid, apiAccountId, strategyId, e)
        print(i)
        logger.error(i)
    finally:
        cur.close()
        conn.close()


def kama_sell_copy(tracer_info, order_param):
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        strategyId = tracer_info['strategyId']
        userUuid = tracer_info['userUuid']
        apiAccountId = tracer_info['apiAccountId']
        platform = tracer_info['platform']
        strategytype = int(tracer_info['strategyType'])
        followstrategyId = tracer_info['followStrategyId']
        direction = order_param['direction']
        source = order_param['source']
        amount = order_param['amount']
        price = order_param['price']
        entryPrice = order_param['entryPrice']
        symbol = order_param['symbol']
        new_order_param = {"direction": direction, "amount": amount, "symbol": symbol, "platform": platform,
                           "price": price, "userUuid": userUuid, "apiAccountId": apiAccountId, "source": source,
                           "strategyId": strategyId, 'tradetype': 1}
        traderes = requests.post(Trade_url, data=new_order_param)
        trade_dict = json.loads(traderes.content.decode())
        print("用户{}子账户{}跟踪自适应均线策略{}订单{}".format(userUuid, apiAccountId, strategyId, trade_dict))
        if trade_dict['message'] in ['资产不足', '连接交易所失败，请稍后重试']:
            i = '用户{}子账户{}策略ID{}跟踪自定义均线下卖单失败，原因交易对{}{}'.format(userUuid, apiAccountId, strategyId, symbol,
                                                               trade_dict['message'])
            print(i)
            logger.error(i)
            return
        orderId = trade_dict["response"]["orderid"]  # 获取订单id
        order_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        insertsql = "INSERT INTO copylist(userUuid,apiAccountId,strategyId,followstrategyId,platform,symbol," \
                    "direction,orderid,order_amount,order_price,order_time,status,uniqueId," \
                    "tradetype,strategytype) VALUES(%s, %s, %s, %s,%s, " \
                    "%s,%s,%s, %s, %s, %s, %s,%s,%s,%s)"
        cur.execute(insertsql, (
            userUuid, apiAccountId, strategyId, followstrategyId, platform, symbol, direction, orderId, amount, price,
            order_time, 0, source, 1, strategytype))
        conn.commit()
        i = '用户{}子账户{}自适应均线策略{}交易对{}跟踪下卖单成功,并将订单插入数据库'.format(userUuid, apiAccountId, strategyId, symbol)
        print(i)
        logger.info(i)
        time.sleep(3)
        # 3秒后检查订单成交状况
        query_params = {"direction": direction, "symbol": symbol, "platform": platform,
                        "orderId": orderId, "apiAccountId": apiAccountId, "userUuid": userUuid, "source": source,
                        "strategyId": strategyId}
        res = requests.post(Queryorder_url, data=query_params)
        querydict = json.loads(res.content.decode())
        logger.info("用户{}子账户{}自适应均线策略{}查询订单{}".format(userUuid, apiAccountId, strategyId, querydict))
        resp = querydict['response']
        if resp and resp['status'] == 'closed':
            queryparams = {"platform": platform, "symbol": symbol, "orderId": orderId, "apiId": apiAccountId,
                           "userUuid": userUuid}
            res = requests.post(Query_tradeprice_url, data=queryparams)
            queryresdict = json.loads(res.content.decode())
            fee = Fee[platform]['sellfee']
            try:
                tradeprice = queryresdict['response']['avgPrice']
                tradetime = queryresdict['response']['createdDate']
                numberDeal = queryresdict['response']['totalAmount']
                sellfee = round(numberDeal * tradeprice * fee, 8)
            except:
                tradeprice = price
                tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                numberDeal = amount
                sellfee = round(numberDeal * tradeprice * fee, 8)
            profit = round((tradeprice - entryPrice - (tradeprice + entryPrice) * fee) * numberDeal, 8)
            profitRate = round(profit / (entryPrice * numberDeal), 8)
            updatesql = "update copylist set trade_amount=%s,trade_price=%s,trade_time=%s,profit=%s," \
                        "profitRate=%s,status=%s,fee=%s where " \
                        "strategyId=%s and orderid=%s"
            cur.execute(updatesql,
                        (numberDeal, tradeprice, tradetime, profit, profitRate, 1, sellfee, strategyId, orderId))
            conn.commit()
            i = "用户{}子账户{}自适应均线策略{}交易对{}跟踪下卖单全部成交".format(userUuid, apiAccountId, strategyId, symbol)
            print(i)
            logger.info(i)
        elif resp['status'] == "open":
            cancel_order(userUuid, apiAccountId, strategyId, platform, symbol, orderId, direction, 'copylist')
    except Exception as e:
        i = '用户{}子账户{}自适应均线策略{}跟单失败{}'.format(userUuid, apiAccountId, strategyId, e)
        print(i)
        logger.error(i)
    finally:
        cur.close()
        conn.close()


# # 清仓
# def clear_kama_copy(tracer_info):
#     userUuid = tracer_info['userUuid']
#     apiAccountId = tracer_info['apiAccountId']
#     platform = tracer_info["platform"]
#     strategyId = tracer_info['strategyId']
#     followstrategyId = tracer_info['followStrategyId']
#     Kama_copylist = r5.hvals('order_param_15:{}'.format(followstrategyId))
#     Kama_copyinfo = [json.loads(i) for i in Kama_copylist]
#     tablename = 'copylist'
#     strategyname = "自适应均线"
#     T_copy = []
#     for i in Kama_copyinfo:
#         symbol = i['symbol']
#         sell_amount = i['numberDeal']
#         entryPrice = i['entryPrice']
#         currentPrice = get_currentprice1(platform, symbol)
#         T_copy.append(Thread(target=sell_symbol, args=(
#             userUuid, apiAccountId, strategyId, platform, symbol, currentPrice, sell_amount, entryPrice, strategyname,
#             tablename, updateCopy_url)))
#         for t in T_copy:
#             t.start()
#         for t in T_copy:
#             t.join()


def kama_strategy_copy(tracer_info, order_param):
    direction = order_param['direction']
    if direction == 1:
        kama_buy_copy(tracer_info, order_param)
    else:
        kama_sell_copy(tracer_info, order_param)

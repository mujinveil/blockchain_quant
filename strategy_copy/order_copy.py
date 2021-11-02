# encode='utf-8'
import json
import time
from threading import Thread
import requests
import sys
sys.path.append("..")
from loggerConfig import logger
from strategy_copy.grid_copy import grid_strategy_copy
from strategy_copy.kama_copy import kama_strategy_copy
from tools.Config import Trade_url, Queryorder_url, Cancel_url, updateCopy_url, Fee, Query_tradeprice_url
from tools.databasePool import r2, r4, POOL, r5


# 订单未成交，撤单
def cancel_order(userUuid, apiAccountId, strategyId, platform, symbol, orderId, direction, source):
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        cancelbuyparams = {"direction": direction, "symbol": symbol, "platform": platform, "orderId": orderId,
                           "apiAccountId": apiAccountId, "userUuid": userUuid, "source": source,
                           "strategyId": strategyId}
        cancelres = requests.post(Cancel_url, data=cancelbuyparams)
        res = json.loads(cancelres.content.decode())
        if res["code"] == 1:
            cur.execute("update copylist set status=2 where strategyId=%s and orderid=%s", (strategyId, orderId))
        else:
            i = "用户{}策略{}撤销{}平台订单出错,原因{}".format(userUuid, strategyId, platform, res['message'])
            print(i)
            logger.info(i)
    except Exception as e:
        i = "用户{}策略{}撤销{}平台订单出错{}".format(userUuid, strategyId, platform, e)
        print(i)
        logger.info(i)
    finally:
        conn.commit()
        cur.close()
        conn.close()


# 判断策略发起人是否手动停止或者报错停止
def sponsor_stop(strategytype, strategyId, followstrategyId):
    strategy_name_dict = {1: 'gridstrategy', 4: 'tracestrategy', 10: 'balance_strategy', 13: 'Cover_strategy',
                          15: "Kama_strategy"}
    strategy_name = strategy_name_dict[strategytype]
    flag = 1
    tracebuy = r2.hget('tracestrategybuy', followstrategyId)
    tracesell = r2.hget('tracestrategysell', followstrategyId)
    if strategytype == 4 and (tracebuy or tracesell):
        flag = 0
    elif r2.hget(strategy_name, followstrategyId):
        flag = 0
    else:
        i = "发起人策略ID{}已手动停止或者报错停止,跟单人需暂停跟单".format(followstrategyId)
        print(i)
        logger.info(i)
        r4.delete(followstrategyId)
        # 调用java停止策略接口
        updateparams = {'strategyId': strategyId, "status": 4}
        res = requests.post(updateCopy_url, data=updateparams)
        print(res.content.decode())
    return flag


# 判断跟单人是否满足跟单条件（如发起人是卖单，此时跟单人没有仓位,则不能卖出),补仓买单资格确认在跟投金额上已进行
def sellorder_qualification(strategyId, followstrategyId, direction, strategytype, symbol):
    conn = POOL.connection()
    cur = conn.cursor()
    flag = 1
    try:
        if strategytype == 13:
            buysql = 'select orderid from copylist where strategyId=%s and followstrategyId=%s and direction=1 and ' \
                     'status=1 '
            cur.execute(buysql, (strategyId, followstrategyId))
            if direction == 2 and not cur.fetchone():
                print("{}没有仓位，不能下卖单".format(strategyId))
                flag = 0
        if strategytype == 15:
            buysql = 'select orderid from copylist where strategyId=%s and followstrategyId=%s and direction=1 and ' \
                     'symbol=%s and status=1 '
            cur.execute(buysql, (strategyId, followstrategyId, symbol))
            if direction == 2 and not cur.fetchone():
                print("{}没有仓位,不能下卖单".format(strategyId))
                flag = 0
    except Exception as e:
        print("查询跟单策略{}持仓出错{}".format(strategyId, e))
    finally:
        conn.commit()
        cur.close()
        conn.close()
        return flag


# 计算智能追踪策略利润
def update_trace_profit(platform, direction, baseprice, tradeprice, amount):
    profit = profitRate = 0
    fee = Fee[platform]['buyfee']
    if direction == 1:
        profit = round((baseprice - tradeprice) * amount - (baseprice + tradeprice) * amount * fee, 8)
        profitRate = round(profit / (baseprice * amount), 8)
    elif direction == 2:
        profit = round((tradeprice - baseprice) * amount - (baseprice + tradeprice) * amount * fee, 8)
        profitRate = round(profit / (baseprice * amount), 8)
    return profit, profitRate


# 计算补仓策略利润
def update_cover_profit(strategyId, followstrategyId, sell_amount, platform):
    # 第一步，查出strategyId最晚的一个status为1的卖单的成交时间
    conn = POOL.connection()
    cur = conn.cursor()
    profit = 0
    profitRate = 0
    try:
        sql = 'select trade_time from copylist where direction=2 and status=1 and strategyId=%s and followstrategyId=%s'
        cur.execute(sql, (strategyId, followstrategyId))
        trade_times = cur.fetchall()
        if trade_times:
            last_trade_time = trade_times[-1]
        else:
            last_trade_time = '2021-01-01 00:00:00'
        # 第二步，查出时间段大于该成交时间的买入订单总成本
        buysql = 'select sum(trade_price*trade_amount) from copylist where strategyId=%s and followstrategyId=%s and ' \
                 'direction=1 and status=1 and trade_time>%s '
        cur.execute(buysql, (strategyId, followstrategyId, last_trade_time,))
        total_buy_amount = cur.fetchone()
        if total_buy_amount[0]:
            total_buy_amount = float(total_buy_amount[0])
        else:
            total_buy_amount = 0
        # 第三步，计算本轮利润与利润率
        if total_buy_amount and sell_amount:
            profit = round(
                (sell_amount - total_buy_amount) - (sell_amount + total_buy_amount) * Fee[platform]['buyfee'], 8)
            profitRate = round(profit / sell_amount, 8)
    except Exception as e:
        logger.info('跟单策略{}计算利润失败{}'.format(strategyId, e))
    finally:
        cur.close()
        conn.close()
        return profit, profitRate


# 计算补仓策略新一轮跟投买单数量,以确定卖单数量
def follow_buy_quantity(userUuid, strategyId, followstrategyId):
    conn = POOL.connection()
    cur = conn.cursor()
    total_buy_amount = 0
    try:
        sql = 'select trade_time from copylist where direction=2 and status=1 and strategyId=%s and followstrategyId=%s'
        cur.execute(sql, (strategyId, followstrategyId))
        trade_times = cur.fetchall()
        if trade_times:
            last_trade_time = trade_times[-1]
        else:
            last_trade_time = '2021-01-01 00:00:00'
        # 第二步，查出时间段大于该成交时间的买入订单
        buysql = 'select sum(trade_amount) from copylist where strategyId=%s and followstrategyId=%s and ' \
                 'direction=1 and status=1 and trade_time>%s '
        cur.execute(buysql, (strategyId, followstrategyId, last_trade_time,))
        total_buy = cur.fetchone()
        if total_buy[0]:
            total_buy_amount = float(total_buy[0])
    except Exception as e:
        i = '用户{}跟单策略{}查询买单数量失败{}'.format(userUuid, strategyId, e)
        print(i)
        logger.info(i)
    finally:
        cur.close()
        conn.close()
        return total_buy_amount


# 计算策略总收益与收益率
def sum_profit(userUuid, apiAccountId, strategyId, followstrategyId):
    totalprofit = 0
    totalprofitRate = 0
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        sql = "select sum(profit),sum(profitRate) from copylist where strategyId=%s and followstrategyId=%s and " \
              "direction=2 and status=1 "
        cur.execute(sql, (strategyId, followstrategyId))
        total_profit, total_profitrate = cur.fetchone()
        if total_profit and total_profitrate:
            totalprofit = float(total_profit)
            totalprofitRate = float(total_profitrate)
    except Exception as e:
        logger.error('用户{}子账户{}跟单策略{}在查询利润时出错{}'.format(userUuid, apiAccountId, strategyId, e))
    finally:
        cur.close()
        conn.close()
        return totalprofit, totalprofitRate


# 依据发起人的订单下单
def order(tracer_info, order_param):
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        strategyId = tracer_info['strategyId']
        userUuid = tracer_info['userUuid']
        apiAccountId = tracer_info['apiAccountId']
        platform = tracer_info['platform']
        symbol = tracer_info['symbol']
        strategytype = int(tracer_info['strategyType'])
        followstrategyId = tracer_info['followStrategyId']
        tradetype = 1 if strategytype in [4, 13] else 2
        direction = order_param['direction']
        source = order_param['source']
        amount = order_param['amount']
        # 买卖单价格放一块
        price = order_param['price']
        if not sellorder_qualification(strategyId, followstrategyId, direction, strategytype):
            return
        # 补仓策略需要计算新一轮跟投买入数量
        if strategytype == 13 and direction == 2:
            amount = follow_buy_quantity(userUuid, strategyId, followstrategyId)
        new_order_param = {"direction": direction, "amount": amount, "symbol": symbol, "platform": platform,
                           "price": price, "userUuid": userUuid, "apiAccountId": apiAccountId, "source": source,
                           "strategyId": strategyId, 'tradetype': tradetype}
        traderes = requests.post(Trade_url, data=new_order_param)
        trade_dict = json.loads(traderes.content.decode())
        orderId = trade_dict["response"]["orderid"]  # 获取订单id
        order_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        insertsql = "INSERT INTO copylist(userUuid,apiAccountId,strategyId,followstrategyId,platform,symbol," \
                    "direction,orderid,order_amount,order_price,order_time,status,uniqueId," \
                    "tradetype,strategytype) VALUES(%s, %s, %s, %s,%s, " \
                    "%s,%s,%s, %s, %s, %s, %s,%s,%s,%s)"
        cur.execute(insertsql, (
            userUuid, apiAccountId, strategyId, followstrategyId, platform, symbol, direction, orderId, amount, price,
            order_time, 0, source, tradetype, strategytype))
        conn.commit()
        i = '用户{}子账户{}跟单策略{}跟踪下单成功,并将订单插入数据库'.format(userUuid, apiAccountId, strategyId)
        print(i)
        logger.info(i)
        time.sleep(3)
        # 3秒后检查订单成交状况
        query_params = {"direction": direction, "symbol": symbol, "platform": platform,
                        "orderId": orderId, "apiAccountId": apiAccountId, "userUuid": userUuid, "source": source,
                        "strategyId": strategyId}
        res = requests.post(Queryorder_url, data=query_params)
        querydict = json.loads(res.content.decode())
        logger.info("用户{}跟单策略{}平台{}查询订单{}".format(userUuid, strategyId, platform, querydict))
        resp = querydict['response']
        if resp and resp['status'] == 'closed':
            numberDeal = float(resp["numberDeal"])
            queryparams = {"platform": platform, "symbol": symbol, "orderId": orderId, "apiId": apiAccountId,
                           "userUuid": userUuid}
            res = requests.post(Query_tradeprice_url, data=queryparams)
            queryresdict = json.loads(res.content.decode())
            fee = Fee[platform]['buyfee']
            try:
                tradeprice = queryresdict["response"]["avgPrice"]
                tradetime = queryresdict['response']['createdDate']
                tradeamount = float(tradeprice * numberDeal)
                copyfee = round(tradeamount * fee, 8)
            except:
                tradeprice = price
                tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                tradeamount = float(tradeprice * numberDeal)
                copyfee = round(tradeamount * fee, 8)
            # 依据不同的跟单策略类型计算利润
            if direction == 2 and strategytype == 13:
                profit, profitRate = update_cover_profit(strategyId, followstrategyId, tradeamount, platform)
            elif strategytype == 4:
                baseprice = order_param['baseprice']
                profit, profitRate = update_trace_profit(platform, direction, baseprice, tradeprice, numberDeal)
            else:
                profit = profitRate = None
            # 更新数据库
            updatesql = "update copylist set trade_amount=%s,trade_price=%s,trade_time=%s,profit=%s,profitRate=%s," \
                        "status=1,fee=%s where strategyId=%s and orderid=%s "
            cur.execute(updatesql, (amount, tradeprice, tradetime, profit, profitRate, copyfee, strategyId, orderId))
            conn.commit()
            i = '用户{}子账户{}跟单策略{}跟踪下单已全部成交,并将成交记录插入数据库'.format(userUuid, apiAccountId, strategyId)
            print(i)
            logger.info(i)
        elif resp and resp['status'] == 'open':
            print("=================订单未成交，撤单=============")
            cancel_order(userUuid, apiAccountId, strategyId, platform, symbol, orderId, direction, source)
    except Exception as e:
        i = '用户{}子账户{}跟单策略{}跟单失败{}'.format(userUuid, apiAccountId, strategyId, e)
        print(i)
        logger.error(i)
        # updateparams = {'strategyId': strategyId, "status": 4}
        # requests.post(updateCopy_url, data=updateparams)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    while True:
        strategyIdList = r4.keys()
        for followstrategyId in strategyIdList:
            tracer_list = r4.hvals(followstrategyId)
            tracer_list = [json.loads(i) for i in tracer_list]
            strategytype = tracer_list[0]['strategyType']
            order_param = r5.hget('order_param_{}'.format(strategytype), followstrategyId)
            if not tracer_list:
                continue
            T = []
            for tracer_info in tracer_list:
                strategyId = tracer_info['strategyId']
                followstrategyId = tracer_info['followStrategyId']
                if sponsor_stop(strategytype, strategyId, followstrategyId) or (not order_param):
                    continue
                if isinstance(order_param, str):
                    order_param = json.loads(order_param)
                if strategytype == 1:  # 网格策略
                    T.append(Thread(target=grid_strategy_copy, args=(tracer_info, order_param,)))
                # elif strategytype == 10:  # 智能追踪策略
                #     T.append(Thread(target=balance_copy, args=(tracer_info, order_param,)))
                elif strategytype == 15:
                    T.append(Thread(target=kama_strategy_copy, args=(tracer_info, order_param,)))
                else:
                    T.append(Thread(target=order, args=(tracer_info, order_param,)))
            for t in T:
                t.start()
            for t in T:
                t.join()
            r5.hdel('order_param_{}'.format(strategytype), followstrategyId)
        time.sleep(1)

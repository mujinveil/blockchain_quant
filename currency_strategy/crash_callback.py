# encoding='utf-8'
import json
import sys
import time
from threading import Thread
import numpy as np
import requests
sys.path.append("..")
from loggerConfig import logger
from tools.Config import pricelimit, Trade_url, Queryorder_url, Fee, Query_tradeprice_url, updateCrash_url, amountlimit, \
    Cancel_url
from tools.Kline_analyze import MA, get_all_symbol_klinedata, get_klinedata, chandelier_stop
from tools.databasePool import r2, POOL
from tools.tool import get_total_profit
from tools.get_market_info import get_currentprice1


# 取消订单
def cancel_order(userUuid, apiAccountId, strategyId, platform, symbol, orderId, direction):
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        cancelbuyparams = {"direction": direction, "symbol": symbol, "platform": platform, "orderId": orderId,
                           "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11, "strategyId": strategyId}
        cancelres = requests.post(Cancel_url, data=cancelbuyparams)
        res = json.loads(cancelres.content.decode())
        if res["code"] == 1:
            cur.execute("update crashlist set status=2 where strategyId=%s and orderid=%s",
                        (strategyId, orderId))
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


# 卖出交易对
def sell_symbol(userUuid, apiAccountId, strategyId, platform, symbol, amount, entryPrice):
    conn = POOL.connection()
    cur = conn.cursor()
    currentprice = get_currentprice1(platform, symbol)
    sell_flag = 0
    try:
        # 下卖单
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # 折价卖出
        sellprice = round(currentprice * 0.99, pricelimit[symbol][platform])
        try:
            x, y = str(amount).split('.')
            amount = float(x + '.' + y[0:amountlimit[symbol][platform]])
        except:
            pass
        sellparams = {"direction": 2, "amount": amount, "symbol": symbol, "platform": platform,
                      "price": sellprice, "userUuid": userUuid, "apiAccountId": apiAccountId, "source": 11,
                      "strategyId": strategyId, 'tradetype': 1}
        i = '用户{}子账户{}暴跌反弹策略{}交易对{}开始卖出,挂单价{},挂单数量{}'.format(userUuid, apiAccountId, strategyId, symbol, sellprice,
                                                             amount)
        print(i)
        logger.info(i)
        res = requests.post(Trade_url, data=sellparams)
        resdict = json.loads(res.content.decode())
        print(resdict)
        if resdict['message'] in ['资产不足', '连接交易所失败，请稍后重试']:
            Crash_label = {'symbol': symbol, 'entryPrice': entryPrice, 'numberDeal': int(amount * 0.998)}
            r2.hset('Crash_label:{}'.format(strategyId), symbol, json.dumps(Crash_label))
            return sell_flag
        if sellprice * amount < 5:
            r2.hdel('Crash_label:{}'.format(strategyId), symbol)
            return sell_flag
        orderId = resdict["response"]["orderid"]  # 获取订单id
        sellinsertsql = "INSERT INTO crashlist(strategyId,userUuid,apiAccountId,platform,symbol,direction," \
                        "orderid,order_amount,order_price,order_time," \
                        "status,uniqueId,tradetype) VALUES(%s, %s, %s, %s, %s, %s,%s,%s,%s, %s, %s, " \
                        "%s,%s)"
        cur.execute(sellinsertsql, (
            strategyId, userUuid, apiAccountId, platform, symbol, 2, orderId, amount, sellprice, ordertime, 0, 11, 1,))
        conn.commit()
        i = "用户{}子账户{}暴跌反弹策略{}交易对{}下卖单成功".format(userUuid, apiAccountId, strategyId, symbol)
        print(i)
        logger.info(i)
        time.sleep(3)
        # 3秒后查询卖单
        queryparams = {"direction": 2, "symbol": symbol, "platform": platform, "orderId": orderId,
                       "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11, "strategyId": strategyId}
        queryres = requests.post(Queryorder_url, data=queryparams)
        querydict = json.loads(queryres.content.decode())
        status = querydict['response']['status']
        if status == 'closed':
            fee = Fee[platform]['sellfee']
            queryparams = {"platform": platform, "symbol": symbol, "orderId": orderId, "apiId": apiAccountId,
                           "userUuid": userUuid}
            res = requests.post(Query_tradeprice_url, data=queryparams)
            queryresdict = json.loads(res.content.decode())
            try:
                tradeprice = queryresdict['response']['avgPrice']
                tradetime = queryresdict['response']['createdDate']
                numberDeal = queryresdict['response']['totalAmount']
                sellfee = round(numberDeal * tradeprice * fee, 8)
            except:
                tradeprice = sellprice
                tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                numberDeal = amount
                sellfee = round(numberDeal * tradeprice * fee, 8)
            profit = round((tradeprice - entryPrice - (tradeprice + entryPrice) * fee) * numberDeal, 8)
            profitRate = round(profit / (entryPrice * numberDeal), 8)
            updatesql = "update crashlist set trade_amount=%s,trade_price=%s,trade_time=%s,profit=%s," \
                        "profitRate=%s,status=%s,fee=%s where " \
                        "strategyId=%s and orderid=%s"
            cur.execute(updatesql,
                        (numberDeal, tradeprice, tradetime, profit, profitRate, 1, sellfee, strategyId, orderId))
            conn.commit()
            i = "用户{}子账户{}暴跌反弹策略{}交易对{}卖单全部成交".format(userUuid, apiAccountId, strategyId, symbol)
            print(i)
            logger.info(i)
            r2.hdel('Crash_label:{}'.format(strategyId), symbol)
            sell_flag = 1
        elif status == "open":
            cancel_order(userUuid, apiAccountId, strategyId, platform, symbol, orderId, 2)
    except Exception as e:
        i = "用户{}子账户{}暴跌反弹策略{}交易对{}下卖单时出错{}".format(userUuid, apiAccountId, strategyId, symbol, e)
        print(i)
        logger.error(i)
        # 调用java停止策略接口
        updateparams = {'strategyId': strategyId, "status": 4}
        res1 = requests.post(updateCrash_url, data=updateparams)
        print(res1)
        sell_flag = 0
    finally:
        cur.close()
        conn.close()
        return sell_flag


# 判断是否金叉且之前有暴跌
def gold_cross(klinedata):
    flag = 0
    if not klinedata:
        return flag
    MA_data = MA(klinedata, 5, 30)
    ma_sign_array = MA_data['MA_sign'].values.tolist()
    low_array = MA_data['low'].values.tolist()
    volumn_array = MA_data['volumn'].values.tolist()
    try:
        if ma_sign_array[-1] == 1 and volumn_array:
            # 5小时均线下穿30小时均线时间点
            dead_cross_position = ma_sign_array[::-1].index(2)
            # 截取从死叉到金叉的行情数据
            low_array = low_array[-(dead_cross_position + 1):]
            volumn_array = volumn_array[-(dead_cross_position + 1):]
            # 算出最低价的位置
            low_min_position = low_array.index(min(low_array))
            if volumn_array[:low_min_position] and (low_array[0] - low_array[low_min_position]) / low_array[0] > 0.05:
                # 最低价的成交量是之前的3倍
                volumn_mean = np.mean(volumn_array[:low_min_position])
                if volumn_array[low_min_position] >= volumn_mean * 3:
                    flag = 1
    except Exception as e:
        print(e)
    finally:
        return flag


# 获取满足条件的交易对
def get_candidate_symbols(platform, stopRatio, granularity):
    symbol_pool = []
    localtime = time.localtime(time.time())
    if localtime.tm_min % 30 == 0:
        symbol_klinedata = get_all_symbol_klinedata(platform, granularity)
        for i in symbol_klinedata:
            symbol, klinedata = i
            if klinedata:
                print(symbol)
            flag = gold_cross(klinedata)
            if flag:
                try:
                    stopprice, currentprice = chandelier_stop(klinedata, stopRatio * 100)
                    if currentprice > stopprice:
                        symbol_pool.append(symbol)
                    print("备选交易对{}".format(symbol))
                except Exception as e:
                    print(e)
    return symbol_pool


# 在持仓数小于阈值时买入币种池中的交易对
def startBuy(strategydata, symbol):
    userUuid = strategydata['userUuid']
    strategyId = strategydata['strategyId']
    apiAccountId = strategydata['apiAccountId']
    amount = strategydata['amount']
    platform = strategydata['platform']
    currentprice = get_currentprice1(platform, symbol)
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        buyprice = round(currentprice * 1.01, pricelimit[symbol][platform])
        buy_amount = amount / buyprice
        try:
            x, y = str(buy_amount).split('.')
            buy_amount = float(x + '.' + y[0:amountlimit[symbol][platform]])
        except:
            pass
        buyparams = {"direction": 1, "amount": buy_amount, "symbol": symbol, "platform": platform,
                     "price": buyprice, "userUuid": userUuid, "apiAccountId": apiAccountId, "source": 11,
                     "strategyId": strategyId, 'tradetype': 1}
        i = '用户{}子账户{}暴跌反弹策略{}开始买入交易对{},挂单价{}'.format(userUuid, apiAccountId, strategyId, symbol, buyprice)
        print(i)
        logger.info(i)
        res = requests.post(Trade_url, data=buyparams)
        resdict = json.loads(res.content.decode())
        orderId = resdict["response"]["orderid"]  # 获取订单id
        buyinsertsql = "INSERT INTO crashlist(strategyId,userUuid,apiAccountId,platform,symbol,direction," \
                       "orderid,order_amount,order_price,order_time," \
                       "status,uniqueId,tradetype) VALUES(%s, %s, %s, %s, %s, %s,%s,%s,%s, %s, %s, " \
                       "%s,%s) "
        cur.execute(buyinsertsql, (
            strategyId, userUuid, apiAccountId, platform, symbol, 1, orderId, buy_amount, buyprice, ordertime, 0, 11,
            1,))
        conn.commit()
        time.sleep(3)
        i = '用户{}子账户{}暴跌反弹策略{}买入{}下单成功'.format(userUuid, apiAccountId, strategyId, symbol)
        print(i)
        logger.info(i)
        # 3秒后查询买单
        queryparams = {"direction": 1, "symbol": symbol, "platform": platform, "orderId": orderId,
                       "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11, "strategyId": strategyId}
        queryres = requests.post(Queryorder_url, data=queryparams)
        querydict = json.loads(queryres.content.decode())
        status = querydict['response']['status']
        if status == 'closed':
            fee = Fee[platform]['buyfee']
            queryparams = {"platform": platform, "symbol": symbol, "orderId": orderId, "apiId": apiAccountId,
                           "userUuid": userUuid}
            res = requests.post(Query_tradeprice_url, data=queryparams)
            queryresdict = json.loads(res.content.decode())
            try:
                tradeprice = queryresdict['response']['avgPrice']
                tradetime = queryresdict['response']['createdDate']
                totalamount = float(queryresdict['response']['totalAmount'])
                totalfees = float(queryresdict['response']['totalFees'])
                numberDeal = totalamount - totalfees
                buyfee = round(numberDeal * tradeprice * fee, 8)
            except:
                tradeprice = buyprice
                tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                numberDeal = round(buy_amount * 0.998, 8)
                buyfee = round(numberDeal * tradeprice * fee, 8)
            updatesql = "update crashlist set trade_amount=%s,trade_price=%s,trade_time=%s," \
                        "status=%s,fee=%s where " \
                        "strategyId=%s and orderid=%s"
            cur.execute(updatesql, (numberDeal, tradeprice, tradetime, 1, buyfee, strategyId, orderId))
            conn.commit()
            i = "用户{}子账户{}暴跌反弹策略{}买入{}全部成交".format(userUuid, apiAccountId, strategyId, symbol)
            print(i)
            logger.info(i)
            Crash_label = {'symbol': symbol, 'entryPrice': tradeprice, 'numberDeal': numberDeal}
            r2.hset('Crash_label:{}'.format(strategyId), symbol, json.dumps(Crash_label))
        elif status == "open":
            cancel_order(userUuid, apiAccountId, strategyId, platform, symbol, orderId, 2)
    except Exception as e:
        i = "用户{}子账户{}暴跌反弹策略{}买入{}时未成交，原因{}".format(userUuid, apiAccountId, strategyId, symbol, e)
        print(i)
        logger.error(i)
        # 调用java停止策略接口
        updateparams = {'strategyId': strategyId, "status": 4}
        res1 = requests.post(updateCrash_url, data=updateparams)
        print(res1)
    finally:
        cur.close()
        conn.close()


# 对持仓的交易对进行吊灯止损
def traceSell(strategydata, symbol):
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    strategyId = strategydata['strategyId']
    amount = strategydata['amount']
    maxPositionNum = strategy_info['maxPositionNum']
    init_amount = amount * maxPositionNum
    platform = strategydata["platform"]
    stopRatio = strategydata['stopRatio']
    Crash_label = json.loads(r2.hget('Crash_label:{}'.format(strategyId), symbol))
    sell_amount = Crash_label['numberDeal']
    entryPrice = Crash_label['entryPrice']
    symbol, kline_data = get_klinedata(platform, symbol, 3600)
    if not kline_data:
        return
    stopPrice, currentPrice = chandelier_stop(kline_data, stopRatio * 100)
    print("交易对{}当前吊灯止损价格为:{},行情价{}".format(symbol, stopPrice, currentPrice))
    # 当价格低于吊灯止损价格
    if currentPrice < stopPrice:
        print('{}当前行情价{}触碰了吊灯止损价，开始卖出,卖出量{}'.format(symbol, currentPrice, sell_amount))
        sell_flag = sell_symbol(userUuid, apiAccountId, strategyId, platform, symbol, sell_amount, entryPrice)
        if sell_flag:
            totalprofit, totalprofitRate = get_total_profit("crashlist", strategyId, init_amount)
            i = "用户{}子账户{}暴跌反弹策略{}开始计算利润{}".format(userUuid, apiAccountId, strategyId, totalprofit)
            print(i)
            logger.info(i)
            params = {'strategyId': strategyId, 'profit': totalprofit, 'profitRate': totalprofitRate}
            res = requests.post(updateCrash_url, data=params)
            resdict = json.loads(res.content.decode())
            print(resdict)


if __name__ == "__main__":
    while True:
        try:
            strategy_list = r2.hvals("Crash_strategy")
            strategy_list = [json.loads(i) for i in strategy_list]
            T = []
            for strategy_info in strategy_list:
                strategyId = strategy_info['strategyId']
                platform = strategy_info['platform']
                maxPositionNum = strategy_info['maxPositionNum']
                stopRatio = float(strategy_info['stopRatio'])
                symbol_pool = get_candidate_symbols(platform, stopRatio, 3600)
                hold_pool = r2.hkeys('Crash_label:{}'.format(strategyId))
                for symbol in hold_pool:
                    T.append(Thread(target=traceSell, args=(strategy_info, symbol)))
                # 限制最大持仓交易对的数量，如最多买入5个
                if len(hold_pool) >= maxPositionNum:
                    continue
                # 依据最大持仓数限制买入交易对的数量
                to_buy = [s for s in symbol_pool if s not in hold_pool]
                if to_buy and len(hold_pool) + len(to_buy) > maxPositionNum:
                    to_buy = to_buy[:(maxPositionNum - len(hold_pool))]
                for symbol in to_buy:
                    T.append(Thread(target=startBuy, args=(strategy_info, symbol)))
            for t in T:
                t.start()
            for t in T:
                t.join()
        except Exception as e:
            print(e)
        finally:
            time.sleep(2)

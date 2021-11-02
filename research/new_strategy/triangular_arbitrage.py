# encoding='utd-8'
import json
import threading
import time
import requests
from threading import Thread
from loggerConfig import logger
from tools.Config import pricelimit, amountlimit, updateArbitrage_url, Trade_url, Queryorder_url, Fee, \
    Query_tradeprice_url
from tools.databasePool import r2, POOL
from tools.get_market_info import get_currentprice1, get_currentprice0
from tools.tool import buy_symbol, cancel_order


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


def multiple_symbol_price(platform):
    t1 = MyThread(get_currentprice0, args=(platform, 'eth_usdt'))
    t2 = MyThread(get_currentprice0, args=(platform, 'eos_eth'))
    t3 = MyThread(get_currentprice0, args=(platform, 'eos_usdt'))
    t1.start()
    t2.start()
    t3.start()
    t1.join(timeout=20)
    t2.join(timeout=20)
    t3.join(timeout=20)
    p1 = t1.get_result()
    p2 = t2.get_result()
    p3 = t3.get_result()
    return p1, p2, p3


def arbitrage(strategydata, p1, p2, p3, ):
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    strategyId = strategydata['strategyId']
    platform = strategydata['platform']
    fee = strategydata['fee']
    init_amount = strategydata['amount']
    strategyname = "三角套利"
    tablename = 'arbitragelist'
    # 正向套利  usdt-->eth-->eos-->usdt
    print("正向套利空间", p3 * ((1 - fee) ** 3) - p1 * p2)
    if p3 * ((1 - fee) ** 3) - p1 * p2 > 0.03:  # 满足正向三角套利条件
        symbol = 'eth_usdt'
        if r2.hget('arbitrage_label:{}'.format(strategyId), symbol):
            return
        buy_symbol(userUuid, apiAccountId, strategyId, platform, symbol, p1, init_amount, strategyname, tablename,
                   updateArbitrage_url)
        # 根据缓存里有无成交量判断买单是否成交
        deal_info = r2.hget('{}_label:{}'.format(tablename.replace('list', ''), strategyId), symbol)
        if deal_info:
            deal_info = json.loads(deal_info)
            numberDeal = deal_info['numberDeal']
            investment = numberDeal * deal_info['entryPrice']
            symbol = 'eos_eth'
            if r2.hget('arbitrage_label:{}'.format(strategyId), symbol):
                return
            current_price = get_currentprice1(platform, symbol)
            print("eos_eth当前价格{}".format(current_price))
            buy_symbol(userUuid, apiAccountId, strategyId, platform, symbol, current_price, numberDeal, strategyname,
                       tablename, updateArbitrage_url)
            deal_info = r2.hget('{}_label:{}'.format(tablename.replace('list', ''), strategyId), symbol)
            if deal_info:
                deal_info = json.loads(deal_info)
                numberDeal = deal_info['numberDeal']
                symbol = 'eos_usdt'
                currentprice = get_currentprice1(platform, symbol)
                conn = POOL.connection()
                cur = conn.cursor()
                try:
                    # 折价下卖单
                    sellprice = round(currentprice * 0.99, pricelimit[symbol][platform])
                    try:
                        x, y = str(numberDeal).split('.')
                        amount = float(x + '.' + y[0:amountlimit[symbol][platform]])
                    except:
                        pass
                    ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    sellparams = {"direction": 2, "amount": amount, "symbol": symbol, "platform": platform,
                                  "price": sellprice, "userUuid": userUuid, "apiAccountId": apiAccountId, "source": 11,
                                  "strategyId": strategyId, 'tradetype': 1}
                    i = '用户{}子账户{}{}策略{}交易对{}开始卖出,挂单价{}'.format(userUuid, apiAccountId, strategyname, strategyId,
                                                                symbol, sellprice)
                    print(i)
                    logger.info(i)
                    res = requests.post(Trade_url, data=sellparams)
                    resdict = json.loads(res.content.decode())
                    orderId = resdict["response"]["orderid"]  # 获取订单id
                    sellinsertsql = "INSERT INTO {}(strategyId,userUuid,apiAccountId,platform,symbol,direction," \
                                    "orderid,order_amount,order_price,order_time," \
                                    "status,uniqueId,tradetype) VALUES(%s, %s, %s, %s, %s, %s,%s,%s,%s, %s, %s, " \
                                    "%s,%s)".format(tablename)
                    cur.execute(sellinsertsql, (
                        strategyId, userUuid, apiAccountId, platform, symbol, 2, orderId, amount, sellprice, ordertime,
                        0, 11, 1,))
                    conn.commit()
                    i = "用户{}子账户{}{}策略{}交易对{}下卖单成功".format(userUuid, apiAccountId, strategyname, strategyId, symbol)
                    print(i)
                    logger.info(i)
                    time.sleep(1)
                    # 2秒后查询卖单
                    queryparams = {"direction": 2, "symbol": symbol, "platform": platform, "orderId": orderId,
                                   "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11,
                                   "strategyId": strategyId}
                    queryres = requests.post(Queryorder_url, data=queryparams)
                    querydict = json.loads(queryres.content.decode())
                    status = querydict['response']['status']
                    if status == 'closed':
                        fee = Fee[platform]['sellfee']
                        queryparams = {"platform": platform, "symbol": symbol, "orderId": orderId,
                                       "apiId": apiAccountId,"userUuid": userUuid}
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
                        profit = round(tradeprice * numberDeal * (1 - fee) - investment, 8)
                        profitRate = round(profit / init_amount, 8)
                        updatesql = "update {} set trade_amount=%s,trade_price=%s,trade_time=%s,profit=%s," \
                                    "profitRate=%s,status=%s,fee=%s where " \
                                    "strategyId=%s and orderid=%s".format(tablename)
                        cur.execute(updatesql, (
                            numberDeal, tradeprice, tradetime, profit, profitRate, 1, sellfee, strategyId, orderId))
                        conn.commit()
                        i = "用户{}子账户{}{}策略{}交易对{}卖单全部成交,此轮三角套利获得利润{}".format(userUuid, apiAccountId, strategyname,
                                                                             strategyId,
                                                                             symbol, profit)
                        print(i)
                        logger.info(i)
                        r2.delete('{}_label:{}'.format(tablename.replace('list', ''), strategyId))
                    elif status == "open":
                        cancel_order(userUuid, apiAccountId, strategyId, platform, symbol, orderId, 2, tablename)
                except Exception as e:
                    i = "用户{}子账户{}{}策略{}交易对{}下卖单时出错{}".format(userUuid, apiAccountId, strategyname, strategyId, symbol,
                                                              e)
                    print(i)
                    logger.error(i)
                    # 调用java停止策略接口
                    updateparams = {'strategyId': strategyId, "status": 4}
                    res1 = requests.post(updateArbitrage_url, data=updateparams)
                    print(res1)
                finally:
                    cur.close()
                    conn.close()
    # 反向套利  usdt-->eos-->eth-->usdt
    print("反向套利空间", p2 * p1 * ((1 - fee) ** 3) - p3)
    if p2 * p1 * ((1 - fee) ** 3) - p3 > 0.03:  # 满足反向三角套利条件
        symbol = 'eos_usdt'
        buy_symbol(userUuid, apiAccountId, strategyId, platform, symbol, p3, init_amount, strategyname, tablename,
                   updateArbitrage_url)
        # 根据缓存里有无成交量判断买单是否成交
        deal_info = r2.hget('{}_label:{}'.format(tablename.replace('list', ''), strategyId), symbol)
        if deal_info:
            deal_info = json.loads(deal_info)
            numberDeal = deal_info['numberDeal']
            symbol = 'eos_eth'
            currentprice = get_currentprice1(platform, symbol)
            conn = POOL.connection()
            cur = conn.cursor()
            try:
                # 折价下卖单
                sellprice = round(currentprice * 0.99, pricelimit[symbol][platform])
                try:
                    x, y = str(numberDeal).split('.')
                    amount = float(x + '.' + y[0:amountlimit[symbol][platform]])
                except:
                    pass
                ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                sellparams = {"direction": 2, "amount": amount, "symbol": symbol, "platform": platform,
                              "price": sellprice, "userUuid": userUuid, "apiAccountId": apiAccountId, "source": 11,
                              "strategyId": strategyId, 'tradetype': 1}
                i = '用户{}子账户{}{}策略{}交易对{}开始卖出,挂单价{}'.format(userUuid, apiAccountId, strategyname, strategyId,
                                                            symbol, sellprice)
                print(i)
                logger.info(i)
                res = requests.post(Trade_url, data=sellparams)
                resdict = json.loads(res.content.decode())
                print(resdict)
                orderId = resdict["response"]["orderid"]  # 获取订单id
                sellinsertsql = "INSERT INTO {}(strategyId,userUuid,apiAccountId,platform,symbol,direction," \
                                "orderid,order_amount,order_price,order_time," \
                                "status,uniqueId,tradetype) VALUES(%s, %s, %s, %s, %s, %s,%s,%s,%s, %s, %s, " \
                                "%s,%s)".format(tablename)
                cur.execute(sellinsertsql, (
                strategyId, userUuid, apiAccountId, platform, symbol, 2, orderId, amount, sellprice, ordertime,0, 11, 1,))
                conn.commit()
                i = "用户{}子账户{}{}策略{}交易对{}下卖单成功".format(userUuid, apiAccountId, strategyname, strategyId, symbol)
                print(i)
                logger.info(i)
                time.sleep(1)
                # 2秒后查询卖单
                queryparams = {"direction": 2, "symbol": symbol, "platform": platform, "orderId": orderId,
                               "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11,
                               "strategyId": strategyId}
                queryres = requests.post(Queryorder_url, data=queryparams)
                querydict = json.loads(queryres.content.decode())
                status = querydict['response']['status']
                if status == 'closed':
                    fee = Fee[platform]['sellfee']
                    queryparams = {"platform": platform, "symbol": symbol, "orderId": orderId,
                                   "apiId": apiAccountId, "userUuid": userUuid}
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
                    updatesql = "update {} set trade_amount=%s,trade_price=%s,trade_time=%s," \
                                "status=%s,fee=%s where " \
                                "strategyId=%s and orderid=%s".format(tablename)
                    cur.execute(updatesql, (numberDeal, tradeprice, tradetime, 1, sellfee, strategyId, orderId))
                    conn.commit()
                    i = "用户{}子账户{}{}策略{}交易对{}卖单全部成交".format(userUuid, apiAccountId, strategyname, strategyId,
                                                            symbol)
                    print(i)
                    logger.info(i)
                    label = {'symbol': symbol, 'entryPrice': tradeprice, 'numberDeal': numberDeal*tradeprice}
                    r2.hset('{}_label:{}'.format(tablename.replace('list', ''), strategyId), symbol, json.dumps(label))
                elif status == "open":
                    cancel_order(userUuid, apiAccountId, strategyId, platform, symbol, orderId, 2, tablename)
            except Exception as e:
                i = "用户{}子账户{}{}策略{}交易对{}下卖单时出错{}".format(userUuid, apiAccountId, strategyname, strategyId, symbol, e)
                print(i)
                logger.error(i)
                # 调用java停止策略接口
                updateparams = {'strategyId': strategyId, "status": 4}
                res1 = requests.post(updateArbitrage_url, data=updateparams)
                print(res1)
            finally:
                cur.close()
                conn.close()
        deal_info = r2.hget('{}_label:{}'.format(tablename.replace('list', ''), strategyId), symbol)
        if deal_info:
            deal_info = json.loads(deal_info)
            numberDeal = deal_info['numberDeal']
            symbol = 'eth_usdt'
            currentprice = get_currentprice1(platform, symbol)
            conn = POOL.connection()
            cur = conn.cursor()
            try:
                # 折价下卖单
                sellprice = round(currentprice * 0.99, pricelimit[symbol][platform])
                try:
                    x, y = str(numberDeal).split('.')
                    amount = float(x + '.' + y[0:amountlimit[symbol][platform]])
                except:
                    pass
                ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                sellparams = {"direction": 2, "amount": amount, "symbol": symbol, "platform": platform,
                              "price": sellprice, "userUuid": userUuid, "apiAccountId": apiAccountId, "source": 11,
                              "strategyId": strategyId, 'tradetype': 1}
                i = '用户{}子账户{}{}策略{}交易对{}开始卖出,挂单价{}'.format(userUuid, apiAccountId, strategyname, strategyId,
                                                            symbol, sellprice)
                print(i)
                logger.info(i)
                res = requests.post(Trade_url, data=sellparams)
                resdict = json.loads(res.content.decode())
                print(resdict)
                orderId = resdict["response"]["orderid"]  # 获取订单id
                sellinsertsql = "INSERT INTO {}(strategyId,userUuid,apiAccountId,platform,symbol,direction," \
                                "orderid,order_amount,order_price,order_time," \
                                "status,uniqueId,tradetype) VALUES(%s, %s, %s, %s, %s, %s,%s,%s,%s, %s, %s, " \
                                "%s,%s)".format(tablename)
                cur.execute(sellinsertsql, (
                    strategyId, userUuid, apiAccountId, platform, symbol, 2, orderId, amount, sellprice, ordertime,
                    0, 11, 1,))
                conn.commit()
                i = "用户{}子账户{}{}策略{}交易对{}下卖单成功".format(userUuid, apiAccountId, strategyname, strategyId, symbol)
                print(i)
                logger.info(i)
                time.sleep(1)
                # 2秒后查询卖单
                queryparams = {"direction": 2, "symbol": symbol, "platform": platform, "orderId": orderId,
                               "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11,
                               "strategyId": strategyId}
                queryres = requests.post(Queryorder_url, data=queryparams)
                querydict = json.loads(queryres.content.decode())
                status = querydict['response']['status']
                if status == 'closed':
                    fee = Fee[platform]['sellfee']
                    queryparams = {"platform": platform, "symbol": symbol, "orderId": orderId,
                                   "apiId": apiAccountId,"userUuid": userUuid}
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
                    profit = round(tradeprice * numberDeal * (1 - fee) - init_amount, 8)
                    profitRate = round(profit / init_amount, 8)
                    updatesql = "update {} set trade_amount=%s,trade_price=%s,trade_time=%s,profit=%s," \
                                "profitRate=%s,status=%s,fee=%s where " \
                                "strategyId=%s and orderid=%s".format(tablename)
                    cur.execute(updatesql,
                                (numberDeal, tradeprice, tradetime, profit, profitRate, 1, sellfee, strategyId, orderId))
                    conn.commit()
                    i = "用户{}子账户{}{}策略{}交易对{}卖单全部成交,此轮三角套利获得利润{}".format(userUuid, apiAccountId, strategyname, strategyId,
                                                            symbol,profit)
                    print(i)
                    logger.info(i)
                    r2.delete('{}_label:{}'.format(tablename.replace('list', ''), strategyId))
                elif status == "open":
                    cancel_order(userUuid, apiAccountId, strategyId, platform, symbol, orderId, 2, tablename)
            except Exception as e:
                i = "用户{}子账户{}{}策略{}交易对{}下卖单时出错{}".format(userUuid, apiAccountId, strategyname, strategyId, symbol,
                                                          e)
                print(i)
                logger.error(i)
                # 调用java停止策略接口
                updateparams = {'strategyId': strategyId, "status": 4}
                res1 = requests.post(updateArbitrage_url, data=updateparams)
                print(res1)
            finally:
                cur.close()
                conn.close()


if __name__ == "__main__":
    while True:
        try:
            strategy_list = r2.hvals("Arbitrage_strategy")
            strategy_list = [json.loads(i) for i in strategy_list]
            T = []
            for strategy_info in strategy_list:
                platform = strategy_info['platform']
                p1, p2, p3 = multiple_symbol_price(platform)
                if p1 and p2 and p3:
                    T.append(Thread(target=arbitrage, args=(strategy_info, p1, p2, p3,)))
            for t in T:
                t.start()
            for t in T:
                t.join()
        except Exception as e:
            print(e)
        finally:
            time.sleep(1)

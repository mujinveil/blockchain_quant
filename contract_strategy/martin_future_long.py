# encoding='utf-8'
import json
import sys
import time
from threading import Thread
import requests
sys.path.append("..")
from loggerConfig import logger
from tools.Config import futurepricelimit, future_strategy_status_update, future_takerFee, contract_size_dict
from tools.databasePool import r0, POOL
from tools.future_trade import contract_usdt_trade, query_contract_usdt_order, cancel_contract_usdt_order
from tools.get_future_market_info import get_perpetualprice


def sumProfit(userUuid, strategyId, init_amount):
    totalprofit = 0
    totalprofitRate = 0
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        cur.execute(
            'SELECT SUM(profit) FROM martin_futurelist WHERE strategyId=%s AND direction=2 AND STATUS=1 AND '
            'OFFSET="close"', (strategyId,))
        total_profit = cur.fetchone()[0]
        if total_profit:
            totalprofit = float(total_profit)
            totalprofitRate = round(totalprofit / init_amount, 8)
    except Exception as e:
        logger.error('用户{}马丁追踪合约策略{}在查询利润时出错{}'.format(userUuid, strategyId, e))
    finally:
        cur.close()
        conn.close()
        return totalprofit, totalprofitRate


def traceLevel(strategydata):
    entry_price = float(strategydata['init_entry_price'])
    coverRatio = strategydata['coverRatio'].split("-")
    strategyId = strategydata['strategyId']
    for i in range(len(coverRatio)):
        coverprice = entry_price * (1 - float(coverRatio[i]))
        label = {'covertraceprice': coverprice, 'stopprice': coverprice, 'coverprice': coverprice, 'touchtag': 0}
        r0.hset('coverlevel:buy', '{0}-{1}'.format(strategyId, i), json.dumps(label))


def first_open(strategydata):
    # 只在初次进场时买入，或在止盈后时再次买入
    if strategydata['flag'] == 1:
        return
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    strategyId = strategydata['strategyId']
    platform = strategydata['platform']
    symbol = strategydata['symbol']
    first_sheets = strategydata['firstSheets']
    leverage = strategydata['leverage']
    takerFee = future_takerFee[platform]['buyfee']
    contract_code = "{}-usdt".format(symbol).upper()
    contract_size = contract_size_dict[symbol][platform]
    current_price = get_perpetualprice(platform, symbol)
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        order_price = round(current_price * 1.01, futurepricelimit[symbol][platform])
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print("用户{}子账户{}开启新一轮马丁追踪合约策略{}，开仓下买单".format(userUuid, apiAccountId, strategyId))
        resdict = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, first_sheets, order_price, 1, 2, 1,
                                      leverage)
        orderId = resdict['response']['orderId'].replace('"', "")
        #  将下单信息插入数据库
        insertsql = "INSERT INTO martin_futurelist(userUuid,apiAccountId,strategyId,platform,contract_code,direction," \
                    "offset,leverage,orderid,order_amount,order_price,order_time,status,uniqueId,tradetype," \
                    "contract_size,coverlevel) VALUES(%s, %s, %s,%s,%s,%s,%s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s) "
        insertdata = (
            userUuid, apiAccountId, strategyId, platform, contract_code, 1, "open", leverage, orderId, first_sheets,
            order_price, ordertime, 0, 11, 2, contract_size, 0)
        cur.execute(insertsql, insertdata)
        conn.commit()
        # 1s后查询订单
        time.sleep(3)
        res = query_contract_usdt_order(userUuid, apiAccountId, platform, orderId, symbol)['response']
        order_status = res['status']
        if order_status == "COMPLETED":
            trade_volume = first_sheets  # 成交数量
            trade_avg_price = res['detail'][0]['price']  # 成交均价
            if platform == "binance":
                trade_amount = res['detail'][0]['tradeBalance']
                fee = trade_amount * takerFee
            else:
                fee = res['detail'][0]['fee']  # 手续费
            # 设置补仓价格档位
            strategydata['buy_num'] = trade_volume
            strategydata['init_entry_price'] = trade_avg_price
            traceLevel(strategydata)
            strategydata['entry_price'] = trade_avg_price
            strategydata['mostprice'] = 0
            strategydata['stopprice'] = 0
            strategydata['touchtag'] = 0
            strategydata['flag'] = 1
            print("用户{}子账户{}新一轮马丁追踪策略{}已开始进场".format(userUuid, apiAccountId, strategyId))
            r0.hset('martin_future_long', strategyId, json.dumps(strategydata))
            tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # 将成交记录更新到数据库
            updatesql = "update martin_futurelist set trade_amount=%s,trade_price=%s,trade_time=%s,status=%s," \
                        "fee=%s where  strategyId=%s and orderid=%s "
            cur.execute(updatesql, (trade_volume, trade_avg_price, tradetime, 1, fee, strategyId, orderId))
            conn.commit()
        elif order_status == "TRADING":
            res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol, platform, orderId)
            if res['success']:
                # 取消订单，将数据库订单状态改为2
                print("马丁开仓下单未成功,已撤销")
                cancel_sql = 'update martin_futurelist set status=2 where strategyId=%s and orderid=%s'
                cur.execute(cancel_sql, (strategyId, orderId))
                conn.commit()
    except Exception as e:
        i = "用户{}子账户{}马丁追踪合约策略{}进场下买单时出错{}".format(userUuid, apiAccountId, strategyId, e)
        print(i)
        # 调用java停止策略接口
        # updateparams = {'strategyId': strategyId, "status": 4}
        # res1 = requests.post(future_strategy_status_update, data=updateparams)
        # print(json.loads(res1.content.decode()))
    finally:
        cur.close()
        conn.close()


def trace_open(strategydata, index):
    strategyId = strategydata['strategyId']
    platform = strategydata["platform"]
    coverRatio = strategydata['coverRatio'].split("-")
    cover_label = r0.hget('coverlevel:buy', '{0}-{1}'.format(strategyId, index))
    if strategydata['flag'] == 0 or (not cover_label):
        return
    if index + 1 < len(coverRatio):
        next_cover_price = float(strategydata['init_entry_price']) * (1 - float(coverRatio[index + 1]))
    else:
        next_cover_price = 0
    cover_label = json.loads(cover_label)
    covercallbackratio = strategydata['coverCallbackRatio']
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    symbol = strategydata['symbol']
    contract_code = "{}-usdt".format(symbol).upper()
    leverage = strategydata['leverage']
    entry_price = float(strategydata['entry_price'])
    buy_num = strategydata['buy_num']
    first_sheets = strategydata['firstSheets']
    startIndex = int(strategydata['startIndex'])
    marginMultiple = int(strategydata['marginMultiple'])
    takerFee = future_takerFee[platform]['buyfee']
    contract_size = contract_size_dict[symbol][platform]
    sheets = int(first_sheets * (marginMultiple ** (index + 2 - startIndex)))
    currentprice = get_perpetualprice(platform, symbol)
    print("追踪补仓", currentprice)
    # 当价格在此档位区间，并触碰到了最低价
    if (cover_label['covertraceprice'] > currentprice > next_cover_price) and (
            cover_label['coverprice'] > currentprice):
        print('当前行情价{}更新最低价与抄底价'.format(currentprice))
        cover_label['covertraceprice'] = currentprice
        cover_label['stopprice'] = currentprice * (1 + covercallbackratio)
        r0.hset("coverlevel:buy", '{0}-{1}'.format(strategyId, index), json.dumps(cover_label))
    # 当价格触碰到了激活价
    if currentprice <= cover_label['coverprice'] and cover_label['touchtag'] == 0:
        print('当前行情价{}触发激活价，作标记'.format(currentprice))
        cover_label['touchtag'] = 1
        r0.hset('coverlevel:buy', '{0}-{1}'.format(strategyId, index), json.dumps(cover_label))
    # 当价格触碰了激活价并回升到抄底价时
    if cover_label["touchtag"] == 1 and cover_label["stopprice"] < currentprice < cover_label['coverprice']:
        conn = POOL.connection()
        cur = conn.cursor()
        try:
            order_price = round(currentprice * 1.01, futurepricelimit[symbol][platform])
            ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print("用户{}子账户{}马丁追踪合约策略{}开始补仓买入".format(userUuid, apiAccountId, strategyId))
            resdict = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, sheets, order_price, 1, 2, 1,
                                          leverage)
            print(resdict)
            orderId = resdict['response']['orderId'].replace('"', "")
            # 补仓下单记录插入数据库
            insertsql = "INSERT INTO martin_futurelist(userUuid,apiAccountId,strategyId,platform,contract_code,direction," \
                        "offset,leverage,orderid,order_amount,order_price,order_time,status,uniqueId,tradetype,contract_size,coverlevel) VALUES(" \
                        "%s, %s, %s,%s,%s,%s,%s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s) "
            insertdata = (
                userUuid, apiAccountId, strategyId, platform, contract_code, 1, "open", leverage, orderId, sheets,
                order_price, ordertime, 0, 11, 2, contract_size, index + 1)
            cur.execute(insertsql, insertdata)
            conn.commit()
            # 3s后查询订单
            time.sleep(3)
            res = query_contract_usdt_order(userUuid, apiAccountId, platform, orderId, symbol)['response']
            order_status = res['status']
            if order_status == "COMPLETED":
                trade_volume = sheets  # 成交数量
                trade_avg_price = float(res['detail'][0]['price'])  # 成交均价
                if platform == "binance":
                    trade_amount = res['detail'][0]['tradeBalance']
                    fee = trade_amount * takerFee
                else:
                    fee = float(res['detail'][0]['fee'])  # 手续费
                strategydata['entry_price'] = (entry_price * buy_num + trade_volume * trade_avg_price) / (
                        buy_num + trade_volume)
                strategydata['buy_num'] += trade_volume
                r0.hset('martin_future_long', strategyId, json.dumps(strategydata))
                r0.hdel('coverlevel:buy', '{0}-{1}'.format(strategyId, index))
                print("用户{}子账户{}马丁追踪{}补仓买入成功".format(userUuid, apiAccountId, strategyId))
                # 将成交记录更新到数据库
                tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                updatesql = "update martin_futurelist set trade_amount=%s,trade_price=%s,trade_time=%s,status=%s," \
                            "fee=%s where  strategyId=%s and orderid=%s "
                cur.execute(updatesql, (trade_volume, trade_avg_price, tradetime, 1, fee, strategyId, orderId))
                conn.commit()
            elif order_status == "TRADING":
                res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol, platform, orderId)
                if res['success']:
                    # 取消订单，将数据库订单状态改为2
                    cancel_sql = 'update martin_futurelist set status=2 where strategyId=%s and orderid=%s'
                    cur.execute(cancel_sql, (strategyId, orderId))
                    conn.commit()
        except Exception as e:
            print("用户{}子账户{}马丁追踪合约策略{}补仓买入失败{}".format(userUuid, apiAccountId, strategyId, e))
            # 调用java停止策略接口
            # updateparams = {'strategyId': strategyId, "status": 4}
            # res1 = requests.post(future_strategy_status_update, data=updateparams)
            # print(json.loads(res1.content.decode()))
        finally:
            cur.close()
            conn.close()


def trace_close(strategyId):
    strategydata = r0.hget('martin_future_long', strategyId)
    if not strategydata:
        return
    strategydata = json.loads(strategydata)
    if strategydata['flag'] == 0:
        return
    userUuid = strategydata['userUuid']
    strategyId = strategydata['strategyId']
    apiAccountId = strategydata['apiAccountId']
    symbol = strategydata['symbol']
    contract_code = "{}-usdt".format(symbol).upper()
    platform = strategydata["platform"]
    contract_size = float(contract_size_dict[symbol][platform])  # 合约面值
    profitStopRatio = float(strategydata['stopRatio'])
    callbackRatio = strategydata['callbackRatio']
    sheets = int(strategydata['buy_num'])
    takerFee = future_takerFee[platform]['sellfee']
    entry_price = float(strategydata['entry_price'])
    leverage = strategydata['leverage']
    first_sheets = int(strategydata['firstSheets'])
    startIndex = int(strategydata['startIndex'])
    marginMultiple = int(strategydata['marginMultiple'])
    currentprice = get_perpetualprice(platform, symbol)
    coverRatio = strategydata['coverRatio'].split("-")
    print("追踪止盈", currentprice, entry_price * (1 + profitStopRatio))
    total_num = first_sheets * (startIndex - 1) + first_sheets * (
            1 - marginMultiple ** (len(coverRatio) + 2 - startIndex)) / (1 - marginMultiple)  # 总投入张数
    currentprice = get_perpetualprice(platform, symbol)
    if platform == "binance":
        init_amount = total_num * currentprice / leverage
    else:
        init_amount = total_num * currentprice * contract_size / leverage
    if strategydata['touchtag'] == 1 and currentprice > strategydata['mostprice']:
        print("当前行情价{}更新最高价与止盈价".format(currentprice))
        strategydata['mostprice'] = currentprice
        strategydata['stopprice'] = currentprice * (1 - callbackRatio)
        r0.hset('martin_future_long', strategyId, json.dumps(strategydata))
    if currentprice >= entry_price * (1 + profitStopRatio) and strategydata['touchtag'] == 0:
        print('当前行情价{}触发激活价，作标记'.format(currentprice))
        strategydata['touchtag'] = 1
        r0.hset('martin_future_long', strategyId, json.dumps(strategydata))
    # 当价格触碰了激活价并回落到止盈价时
    if strategydata["stopprice"] is not None and strategydata["touchtag"] == 1 and strategydata[
        "stopprice"] >= currentprice > entry_price:
        print('价格触碰了激活价并回落到止盈价,用户{}子账户{}马丁追踪{}开始卖出平仓'.format(userUuid, apiAccountId, strategyId))
        conn = POOL.connection()
        cur = conn.cursor()
        try:
            order_price = round(currentprice * 0.99, futurepricelimit[symbol][platform])
            print(order_price)
            ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print("用户{}子账户{}马丁追踪策略{}开始止盈卖出".format(userUuid, apiAccountId, strategyId))
            resdict = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, sheets, order_price, 2, 2, 4,
                                          leverage)
            orderId = resdict['response']['orderId'].replace('"', "")
            print(resdict, orderId)
            # 补仓下单记录插入数据库
            insertsql = "INSERT INTO martin_futurelist(userUuid,apiAccountId,strategyId,platform,contract_code,direction," \
                        "offset,leverage,orderid,order_amount,order_price,order_time,status,uniqueId,tradetype,contract_size,coverlevel) VALUES(" \
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            insertdata = (
                userUuid, apiAccountId, strategyId, platform, contract_code, 2, "close", leverage, orderId, sheets,
                order_price, ordertime, 0, 11, 2, contract_size, 0)
            cur.execute(insertsql, insertdata)
            conn.commit()
            # 3s后查询订单
            time.sleep(3)
            res = query_contract_usdt_order(userUuid, apiAccountId, platform, orderId, symbol)['response']
            order_status = res['status']
            if order_status == "COMPLETED":
                trade_volume = sheets  # 成交数量
                trade_avg_price = float(res['detail'][0]['price'])  # 成交均价
                if platform == "binance":
                    trade_amount = res['detail'][0]['tradeBalance']
                    fee = trade_amount * takerFee
                    # 利润 （平仓 - 开仓)*成交的交易币数量
                    profit = (trade_avg_price - entry_price) * trade_volume
                    # 手续费 成交价*成交的交易币数量*费率
                    total_fee = (trade_avg_price + entry_price) * trade_volume * takerFee
                    profit = round((profit - total_fee), 8)
                    profitRate = round(profit / (entry_price * trade_volume / leverage), 8)
                else:
                    fee = res['detail'][0]['fee']  # 手续费
                    # （平仓 - 开仓)*成交合约张数 * 合约面值
                    profit = (trade_avg_price - entry_price) * trade_volume * contract_size
                    # 手续费 成交价*成交合约张数*合约面值*费率
                    total_fee = (trade_avg_price + entry_price) * trade_volume * contract_size * takerFee
                    profit = profit - total_fee
                    profitRate = round(profit / (entry_price * trade_volume * contract_size / leverage), 8)
                print("用户{}子账户{}马丁追踪策略{}止盈卖出平仓，此轮利润{}".format(userUuid, apiAccountId, strategyId, profit))
                # 成交记录存入数据库
                tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                updatesql = "update martin_futurelist set trade_amount=%s,trade_price=%s,trade_time=%s,profit=%s," \
                            "profitRate=%s,status=%s,fee=%s where strategyId=%s and orderid=%s"
                cur.execute(updatesql,
                            (trade_volume, trade_avg_price, tradetime, profit, profitRate, 1, fee, strategyId, orderId))
                conn.commit()
                strategydata['flag'] = 0
                r0.hset('martin_future_long', strategyId, json.dumps(strategydata))
                cover_ratio = strategydata['coverRatio'].split("-")
                for i in range(len(cover_ratio)):
                    r0.hdel('coverlevel:buy', '{}-{}'.format(strategyId, i))
                # 计算总利润
                totalprofit, totalprofitRate = sumProfit(userUuid, strategyId, init_amount)
                params = {'strategyId': strategyId, 'profit': totalprofit, 'profitRate': totalprofitRate}
                res = requests.post(future_strategy_status_update, data=params)
                resdict = json.loads(res.content.decode())
                print(resdict)
            elif order_status == "TRADING":
                res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol, platform, orderId)
                if res['success']:
                    # 取消订单，将数据库订单状态改为2
                    cancel_sql = 'update martin_futurelist set status=2 where strategyId=%s and orderid=%s'
                    cur.execute(cancel_sql, (strategyId, orderId))
                    conn.commit()
        except Exception as e:
            i = "用户{}子账户{}马丁追踪合约策略{}止盈卖出失败{}".format(userUuid, apiAccountId, strategyId, e)
            print(i)
            # 调用java停止策略接口
            # updateparams = {'strategyId': strategyId, "status": 4}
            # res1 = requests.post(future_strategy_status_update, data=updateparams)
            # print(json.loads(res1.content.decode()))
        finally:
            cur.close()
            conn.close()


def long_stopout(strategydata):  # 强制平仓
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    strategyId = strategydata['strategyId']
    totalprofit = 0
    totalprofitRate = 0
    conn = POOL.connection()
    cur = conn.cursor()
    if strategydata['flag'] == 0:
        # 从redis删除该策略缓存
        r0.hdel('martin_future_long', strategyId)
        cover_ratio = strategydata['coverRatio'].split("-")
        for i in range(len(cover_ratio)):
            r0.hdel('coverlevel:buy', '{}-{}'.format(strategyId, i))
        cur.close()
        conn.close()
        return totalprofit, totalprofitRate
    strategyId = strategydata['strategyId']
    symbol = strategydata['symbol']
    contract_code = "{}-usdt".format(symbol).upper()
    platform = strategydata["platform"]
    direction = strategydata['direction']
    amount = int(strategydata['buy_num'])
    coverRatio = strategydata['coverRatio'].split("-")
    first_sheets = int(strategydata['firstSheets'])
    startIndex = int(strategydata['startIndex'])
    marginMultiple = int(strategydata['marginMultiple'])
    entry_price = float(strategydata['entry_price'])
    takerFee = float(future_takerFee[platform]['sellfee'])  # 合约费率
    contract_size = float(contract_size_dict[symbol][platform])  # 合约面值
    leverage = int(strategydata['leverage'])
    total_num = first_sheets * (startIndex - 1) + first_sheets * (
            1 - marginMultiple ** (len(coverRatio) + 2 - startIndex)) / (1 - marginMultiple)  # 总投入张数
    currentprice = get_perpetualprice(platform, symbol)
    if platform == "binance":
        init_amount = total_num * currentprice / leverage
    else:
        init_amount = total_num * currentprice * contract_size / leverage
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        order_price = round(currentprice * 0.99, futurepricelimit[symbol][platform])
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print("用户{}子账户{}马丁追踪策略{}开始强制平仓卖出,数量{}".format(userUuid, apiAccountId, strategyId, amount))
        resdict = contract_usdt_trade(userUuid, apiAccountId, symbol, platform, amount, order_price, 2, 2, 4, leverage)
        orderId = resdict['response']['orderId'].replace('"', "")
        # 补仓下单记录插入数据库
        insertsql = "INSERT INTO martin_futurelist(userUuid,apiAccountId,strategyId,platform,contract_code,direction," \
                    "offset,leverage,orderid,order_amount,order_price,order_time,status,uniqueId,tradetype,contract_size,coverlevel) VALUES(" \
                    "%s, %s, %s,%s,%s,%s,%s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s) "
        insertdata = (
            userUuid, apiAccountId, strategyId, platform, contract_code, 2, "close", leverage, orderId, amount,
            order_price, ordertime, 0, 11, 2, contract_size, 0)
        cur.execute(insertsql, insertdata)
        conn.commit()
        # 3s后查询订单情况
        time.sleep(3)
        res = query_contract_usdt_order(userUuid, apiAccountId, platform, orderId, symbol)['response']
        print(res)
        order_status = res['status']
        if order_status == "COMPLETED":
            trade_volume = amount  # 成交数量
            if platform == "binance":
                trade_amount = res['detail'][0]['tradeBalance']
                fee = trade_amount * takerFee
                trade_avg_price = float(res['detail'][0]['price'])  # 成交均价
                profit = (trade_avg_price - entry_price) * trade_volume
                total_fee = (trade_avg_price + entry_price) * trade_volume * takerFee
                profit = profit - total_fee
                profitRate = round(profit / (entry_price * trade_volume / leverage), 8)
            else:
                fee = res['detail'][0]['fee']  # 手续费
                trade_avg_price = float(res['detail'][0]['price'])  # 成交均价
                # （平仓 - 开仓)*成交合约张数 * 合约面值
                profit = (trade_avg_price - entry_price) * trade_volume * contract_size
                # 手续费 成交价*成交合约张数*合约面值*费率
                total_fee = (trade_avg_price + entry_price) * trade_volume * contract_size * takerFee
                profit = profit - total_fee
                profitRate = round(profit / (entry_price * trade_volume * contract_size / leverage), 8)
            # 成交记录存入数据库
            tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            updatesql = "update martin_futurelist set trade_amount=%s,trade_price=%s,trade_time=%s,profit=%s," \
                        "profitRate=%s,status=%s,fee=%s where  strategyId=%s and orderid=%s"
            cur.execute(updatesql,
                        (trade_volume, trade_avg_price, tradetime, profit, profitRate, 1, fee, strategyId, orderId))
            conn.commit()
            print("订单已成交，并成功更新数据库")
            # 计算总利润
            totalprofit, totalprofitRate = sumProfit(userUuid, strategyId, init_amount)
            i = "用户{}马丁追踪合约策略{}，买入均价{}，在价位{}时强制平仓，盈利{}，盈利率{}".format(userUuid, strategyId,
                                                                     entry_price, currentprice, totalprofit,
                                                                     totalprofitRate)
            print(i)
            logger.info(i)
        elif order_status == "TRADING":
            res = cancel_contract_usdt_order(userUuid, apiAccountId, symbol, platform, orderId)
            if res['success']:
                # 取消订单，将数据库订单状态改为2
                cancel_sql = 'update martin_futurelist set status=2 where strategyId=%s and orderid=%s'
                cur.execute(cancel_sql, (strategyId, orderId))
                conn.commit()
                print("订单尚未成交,已撤单")
    except Exception as e:
        i = "用户{}子账户{}马丁追踪合约策略{}强制平仓卖出失败{}".format(userUuid, apiAccountId, strategyId, e)
        print(i)
        # 调用java停止策略接口
        # updateparams = {'strategyId': strategyId, "status": 4}
        # res1 = requests.post(future_strategy_status_update, data=updateparams)
        # print(json.loads(res1.content.decode()))
    finally:
        # 从redis删除该策略缓存
        r0.hdel('martin_future_long', strategyId)
        cover_ratio = strategydata['coverRatio'].split("-")
        for i in range(len(cover_ratio)):
            r0.hdel('coverlevel:buy', '{}-{}'.format(strategyId, i))
        cur.close()
        conn.close()
        return totalprofit, totalprofitRate


def run(strategydata):
    first_open(strategydata)
    for index in range(len(strategydata['coverRatio'].split('-'))):
        trace_open(strategydata, index)
    trace_close(strategydata['strategyId'])


if __name__ == "__main__":
    while True:
        try:
            strategy_list = r0.hvals("martin_future_long")
            strategy_list = [json.loads(i) for i in strategy_list]
            T = []
            for strategy_info in strategy_list:
                T.append(Thread(target=run, args=(strategy_info,)))
            for t in T:
                t.start()
            for t in T:
                t.join()
        except Exception as e:
            print(e)
        finally:
            time.sleep(1)

    # strategydata =
    # {
    #     "id": null,
    #     "tenantCode": null,
    #     "createDate": null,
    #     "updateDate": null,
    #     "strategyId": 1023,
    #     "apiAccountId": 10162,
    #     "accountName": "\u6d4b\u8bd5\u4e00",
    #     "userUuid": "398051ac70ef4da9aafd33ce0b95195f",
    #     "platId": 10002,
    #     "platform": "huobi",
    #     "symbol": "eth",
    #     "strategyType": 16,
    #     "entryPrice": null,
    #     "investmentTotal": 27.4536,
    #     "initialTradeAssets": null,
    #     "initialValAssets": 50.65408374592606,
    #     "initialTotalAssets": null,
    #     "endTradeAssets": null,
    #     "endValAssets": null,
    #     "endTotalAssets": null,
    #     "profit": 0,
    #     "profitRate": 0,
    #     "status": 1,
    #     "createTime": "2021-07-02 16:36:25",
    #     "stopTime": null,
    #     "platformFee": null,
    #     "goldStopType": null,
    #     "copyType": 2,
    #     "amount": null,
    #     "maxPositionNum": 2,
    #     "stopRatio": 0.01,
    #     "isContract": 1,
    #     "direction": "buy",
    #     "firstSheets": 1,
    #     "marginMultiple": 2,
    #     "followUpinterVal": 0.02,
    #     "coverCallbackRatio": 0.01,
    #     "callbackRatio": 0.005,
    #     "startIndex": 2,
    #     "longOrderQuantities": null,
    #     "shortOrderQuantities": null,
    #     "leverage": 3,
    #     "entry_price": "2058.42",
    #     "flag": 1,
    #     "init_entry_price": "2058.42",
    #     "mostprice": 0,
    #     "stopprice": 0,
    #     "touchtag": 0,
    #     "coverRatio": "0.02-0.04",
    #     "buy_num": 1
    # }



    # {
    #     "strategyId": 63,
    #     "userUuid": "536",
    #     "apiAccountId": 10210,
    #     "symbol": "eth",
    #     "platform": "huobi",
    #     "firstSheets": 2,
    #     "leverage": 5,
    #     "flag": 1,
    #     "coverRatio": "0.03-0.06-0.09",
    #     "buy_num": 2,
    #     "init_entry_price": "3326.32",
    #     "entry_price": "3326.32",
    #     "mostprice": 0,
    #     "stopprice": 0,
    #     "touchtag": 0,
    #     "direction": "buy",
    #     "startIndex":1
    # }

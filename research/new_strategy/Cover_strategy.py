import json
import smtplib
import time
from email.mime.text import MIMEText
from email.utils import formataddr
from threading import Thread
import numpy as np
import requests
from tools.Config import Queryorder_url, Trade_url, amountlimit, pricelimit, \
    updateCover_url, Cancel_url
from tools.databasePool import r2, POOL
from tools.get_market_info import get_currentprice1
from loggerConfig import logger

np.set_printoptions(suppress=True)  # 取消科学计数法


def sendEmail(text):
    my_sender = '651006067@qq.com'  # 发件人邮箱账号
    my_pass = 'sbomjdyhtwfkbcca'  # 发件人邮箱密码
    my_user = '651006067@qq.com'  # 收件人邮箱账号
    # my_user = '18802675946@163.com'  # 收件人邮箱账号
    ret = True
    try:
        msg = MIMEText(text, 'plain', 'utf-8')
        msg['From'] = formataddr(("量化交易系统", my_sender))  # 括号里的对应发件人邮箱昵称、发件人邮箱账号
        msg['To'] = formataddr(("chenxiao", my_user))  # 括号里的对应收件人邮箱昵称、收件人邮箱账号
        msg['Subject'] = "补仓行情指标"  # 邮件的主题，也可以说是标题
        server = smtplib.SMTP_SSL("smtp.qq.com", 465)  # 发件人邮箱中的SMTP服务器，端口是25
        server.login(my_sender, my_pass)  # 括号中对应的是发件人邮箱账号、邮箱密码
        server.sendmail(my_sender, [my_user, ], msg.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
        server.quit()  # 关闭连接
    except Exception as e:  # 如果 try 中的语句没有执行，则会执行下面的 ret=False
        ret = False
    return ret


def sumProfit(strategydata):
    strategyId = strategydata['strategyId']
    orderAmount = strategydata['orderAmount']  # 首单额度
    orderQuantities = strategydata['orderQuantities']  # 做单数量
    totalInvest = orderAmount * (orderQuantities+1)
    totalprofit = 0
    totalprofitRate = 0
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        cur.execute('select sum(profit) from coverlist where strategyId=%s and direction=2 and status=1',
                    (strategyId,))
        total_profit = cur.fetchone()[0]
        if total_profit:
            totalprofit = float(total_profit)
        if totalInvest:
            totalprofitRate = round(totalprofit / totalInvest, 8)
        cur.close()
        conn.close()
    except Exception as e:
        logger.error('策略{}在查询利润时出错{}'.format(strategyId, e))
    return totalprofit, totalprofitRate


def traceLevel(strategydata):
    entry_price = strategydata['entry_price']
    coverRatio = strategydata['coverRatio'].split("-")
    strategyId = strategydata['strategyId']
    for i in range(len(coverRatio)):
        coverprice = entry_price * (1 - float(coverRatio[i]))
        label = {'covertraceprice': coverprice, 'stopprice': coverprice, 'coverprice': coverprice, 'touchtag': 0}
        r2.hset('coverlevel', '{0}-{1}'.format(strategyId, i), json.dumps(label))


def cancel_order(cur, userUuid, apiAccountId, strategyId, platform, symbol, orderId, direction):
    try:
        cancelbuyparams = {"direction": direction, "symbol": symbol, "platform": platform, "orderId": orderId,
                           "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11, "strategyId": strategyId}
        cancelres = requests.post(Cancel_url, data=cancelbuyparams)
        res = json.loads(cancelres.content.decode())
        if res["code"] == 1:
            cur.execute("update coverlist set status=2 where strategyId=%s and orderid=%s",
                        (strategyId, orderId))
        else:
            i = "用户{}策略{}撤销{}平台订单出错,原因{}".format(userUuid, strategyId, platform, res['message'])
            print(i)
            logger.info(i)
    except Exception as e:
        i = "用户{}策略{}撤销{}平台订单出错{}".format(userUuid, strategyId, platform, e)
        print(i)
        logger.info(i)


# 首次进场，执行买单
def startBuy(strategydata):
    # 只在初次进场时买入，或在止盈后时再次买入
    if strategydata['flag'] == 1:
        return
    userUuid = strategydata["userUuid"]  # 用户id
    apiAccountId = strategydata["apiAccountId"]  # 用户子账户id
    strategyId = strategydata["strategyId"]  # 策略id
    platform = strategydata["platform"]  # 交易平台
    symbol = strategydata["symbol"]  # 交易对
    orderAmount = strategydata['orderAmount']  # 首单额度
    orderQuantities = strategydata['orderQuantities']  # 做单数量
    profitStopRatio = strategydata['profitStopRatio']  # 止盈比例
    callbackRatio = strategydata['callbackRatio']  # 止盈回调
    coverRatio = strategydata['coverRatio'].split('-')  # 补仓跌幅
    coverCallbackRatio = strategydata['coverCallbackRatio']  # 补仓回调比例
    i = '用户{}子账户{}开始新一轮补仓策略{}，设置的首单额度{}，做单数量{}，止盈比例{}，止盈回调比例{}，补仓跌幅{}，补仓回调比例{}'.format(userUuid, apiAccountId,
                                                                                       strategyId,
                                                                                       orderAmount, orderQuantities,
                                                                                       profitStopRatio,
                                                                                       callbackRatio, coverRatio,
                                                                                       coverCallbackRatio)
    print(i)
    logger.info(i)
    # 参数初始化
    strategydata['touchtag'] = 0
    strategydata['buy_value'] = 0
    strategydata['buy_num'] = 0
    strategydata["stopprice"] = 0
    strategydata['mostprice'] = 0
    current_price = get_currentprice1(platform, symbol)
    strategydata['entry_price'] = current_price
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        # 计算下单金额（溢价买入）
        # buy_price = round(current_price + premiumdict[symbol] / 2, pricelimit[symbol][platform])
        buy_price = round(current_price * 1.001, pricelimit[symbol][platform])
        amount = orderAmount / buy_price
        x, y = str(amount).split('.')
        amount = float(x + '.' + y[0:amountlimit[symbol][platform]])
        if amount == 0:
            return
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        tradeparams = {"direction": 1, "amount": amount, "symbol": symbol, "platform": platform, "price": buy_price,
                       "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11, "strategyId": strategyId,
                       'tradetype': 1}
        i = '用户{}策略{}开始开仓，下买单，价格{}'.format(userUuid, strategyId, buy_price)
        print(i)
        logger.info(i)
        traderes = requests.post(Trade_url, data=tradeparams)
        trade_dict = json.loads(traderes.content.decode())
        orderId = trade_dict["response"]["orderid"]  # 获取订单id
        insertsql = "INSERT INTO coverlist(userUuid,apiAccountId,strategyId,platform,symbol,direction,orderid," \
                    "order_amount,order_price,order_time,status,uniqueId,tradetype,coverlevel) VALUES(%s, %s, %s, %s, " \
                    "%s,%s,%s, %s, %s, %s, %s,%s,%s,%s) "
        insertdata = (
            userUuid, apiAccountId, strategyId, platform, symbol, 1, orderId, amount, buy_price, ordertime, 0, 11, 1, 0)
        cur.execute(insertsql, insertdata)
        time.sleep(3)
        # 3秒后查询订单是否成交
        queryparams = {"direction": 1, "symbol": symbol, "platform": platform, "orderId": orderId,
                       "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11, "strategyId": strategyId}
        res = requests.post(Queryorder_url, data=queryparams)
        querydict = json.loads(res.content.decode())
        status = querydict['response']['status']
        if status == 'closed':
            numberDeal = float(querydict["response"]["numberDeal"])
            tradeprice = querydict['response']['tradingPrice']
            tradetime = querydict['response']['tradeTime']
            buyfee = round(numberDeal * tradeprice * 0.002, 8)
            # 设置补仓价格档位
            traceLevel(strategydata)
            strategydata['buy_value'] = numberDeal * tradeprice
            strategydata['buy_num'] = numberDeal
            strategydata['entry_price'] = tradeprice
            strategydata['flag'] = 1
            r2.hset('Cover_strategy', strategyId, json.dumps(strategydata))
            # 存入数据库
            updatesql = "update coverlist set trade_amount=%s,trade_price=%s,trade_time=%s,status=%s,fee=%s where " \
                        "strategyId=%s and orderid=%s "
            cur.execute(updatesql, (numberDeal, tradeprice, tradetime, 1, buyfee, strategyId, orderId))
            params = {'strategyId': strategyId, 'averagePrice': tradeprice}
            res = requests.post(updateCover_url, data=params)
            resdict = json.loads(res.content.decode())
            print(resdict)
        elif status == 'open':
            cancel_order(cur, userUuid, apiAccountId, strategyId, platform, symbol, orderId, 1)
    except Exception as e:
        logger.info("用户{}补仓策略{}进场下买单时出错{}".format(userUuid, strategyId, e))
        # 调用java停止策略接口
        updateparams = {'strategyId': strategyId, "status": 4}
        res1 = requests.post(updateCover_url, data=updateparams)
    finally:
        conn.commit()
        cur.close()
        conn.close()


def stopOut(strategyId):
    strategydata = json.loads(r2.hget('Cover_strategy', strategyId))
    totalprofit = 0
    totalprofitRate = 0
    if strategydata['flag'] == 0:
        return totalprofit, totalprofitRate
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    symbol = strategydata['symbol']
    platform = strategydata["platform"]
    amount = strategydata['buy_num']
    currentprice = get_currentprice1(platform, symbol)
    if amount != 0:
        entry_price_avg = strategydata['buy_value'] / strategydata['buy_num']
    else:
        entry_price_avg = strategydata['entry_price']
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        # 下卖单
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # 折价卖出
        # sellprice = round(currentprice - premiumdict[symbol] / 2, pricelimit[symbol][platform])
        sellprice = round(currentprice * 0.999, pricelimit[symbol][platform])
        i = '用户{}策略{}开始强制卖出，价格{}'.format(userUuid, strategyId, sellprice)
        print(i)
        logger.info(i)
        sellparams = {"direction": 2, "amount": amount, "symbol": symbol, "platform": platform, "price": sellprice,
                      "userUuid": userUuid, "apiAccountId": apiAccountId, "source": 11, "strategyId": strategyId,
                      'tradetype': 1}
        res = requests.post(Trade_url, data=sellparams)
        resdict = json.loads(res.content.decode())
        orderId = resdict["response"]["orderid"]  # 获取订单id
        sellinsertsql = "INSERT INTO coverlist(strategyId,userUuid,apiAccountId,platform,symbol,direction," \
                        "orderid,order_amount,order_price,order_time," \
                        "status,uniqueId,tradetype,coverlevel) VALUES(%s, %s, %s, %s, %s, %s,%s,%s,%s, %s, %s, " \
                        "%s,%s,%s) "
        cur.execute(sellinsertsql, (
            strategyId, userUuid, apiAccountId, platform, symbol, 2, orderId, amount, sellprice,
            ordertime, 0, 11, 1, 0))
        time.sleep(3)
        # 3秒后查询卖单
        queryparams = {"direction": 2, "symbol": symbol, "platform": platform, "orderId": orderId,
                       "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11,
                       "strategyId": strategyId}
        queryres = requests.post(Queryorder_url, data=queryparams)
        querydict = json.loads(queryres.content.decode())
        status = querydict['response']['status']
        if status == 'closed':
            numberDeal = float(querydict["response"]["numberDeal"])
            tradeprice = querydict['response']['tradingPrice']
            tradetime = querydict['response']['tradeTime']
            sellfee = round(numberDeal * tradeprice * 0.002, 8)
            profit = round((tradeprice - entry_price_avg - (tradeprice + entry_price_avg) * 0.002) * numberDeal, 8)
            profitRate = round(profit / (entry_price_avg * numberDeal), 8)
            updatesql = "update coverlist set trade_amount=%s,trade_price=%s,trade_time=%s,profit=%s,profitRate=%s," \
                        "status=%s,fee=%s where " \
                        "strategyId=%s and orderid=%s "
            cur.execute(updatesql,
                        (numberDeal, tradeprice, tradetime, profit, profitRate, 1, sellfee, strategyId, orderId))
            conn.commit()
            totalprofit, totalprofitRate = sumProfit(strategydata)
            i = "用户{}补仓策略{}，买入均价{}，在价位{}时强制平仓，盈利{}，盈利率{}".format(userUuid, strategyId,
                                                                 entry_price_avg,
                                                                 currentprice,
                                                                 totalprofit,
                                                                 totalprofitRate)
            print(i)
            logger.info(i)
        elif status == 'open':
            cancel_order(cur, userUuid, apiAccountId, strategyId, platform, symbol, orderId, 2)
    except Exception as e:
        i = "用户{}补仓策略{}清仓下卖单时出错{}".format(userUuid, strategyId, e)
        logger.error(i)
        # 调用java停止策略接口
        updateparams = {'strategyId': strategyId, "status": 4}
        res1 = requests.post(updateCover_url, data=updateparams)
    finally:
        # 从redis删除该策略缓存
        r2.hdel("Cover_strategy", strategyId)
        cover_ratio = strategydata['coverRatio'].split("-")
        for i in range(len(cover_ratio)):
            r2.hdel('coverlevel', '{}-{}'.format(strategyId, i))
        conn.commit()
        cur.close()
        conn.close()
        return totalprofit, totalprofitRate


# 执行追踪止盈
def traceSell(strategyId):
    strategydata = json.loads(r2.hget('Cover_strategy', strategyId))
    if strategydata['flag'] == 0:
        return
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    symbol = strategydata['symbol']
    platform = strategydata["platform"]
    profitStopRatio = strategydata['profitStopRatio']
    callbackRatio = strategydata['callbackRatio']
    amount = strategydata['buy_num']
    currentprice = get_currentprice1(platform, symbol)
    # print('用户{}策略{}开始追踪止盈，当前行情价{}'.format(userUuid, strategyId, currentprice))
    if amount != 0:
        entry_price_avg = strategydata['buy_value'] / strategydata['buy_num']
    else:
        entry_price_avg = strategydata['entry_price']
    if strategydata['touchtag'] == 1 and currentprice > strategydata['mostprice']:
        print("当前行情价{}更新最高价与止盈价".format(currentprice))
        strategydata['mostprice'] = currentprice
        strategydata['stopprice'] = currentprice * (1 - callbackRatio)
        r2.hset('Cover_strategy', strategyId, json.dumps(strategydata))
    if strategydata['touchtag'] == 0 and currentprice >= entry_price_avg * (1 + profitStopRatio):
        print('当前行情价{}触发激活价，作标记'.format(currentprice))
        strategydata['touchtag'] = 1
        r2.hset('Cover_strategy', strategyId, json.dumps(strategydata))
    # 当价格触碰了激活价并回落到止盈价时
    if strategydata["touchtag"] == 1 and strategydata["stopprice"] is not None and strategydata[
        "stopprice"] >= currentprice > entry_price_avg:
        conn = POOL.connection()
        cur = conn.cursor()
        try:
            # 下卖单
            ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # 折价卖出
            # sellprice = round(currentprice - premiumdict[symbol] / 2, pricelimit[symbol][platform])
            sellprice = round(currentprice * 0.999, pricelimit[symbol][platform])
            sellparams = {"direction": 2, "amount": amount, "symbol": symbol, "platform": platform, "price": sellprice,
                          "userUuid": userUuid, "apiAccountId": apiAccountId, "source": 11, "strategyId": strategyId,
                          'tradetype': 1}
            i = '用户{}策略{}当前价格{}触碰了激活价并回落到止盈价,开始止盈卖出,卖出价{}'.format(userUuid, strategyId, currentprice, sellprice)
            print(i)
            logger.info(i)
            res = requests.post(Trade_url, data=sellparams)
            resdict = json.loads(res.content.decode())
            orderId = resdict["response"]["orderid"]  # 获取订单id
            sellinsertsql = "INSERT INTO coverlist(strategyId,userUuid,apiAccountId,platform,symbol,direction," \
                            "orderid,order_amount,order_price,order_time," \
                            "status,uniqueId,tradetype,coverlevel) VALUES(%s, %s, %s, %s, %s, %s,%s,%s,%s, %s, %s, " \
                            "%s,%s,%s) "
            cur.execute(sellinsertsql, (
                strategyId, userUuid, apiAccountId, platform, symbol, 2, orderId, amount, sellprice,
                ordertime, 0, 11, 1, 0))
            time.sleep(3)
            # 3秒后查询卖单
            queryparams = {"direction": 2, "symbol": symbol, "platform": platform, "orderId": orderId,
                           "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11,
                           "strategyId": strategyId}
            queryres = requests.post(Queryorder_url, data=queryparams)
            querydict = json.loads(queryres.content.decode())
            status = querydict['response']['status']
            if status == 'closed':
                numberDeal = float(querydict["response"]["numberDeal"])
                tradeprice = querydict['response']['tradingPrice']
                tradetime = querydict['response']['tradeTime']
                sellfee = round(numberDeal * tradeprice * 0.002, 8)
                profit = round((tradeprice - entry_price_avg - (tradeprice + entry_price_avg) * 0.002) * amount, 8)
                profitRate = round(profit / (entry_price_avg * amount), 8)
                i = "用户{}新一轮补仓策略{}，基准价{}，价格涨到最高价{}后,又回调{}%至{},追踪止盈，盈利{}，盈利率{}".format(userUuid, strategyId,
                                                                                      entry_price_avg,
                                                                                      strategydata["mostprice"],
                                                                                      callbackRatio * 100,
                                                                                      strategydata["stopprice"], profit,
                                                                                      profitRate)
                print(i)
                logger.info(i)
                updatesql = "update coverlist set trade_amount=%s,trade_price=%s,trade_time=%s,profit=%s," \
                            "profitRate=%s,status=%s,fee=%s where " \
                            "strategyId=%s and orderid=%s "
                cur.execute(updatesql,
                            (numberDeal, tradeprice, tradetime, profit, profitRate, 1, sellfee, strategyId, orderId))
                conn.commit()
                strategydata['flag'] = 0
                r2.hset('Cover_strategy', strategyId, json.dumps(strategydata))
                totalprofit, totalprofitRate = sumProfit(strategydata)
                params = {'strategyId': strategyId, 'profit': totalprofit, 'profitRate': totalprofitRate}
                res = requests.post(updateCover_url, data=params)
                resdict = json.loads(res.content.decode())
                print(resdict)
            elif status == "open":
                cancel_order(cur, userUuid, apiAccountId, strategyId, platform, symbol, orderId, 2)
        except Exception as e:
            i = "用户{}补仓策略{}止盈下卖单时出错{}".format(userUuid, strategyId, e)
            logger.error(i)
            # 调用java停止策略接口
            updateparams = {'strategyId': strategyId, "status": 4, }
            res1 = requests.post(updateCover_url, data=updateparams)
        finally:
            conn.commit()
            cur.close()
            conn.close()


# 执行各档位追踪补仓
def traceBuy(strategydata, index):
    strategyId = strategydata['strategyId']
    cover_label = r2.hget('coverlevel', '{0}-{1}'.format(strategyId, index))
    if strategydata['flag'] == 0 or (not cover_label):
        return
    coverRatio = strategydata['coverRatio'].split("-")
    if index + 1 < len(coverRatio):
        next_cover_price = strategydata['entry_price'] * (1 - float(coverRatio[index + 1]))
    else:
        next_cover_price = 0
    cover_label = json.loads(cover_label)
    covercallbackratio = strategydata['coverCallbackRatio']
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    count = strategydata['orderAmount']
    symbol = strategydata['symbol']
    entry_price = strategydata['entry_price']
    platform = strategydata["platform"]
    currentprice = get_currentprice1(platform, symbol)
    # print('用户{}策略{}开始追踪补仓，当前行情价{}'.format(userUuid, strategyId, currentprice))
    # 当价格在此档位区间，并触碰到了最低价
    if (cover_label['covertraceprice'] > currentprice > next_cover_price) and (
            cover_label['coverprice'] > currentprice):
        print('当前行情价{}更新最低价与抄底价'.format(currentprice))
        cover_label['covertraceprice'] = currentprice
        cover_label['stopprice'] = currentprice * (1 + covercallbackratio)
        r2.hset("coverlevel", '{0}-{1}'.format(strategyId, index), json.dumps(cover_label))
    # 当价格触碰到了激活价
    if currentprice <= cover_label['coverprice'] and cover_label['touchtag'] == 0:
        print('当前行情价{}触发激活价，作标记'.format(currentprice))
        cover_label['touchtag'] = 1
        r2.hset('coverlevel', '{0}-{1}'.format(strategyId, index), json.dumps(cover_label))
    # 当价格触碰了激活价并回升到抄底价时
    if cover_label["touchtag"] == 1 and cover_label["stopprice"] < currentprice < cover_label['coverprice']:
        conn = POOL.connection()
        cur = conn.cursor()
        try:
            # 下买单
            ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # 溢价买入
            # buyprice = round(currentprice + premiumdict[symbol] / 2, pricelimit[symbol][platform])
            buyprice = round(currentprice * 1.001, pricelimit[symbol][platform])
            # if index + 1 >= startindex:
            #     if incrementaltype == 1:  # 是否开启倍投
            #         count = count * (marginmultiple ** (index + 2-startindex))
            #     elif incrementaltype == 2:  # 是否等差额增仓
            #         count += increment * (index + 2-startindex)
            amount = count / buyprice
            x, y = str(amount).split('.')
            amount = float(x + '.' + y[0:amountlimit[symbol][platform]])
            buyparams = {"direction": 1, "amount": amount, "symbol": symbol, "platform": platform, "price": buyprice,
                         "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11, "strategyId": strategyId,
                         'tradetype': 1}
            i = '用户{}策略{}当前价格{}触碰了补仓{}档并回升到抄底价，补仓买入价格{}'.format(userUuid, strategyId, currentprice, index + 1, buyprice)
            print(i)
            logger.info(i)
            res = requests.post(Trade_url, data=buyparams)
            resdict = json.loads(res.content.decode())
            orderid = resdict["response"]["orderid"]  # 获取订单id
            insertsql = "INSERT INTO coverlist(userUuid,apiAccountId,strategyId,platform,symbol,direction,orderid," \
                        "order_amount,order_price,order_time,status,uniqueId,tradetype,coverlevel) VALUES(%s, %s, %s, " \
                        "%s, %s,%s,%s, %s, %s, %s," \
                        " %s,%s,%s,%s) "
            insertdata = (
                userUuid, apiAccountId, strategyId, platform, symbol, 1, orderid, amount, buyprice, ordertime, 0,
                11, 1, index + 1)
            cur.execute(insertsql, insertdata)
            time.sleep(3)
            # 3s后查询买单
            queryparams = {"direction": 1, "symbol": symbol, "platform": platform, "orderId": orderid,
                           "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11,
                           "strategyId": strategyId}
            queryres = requests.post(Queryorder_url, data=queryparams)
            querydict = json.loads(queryres.content.decode())
            logger.info("用户{}策略{}平台{}查询买单{}".format(userUuid, strategyId, platform, querydict))
            status = querydict['response']['status']
            if status == 'closed':
                numberDeal = float(querydict["response"]["numberDeal"])
                tradeprice = querydict["response"]["tradePrice"]
                tradetime = querydict['response']['tradeTime']
                buyfee = round(tradeprice * numberDeal * 0.002, 8)
                # 存入数据库
                updatesql = "update coverlist set trade_amount=%s,trade_price=%s,trade_time=%s,status=%s,fee=%s where " \
                            "strategyId=%s and orderid=%s"
                cur.execute(updatesql, (numberDeal, tradeprice, tradetime, 1, buyfee, strategyId, orderid))
                strategydata['buy_value'] += numberDeal * tradeprice
                strategydata['buy_num'] += numberDeal
                r2.hset('Cover_strategy', strategyId, json.dumps(strategydata))
                r2.hdel('coverlevel', '{0}-{1}'.format(strategyId, index))
                # 补仓后修改买入均价
                params = {'strategyId': strategyId, 'averagePrice': strategydata['buy_value'] / strategydata['buy_num']}
                res = requests.post(updateCover_url, data=params)
                resdict = json.loads(res.content.decode())
                print(resdict)
            elif status == 'open':
                cancel_order(cur, userUuid, apiAccountId, strategyId, platform, symbol, orderid, 1)
        except Exception as e:
            i = "用户{}补仓策略{}补仓时出错{}".format(userUuid, strategyId, e)
            logger.error(i)
            # 调用java停止策略接口
            updateparams = {'strategyId': strategyId, "status": 4, }
            res1 = requests.post(updateCover_url, data=updateparams)
        finally:
            conn.commit()
            cur.close()
            conn.close()


# 依次执行买入，补仓、止盈
def run(strategydata):
    startBuy(strategydata)
    for index in range(len(strategydata['coverRatio'].split('-'))):
        traceBuy(strategydata, index)
    traceSell(strategydata['strategyId'])


if __name__ == '__main__':
    while True:
        try:
            strategy_list = r2.hvals("Cover_strategy")
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

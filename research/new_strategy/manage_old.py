# -*- coding: utf-8 -*-
# @Time : 2020/3/23 18:14
import json
import time
from threading import Thread

import requests
from flasgger import Swagger
from flask import Flask, request, jsonify
from flask_cors import *

from currency_strategy.Cover_strategy import stopOut
from currency_strategy.balance_strategy import balancestrategy_begin, cancel_balancestrategy_orders, \
    clear_tradecoin_remains, \
    get_total_fee
from currency_strategy.crash_callback import clear_remain
from contract_strategy.dual_thrust import stopout
from contract_strategy.grid_future_long import future_grid_stop, longgridbegin
from contract_strategy.grid_future_short import shortgridbegin
from currency_strategy.grid_strategy import gridbegin, cancelgridorders, clear_grid_remain, gridbegin2
from currency_strategy.kama_strategy import clear_kama_remain
from loggerConfig import logger
from contract_strategy.martin_future_long import long_stopout
from contract_strategy.martin_future_short import short_stopout
from contract_strategy.macd_atr import macdatrout
from strategy_copy.order_copy import sum_profit
from strategy_copy.balance_copy import balance_strategy_copy
from strategy_copy.grid_copy import grid_stop
from tools.Config import gap_amount, amountlimit, Cancel_url, Remain_url, future_remain_url
from tools.databasePool import r0, r1, r2, r4, POOL_grid, POOL
from tools.future_trade import cancel_contract_usdt_order, switch_lever_rate
from tools.get_market_info import get_currentprice1, get_currentprice0
from tools.handle import get_grid_orderlist, synchronize_grid_orderlist
from tools.tool import get_total_profit

app = Flask(__name__)
Swagger(app)
CORS(app, supports_credentials=True)
app.logger.disabled = False


# 开启/关闭高频刷单
@app.route('/runMaketMakerStrategy', methods=['get', 'post'])
def runMaketMakerStrategy():
    userUuid = request.form["userUuid"]  # 获取用户唯一id
    strategyId = int(request.form["strategyId"])  # 获取策略id
    status = int(request.form["status"])
    flag = "开启" if status == 1 else "关闭"
    marketmakerdata = json.loads(request.form["str"])["data"]  # 策略详情
    print(marketmakerdata)
    userUuid = marketmakerdata["userUuid"]  # 获取用户唯一id
    strategyId = marketmakerdata["strategyId"]  # 获取策略id
    apiAccountId = marketmakerdata["apiAccountId"]  # api——id
    platform = marketmakerdata["platform"]  # 交易所
    symbol = marketmakerdata["symbol"]  # 交易对
    tradeCoin = symbol.split("_")[0]  # 交易币
    valueCoin = symbol.split("_")[1]  # 计价币
    amount_min = marketmakerdata["amount_min"]  # 每笔最小交易量
    amount_max = marketmakerdata["amount_max"]  # 每笔最大交易量
    starttime = marketmakerdata["starttime"]  # 开启时间精确到s
    spendtime = marketmakerdata["spendtime"]  # 运行时间
    range = marketmakerdata["range"]  # 涨跌幅度
    try:
        type = marketmakerdata["type"]
        # 区间震荡
        if type == 1:
            if status == 1:
                marketmakerdata["initial_price"] = get_currentprice0(platform, symbol)  # 初始价格
                marketmakerdata["sidelist"] = ["sell", "sell", "buy", "buy", "buy"]  # 控制买卖单次数,初始默认主买单
                marketmakerdata["sideflag"] = 1  # 标记1主买，2主卖
                marketmakerdata["time_space"] = 1800  # 初始默认1800次后切换买卖方向
                r1.hset("MaketMaker_Strategy", strategyId, json.dumps(marketmakerdata))
            if status == 2 or status == 3:
                r1.hdel("MaketMaker_Strategy", strategyId)
        # 拉升行情
        if type == 2:
            if status == 1:
                marketmakerdata["initial_price"] = get_currentprice0(platform, symbol)  # 初始价格
                marketmakerdata["sidelist"] = ["sell", "buy", "buy", "buy", "buy"]  # 控制买卖单概率
                marketmakerdata["sideflag"] = 1  # 标记1主买，2主卖
                # 查找有没有震荡策略，有的话将其状态改为2
                marketmakerdatalist = r1.hvals("MaketMaker_Strategy")
                marketmakerdatalist = [json.loads(i) for i in marketmakerdatalist]
                for data in marketmakerdatalist:
                    if data["type"] == 1 and data["platform"] == platform and data["symbol"] == symbol:
                        data["status"] = 2
                        r1.hset("MaketMaker_Strategy", data["strategyId"], json.dumps(data))
                r1.hset("MaketMaker_Strategy", strategyId, json.dumps(marketmakerdata))
            if status == 2 or status == 3:
                r1.hdel("MaketMaker_Strategy", strategyId)
                # 查找有没有震荡策略，有的话将其状态改为1,(没有的话新增一条)
                marketmakerdatalist = r1.hvals("MaketMaker_Strategy")
                marketmakerdatalist = [json.loads(i) for i in marketmakerdatalist]
                for data in marketmakerdatalist:
                    if data["type"] == 1 and data["platform"] == platform and data["symbol"] == symbol:
                        data["status"] = 1
                        data["initial_price"] = get_currentprice0(platform, symbol)
                        r1.hset("MaketMaker_Strategy", data["strategyId"], json.dumps(data))
        # 下跌行情
        if type == 3:
            if status == 1:
                marketmakerdata["initial_price"] = get_currentprice0(platform, symbol)  # 初始价格
                marketmakerdata["sidelist"] = ["sell", "sell", "sell", "sell", "buy"]  # 控制买卖单概率
                marketmakerdata["sideflag"] = 2  # 标记1主买，2主卖
                r1.hset("MaketMaker_Strategy", strategyId, json.dumps(marketmakerdata))
                # 查找有没有震荡策略，有的话将其状态改为2
                marketmakerdatalist = r1.hvals("MaketMaker_Strategy")
                marketmakerdatalist = [json.loads(i) for i in marketmakerdatalist]
                for data in marketmakerdatalist:
                    if data["type"] == 1 and data["platform"] == platform and data["symbol"] == symbol:
                        data["status"] = 2
                        r1.hset("MaketMaker_Strategy", data["strategyId"], json.dumps(data))
            if status == 2 or status == 3:
                # 查找有没有震荡策略，有的话将其状态改为1，没有的话新增一条
                r1.hdel("MaketMaker_Strategy", strategyId)
                # 查找有没有震荡策略，有的话将其状态改为1,(没有的话新增一条)
                marketmakerdatalist = r1.hvals("MaketMaker_Strategy")
                marketmakerdatalist = [json.loads(i) for i in marketmakerdatalist]
                for data in marketmakerdatalist:
                    if data["type"] == 1 and data["platform"] == platform and data["symbol"] == symbol:
                        data["status"] = 1
                        data["initial_price"] = get_currentprice0(platform, symbol)
                        r1.hset("MaketMaker_Strategy", data["strategyId"], json.dumps(data))
        return jsonify({"success": "true", "data": {"msg": "做市商策略{}{}成功".format(strategyId, flag), "code": 1,
                                                    "data": {"strategyId": strategyId, "status": status}},
                        "errCode": "SUCCESS",
                        "errMsg": "", "sysTime": int(time.time() * 1000)})
    except Exception as e:
        return jsonify({"success": "true", "data": {"msg": "做市商策略{}报错信息{}".format(strategyId, e), "code": 0,
                                                    "data": {"strategyId": strategyId, "status": status}},
                        "errCode": "SUCCESS",
                        "errMsg": "", "sysTime": int(time.time() * 1000)})


# 开启/关闭行情跟随刷单
@app.route('/runFollowStrategy', methods=['get', 'post'])
def runFollowStrategy():
    userUuid = request.form["userUuid"]  # 获取用户唯一id
    strategyId = int(request.form["strategyId"])  # 获取策略id
    status = int(request.form["status"])  # 获取状态1开启，0关闭，3手动停止
    followdata = json.loads(request.form["str"])["data"]  # 策略详情
    print(followdata)
    try:
        flag = "启动"
        if status == 1:
            userUuid = followdata["userUuid"]  # 获取用户唯一id
            strategyId = followdata["strategyId"]  # 获取策略id
            status = followdata["status"]  # 获取状态1开启，0关闭，3手动停止
            apiAccountId1 = followdata["apiAccountId1"]  # 子账户id 1
            apiAccountId2 = followdata["apiAccountId2"]  # 子账户id 2
            platform = followdata["platform"]  # 交易所
            symbol = followdata["symbol"]  # 交易对
            tradeCoin = symbol.split("_")[0]  # 交易币
            valueCoin = symbol.split("_")[1]  # 计价币
            amount_min = followdata["amount_min"]  # 每笔最小交易量
            amount_max = followdata["amount_max"]  # 每笔最大交易量
            # frequency = followdata["frequency"]  # 交易频次
            r1.hset("Follow_Strategy", strategyId, json.dumps(followdata))
        if status == 2:
            flag = "停止"
            r1.hdel("Follow_Strategy", strategyId)
        return jsonify({"success": "true", "data": {"msg": "做市高频刷单策略{}{}成功".format(strategyId, flag), "code": 1,
                                                    "data": {"strategyId": strategyId, "status": status}},
                        "errCode": "SUCCESS",
                        "errMsg": "", "sysTime": int(time.time() * 1000)})
    except Exception as e:
        return jsonify({"success": "true", "data": {"msg": "做市高频刷单策略{}报错信息{}".format(strategyId, e), "code": 0,
                                                    "data": {"strategyId": strategyId, "status": status}},
                        "errCode": "SUCCESS",
                        "errMsg": "", "sysTime": int(time.time() * 1000)})


# 开启/关闭市场深度策略
@app.route('/runMarketDepthStrategy', methods=['get', 'post'])
def runMarketDepthStrategy():
    userUuid = request.form["userUuid"]  # 获取用户唯一id
    strategyId = int(request.form["strategyId"])  # 获取策略id
    status = int(request.form["status"])  # 获取状态1开启，2关闭
    marketdata = json.loads(request.form["str"])["data"]  # 策略详情
    print(marketdata)
    try:
        flag = "启动"
        if status == 1:
            r1.hset("Market_Strategy", strategyId, json.dumps(marketdata))
        if status == 2:
            flag = "停止"
            marketdata["status"] = 2
            r1.hset("Market_Strategy", strategyId, json.dumps(marketdata))
        return jsonify({"success": "true", "data": {"msg": "市值流动性策略{}{}成功".format(strategyId, flag), "code": 1,
                                                    "data": {"strategyId": strategyId, "status": status}},
                        "errCode": "SUCCESS",
                        "errMsg": "", "sysTime": int(time.time() * 1000)})
    except Exception as e:
        return jsonify({"success": "true", "data": {"msg": "市值流动性策略{}报错信息{}".format(strategyId, e), "code": 0,
                                                    "data": {"strategyId": strategyId, "status": status}},
                        "errCode": "SUCCESS",
                        "errMsg": "", "sysTime": int(time.time() * 1000)})


# 开启/关闭网格策略
@app.route('/runGridStrategy', methods=['get', 'post'])
def runGridStrategy():
    try:
        userUuid = request.form["userUuid"]  # 获取用户唯一id
        apiAccountId = int(request.form["apiAccountId"])  # 获取子账户id
        strategyId = int(request.form["strategyId"])  # 获取网格策略id
        status = int(request.form["status"])  # 获取状态1开启，0关闭，3手动停止
        stopType = int(request.form["stopType"])  # 1 停止不平仓 2 停止平仓
        strategyType = int(request.form["strategyType"])  # 策略类型，1简易2灵活3单边
        griddata = json.loads(request.form["str"])["data"]  # 策略详情
        if strategyType == 1 or strategyType == 3:
            platform = griddata["platform"]  # 平台
            symbol = griddata["symbol"]  # 交易对
            tradeCoin = symbol.split("_")[0]  # 交易币
            entryPrice = griddata["entryPrice"]  # 入场价格
            lowprice = griddata["stopLossPrice"]  # 止损价
            counterCoinName = symbol.split("_")[0]  # 交易币
            valueCoinName = symbol.split("_")[1]  # 计价币
            initialTradeCoin = griddata["initialTradeCoin"]  # 用于运行策略的交易币数量
            initialValCoin = griddata["initialValCoin"]  # 用于运行策略的计价币数量
            initialCoin = griddata["existingUsdt"]  # 折合成计价币
            gap = griddata["gridSpacing"]  # 网格间距
            count = initialTradeCoin / gap_amount[symbol]
            try:
                x, y = str(count).split('.')
                count = float(x + '.' + y[0:amountlimit[symbol][platform]])
            except Exception as e:
                info = "单笔下单量为整数，无需截取小数位"
                print(info)
            griddata["minTradeQuantity"] = count  # 计算每次下单量
            print("计算出每次下单量：{}".format(count))
            if status == 1:
                currentprice = get_currentprice1(platform, symbol)
                if entryPrice - currentprice >= gap or currentprice - entryPrice >= gap:  # 如果用户的入场价与现价相差太大
                    griddata["entryPrice"] = currentprice  # 入场价改为当前价
                    i = "您的入场价{}与当前价{}相差太大，已为您调整到最合适的价格部署网格".format(entryPrice, currentprice)
                    print(i)
                res = gridbegin(griddata, userUuid, strategyId)  # 网格部署
                if res == 1:
                    if strategyType == 3:
                        # 如果是单边入金策略，则需要获取出场类型，并加入到redis策略里
                        data1 = json.loads(request.form["str"])["data1"]  # 这一条是单边策略详情
                        griddata["finishType"] = data1["finishType"]  # 出场类型
                        griddata["initialTradeCoin0"] = data1["initialTradeCoin"]  # 初始总交易币数（区别于用于网格的交易币数）
                        griddata["initialValCoin0"] = data1["initialValCoin"]  # 初始总计价币数（区别于用于网格的计价币数）
                        griddata["expectedProfitRate"] = data1["expectedProfitRate"]  # 预期收益（1默认出场时此值为0，2固定收益出场）
                        griddata["entryPrice0"] = data1["basePrice"]  # 单边入金策略初始价
                        griddata["direction"] = data1["direction"]  # 单边策略方向
                    r2.hset("gridstrategy", strategyId, json.dumps(griddata))  # 策略加入redis
                    r2.hset("gridpausetags", strategyId, 0)  # 创建一个策略暂停标记
                    print("开启网格策略{}".format(strategyId))
                    return jsonify({"success": "true", "data": {"msg": "开启网格策略{}".format(strategyId), "code": 1,
                                                                "data": {"strategyId": strategyId, "status": status}},
                                    "errCode": "SUCCESS",
                                    "errMsg": "", "sysTime": int(round(time.time() * 1000))})
                elif res == 0:
                    # 如果初始化部署网格单不成功，撤销已经下好的买卖单
                    try:
                        conn = POOL_grid.connection()
                        cur = conn.cursor()
                        cur.execute("select buyorderid,sellorderid from t_gridtrade where strategyId=%s", (strategyId,))
                        selectres = cur.fetchall()
                        cur.execute("delete from t_gridtrade where strategyId=%s", (strategyId,))
                        conn.commit()
                        cur.close()
                        conn.close()
                        for i in selectres:
                            if i[0] is not None:
                                res = requests.post(Cancel_url,
                                                    data={"direction": 1, "symbol": symbol, "platform": platform,
                                                          "orderId": i[0], "apiAccountId": apiAccountId,
                                                          "userUuid": userUuid,
                                                          "source": 4, "strategyId": strategyId})
                            if i[1] is not None:
                                res = requests.post(Cancel_url,
                                                    data={"direction": 2, "symbol": symbol, "platform": platform,
                                                          "orderId": i[1], "apiAccountId": apiAccountId,
                                                          "userUuid": userUuid,
                                                          "source": 4, "strategyId": strategyId})
                    except Exception as e:
                        info = "网格策略{}初始化部署买卖单不成功后，撤单失败{}".format(strategyId, e)
                        logger.error(info)
                    return jsonify(
                        {"success": "true", "data": {"msg": "交易所接口不通导致开启网格策略{}失败".format(strategyId), "code": 0,
                                                     "data": {"strategyId": strategyId, "status": status}},
                         "errCode": "SUCCESS",
                         "errMsg": "", "sysTime": int(round(time.time() * 1000))})

            else:
                try:
                    r2.hdel("gridstrategy", strategyId)  # 从redis删除该策略
                    r2.hdel("gridpausetags", strategyId)  # 删除暂停标记
                    r2.delete(*r2.keys('*' + str(strategyId) + '*'))  # 从redis删除该策略订单
                except Exception as e:  # 防止redis撤销出问题
                    errorinfo = "策略{}撤单时redis出问题{}".format(strategyId, e)
                    logger.error(errorinfo)
                info = "手动停止网格策略，系统正在为你撤销{}平台所有的委托单".format(platform)
                print(info)
                conn = POOL_grid.connection()
                cur = conn.cursor()
                cur.execute("select sellorderid from t_gridtrade where strategyId=%s and sellstatus=0", (strategyId,))
                selectres1 = cur.fetchall()
                cur.execute("select buyorderid from t_gridtrade where strategyId=%s and buystatus=0", (strategyId,))
                selectres2 = cur.fetchall()
                cur.execute(
                    "select sum(profit),sum(netprofit) from t_gridtrade where strategyId=%s and sellstatus=1 and (buystatus=1 or buystatus is NULL)",
                    (strategyId,))
                profitres = cur.fetchone()
                cur.execute("select count(*),sum(buycount) from t_gridtrade where strategyId=%s and buystatus=1",
                            (strategyId,))
                buyquery = cur.fetchone()
                cur.execute("select count(*),sum(sellcount) from t_gridtrade where strategyId=%s and sellstatus=1",
                            (strategyId,))
                sellquery = cur.fetchone()
                cur.execute(
                    "select avg(sellprice) from t_gridtrade where strategyId=%s and sellstatus=1 and (buystatus=2 or buystatus is NULL)",
                    (strategyId,))
                unfinishbuy = cur.fetchone()
                cur.execute(
                    "select avg(buyprice) from t_gridtrade where strategyId=%s and buystatus=1 and (sellstatus=2 or sellstatus=0 or sellstatus is NULL)",
                    (strategyId,))
                unfinishsell = cur.fetchone()
                cur.close()
                conn.close()
                profit = 0
                profitrate = 0
                netprofit = 0
                netprofitrate = 0
                buyfinishamount = 0
                sellfinishamount = 0
                if profitres != (None, None):
                    profit = round(float(profitres[0]), 4)  # 网格收益
                    profitrate = round(profit / initialCoin, 6)
                    netprofit = round(float(profitres[1]), 4)
                    netprofitrate = round(netprofit / initialCoin, 6)  # 网格净收益（扣除手续费)
                buyfinishcount = buyquery[0]
                sellfinishcount = sellquery[0]
                if buyquery[1] is not None:
                    buyfinishamount = round(float(buyquery[1]), 4)
                if sellquery[1] is not None:
                    sellfinishamount = round(float(sellquery[1]), 4)
                difamount = round(sellfinishamount - buyfinishamount, 4)
                print("已成交买单数{}".format(buyfinishcount))
                print("已成交买单总量{}".format(buyfinishamount))
                print("已成交卖单数{}".format(sellfinishcount))
                print("已成交卖单数量{}".format(sellfinishamount))
                print("买卖单差量{}".format(difamount))
                unfinishprice = 0
                if difamount > 0:
                    unfinishprice = float(unfinishbuy[0]) - gap
                    print("未买入均价{}".format(unfinishprice))
                if difamount < 0:
                    unfinishprice = float(unfinishsell[0]) + gap
                    print("未卖出均价{}".format(unfinishprice))
                if difamount == 0:
                    unfinishprice = 0
                sellorderlist = [i[0] for i in selectres1]  # 未成交卖单
                buyorderlist = [i[0] for i in selectres2]  # 未成交买单
                # 查询策略未成交单是否成交,没有成交则撤单
                tlist = []
                for orderid in sellorderlist + buyorderlist:
                    tlist.append(Thread(target=cancelgridorders,
                                        args=(
                                            userUuid, apiAccountId, strategyId, platform, symbol, orderid,
                                            sellorderlist,
                                            buyorderlist)))
                for t in tlist:
                    t.start()
                for t in tlist:
                    t.join()
                profitinfo = "手动停止网格策略{}，统计收益：网格收益{}，净收益{}".format(strategyId, profit, netprofit)
                print(profitinfo)
                if stopType == 2:
                    print("网格策略{}停止时平仓处理".format(strategyId))
                    # 查询资产
                    remainres = requests.get(Remain_url, params={"userUuid": userUuid, "apiAccountId": apiAccountId})
                    remaindict = json.loads(remainres.content.decode())
                    TradeCoin_amount = [i["over"] for i in remaindict["response"] if i["coin"] == tradeCoin][0]
                    # 平仓处理
                    clear_grid_remain(userUuid, apiAccountId, strategyId, platform, symbol, TradeCoin_amount)
                return jsonify({"success": "true", "data": {"msg": "手动停止网格策略{}并返回盈利信息".format(strategyId), "code": 1,
                                                            "data": {"userUuid": userUuid,
                                                                     "apiAccountId": apiAccountId,
                                                                     "strategyId": strategyId,
                                                                     "status": status,
                                                                     "profits": profit, "profitRate": profitrate,
                                                                     "netprofit": netprofit,
                                                                     "netprofitRate": netprofitrate,
                                                                     "totalprofit": 0, "totalprofitRate": 0,
                                                                     "grossprofit": 0,
                                                                     "buyfinishcount": buyfinishcount,
                                                                     "buyfinishamount": buyfinishamount,
                                                                     "sellfinishcount": sellfinishcount,
                                                                     "sellfinishamount": sellfinishamount,
                                                                     "difamount": difamount,
                                                                     "unfinishprice": unfinishprice,
                                                                     "clientSource": 1, "version": 1.3,
                                                                     "client": "python",
                                                                     "ticket": "7b3d556d-d45f-4595-be5f-a05c2fc4fcf913"}},
                                "errCode": "SUCCESS",
                                "errMsg": "", "sysTime": int(round(time.time() * 1000))})
        elif strategyType == 2:
            platform = griddata["platform"]  # 平台
            symbol = griddata["symbol"]  # 交易对
            tradeCoin = symbol.split("_")[0]
            entryPrice = griddata["entryPrice"]  # 入场价格
            lowprice = griddata["stopLossPrice"]  # 止损价
            counterCoinName = symbol.split("_")[0]  # 交易币
            valueCoinName = symbol.split("_")[1]  # 计价币
            initialTradeCoin = griddata["initialTradeCoin"]  # 用于运行策略的交易币数量
            initialValCoin = griddata["initialValCoin"]  # 用于运行策略的计价币数量
            initialCoin = griddata["existingUsdt"]  # 折合成计价币
            gap = griddata["gridSpacing"]  # 网格间距
            makerFee = griddata["makerFee"]
            count = griddata["minTradeQuantity"]  # 每笔下单量（需要截取小数位）
            try:
                x, y = str(count).split('.')
                count = float(x + '.' + y[0:amountlimit[symbol][platform]])
            except Exception as e:
                info = "单笔下单量为整数，无需截取小数位"
                print(info)
            griddata["minTradeQuantity"] = count  # 更新每笔下单量
            print("计算出每次下单量：{}".format(count))
            if status == 1:
                currentprice = get_currentprice1(platform, symbol)
                if entryPrice - currentprice >= gap or currentprice - entryPrice >= gap:  # 如果用户的入场价与现价相差太大
                    griddata["entryPrice"] = currentprice  # 入场价改为当前价
                    i = "您的入场价{}与当前价{}相差太大，已为您调整到最合适的价格部署网格".format(entryPrice, currentprice)
                    print(i)
                res = gridbegin2(griddata, userUuid, strategyId)  # 网格部署
                if res == 1:
                    r2.hset("gridstrategy", strategyId, json.dumps(griddata))  # 策略加入redis
                    r2.hset("gridpausetags", strategyId, 0)  # 创建一个策略暂停标记
                    print("开启网格策略{}".format(strategyId))
                    return jsonify({"success": "true", "data": {"msg": "开启网格策略{}".format(strategyId), "code": 1,
                                                                "data": {"strategyId": strategyId, "status": status}},
                                    "errCode": "SUCCESS", "errMsg": "", "sysTime": int(round(time.time() * 1000))})
                elif res == 0:
                    # 如果初始化部署网格单不成功，撤销已经下好的买卖单
                    try:
                        conn = POOL_grid.connection()
                        cur = conn.cursor()
                        cur.execute("select buyorderid,sellorderid from t_gridtrade where strategyId=%s", (strategyId,))
                        selectres = cur.fetchall()
                        cur.execute("delete from t_gridtrade where strategyId=%s", (strategyId,))
                        conn.commit()
                        cur.close()
                        conn.close()
                        for i in selectres:
                            if i[0] is not None:
                                res = requests.post(Cancel_url,
                                                    data={"direction": 1, "symbol": symbol, "platform": platform,
                                                          "orderId": i[0], "apiAccountId": apiAccountId,
                                                          "userUuid": userUuid,
                                                          "source": 4, "strategyId": strategyId})
                            if i[1] is not None:
                                res = requests.post(Cancel_url,
                                                    data={"direction": 2, "symbol": symbol, "platform": platform,
                                                          "orderId": i[1], "apiAccountId": apiAccountId,
                                                          "userUuid": userUuid,
                                                          "source": 4, "strategyId": strategyId})
                    except Exception as e:
                        info = "网格策略{}初始化部署买卖单不成功后，撤单失败{}".format(strategyId, e)
                        logger.error(info)
                    return jsonify(
                        {"success": "true", "data": {"msg": "交易所接口不通导致开启网格策略{}失败".format(strategyId), "code": 0,
                                                     "data": {"strategyId": strategyId, "status": status}},
                         "errCode": "SUCCESS",
                         "errMsg": "", "sysTime": int(round(time.time() * 1000))})

            else:
                try:
                    r2.hdel("gridstrategy", strategyId)  # 从redis删除该策略
                    r2.hdel("gridpausetags", strategyId)  # 删除暂停标记
                    r2.delete(*r2.keys('*' + str(strategyId) + '*'))  # 从redis删除该策略订单
                except Exception as e:  # 防止redis撤销出问题
                    errorinfo = "策略{}撤单时redis出问题{}".format(strategyId, e)
                    logger.error(errorinfo)
                try:
                    info = "手动停止网格策略，系统正在为你撤销{}平台所有的委托单".format(platform)
                    print(info)
                    logger.info(info)
                    conn = POOL_grid.connection()
                    cur = conn.cursor()
                    cur.execute("select sellorderid from t_gridtrade where strategyId=%s and sellstatus=0",
                                (strategyId,))
                    selectres1 = cur.fetchall()
                    cur.execute("select buyorderid from t_gridtrade where strategyId=%s and buystatus=0", (strategyId,))
                    selectres2 = cur.fetchall()
                    cur.execute(
                        "select sum(profit),sum(netprofit) from t_gridtrade where strategyId=%s and sellstatus=1 and ("
                        "buystatus=1 or buystatus is NULL)",
                        (strategyId,))
                    profitres = cur.fetchone()
                    cur.execute("select count(*),sum(buycount) from t_gridtrade where strategyId=%s and buystatus=1",
                                (strategyId,))
                    buyquery = cur.fetchone()
                    cur.execute("select count(*),sum(sellcount) from t_gridtrade where strategyId=%s and sellstatus=1",
                                (strategyId,))
                    sellquery = cur.fetchone()
                    cur.execute(
                        "select avg(sellprice) from t_gridtrade where strategyId=%s and sellstatus=1 and (buystatus=2 or buystatus is NULL)",
                        (strategyId,))
                    unfinishbuy = cur.fetchone()
                    cur.execute(
                        "select avg(buyprice) from t_gridtrade where strategyId=%s and buystatus=1 and (sellstatus=2 or sellstatus=0 or sellstatus is NULL)",
                        (strategyId,))
                    unfinishsell = cur.fetchone()
                    cur.close()
                    conn.close()
                    profit = 0
                    profitrate = 0
                    netprofit = 0
                    netprofitrate = 0
                    buyfinishamount = 0
                    sellfinishamount = 0
                    if profitres != (None, None):
                        profit = round(float(profitres[0]), 4)  # 网格收益
                        profitrate = round(profit / initialCoin, 6)
                        netprofit = round(float(profitres[1]), 4)
                        netprofitrate = round(netprofit / initialCoin, 6)  # 网格净收益（扣除手续费)
                    buyfinishcount = buyquery[0]
                    sellfinishcount = sellquery[0]
                    if buyquery[1] is not None:
                        buyfinishamount = round(float(buyquery[1]), 4)
                    if sellquery[1] is not None:
                        sellfinishamount = round(float(sellquery[1]), 4)
                    difamount = round(sellfinishamount - buyfinishamount, 4)
                    print("已成交买单数{}".format(buyfinishcount))
                    print("已成交买单总量{}".format(buyfinishamount))
                    print("已成交卖单数{}".format(sellfinishcount))
                    print("已成交卖单数量{}".format(sellfinishamount))
                    print("买卖单差量{}".format(difamount))
                    unfinishprice = 0
                    if difamount > 0:
                        unfinishprice = float(unfinishbuy[0]) - gap
                        print("未买入均价{}".format(unfinishprice))
                    if difamount < 0:
                        unfinishprice = float(unfinishsell[0]) + gap
                        print("未卖出均价{}".format(unfinishprice))
                    if difamount == 0:
                        unfinishprice = 0
                    sellorderlist = [i[0] for i in selectres1]  # 未成交卖单
                    buyorderlist = [i[0] for i in selectres2]  # 未成交买单
                    # 查询策略未成交单是否成交,没有成交则撤单
                    tlist = []
                    for orderid in sellorderlist + buyorderlist:
                        tlist.append(Thread(target=cancelgridorders,
                                            args=(
                                                userUuid, apiAccountId, strategyId, platform, symbol, orderid,
                                                sellorderlist,
                                                buyorderlist)))
                    for t in tlist:
                        t.start()
                    for t in tlist:
                        t.join()
                    profitinfo = "手动停止网格策略{}，统计收益：网格收益{}，净收益{}".format(strategyId, profit, netprofit)
                    print(profitinfo)
                    if stopType == 2:
                        print("网格策略{}停止时平仓处理".format(strategyId))
                        # 查询资产，平仓处理
                        remainres = requests.get(Remain_url,
                                                 params={"userUuid": userUuid, "apiAccountId": apiAccountId})
                        remaindict = json.loads(remainres.content.decode())
                        TradeCoin_amount = [i["over"] for i in remaindict["response"] if i["coin"] == tradeCoin][0]
                        clear_grid_remain(userUuid, apiAccountId, strategyId, platform, symbol, TradeCoin_amount)
                    return jsonify(
                        {"success": "true", "data": {"msg": "手动停止网格策略{}并返回盈利信息".format(strategyId), "code": 1,
                                                     "data": {"userUuid": userUuid,
                                                              "apiAccountId": apiAccountId,
                                                              "strategyId": strategyId,
                                                              "status": status,
                                                              "profits": profit, "profitRate": profitrate,
                                                              "netprofit": netprofit,
                                                              "netprofitRate": netprofitrate,
                                                              "totalprofit": 0, "totalprofitRate": 0,
                                                              "grossprofit": 0,
                                                              "buyfinishcount": buyfinishcount,
                                                              "buyfinishamount": buyfinishamount,
                                                              "sellfinishcount": sellfinishcount,
                                                              "sellfinishamount": sellfinishamount,
                                                              "difamount": difamount,
                                                              "unfinishprice": unfinishprice,
                                                              "clientSource": 1, "version": 1.3,
                                                              "client": "python",
                                                              "ticket": "7b3d556d-d45f-4595-be5f-a05c2fc4fcf913"}},
                         "errCode": "SUCCESS",
                         "errMsg": "", "sysTime": int(round(time.time() * 1000))})
                except Exception as e:
                    i = '用户{}网格策略{}手动停止报错{}'.format(userUuid, strategyId, e)
                    logger.error(i)


    except Exception as e:
        i = "启动网格报错{}".format(e)
        print(i)
        logger.error(i)
        return jsonify({"success": "true", "data": {"msg": "网格策略启动/停止失败,报错信息{}".format(e), "code": 0, "data": {}},
                        "errCode": "SUCCESS",
                        "errMsg": "", "sysTime": int(round(time.time() * 1000))})


# 开启/关闭对冲套利策略
@app.route('/runHedgeStrategy', methods=['post'])
def runHedgeStrategy():
    """
        对冲策略API
        调用此接口启动、停止对冲策略
        ---
        tags:
          - 对冲策略API
        description:
          - 对冲策略接口，json格式
        parameters:
          - name: body
            in: body
            required: true
            schema:
              id: 对冲策略API
              required:
                - userUuid
                - strategyId
                - status
                - symbol
                - platform1
                - apiAccountId1
                - takerFee1
                - initialTradeCoin1
                - initialValCoin1
                - platform2
                - apiAccountId2
                - takerFee2
                - initialTradeCoin2
                - initialValCoin2
              properties:
                userUuid:
                  type: string
                  description: 用户ID.
                strategyId:
                  type: int
                  description: 策略ID.
                status:
                  type: int
                  description: 1开启2停止.
                symbol:
                  type: string
                  description: 交易对.
                platform1:
                  type: string
                  description: 交易所1.
                apiAccountId1:
                  type: int
                  description: 交易所1对应的apiId.
                takerFee1:
                  type: float
                  description: 交易所1对应的吃单手续费.
                initialTradeCoin1:
                  type: float
                  description: 交易所1对应的初始交易币数量.
                initialValCoin1:
                  type: float
                  description: 交易所1对应的初始计价币币数量.
                platform2:
                  type: string
                  description: 交易所2.
                apiAccountId2:
                  type: int
                  description: 交易所2对应的apiId.
                takerFee2:
                  type: float
                  description: 交易所2对应的吃单手续费.
                initialTradeCoin2:
                  type: float
                  description: 交易所2对应的初始交易币数量.
                initialValCoin2:
                  type: float
                  description: 交易所2对应的初始计价币币数量.
        responses:
          200:
            description: 创建成功
          500:
            description: 创建失败，核对参数
        """
    userUuid = request.form["userUuid"]  # 获取用户唯一id
    status = int(request.form["status"])
    strategyId = int(request.form["strategyId"])
    symbol = request.form["symbol"]  # 交易对
    # 交易所1信息
    apiAccountId1 = int(request.form["apiAccountId1"])
    platform1 = request.form["platform1"]
    takerFee1 = float(request.form["takerFee1"])
    initialTradeCoin1 = float(request.form["initialTradeCoin1"])
    initialValCoin1 = float(request.form["initialValCoin1"])
    # 交易所2信息
    apiAccountId2 = int(request.form["apiAccountId2"])
    platform2 = request.form["platform2"]
    takerFee2 = float(request.form["takerFee2"])
    initialTradeCoin2 = float(request.form["initialTradeCoin2"])
    initialValCoin2 = float(request.form["initialValCoin2"])
    hedgedata = {
        "apiAccountId1": apiAccountId1,
        "apiAccountId2": apiAccountId2,
        "initialTradeCoin1": initialTradeCoin1,
        "initialTradeCoin2": initialTradeCoin2,
        "initialValCoin1": initialValCoin1,
        "initialValCoin2": initialValCoin2,
        "platform1": platform1,
        "platform2": platform2,
        "status": status,
        "strategyId": strategyId,
        "symbol": symbol,
        "takerFee1": takerFee1,
        "takerFee2": takerFee2,
        "userUuid": userUuid
    }
    print(hedgedata)
    try:
        if status == 1:
            # 获取当前价，计算初始资金
            currentprice = get_currentprice1(platform1, symbol)
            totalcoin = (initialTradeCoin1 + initialTradeCoin2) * currentprice + initialValCoin1 + initialValCoin2
            hedgedata["totalcoin"] = totalcoin
            r2.hset("Hedge_Strategy", strategyId, json.dumps(hedgedata))
            return jsonify({"success": "true", "data": {"msg": "对冲套利策略{}开启成功".format(strategyId), "code": 1,
                                                        "data": {"strategyId": strategyId, "status": status}},
                            "errCode": "SUCCESS",
                            "errMsg": "", "sysTime": int(time.time() * 1000)})
        if status == 2:
            totalcoin = json.loads(r2.hget("Hedge_Strategy", strategyId))["totalcoin"]
            r2.hdel("Hedge_Strategy", strategyId)
            # 数据库查询该策略盈利
            profit = 0
            profitRate = 0
            try:
                conn = POOL.connection()
                cur = conn.cursor()
                cur.execute(
                    "select sum(profit) from hedgelist where strategyId=%s",
                    (strategyId,))
                profitres = cur.fetchone()
                profit = float(profitres[0]) / 2
                profitRate = profit / totalcoin
                print(profit)
                cur.close()
                conn.close()
            except Exception as e:
                errorinfo = e
                print(errorinfo)
            return jsonify({"success": "true", "data": {"msg": "对冲套利策略{}停止成功".format(strategyId), "code": 1,
                                                        "data": {"strategyId": strategyId, "status": status,
                                                                 "profit": profit, "profitRate": profitRate}},
                            "errCode": "SUCCESS",
                            "errMsg": "", "sysTime": int(time.time() * 1000)})
    except Exception as e:
        return jsonify({"success": "true", "data": {"msg": "对冲套利策略{}报错信息{}".format(strategyId, e), "code": 0,
                                                    "data": {"strategyId": strategyId, "status": status}},
                        "errCode": "SUCCESS",
                        "errMsg": "", "sysTime": int(time.time() * 1000)})


# 开启/关闭追踪策略
@app.route('/runTraceStrategy', methods=['get', 'post'])
def runTraceStrategy():
    userUuid = request.form["userUuid"]  # 获取用户唯一id
    apiAccountId = int(request.form["apiAccountId"])  # 获取子账户id
    strategyId = int(request.form["strategyId"])  # 获取网格策略id
    status = int(request.form["status"])  # 获取状态1开启，3停止，5防错停止
    direction = int(request.form["direction"])
    tracedata = json.loads(request.form["str"])["data"]  # 策略详情
    tracedata["strategyType"] = 1  # 定义策略类型为1，与组合策略分开
    print(status, direction, tracedata)
    try:
        if status == 1:
            tracedata["touchTags"] = 0  # 初始触发标记为0
            if direction == 2:
                tracedata["mostPrice"] = 0
                r2.hset("tracestrategysell", strategyId, json.dumps(tracedata))  # 策略加入redis
            if direction == 1:
                tracedata["mostPrice"] = tracedata["basePrice"]  # 默认为0，此处应改为baseprice
                r2.hset("tracestrategybuy", strategyId, json.dumps(tracedata))  # 长线追盈策略（买）存入redis
            print("用户{}开启长线追盈策略{}".format(userUuid, strategyId))
            return jsonify({"success": "true", "data": {"msg": "用户{}开启追踪委托策略{}".format(userUuid, strategyId), "code": 1,
                                                        "data": {"strategyId": strategyId, "status": status}},
                            "errCode": "SUCCESS",
                            "errMsg": "", "sysTime": int(round(time.time() * 1000))})
        else:
            if direction == 2:
                r2.hdel("tracestrategysell", strategyId)  # 从redis删除该策略
            if direction == 1:
                r2.hdel("tracestrategybuy", strategyId)  # 从redis删除该策略
            return jsonify({"success": "true", "data": {"msg": "用户{}停止追踪委托策略{}".format(userUuid, strategyId), "code": 1,
                                                        "data": {"strategyId": strategyId, "status": status}},
                            "errCode": "SUCCESS",
                            "errMsg": "", "sysTime": int(round(time.time() * 1000))})
    except Exception as e:
        return jsonify(
            {"success": "true", "data": {"msg": "用户{}开启/停止追踪委托策略{}失败，报错信息{}".format(userUuid, strategyId, e), "code": 0,
                                         "data": {"strategyId": strategyId, "status": status}},
             "errCode": "SUCCESS",
             "errMsg": "", "sysTime": int(round(time.time() * 1000))})


# 动态平衡策略
@app.route('/runBalanceStrategy', methods=['get', 'post'])
def runBalanceStrategy():
    userUuid = request.form["userUuid"]  # 获取用户userUuid
    apiAccountId = int(request.form["apiAccountId"])  # 获取用户子账户id
    strategyId = int(request.form["strategyId"])  # 获取网格策略id
    status = int(request.form["status"])  # 获取状态1开启，2手动停止，3止损停止，4防错停止。
    directionType = int(request.form["directionType"])  # 交易方向，0为双向入金，1为usdt单边入金需先买，2为btc单边入金需先卖
    balancestrategydata = json.loads(request.form["str"])["data"]  # 策略详情
    print(balancestrategydata)
    platform = balancestrategydata["platform"]  # 交易平台
    symbol = balancestrategydata["symbol"]  # 交易对
    entryPrice = balancestrategydata["entryPrice"]  # 入场价
    tradeCoin = symbol.split("_")[0]
    valCoin = symbol.split("_")[1]
    initialTradeCoin = balancestrategydata["initialTradeCoin"]  # 用于策略的初始交易币数量
    initialValCoin = balancestrategydata["initialValCoin"]  # 用于策略的初始计价币数量
    # existingUsdt = balancestrategydata["existingUsdt"]  # 用于策略的币折合成计价币
    # initialTradeAssets = balancestrategydata["initialTradeAssets"]  # 账户初始交易币
    # initialValAssets = balancestrategydata["initialValAssets"]  # 账户初始计价币
    initialTotalAssets = balancestrategydata["initialTotalAssets"]  # 账户初始总资产
    spacingRatio = balancestrategydata["spacingRatio"]  # 间距比例，默认0.5%（0.005）
    if status == 1:  # 开启
        try:
            if directionType == 0:
                res = balancestrategy_begin(userUuid, apiAccountId, strategyId, platform, symbol, entryPrice,
                                            initialTradeCoin, initialValCoin,
                                            spacingRatio)
                if res == 1:
                    print("用户{}子账户{}开启动态平衡策略{}".format(userUuid, apiAccountId, strategyId))
                    r2.hset("balancestrategy", strategyId, json.dumps(balancestrategydata))
            elif directionType == 1:
                pass
            elif directionType == 2:
                pass
            return jsonify({"success": "true",
                            "data": {"msg": "用户{}子账户{}开启动态平衡策略{}".format(userUuid, apiAccountId, strategyId), "code": 1,
                                     "data": {"strategyId": strategyId, "status": status}},
                            "errCode": "SUCCESS",
                            "errMsg": "", "sysTime": int(round(time.time() * 1000))})
        except Exception as e:
            return jsonify({"success": "true",
                            "data": {"msg": "用户{}开启动态平衡策略{}失败，报错信息{}".format(userUuid, strategyId, e), "code": 0,
                                     "data": {"strategyId": strategyId, "status": status}},
                            "errCode": "SUCCESS",
                            "errMsg": "", "sysTime": int(round(time.time() * 1000))})

    else:
        try:
            if directionType == 0:
                # 1、删除策略以及redis缓存的订单信息
                r2.hdel("balancestrategy", strategyId)  # 从redis删除该策略
                r2.delete("balance:sell:{}".format(strategyId))
                r2.delete("balance:buy:{}".format(strategyId))
                # 2、撤销订单并改变数据库状态
                cancel_balancestrategy_orders(userUuid, apiAccountId, strategyId, platform, symbol)
                if status == 3:  # 手动停止并且选择了平仓
                    stopType = int(request.form["stopType"])  # 1为停止时不平仓，2为停止时平仓处理
                    print("动态平衡手动停止,平仓类型{}".format(stopType))
                    logger.info("动态平衡手动停止,平仓类型{}".format(stopType))
                    if stopType == 2:
                        # 查询交易币余额
                        remainres = requests.get(Remain_url,
                                                 params={"userUuid": userUuid, "apiAccountId": apiAccountId})
                        remaindict = json.loads(remainres.content.decode())
                        TradeCoin_amount = [i["over"] for i in remaindict["response"] if i["coin"] == tradeCoin][0]
                        # 平仓处理
                        clear_tradecoin_remains(userUuid, apiAccountId, strategyId, platform, symbol, TradeCoin_amount)
                elif status == 2:  # 或者止损停止时选择了平仓
                    stoplossType = int(request.form["stoplossType"])  # 1为止损停止时不平仓，2为止损停止时平仓处理
                    if stoplossType == 2:
                        # 查询交易币余额
                        remainres = requests.get(Remain_url,
                                                 params={"userUuid": userUuid, "apiAccountId": apiAccountId})
                        remaindict = json.loads(remainres.content.decode())
                        TradeCoin_amount = [i["over"] for i in remaindict["response"] if i["coin"] == tradeCoin][0]
                        # 平仓处理
                        clear_tradecoin_remains(userUuid, apiAccountId, strategyId, platform, symbol, TradeCoin_amount)
                # 统计收益
                remainres = requests.get(Remain_url, params={"userUuid": userUuid, "apiAccountId": apiAccountId})
                remaindict = json.loads(remainres.content.decode())
                TradeCoin_amount = [i["remains"] for i in remaindict["response"] if i["coin"] == tradeCoin][0]
                ValCoin_amount = [i["remains"] for i in remaindict["response"] if i["coin"] == valCoin][0]
                outPrice = get_currentprice1(platform, symbol)
                priceFloating = (outPrice - entryPrice) / entryPrice
                profit = TradeCoin_amount * outPrice + ValCoin_amount - initialTotalAssets  # 按资产折算成usdt计算盈利
                profitRate = profit / initialTotalAssets  # 盈利率
                totalTradecoinFee, totalValcoinFee = get_total_fee(strategyId)  # 获取总手续费
                return jsonify(
                    {"success": "true",
                     "data": {"msg": "用户{}子账户{}停止动态平衡策略{}".format(userUuid, apiAccountId, strategyId), "code": 1,
                              "data": {"strategyId": strategyId, "status": status, "userUuid": userUuid,
                                       "apiAccountId": apiAccountId, "totalTradecoinFee": totalTradecoinFee,
                                       "totalValcoinFee": totalValcoinFee, "profit": str(profit),
                                       "profitRate": str(profitRate),
                                       "outPrice": outPrice, "priceFloating": priceFloating}},
                     "errCode": "SUCCESS", "errMsg": "", "sysTime": int(round(time.time() * 1000))})
            elif directionType == 1:
                pass
            elif directionType == 2:
                pass
        except Exception as e:
            return jsonify({"success": "true",
                            "data": {"msg": "用户{}停止动态平衡策略{}失败，报错信息{}".format(userUuid, strategyId, e), "code": 0,
                                     "data": {"strategyId": strategyId, "status": status}},
                            "errCode": "SUCCESS",
                            "errMsg": "", "sysTime": int(round(time.time() * 1000))})


#  获取网格策略买卖单对比
@app.route('/getGridOrderList', methods=['get', 'post'])
def getGridOrders():
    strategyId = int(request.form["strategyId"])  # 获取网格策略id
    data = get_grid_orderlist(strategyId)
    if data != {}:
        code = 1
        msg = "获取网格策略{}买卖单成功".format(strategyId)
    else:
        code = 0
        msg = "获取网格策略{}买卖单失败".format(strategyId)
    return jsonify(
        {"success": "true", "data": {"msg": msg, "code": code, "data": data},
         "errCode": "SUCCESS", "errMsg": "", "sysTime": int(round(time.time() * 1000))})


# 同步网格买卖单
@app.route('/SynchronizeGridOrderlist', methods=['get', 'post'])
def SynchronizeGridOrderlist():
    id = int(request.form["id"])  # 获取网格策略id
    data = synchronize_grid_orderlist(id)
    if data != {}:
        code = 1
        msg = "同步网格买卖单成功"
    else:
        code = 0
        msg = "同步网格买卖单失败，稍后重试"
    return jsonify(
        {"success": "true", "data": {"msg": msg, "code": code, "data": data},
         "errCode": "SUCCESS", "errMsg": "", "sysTime": int(round(time.time() * 1000))})


@app.route('/CoverStrategy', methods=['get', 'post'])
def runCoverStrategy():
    userUuid = request.form['userUuid']  # 获取用户userUuid
    apiAccountId = int(request.form['apiAccountId'])  # 获取用户子账户id
    strategyId = int(request.form['strategyId'])  # 获取补仓策略id
    status = int(request.form['status'])  # 获取状态1开启，2完成 3.手动停止 4. 防错停止
    strategydata = json.loads(request.form['str'])['data']  # 策略详情
    if status == 1:
        strategydata['flag'] = 0
        r2.hset('Cover_strategy', strategyId, json.dumps(strategydata))
        print("用户{}开启智能人工网格策略{}".format(userUuid, strategyId))
        return jsonify({"success": "true",
                        "data": {
                            "msg": "用户{}子账户{}开启补仓策略{}".format(userUuid, apiAccountId, strategyId),
                            "code": 1,
                            "data": {"strategyId": strategyId, 'status': status}},
                        "errCode": "SUCCESS",
                        "errMsg": "",
                        "sysTime": int(round(time.time() * 1000))}
                       )
    else:
        try:
            profit, profitRate = stopOut(strategyId)
        except:
            profit, profitRate = 0, 0
        return jsonify({"success": "true",
                        "data": {"msg": "用户{}子账户{}停止智能人工网格策略{}".format(userUuid, apiAccountId,
                                                                       strategyId),
                                 "code": 1,
                                 "data": {"strategyId": strategyId, "profit": profit,
                                          "profitRate": profitRate,
                                          "status": status}},
                        "errCode": "SUCCESS",
                        "errMsg": "",
                        "sysTime": int(round(time.time() * 1000))})


# 启动/停止新增策略，10-balance策略，11-MACD策略，12-KDJ策略，13-BOLL策略
@app.route('/runKlineStrategy', methods=['get', 'post'])
def runKlineStrategy():
    userUuid = request.form["userUuid"]  # 获取用户userUuid
    apiAccountId = int(request.form["apiAccountId"])  # 获取用户子账户id
    strategyId = int(request.form["strategyId"])  # 获取网格策略id
    status = int(request.form["status"])  # 获取状态1开启，2手动停止，3止损停止，4防错停止。
    strategyType = int(request.form["strategyType"])
    strategydata = json.loads(request.form["str"])["data"]  # 策略详情
    platform = strategydata["platform"]  # 交易平台
    symbol = strategydata["symbol"]  # 交易对
    strategy_name_dict = {10: "balance", 11: "MACD", 12: "KDJ", 13: "BOLL"}
    strategy_name = strategy_name_dict[strategyType]
    print(userUuid, apiAccountId, strategyId, status, strategyType, strategydata, platform, symbol, strategy_name)
    if status == 1:  # 开启
        entryPrice = get_currentprice1(platform, symbol)
        strategydata['entryPrice'] = entryPrice
        if strategyType == 10:
            strategydata['spacingRatio'] = 0.05  # 定义平衡策略下单间距为5%
            strategydata["init_trade"] = 0  # 定义第一笔买单还未进行
            strategydata['stoplossRate'] = 0.5  # 止损比例50%
        r2.hset("{}_strategy".format(strategy_name), strategyId, json.dumps(strategydata))
        return jsonify({"success": "true",
                        "data": {
                            "msg": "用户{}子账户{}开启{}策略{}".format(userUuid, apiAccountId, strategy_name,
                                                              strategyId),
                            "code": 1,
                            "data": {"strategyId": strategyId, "entryPrice": entryPrice, "status": status}},
                        "errCode": "SUCCESS",
                        "errMsg": "", "sysTime": int(round(time.time() * 1000))})
    else:
        outPrice = get_currentprice1(platform, symbol)
        tradeCoin = symbol.split("_")[0]  # 交易币
        if strategyType == 10:  # 动态平衡需撤单
            # 撤单
            cancel_balancestrategy_orders(userUuid, apiAccountId, strategyId, platform, symbol)
            r2.delete("balance:sell:{}".format(strategyId))
            r2.delete("balance:buy:{}".format(strategyId))
            # 查询交易币余额
            remainres = requests.get(Remain_url, params={"userUuid": userUuid, "apiAccountId": apiAccountId})
            remaindict = json.loads(remainres.content.decode())
            TradeCoin_amount = [i["over"] for i in remaindict["response"] if i["coin"] == tradeCoin][0]
            # 平仓处理
            clear_tradecoin_remains(userUuid, apiAccountId, strategyId, platform, symbol, TradeCoin_amount)
        # 查询此时的资产
        remainres = requests.get(Remain_url, params={"userUuid": userUuid, "apiAccountId": apiAccountId})
        remaindict = json.loads(remainres.content.decode())
        tradeCoin = symbol.split("_")[0]  # 交易币
        valCoin = symbol.split("_")[1]  # 计价币
        TradeCoin_amount = [i["remains"] for i in remaindict["response"] if i["coin"] == tradeCoin][0]
        ValCoin_amount = [i["remains"] for i in remaindict["response"] if i["coin"] == valCoin][0]
        if strategydata['initialTotalAssets'] > 1000:
            profit = TradeCoin_amount * outPrice + ValCoin_amount - strategydata['initialTotalAssets']
            profitRate = profit / strategydata['initialTotalAssets']
        else:
            profit = profitRate = 0
        r2.hdel("{}_strategy".format(strategy_name), strategyId)  # 从redis删除该策略
        return jsonify({"success": "true",
                        "data": {"msg": "用户{}子账户{}停止{}策略{}".format(userUuid, apiAccountId, strategy_name,
                                                                   strategyId),
                                 "code": 1,
                                 "data": {"strategyId": strategyId, "outPrice": outPrice, "profit": profit,
                                          "profitRate": profitRate,
                                          "status": status}},
                        "errCode": "SUCCESS",
                        "errMsg": "", "sysTime": int(round(time.time() * 1000))})


# 启动跟单
@app.route('/runCopy', methods=['get', 'post'])
def runCopy():
    userUuid = request.form["userUuid"]  # 获取用户userUuid
    apiAccountId = int(request.form["apiAccountId"])  # 获取用户子账户id
    strategyId = int(request.form["strategyId"])  # 获取策略id
    followStrategyId = int(request.form['followStrategyId'])  # 获取跟单的策略id
    status = int(request.form["status"])  # 获取状态1开启，2停止，4防错停止
    strategyType = int(request.form["strategyType"])  # 策略类型
    platform = request.form["platform"]  # 交易平台
    symbol = request.form["symbol"]  # 交易对
    createTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  # 跟单开始时间
    strategyname_dict = {1: '网格策略', 4: '智能追踪策略', 10: '动态平衡策略', 13: '补仓策略', 15: '自适应均线策略'}
    strategy_name = strategyname_dict[strategyType]
    followdata = {'userUuid': userUuid, 'apiAccountId': apiAccountId, 'strategyId': strategyId,
                  'followStrategyId': followStrategyId, 'status': status, 'strategyType': strategyType,
                  'platform': platform, 'symbol': symbol}
    if strategyType in [1, 10]:  # 网格策略新增跟投金额，入场价与跟单时间
        followAmount = float(request.form['followAmount'])  # 跟投金额
        followTradeCoin = float(request.form['followTradeCoin'])  # 跟投交易币
        followdata['follow_trade_coin'] = followTradeCoin
        followdata['follow_amount'] = followAmount
        followdata['entryPrice'] = float(request.form['entryPrice'])  # 进场价
        followdata['initialCoin'] = float(followAmount + followTradeCoin * followdata['entryPrice'])
        followdata['createTime'] = createTime
    if status == 1:
        if strategyType == 10:
            balance_res = balance_strategy_copy(followdata)
            if balance_res:  # 0或1
                return jsonify({"success": "true",
                                "data": {"msg": "用户{}子账户{}开启动态平衡跟单，策略ID{}".format(userUuid, apiAccountId, strategyId),
                                         "code": 1, "data": {"strategyId": strategyId, "status": status}},
                                "errCode": "SUCCESS",
                                "errMsg": "", "sysTime": int(round(time.time() * 1000))})
            else:
                return jsonify({"success": "true",
                                "data": {
                                    "msg": "用户{}子账户{}开启{}动态平衡跟单失败，策略ID{}".format(userUuid, apiAccountId, strategyId),
                                    "code": 0, "data": {"strategyId": strategyId, "status": status}},
                                "errCode": "SUCCESS",
                                "errMsg": "", "sysTime": int(round(time.time() * 1000))})
        else:
            r4.hset(followStrategyId, strategyId, json.dumps(followdata))
            return jsonify({"success": "true",
                            "data": {
                                "msg": "用户{}子账户{}开启{}跟单，策略ID{}".format(userUuid, apiAccountId, strategy_name,
                                                                       strategyId),
                                "code": 1, "data": {"strategyId": strategyId, 'status': status}},
                            "errCode": "SUCCESS",
                            "errMsg": "", "sysTime": int(round(time.time() * 1000))})
    else:
        # if strategyType == 10:
        #     outPrice = get_currentprice1(platform, symbol)
        #     cancel_balancopy_orders(userUuid, apiAccountId, strategyId, platform, symbol)
        #     # 查询此时的资产
        #     remainres = requests.get(Remain_url, params={"userUuid": userUuid, "apiAccountId": apiAccountId})
        #     remaindict = json.loads(remainres.content.decode())
        #     tradeCoin = symbol.split("_")[0]  # 交易币
        #     valCoin = symbol.split("_")[1]  # 计价币
        #     TradeCoin_amount = [i["remains"] for i in remaindict["response"] if i["coin"] == tradeCoin][0]
        #     ValCoin_amount = [i["remains"] for i in remaindict["response"] if i["coin"] == valCoin][0]
        #     if initialTotalAssets > 1000:
        #         profit = TradeCoin_amount * outPrice + ValCoin_amount - initialTotalAssets
        #         profitRate = profit / initialTotalAssets
        #     else:
        #         profit = profitRate = 0
        if strategyType == 10:
            balance_res = balance_strategy_copy(followdata)
            if balance_res['profit']:
                return jsonify({"success": "true",
                                "data": {"msg": "用户{}子账户{}停止{}跟单，策略ID{}".format(userUuid, apiAccountId, strategy_name,
                                                                                strategyId),
                                         "code": 1,
                                         "data": {"strategyId": strategyId, "status": status,
                                                  'profit': balance_res['profit'],
                                                  'profitRate': balance_res['profitRate']}
                                         },
                                "errCode": "SUCCESS",
                                "errMsg": "", "sysTime": int(round(time.time() * 1000))})
            else:
                return jsonify({"success": "true",
                                "data": {"msg": "用户{}子账户{}停止{}跟单失败，策略ID{}".format(userUuid, apiAccountId, strategy_name,
                                                                                  strategyId),
                                         "code": 1,
                                         "data": {"strategyId": strategyId, "status": status,
                                                  'profit': None, 'profitRate': None}
                                         },
                                "errCode": "SUCCESS",
                                "errMsg": "", "sysTime": int(round(time.time() * 1000))})
        else:
            # 不平仓，计算利润
            if strategyType == 1:
                profit, profitRate = grid_stop(followdata)
            else:
                profit, profitRate = sum_profit(userUuid, apiAccountId, strategyId, followStrategyId)
            # 解绑跟单人与发起人的关系
            r4.hdel(followStrategyId, strategyId)
            return jsonify({"success": "true",
                            "data": {"msg": "用户{}子账户{}停止{}跟单，策略ID{}".format(userUuid, apiAccountId, strategy_name,
                                                                            strategyId),
                                     "code": 1,
                                     "data": {"strategyId": strategyId, "status": status,
                                              'profit': profit, 'profitRate': profitRate}
                                     },
                            "errCode": "SUCCESS",
                            "errMsg": "", "sysTime": int(round(time.time() * 1000))})


# 启动/停止暴跌反弹策略
@app.route('/runCrashStrategy', methods=['get', 'post'])
def runCrashStrategy():
    userUuid = request.form['userUuid']  # 获取用户userUuid
    apiAccountId = int(request.form['apiAccountId'])  # 获取用户子账户id
    strategyId = int(request.form['strategyId'])  # 获取策略id
    status = int(request.form['status'])  # 获取状态1开启，2.手动停止 4. 防错停止
    strategydata = json.loads(request.form['str'])['data']  # 策略详情
    if status == 1:
        r2.hset('Crash_strategy', strategyId, json.dumps(strategydata))
        print("用户{}子账户{}开启暴跌反弹策略{}".format(userUuid, apiAccountId, strategyId))
        return jsonify({"success": "true",
                        "data": {
                            "msg": "用户{}子账户{}开启暴跌反弹策略{}".format(userUuid, apiAccountId, strategyId),
                            "code": 1, "data": {"strategyId": strategyId, 'status': status}
                        },
                        "errCode": "SUCCESS",
                        "errMsg": "",
                        "sysTime": int(round(time.time() * 1000))})
    else:
        try:
            profit, profitRate = clear_remain(strategydata)
        except:
            profit, profitRate = 0, 0
        r2.hdel('Crash_strategy', strategyId)
        return jsonify({"success": "true",
                        "data": {
                            "msg": "用户{}子账户{}停止暴跌反弹策略{}".format(userUuid, apiAccountId, strategyId),
                            "code": 1,
                            "data": {"strategyId": strategyId, "profit": profit, "profitRate": profitRate,
                                     "status": status}
                        },
                        "errCode": "SUCCESS",
                        "errMsg": "",
                        "sysTime": int(round(time.time() * 1000))})


@app.route('/runStrategy', methods=['get', 'post'])
def runStrategy():
    userUuid = request.form['userUuid']  # 获取用户userUuid
    apiAccountId = int(request.form['apiAccountId'])  # 获取用户子账户id
    strategyId = int(request.form['strategyId'])  # 获取策略id
    status = int(request.form['status'])  # 获取状态1开启，2.手动停止 4. 防错停止
    strategyType = int(request.form['strategyType'])  # 策略名字
    strategydata = json.loads(request.form['str'])['data']  # 策略详情
    strategyname_dict = {1: '简易', 4: '长线追盈', 10: '动态平衡', 13: '补仓', 14: '暴跌反弹', 15: '自适应均线', 16: '三角套利'}
    cache_dict = {14: 'Crash', 15: 'Kama', 16: 'Arbitrage'}
    strategyname = strategyname_dict[strategyType]
    cache = cache_dict[strategyType]
    if status == 1:
        if strategyType in [14, 15, 16]:
            r2.hset('{}_strategy'.format(cache), strategyId, json.dumps(strategydata))
            print("用户{}子账户{}开启{}策略{}".format(userUuid, apiAccountId, strategyname, strategyId))
            return jsonify({"success": "true",
                            "data": {
                                "msg": "用户{}子账户{}开启{}策略{}".format(userUuid, apiAccountId, strategyname, strategyId),
                                "code": 1, "data": {"strategyId": strategyId, 'status': status}
                            },
                            "errCode": "SUCCESS",
                            "errMsg": "",
                            "sysTime": int(round(time.time() * 1000))}
                           )
    else:
        if strategyType in [14, 15, 16]:
            try:
                if strategyType == 14:
                    profit, profitRate = clear_remain(strategydata)
                    r2.hdel('Crash_strategy', strategyId)
                elif strategyType == 15:
                    profit, profitRate = clear_kama_remain(strategydata)
                    r2.hdel('Kama_strategy', strategyId)
                elif strategyType == 16:
                    profit, profitRate = get_total_profit('arbitragelist', strategyId)
                    r2.hdel('Arbitrage_strategy', strategyId)
            except:
                profit, profitRate = 0, 0
            return jsonify({"success": "true",
                            "data": {
                                "msg": "用户{}子账户{}停止{}策略{}".format(userUuid, apiAccountId, strategyname, strategyId),
                                "code": 1,
                                "data": {"strategyId": strategyId, "profit": profit, "profitRate": profitRate,
                                         "status": status}
                            },
                            "errCode": "SUCCESS",
                            "errMsg": "",
                            "sysTime": int(round(time.time() * 1000))})


@app.route('/runFutureStrategy', methods=['get', 'post'])
def runFutureStrategy():
    userUuid = request.form['userUuid']
    apiAccountId = int(request.form['apiAccountId'])
    strategyId = request.form['strategyId']
    status = int(request.form['status'])
    strategyType = int(request.form['strategyType'])  # 策略名字
    strategydata = json.loads(request.form['str'].replace(" ", "+"))['data']
    strategydata['symbol'] = strategydata['symbol'].replace("_usdt", "")
    platform = strategydata['platform']
    symbol = strategydata['symbol']
    leverage = strategydata['leverage']
    if status == 1:
        if platform == "T8ex":
            position_param = {"userUuid": userUuid, "apiAccountId": apiAccountId, "platform": platform,
                              "symbol": symbol}
            remainres = requests.get(future_remain_url, params=position_param)
            remaindict = remainres.json()
            if remaindict['success']:
                if remaindict['response']:  # 当前账户有持仓
                    if strategyType in [17,19]:
                        msg = "须保证当前合约账户中无持仓，如有，请清仓"
                        return jsonify({"success": "true",
                                        "data": {
                                            "msg": msg,
                                            "code": 0, "data": {"strategyId": strategyId, 'status': status}
                                        },
                                        "errCode": "SUCCESS",
                                        "errMsg": "",
                                        "sysTime": int(round(time.time() * 1000))})
                    leverate = remaindict['response']['leverage']  # 账户中的杠杆倍数
                    position_direction = remaindict['response']['direction']  # 账户中的仓位持仓方向
                    print("当前持仓杠杆{},持仓方向{}".format(leverate,position_direction))
                    if (strategydata['direction'] and strategydata[
                        'direction'] != position_direction) or leverage != leverate:
                        msg = "请保证当前合约账户持仓方向、杠杆倍数与策略一致"
                        return jsonify({"success": "true",
                                        "data": {
                                            "msg": msg,
                                            "code": 0, "data": {"strategyId": strategyId, 'status': status}
                                        },
                                        "errCode": "SUCCESS",
                                        "errMsg": "",
                                        "sysTime": int(round(time.time() * 1000))})
                else:  # 当前账户无持仓
                    switch_res = switch_lever_rate(userUuid, apiAccountId, platform, leverage, symbol)
                    if "fail" in switch_res:
                        msg = "请保证当前合约账户持仓方向、杠杆倍数与策略一致"
                        return jsonify({"success": "true",
                                        "data": {
                                            "msg": msg,
                                            "code": 0, "data": {"strategyId": strategyId, 'status': status}
                                        },
                                        "errCode": "SUCCESS",
                                        "errMsg": "",
                                        "sysTime": int(round(time.time() * 1000))})
        else:
            switch_res = switch_lever_rate(userUuid, apiAccountId, platform, leverage, symbol)
            if "fail" in switch_res:
                msg = "请保证当前合约账户持仓方向、杠杆倍数与策略一致"
                return jsonify({"success": "true",
                                "data": {
                                    "msg": msg,
                                    "code": 0, "data": {"strategyId": strategyId, 'status': status}
                                },
                                "errCode": "SUCCESS",
                                "errMsg": "",
                                "sysTime": int(round(time.time() * 1000))})

    if strategyType == 16:
        strategydata['entry_price'] = 0
        strategydata['flag'] = 0
        strategydata['init_entry_price'] = 0
        strategydata['mostprice'] = 0
        strategydata['stopprice'] = 0
        strategydata['touchtag'] = 0
        direction = strategydata['direction']  # 做多、做空、双向
        followUpInterval = float(strategydata['followUpinterVal'])  # 追投间隔
        if status == 1:
            if direction == 'buy':
                orderQuantities = int(strategydata['longOrderQuantities'])  # 最大做多单数
                coverRatio = [str(followUpInterval * (i + 1)) for i in range(orderQuantities)]
                strategydata['coverRatio'] = '-'.join(coverRatio)
                strategydata['buy_num'] = 0
                r0.hset('martin_future_long', strategyId, json.dumps(strategydata))
                msg = "用户{}子账户{}开启马丁追踪合约做多策略{}".format(userUuid, apiAccountId, strategyId)
            elif direction == 'sell':
                orderQuantities = int(strategydata['shortOrderQuantities'])  # 最大做空单数
                coverRatio = [str(followUpInterval * (i + 1)) for i in range(orderQuantities)]
                strategydata['coverRatio'] = '-'.join(coverRatio)
                strategydata['sell_num'] = 0
                r0.hset('martin_future_short', strategyId, json.dumps(strategydata))
                msg = "用户{}子账户{}开启马丁追踪合约做空策略{}".format(userUuid, apiAccountId, strategyId)
            elif direction == 'two_way':
                longOrderQuantities = int(strategydata['longOrderQuantities'])  # 最大做多单数
                coverRatio = [str(followUpInterval * (i + 1)) for i in range(longOrderQuantities)]
                strategydata['coverRatio'] = '-'.join(coverRatio)
                strategydata['direction'] = 'buy'
                strategydata['buy_num'] = 0
                r0.hset('martin_future_long', strategyId, json.dumps(strategydata))
                strategydata.pop("buy_num")
                shortOrderQuantities = int(strategydata['shortOrderQuantities'])  # 最大做空单数
                coverRatio = [str(followUpInterval * (i + 1)) for i in range(shortOrderQuantities)]
                strategydata['coverRatio'] = '-'.join(coverRatio)
                strategydata['direction'] = 'sell'
                strategydata['sell_num'] = 0
                r0.hset('martin_future_short', strategyId, json.dumps(strategydata))
                msg = "用户{}子账户{}开启马丁追踪合约双向策略{}".format(userUuid, apiAccountId, strategyId)
            return jsonify({"success": "true",
                            "data": {
                                "msg": msg,
                                "code": 1, "data": {"strategyId": strategyId, 'status': status}
                            },
                            "errCode": "SUCCESS",
                            "errMsg": "",
                            "sysTime": int(round(time.time() * 1000))}
                           )
        else:
            if direction == 'buy':
                strategydata = json.loads(r0.hget('martin_future_long', strategyId))
                totalprofit, totalprofitRate = long_stopout(strategydata)
                msg = "用户{}子账户{}手动停止马丁追踪合约做多策略{}".format(userUuid, apiAccountId, strategyId)
            elif direction == 'sell':
                strategydata = json.loads(r0.hget('martin_future_short', strategyId))
                totalprofit, totalprofitRate = short_stopout(strategydata)
                msg = "用户{}子账户{}手动停止追踪合约做空策略{}".format(userUuid, apiAccountId, strategyId)
            elif direction == 'two_way':
                strategybuydata = json.loads(r0.hget('martin_future_long', strategyId))
                lprofit, lprofitRate = long_stopout(strategybuydata)
                strategyselldata = json.loads(r0.hget('martin_future_short', strategyId))
                sprofit, sprofitRate = short_stopout(strategyselldata)
                totalprofit = lprofit + sprofit
                if lprofitRate and sprofitRate:
                    totalprofitRate = totalprofit / (lprofit / lprofitRate + sprofit / sprofitRate)
                else:
                    totalprofitRate = lprofitRate + sprofitRate
                msg = "用户{}子账户{}手动停止追踪合约双边策略{}".format(userUuid, apiAccountId, strategyId)
            return jsonify({"success": "true",
                            "data": {
                                "msg": msg,
                                "code": 1,
                                "data": {"strategyId": strategyId, "status": status,
                                         'profit': totalprofit, 'profitRate': totalprofitRate}
                            },
                            "errCode": "SUCCESS",
                            "errMsg": "",
                            "sysTime": int(round(time.time() * 1000))})
    elif strategyType == 17:
        if status == 1:
            strategydata['flag'] = 0
            r0.hset('dual_thrust', strategyId, json.dumps(strategydata))
            msg = "用户{}子账户{}开启Dual_thrust策略{}".format(userUuid, apiAccountId, strategyId)
            return jsonify({"success": "true",
                            "data": {
                                "msg": msg,
                                "code": 1, "data": {"strategyId": strategyId, 'status': status}
                            },
                            "errCode": "SUCCESS",
                            "errMsg": "",
                            "sysTime": int(round(time.time() * 1000))}
                           )
        else:
            strategydata = json.loads(r0.hget("dual_thrust", strategyId))
            totalprofit, totalprofitRate = stopout(strategydata)
            r0.hdel('dual_thrust', strategyId)
            msg = "用户{}子账户{}停止Dual_thrust策略{}".format(userUuid, apiAccountId, strategyId)
            return jsonify({"success": "true",
                            "data": {
                                "msg": msg,
                                "code": 1, "data": {"strategyId": strategyId, "status": status,
                                                    "profit": totalprofit, "profitRate": totalprofitRate}
                            },
                            "errCode": "SUCCESS",
                            "errMsg": "",
                            "sysTime": int(round(time.time() * 1000))
                            }
                           )
    elif strategyType == 18:
        # 开启/关闭网格策略
        try:
            userUuid = request.form["userUuid"]  # 获取用户唯一id
            apiAccountId = int(request.form["apiAccountId"])  # 获取子账户id
            strategyId = int(request.form["strategyId"])  # 获取网格策略id
            status = int(request.form["status"])  # 获取状态1开启，0关闭，3手动停止,4止盈止损停止
            griddata = strategydata  # 策略详情
            platform = griddata["platform"]  # 平台
            direction = griddata['direction']
            griddata['createTime'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            if status == 1:
                # 校验做多初始计价币与交易币的最低投入是否满足要求
                if direction == "buy":
                    res = longgridbegin(griddata)
                else:
                    res = shortgridbegin(griddata)
                if res == 1:
                    if direction == "buy":
                        msg = "用户{}子账户{}成功开启合约网格做多策略{}".format(userUuid, apiAccountId, strategyId)
                    else:
                        msg = "用户{}子账户{}成功开启合约网格做空策略{}".format(userUuid, apiAccountId, strategyId)
                    return jsonify({"success": "true", "data": {"msg": msg, "code": 1,
                                                                "data": {"strategyId": strategyId,
                                                                         "status": status}},
                                    "errCode": "SUCCESS",
                                    "errMsg": "", "sysTime": int(round(time.time() * 1000))})
                elif res == 0:
                    # 如果初始化部署网格单不成功，撤销已经下好的买卖单
                    try:
                        conn = POOL_grid.connection()
                        cur = conn.cursor()
                        cur.execute("select buyorderid,sellorderid from t_contractgrid where strategyId=%s ",
                                    (strategyId,))
                        selectres = cur.fetchall()
                        cur.execute("delete from t_contractgrid where strategyId=%s", (strategyId,))
                        conn.commit()
                        cur.close()
                        conn.close()
                        for i in selectres:
                            if i[0] is not None:
                                cancelres = cancel_contract_usdt_order(userUuid, apiAccountId, platform, i[0])
                                if cancelres['success']:
                                    print("初始合约网格买单撤单成功")
                            if i[1] is not None:
                                cancelres = cancel_contract_usdt_order(userUuid, apiAccountId, platform, i[1])
                                if cancelres['success']:
                                    print("初始合约网格卖单撤单成功")
                    except Exception as e:
                        info = "合约网格策略{}初始化部署买卖单不成功后，撤单失败{}".format(strategyId, e)
                        print(info)
                        logger.error(info)
                    return jsonify(
                        {"success": "true",
                         "data": {"msg": "交易所接口不通或首单未成交导致开启合约网格策略{}失败".format(strategyId), "code": 0,
                                  "data": {"strategyId": strategyId, "status": status}},
                         "errCode": "SUCCESS",
                         "errMsg": "", "sysTime": int(round(time.time() * 1000))})
            else:
                profit, profitRate = future_grid_stop(griddata)
                return jsonify(
                    {"success": "true", "data": {"msg": "停止合约网格策略{}并返回盈利信息".format(strategyId), "code": 1,
                                                 "data": {"userUuid": userUuid,
                                                          "apiAccountId": apiAccountId,
                                                          "strategyId": strategyId,
                                                          "status": status,
                                                          "profits": profit, "profitRate": profitRate,
                                                          "clientSource": 1, "version": 1.3,
                                                          "client": "python",
                                                          "ticket": "7b3d556d-d45f-4595-be5f-a05c2fc4fcf913"}},
                     "errCode": "SUCCESS",
                     "errMsg": "", "sysTime": int(round(time.time() * 1000))})
        except Exception as e:
            i = '用户{}合约网格策略{}开启关闭报错{}'.format(userUuid, strategyId, e)
            print(i)
            logger.error(i)
            return jsonify({"success": "true", "data": {"msg": "合约网格策略启动/停止失败,报错信息{}".format(e), "code": 0, "data": {}},
                            "errCode": "SUCCESS",
                            "errMsg": "", "sysTime": int(round(time.time() * 1000))})
    elif strategyType == 19:
        # 开启/关闭MACDATR策略
        if status == 1:
            strategydata['flag'] = 0
            r0.hset('macdatrstrategy', strategyId, json.dumps(strategydata))
            msg = "用户{}子账户{}开启macd_atr策略{}".format(userUuid, apiAccountId, strategyId)
            return jsonify({"success": "true",
                            "data": {
                                "msg": msg,
                                "code": 1, "data": {"strategyId": strategyId, 'status': status}
                            },
                            "errCode": "SUCCESS",
                            "errMsg": "",
                            "sysTime": int(round(time.time() * 1000))}
                           )
        else:
            strategydata = json.loads(r0.hget("macdatrstrategy", strategyId))
            totalprofit, totalprofitRate = macdatrout(strategydata)
            r0.hdel('macdatrstrategy', strategyId)
            msg = "用户{}子账户{}停止macd_atr策略{}".format(userUuid, apiAccountId, strategyId)
            return jsonify({"success": "true",
                            "data": {
                                "msg": msg,
                                "code": 1, "data": {"strategyId": strategyId, "status": status,
                                                    "profit": totalprofit, "profitRate": totalprofitRate}
                            },
                            "errCode": "SUCCESS",
                            "errMsg": "",
                            "sysTime": int(round(time.time() * 1000))
                            }
                           )


# 启动
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)

# encoding='utf-8'
import json
import requests
from currency_strategy.balance_strategy import balancestrategy_begin, cancel_balancestrategy_orders, clear_tradecoin_remains
from tools.Config import Remain_url
from tools.databasePool import r2
from tools.get_market_info import get_currentprice1

def balance_strategy_copy(tracer_info):
    followstrategyId = tracer_info['followStrategyId']
    sponsor_info = json.loads(r2.hget('balance_strategy', followstrategyId))
    userUuid = tracer_info["userUuid"]  # 获取用户userUuid
    apiAccountId = int(tracer_info["apiAccountId"])  # 获取用户子账户id
    strategyId = int(tracer_info["strategyId"])  # 获取网格策略id
    status = int(tracer_info['status'])
    entryPrice = float(tracer_info["entryPrice"])  # 入场价
    initialTradeCoin = float(tracer_info["follow_trade_coin"])  # 用于策略的初始交易币数量
    initialValCoin = float(tracer_info["follow_amount"])  # 用于策略的初始计价币数量
    existingUsdt = float(tracer_info['initialCoin'])
    print("+++++++++跟换跟单人信息++++++++")
    sponsor_info['userUuid'] = userUuid
    sponsor_info['apiAccountId'] = apiAccountId
    sponsor_info['strategyId'] = strategyId
    sponsor_info['status'] = status
    sponsor_info['entryPrice'] = entryPrice
    sponsor_info['initialTradeCoin'] = initialTradeCoin
    sponsor_info['initialValCoin'] = initialValCoin
    sponsor_info['existingUsdt'] = existingUsdt

    platform = sponsor_info["platform"]  # 交易平台
    symbol = sponsor_info["symbol"]  # 交易对
    spacingRatio = sponsor_info["spacingRatio"]  # 间距比例，默认0.5%（0.005）
    directionType = int(sponsor_info["directionType"])
    tradeCoin = symbol.split("_")[0]
    valCoin = symbol.split("_")[1]
    if status == 1:  # 开启
        try:
            if directionType == 0:
                print("用户{}动态平衡跟踪策略{}开始部署初始网格".format(userUuid, strategyId))
                res = balancestrategy_begin(userUuid, apiAccountId, strategyId, platform, symbol, entryPrice,
                                            initialTradeCoin, initialValCoin,
                                            spacingRatio)
                if res == 1:
                    print('++++++++++++创建一个新的动态平衡策略+++++++++')
                    r2.hset("balancestrategy", strategyId, json.dumps(sponsor_info))
            elif directionType == 1:
                pass
            elif directionType == 2:
                pass
            return 1
        except:
            return 0

    else:
        try:
            if directionType == 0:
                # 1、删除策略以及redis缓存的订单信息
                r2.hdel("balancestrategy", strategyId)  # 从redis删除该策略
                r2.delete("balance:sell:{}".format(strategyId))
                r2.delete("balance:buy:{}".format(strategyId))
                # 2、撤销订单并改变数据库状态
                cancel_balancestrategy_orders(userUuid, apiAccountId, strategyId, platform, symbol)
                if status == 2:  # 或者止损停止时选择了平仓
                    stoplossType = int(sponsor_info["stoplossType"])  # 1为止损停止时不平仓，2为止损停止时平仓处理
                    if stoplossType == 2:
                        # 查询交易币余额
                        remainres = requests.get(Remain_url,params={"userUuid": userUuid, "apiAccountId": apiAccountId})
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
                profit = round(TradeCoin_amount * outPrice + ValCoin_amount - existingUsdt, 8)  # 按资产折算成usdt计算盈利
                profitRate = round(profit / existingUsdt, 8)  # 盈利率
                return {'profit': profit, 'profitRate': profitRate}
            elif directionType == 1:
                pass
            elif directionType == 2:
                pass
        except:
            return {'profit': None, 'profitRate': None}



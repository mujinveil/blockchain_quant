# encoding="utf-8"
import json
from tools.Config import updateCover_url
from tools.databasePool import r2
from tools.get_market_info import get_currentprice1
from tools.tool import receiveEmail, buy_symbol, sell_symbol, get_total_profit


def trade(strategydata):
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    strategyId = strategydata['strategyId']
    platform = strategydata['platform']
    amount = strategydata['amount']
    flag = strategydata['flag']
    symbol = "btc_usdt"
    currentprice = get_currentprice1(platform, symbol)
    direction = int(receiveEmail(receiverMail, authCode))
    strategyname = "易经预测"
    tablename = "yijinglist"
    if flag == 0:  # 账户无持仓
        if direction == 1:  # 易经预测看涨
            buy_symbol(userUuid, apiAccountId, strategyId, platform, symbol, currentprice, amount, strategyname,
                       tablename, updateCover_url)
            strategydata['flag'] = 1
            r2.hset("Yijing_strategy", strategyId, json.dumps(strategydata))
        elif direction == -1:  # 易经预测看跌
            # 账户无持仓，无需清仓
            pass
    elif flag == 1:  # 账户已买入btc
        if direction == 1:  # 易经预测看涨
            # 账户有持仓，无需在买入
            pass
        elif direction == -1:  # 易经预测看跌
            trade_info = json.loads(r2.hget('yijing_label:{}'.format(strategyId), symbol))
            amount = trade_info['numberDeal']
            entryPrice = trade_info['entryPrice']
            sell_symbol(userUuid, apiAccountId, strategyId, platform, symbol, currentprice, amount, entryPrice,
                        strategyname, tablename, updateCover_url)
            strategydata['flag'] = 0
            r2.hset("Yijing_strategy", strategyId, json.dumps(strategydata))


def yijing_stop_out(strategydata):
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    strategyId = strategydata['strategyId']
    platform = strategydata['platform']
    amount = strategydata['amount']
    symbol = "btc_usdt"
    currentprice = get_currentprice1(platform, symbol)
    strategyname = "易经预测"
    tablename = "yijinglist"
    trade_info = json.loads(r2.hget('yijing_label:{}'.format(strategyId), symbol))
    amount = trade_info['numberDeal']
    entryPrice = trade_info['entryPrice']
    sell_symbol(userUuid, apiAccountId, strategyId, platform, symbol, currentprice, amount, entryPrice,
                strategyname, tablename, updateCover_url)
    totalprofit, totalprofitRate = get_total_profit('yijinglist', strategyId,amount)
    return totalprofit, totalprofitRate


if __name__ == "__main__":
    receiverMail = '1518219336@qq.com'
    authCode = 'esaveuhwlpkyfhcc'
    direction = int(receiveEmail(receiverMail, authCode))
    # strategydata = {
    #     "userUuid": "398051ac70ef4da9aafd33ce0b95195f",
    #     "apiAccountId": 10209,
    #     "strategyId": 785,
    #     "platform": "T8ex",
    #     "strategyType": 25,
    #     "amount": 30,
    #     "flag": 1
    # }
    # trade(strategydata)
    # while True:
    #     try:
    #         strategy_list = r2.hvals("Yijing_strategy")
    #         strategy_list = [json.loads(i) for i in strategy_list]
    #         T = []
    #         for strategy_info in strategy_list:
    #             T.append(Thread(target=trade, args=(strategy_info,)))
    #         for t in T:
    #             t.start()
    #         for t in T:
    #             t.join()
    #     except Exception as e:
    #         print(e)
    #     finally:
    #         time.sleep(1)

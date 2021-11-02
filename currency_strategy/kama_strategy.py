# encoding='utf-8'
import json
import sys
import time
import pandas as pd
import requests
from threading import Thread
sys.path.append("..")
from tools.Config import updateCrash_url, pricelimit, amountlimit
from tools.Kline_analyze import get_klinedata, get_all_symbol_klinedata, KAMA, chandelier_stop
from tools.databasePool import r2, r5
from tools.get_market_info import get_currentprice1
from tools.tool import sell_symbol, buy_symbol, get_total_profit


def gold_cross(klinedata):
    if not klinedata:
        return 0
    close = pd.Series([i[4] for i in klinedata])
    AMA_10 = KAMA(close, 10)
    AMA_100 = KAMA(close, 100)
    if AMA_10[-1] > AMA_100[-1]:
        return 1
    else:
        return 0


# 获取满足条件的交易对
def get_candidate_symbols(platform, stopRatio, granularity):
    symbol_pool = []
    localtime = time.localtime(time.time())
    if localtime.tm_min % 30 == 0:
        symbol_klinedata = get_all_symbol_klinedata(platform, granularity)
        for i in symbol_klinedata:
            symbol, klinedata = i
            try:
                flag = gold_cross(klinedata)
                symbol_candidate = ['btc_usdt', 'eth_usdt', 'hc_usdt', 'glm_usdt', 'xmr_usdt', 'lamb_usdt',
                                    'hpt_usdt', 'omg_usdt', 'xem_usdt', 'akro_usdt', 'neo_usdt', 'luna_usdt',
                                    'vet_usdt', 'gt_usdt', 'ada_usdt', 'nkn_usdt', 'hit_usdt', 'etc_usdt', 'ht_usdt',
                                    'bix_usdt', 'hpt_usdt', 'one_usdt', 'hc_usdt', 'storj_usdt', 'chr_usdt', 'uni_usdt',
                                    'sushi_usdt', 'xmr_usdt']
                if flag and (symbol in symbol_candidate):
                    stopprice, currentprice = chandelier_stop(klinedata, stopRatio * 100)
                    print("交易对{},当前价{},吊灯止损价{}".format(symbol, currentprice, stopprice))
                    if currentprice > stopprice:
                        symbol_pool.append(symbol)
                        print("备选交易对{},当前价{},吊灯止损价{}".format(symbol, currentprice, stopprice))
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
    current_price = get_currentprice1(platform, symbol)
    buyprice = round(current_price * 1.01, pricelimit[symbol][platform])
    buy_amount = amount / buyprice
    try:
        x, y = str(buy_amount).split('.')
        buy_amount = float(x + '.' + y[0:amountlimit[symbol][platform]])
    except:
        pass
    tradeparams = {"direction": 1, "amount": buy_amount, "symbol": symbol, "platform": platform, "price": buyprice,
                   "source": 11}
    # 将下单参数传给跟单策略
    r5.hset('order_param_15', strategyId, json.dumps(tradeparams))
    time.sleep(1)
    buy_symbol(userUuid, apiAccountId, strategyId, platform, symbol, current_price, amount, "自适应均线", 'kamalist',
               updateCrash_url)


# 对持仓的交易对进行吊灯止损
def trailingSell(strategydata, symbol):
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    strategyId = strategydata['strategyId']
    platform = strategydata["platform"]
    stopRatio = strategydata['stopRatio']
    amount = strategydata['amount']
    maxPositionNum = strategy_info['maxPositionNum']
    init_amount = amount * maxPositionNum
    Kama_label = json.loads(r2.hget('kama_label:{}'.format(strategyId), symbol))
    sell_amount = Kama_label['numberDeal']
    entryPrice = Kama_label['entryPrice']
    symbol, kline_data = get_klinedata(platform, symbol, 86400)
    if not kline_data:
        return
    stopPrice, currentPrice = chandelier_stop(kline_data, stopRatio * 100)
    print("交易对{}当前吊灯止损价格为:{},行情价{}".format(symbol, stopPrice, currentPrice))
    # 当价格低于吊灯止损价格
    if currentPrice < stopPrice:
        sellprice = round(currentPrice * 0.99, pricelimit[symbol][platform])
        sellparams = {"direction": 2, "amount": sell_amount, "symbol": symbol, "price": sellprice, "source": 11,
                      'entryPrice': entryPrice, 'tradetype': 1}
        # 将下单参数传给跟单策略
        r5.hset('order_param_15', strategyId, json.dumps(sellparams))
        print('交易对{}当前行情价{}触碰了吊灯止损价，开始卖出'.format(symbol, currentPrice))
        sell_flag = sell_symbol(userUuid, apiAccountId, strategyId, platform, symbol, currentPrice, sell_amount,
                                entryPrice, "自适应均线", 'kamalist', updateCrash_url)
        if sell_flag:
            totalprofit, totalprofitRate = get_total_profit("kamalist", strategyId, init_amount)
            i = "用户{}子账户{}自适应均线策略{}开始计算利润{}".format(userUuid, apiAccountId, strategyId, totalprofit)
            print(i)
            # logger.info(i)
            params = {'strategyId': strategyId, 'profit': totalprofit, 'profitRate': totalprofitRate}
            res = requests.post(updateCrash_url, data=params)
            resdict = json.loads(res.content.decode())
            print(resdict)


if __name__ == "__main__":
    while True:
        try:
            strategy_list = r2.hvals("Kama_strategy")
            strategy_list = [json.loads(i) for i in strategy_list]
            T = []
            for strategy_info in strategy_list:
                strategyId = strategy_info['strategyId']
                platform = strategy_info['platform']
                maxPositionNum = strategy_info['maxPositionNum']
                stopRatio = float(strategy_info['stopRatio'])
                symbol_pool = get_candidate_symbols(platform, stopRatio, 86400)
                hold_pool = r2.hkeys('kama_label:{}'.format(strategyId))
                for symbol in hold_pool:
                    T.append(Thread(target=trailingSell, args=(strategy_info, symbol)))
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
            time.sleep(1)

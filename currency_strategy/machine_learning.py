# encoding="utf-8"
import json
import time
import warnings
from threading import Thread
import numpy as np
import pandas as pd
import requests
import scipy.optimize as sco
import talib
import tensorflow as tf
import sys
sys.path.append("..")
warnings.filterwarnings("ignore")
from tools.Config import updateCover_url
from tools.databasePool import r2
from tools.tool import buy_multiple_symbols
from tools.strategy_clearout import currency_stop_out

tf.random.set_seed(10)


# 获取K线数据
def get_klinedata(platform, symbol, granularity):
    if platform == "huobi":  # 2000条数据，时间粒度1min, 5min, 15min, 30min, 60min, 4hour, 1day, 1mon, 1week, 1year
        huobi_granularity_dict = {60: "1min", 300: "5min", 900: "15min", 1800: "30min", 3600: "60min",
                                  14400: "4hour", 86400: "1day", 604800: "1week", 2592000: "mon",
                                  946080000: "1year"}
        for _ in range(3):
            try:
                res = requests.get("https://api.huobi.pro/market/history/kline?period={}&size=2000&symbol={}".format(
                    huobi_granularity_dict[granularity], symbol.replace("_", "")), timeout=1)
                df = pd.DataFrame()
                if res.status_code == 200:
                    data = json.loads(res.content.decode())["data"][::-1]
                    df['close'] = [i['close'] for i in data]
                    df['open'] = [i['open'] for i in data]
                    df['high'] = [i['high'] for i in data]
                    df['low'] = [i['low'] for i in data]
                    df['volume'] = [i['vol'] for i in data]
                    df['time'] = [time.strftime("%Y-%m-%d", time.localtime(i['id'])) for i in data]
                    break
            except Exception as e:
                print(e)
                df = pd.DataFrame()
        return df


# 数据去极值及标准化
def winsorize_and_standarlize(data, qrange=[0.05, 0.95], axis=0):
    '''
    input:
    data:Dataframe or series,输入数据
    qrange:list,list[0]下分位数，list[1]，上分位数，极值用分位数代替
    '''
    if isinstance(data, pd.DataFrame):
        if axis == 0:
            q_down = data.quantile(qrange[0])
            q_up = data.quantile(qrange[1])
            index = data.index
            col = data.columns
            for n in col:
                data[n][data[n] > q_up[n]] = q_up[n]
                data[n][data[n] < q_down[n]] = q_down[n]
            data = (data - data.mean()) / data.std()
            data = data.fillna(0)
        else:
            data = data.stack()
            data = data.unstack(0)
            q = data.quantile(qrange)
            index = data.index
            col = data.columns
            for n in col:
                data[n][data[n] > q[n]] = q[n]
            data = (data - data.mean()) / data.std()
            data = data.stack().unstack(0)
            data = data.fillna(0)

    elif isinstance(data, pd.Series):
        name = data.name
        q = data.quantile(qrange)
        data[data > q] = q
        data = (data - data.mean()) / data.std()
    return data


# 获得单个币种的K线数据以及构建各个因子
def get_factor(symbol):
    df = get_klinedata("huobi", symbol, granularity=86400)
    df['symbol'] = symbol
    df['MOM_10'] = df['close'] / df['close'].shift(9) - 1
    df['MOM_60'] = df['close'] / df['close'].shift(59) - 1
    df['MOM_120'] = df['close'] / df['close'].shift(119) - 1
    df['VSTD_20'] = df['volume'].rolling(20).std()
    df['std_20'] = df['close'].rolling(20).std()
    df['std_40'] = df['close'].rolling(40).std()
    df['std_120'] = df['close'].rolling(120).std()
    df['MA_5'] = df['close'].rolling(5).mean()
    df['MA_10'] = df['close'].rolling(10).mean()
    df['MA_Cross'] = np.where(df['MA_5'] > df['MA_10'], 1, 0)
    df['label'] = np.where(df['close'] > df['close'].shift(-7), 0, 1)  # 7日后涨跌
    # 复合动量因子
    df['ret'] = np.log(df['close'] / df['close'].shift())
    df['ret_60'] = df['ret'].rolling(60).sum()
    # 收益率偏度因子
    df['skew_60'] = df['ret'].rolling(60).skew()
    df['MACD'] = talib.MACD(df.close, fastperiod=6, slowperiod=12, signalperiod=9)[-1]
    df['RSI'] = talib.RSI(df.close, timeperiod=12)
    df['MOM'] = talib.MOM(df.close, timeperiod=5)
    df['CCI'] = talib.CCI(df.high, df.low, df.close, timeperiod=14)
    df['WILLR'] = talib.WILLR(df.high, df.low, df.close, timeperiod=14)
    df['SAR'] = talib.SAR(df.high, df.low)
    df['OBV'] = talib.OBV(df.close, df.volume)
    df['ADOSC'] = talib.ADOSC(df.high, df.low, df.close, df.volume)
    df['ROC'] = talib.ROC(df.volume)
    # 新增1
    df['SLOWK'], df['SLOWD'] = talib.STOCH(df.high, df.low, df.close)
    df['HT'] = talib.HT_TRENDLINE(df.close)
    df['ADX'] = talib.ADX(df.high, df.low, df.close)
    df['APO'] = talib.APO(df.close)
    df['AROONDOWN'], df['AROONUP'] = talib.AROON(df.high, df.low)
    df['AROONOSC'] = talib.AROONOSC(df.high, df.low)
    df['BOP'] = talib.BOP(df.open, df.high, df.low, df.close)
    df['CMO'] = talib.CMO(df.close)
    df['MFI'] = talib.MFI(df.high, df.low, df.close, df.volume)
    df['PPO'] = talib.PPO(df.close)
    df['ULTOSC'] = talib.ULTOSC(df.high, df.low, df.close)
    # 新增2
    df['AD'] = talib.AD(df.high, df.low, df.close, df.volume)
    df['COL3BLACKCROWS'] = talib.CDL3BLACKCROWS(df.open, df.high, df.low, df.close)
    df['CDLDOJI'] = talib.CDLDOJI(df.open, df.high, df.low, df.close)
    df['CDLENGULFING'] = talib.CDLENGULFING(df.open, df.high, df.low, df.close)
    df['CDLHAMMER'] = talib.CDLHAMMER(df.open, df.high, df.low, df.close)
    df['CDLMORNINGSTAR'] = talib.CDLMORNINGSTAR(df.open, df.high, df.low, df.close)
    df['TSF'] = talib.TSF(df.close)
    df.dropna(inplace=True)
    return df


# 合并多个交易对的因子数据
def get_symbols_factor(symbols):
    data_train = pd.DataFrame()
    data_test = pd.DataFrame()
    data_close = pd.DataFrame()
    for symbol in symbols:
        symbol = "{}_usdt".format(symbol)
        df = get_factor(symbol)
        df_train = df.iloc[:-5]
        df_test = df.iloc[-1:]
        data_train = pd.concat([data_train, df_train], axis=0)
        data_test = pd.concat([data_test, df_test], axis=0)
        data_close[symbol] = np.array(df['close'])[-200:]
    return data_train, data_test, data_close


# # 利用SVM模型进行二分类训练
# def model_train(before_df):  # 需踢掉一个周期(即5日)的数据
#     model_svm = SVC(C=1000, probability=True)
#     df = before_df.sample(frac=1.0).reset_index(drop=True)  # 打乱所有数据,并重置索引
#     features = df.columns.difference(
#         ['close', 'high', 'low', 'open', 'volume', 'time', 'label', 'ret', 'MA_5', 'MA_10', 'symbol'])
#     model = Pipeline([
#         ('pca', PCA()),
#         ('svc', model_svm)])
#     model.fit(winsorize_and_standarlize(df[features]), df.label)
#     return model
#
#
# # 利用训练后的模型，输入当日因子数据,预测5日后涨跌
# def model_predict(model, current_df):  # 取最后一个数据
#     features = current_df.columns.difference(
#         ['close', 'high', 'low', 'open', 'volume', 'time', 'label', 'ret', 'MA_5', 'MA_10', 'symbol'])
#     current_df['score'] = model.predict_proba(winsorize_and_standarlize(current_df[features]))[:, 1]
#     current_df['label'] = model.predict(winsorize_and_standarlize(current_df[features]))
#     stock_to_buy = current_df[current_df.label == 1].sort_values('score').tail(3)
#     buy_list = list(stock_to_buy['symbol'])
#     return buy_list

# Keras 训练DNN
def model_train(data_train):
    df = data_train.sample(frac=1.0, random_state=1).reset_index(drop=True)  # 打乱所有数据,并重置索引
    features = df.columns.difference(
        ['close', 'high', 'low', 'open', 'volume', 'time', 'label', 'ret', 'MA_5', 'MA_10', 'symbol'])
    x_train = np.array(winsorize_and_standarlize(df[features]))
    y_train = np.array(df.label)
    x_train = tf.keras.utils.normalize(x_train, axis=1).reshape(x_train.shape[0], -1)
    model = tf.keras.models.Sequential()
    model.add(tf.keras.layers.Flatten())  # Flatten the images! Could be done with numpy reshape
    model.add(tf.keras.layers.Dense(256, activation=tf.nn.relu, input_shape=x_train.shape[1:]))
    model.add(tf.keras.layers.Dense(128, activation=tf.nn.relu))
    model.add(tf.keras.layers.Dense(128, activation=tf.nn.relu))
    model.add(tf.keras.layers.Dense(64, activation=tf.nn.relu))
    model.add(tf.keras.layers.Dense(2, activation=tf.nn.softmax))  # 2 because dataset is numbers from 0 - 1
    model.compile(optimizer='adam',  # Good default optimizer to start with
                  loss='sparse_categorical_crossentropy',
                  # how will we calculate our "error." Neural network aims to minimize loss.
                  metrics=['accuracy'])  # what to track
    model.load_weights('store_dnn_data/dnn_weights')
    model.fit(x_train, y_train, epochs=50)
    model.save_weights('store_dnn_data/dnn_weights')
    return model


def model_predict(model, data_test):
    features = data_test.columns.difference(
        ['close', 'high', 'low', 'open', 'volume', 'time', 'label', 'ret', 'MA_5', 'MA_10', 'symbol'])
    x_test = winsorize_and_standarlize(data_test[features])
    predictions = model.predict(x_test)
    data_test['up_score'] = [i[1] for i in predictions]
    data_test.sort_values(by=["up_score", "symbol"], inplace=True)
    buy_list = list(data_test['symbol'])[-3:]
    return buy_list


# 投资组合优化
def optimize_weights(buy_list, data_close):
    df = data_close[buy_list]
    buy_num = len(buy_list)
    rets = np.log(df / df.shift(1))

    def port_vol(weights):
        return np.sqrt(np.dot(weights.T, np.dot(rets.cov() * 200, weights)))

    cons = ({'type': 'eq', 'fun': lambda x: np.sum(x) - 1})
    eweights = np.array(buy_num * [1. / buy_num, ])
    bnds = tuple((0, 1) for _ in range(buy_num))
    opts = sco.minimize(port_vol, eweights, method="SLSQP", bounds=bnds, constraints=cons)
    buy_weights = list(opts.x.round(3))
    return buy_weights


# 剩余资产查询
def remain_amount(strategydata):
    pass


# 交易主函数
def trade(strategydata, buy_info):
    flag = strategydata['flag']
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    strategyId = strategydata['strategyId']
    platform = strategydata['platform']
    strategyType = strategydata['strategyType']
    amount = strategydata['amount']
    if not buy_info:
        return
    buy_list = [{'symbol': symbol, 'numberDeal': amount * weight} for symbol, weight in buy_info.items()]
    if flag == 0:  # 无持仓
        buy_multiple_symbols(userUuid, apiAccountId, strategyId, platform, strategyType, buy_list, updateCover_url)
        strategydata['flag'] = 1
        r2.hset("ML_strategy", strategyId, json.dumps(strategydata))
    if flag == 1:  # 有持仓
        print("该账户有持仓")
        # 检查是否需要调仓换股
        ml_list = r2.hvals('ml_label:{}'.format(strategyId))
        ml_info = [json.loads(i)['symbol'] for i in ml_list]
        if set(ml_info) != set(buy_info.keys()):  # 待买入股票池与持仓不同,需调仓换股
            # 先清仓
            currency_stop_out(strategydata)
            # 再买入股票
            buy_multiple_symbols(userUuid, apiAccountId, strategyId, platform, strategyType, buy_list, updateCover_url)


if __name__ == "__main__":
    while True:
        now = time.localtime(time.time())
        weekday = now.tm_wday
        hour = now.tm_hour
        if weekday == 2 and hour == 9 and now.tm_min == 0:  # 每周三8:00更新需要买入的symbols及其权重
            start_time = time.time()
            huobifuture_api_url = 'https://api.btcgateway.pro'
            symbols = ['btc', 'eth', 'link', 'eos', 'fil', 'ltc', 'dot', 'doge']
            data_train, data_test, data_close = get_symbols_factor(symbols)
            model = model_train(data_train)
            buy_symbols = model_predict(model, data_test)
            buy_weights = optimize_weights(buy_symbols, data_close)
            print(buy_symbols, buy_weights)
            end_time = time.time()
            print(f"共耗时{end_time - start_time}秒")
            buy_info = dict()
            for i in range(3):
                if buy_weights[i]:
                    symbol = buy_symbols[i]
                    buy_info[symbol] = buy_weights[i]
            r2.hset('ml_buy_info', 'cache', json.dumps(buy_info))
        if weekday == 2 and hour == 10 and now.tm_min == 0:
            try:
                strategy_list = r2.hvals("ML_strategy")
                strategy_list = [json.loads(i) for i in strategy_list]
                buy_info = json.loads(r2.hget('ml_buy_info', 'cache'))
                T = []
                for strategy_info in strategy_list:
                    T.append(Thread(target=trade, args=(strategy_info, buy_info)))
                for t in T:
                    t.start()
                for t in T:
                    t.join()
            except Exception as e:
                print(e)
        time.sleep(2)
    # start_time = time.time()
    # symbols = ['btc', 'eth', 'link', 'eos', 'fil', 'ltc', 'dot', 'doge']
    # data_train, data_test, data_close = get_symbols_factor(symbols)
    # print(data_train.shape)
    # model = model_train(data_train)
    # buy_symbols = model_predict(model, data_test)
    # buy_weights = optimize_weights(buy_symbols, data_close)
    # print(buy_symbols, buy_weights)
    # end_time = time.time()
    # print(f"共耗时{end_time - start_time}秒")
    # strategydata = {
    #     "userUuid": "398051ac70ef4da9aafd33ce0b95195f",
    #     "apiAccountId": 10209,
    #     "strategyId": 786,
    #     "platform": "T8ex",
    #     "strategyType": 27,
    #     "amount": 100,
    #     "flag": 1
    # }
    # print(ml_stop_out(strategydata))

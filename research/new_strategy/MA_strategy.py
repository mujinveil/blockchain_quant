import smtplib
import time
from email.mime.text import MIMEText
from email.utils import formataddr
from threading import Thread
import numpy as np
import pandas as pd
import requests
import json

from tools.Config import Trade_url, Remain_url, pricelimit, amountlimit
from tools.get_market_info import get_currentprice0

np.set_printoptions(suppress=True)  # 取消科学计数法
from tools.databasePool import r2


def sendEmail(text):
    my_sender = '651006067@qq.com'  # 发件人邮箱账号
    my_pass = 'sbomjdyhtwfkbcca'  # 发件人邮箱密码
    my_user = '651006067@qq.com'  # 收件人邮箱账号
    # my_user = '18802675946@163.com'  # 收件人邮箱账号
    ret = True
    try:
        msg = MIMEText(text, 'plain', 'utf-8')
        msg['From'] = formataddr(["量化交易系统", my_sender])  # 括号里的对应发件人邮箱昵称、发件人邮箱账号
        msg['To'] = formataddr(["chenxiao", my_user])  # 括号里的对应收件人邮箱昵称、收件人邮箱账号
        msg['Subject'] = "KDJ行情指标"  # 邮件的主题，也可以说是标题
        server = smtplib.SMTP_SSL("smtp.qq.com", 465)  # 发件人邮箱中的SMTP服务器，端口是25
        server.login(my_sender, my_pass)  # 括号中对应的是发件人邮箱账号、邮箱密码
        server.sendmail(my_sender, [my_user, ], msg.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
        server.quit()  # 关闭连接
    except Exception:  # 如果 try 中的语句没有执行，则会执行下面的 ret=False
        ret = False
    return ret


# 获取k线数据（现货）
def get_klinedata(platform, symbol, granularity):
    klinedata = []
    try:
        # [时间，开盘价，最高价，最低价，收盘价，交易量]
        if platform == "okex":  # 200条数据，时间粒度60,300,3600,86400……
            res = requests.get("https://www.okexcn.com/api/spot/v3/instruments/{}/candles?granularity={}".format(
                symbol.upper().replace("_", "-"), granularity), timeout=1)
            klinedata = json.loads(res.content.decode())[::-1]  # 时间升序排列
            for i in klinedata:
                t = i[0].replace("T", " ").replace(".000Z", "")
                timeStruct = time.strptime(t, "%Y-%m-%d %H:%M:%S")
                timeStamp = int(time.mktime(timeStruct)) + 60 * 60 * 8
                i[0] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timeStamp))

        if platform == "huobi":  # 200条数据，时间粒度1min, 5min, 15min, 30min, 60min, 4hour, 1day, 1mon, 1week, 1year
            huobi_granularity_dict = {60: "1min", 300: "5min", 900: "15min", 1800: "30min", 3600: "60min",
                                      14400: "4hour", 86400: "1day", 604800: "1week", 2592000: "mon",
                                      946080000: "1year"}
            res = requests.get("https://api.huobi.fm/market/history/kline?period={}&size=200&symbol={}".format(
                huobi_granularity_dict[granularity], symbol.replace("_", "")), timeout=1)
            data = json.loads(res.content.decode())["data"]
            klinedata = []
            for i in data:
                l = [
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(i["id"])),
                    i["open"],
                    i["high"],
                    i["low"],
                    i["close"],
                    i["amount"]
                ]
                klinedata.append(l)
            klinedata = klinedata[::-1]
        if platform == "binance":  # 200条数据，时间粒度1m, 5m, 15m, 30m, 1h, 4h, 1d
            binance_granularity_dict = {60: "1m", 300: "5m", 900: "15m", 1800: "30m", 3600: "1h",
                                        14400: "4h", 86400: "1d"}
            res = requests.get("https://www.binancezh.cc/api/v3/klines?symbol={}&interval={}&limit=200".format(
                symbol.upper().replace("_", ""), binance_granularity_dict[granularity]), timeout=1)
            data = json.loads(res.content.decode())
            klinedata = []
            for i in data:
                l = [
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(str(i[0])[:10]))),
                    i[1],
                    i[2],
                    i[3],
                    i[4],
                    i[5],
                ]
                klinedata.append(l)
    except:
        pass
    return klinedata


"""定义带返回值的线程类"""


class MyThread(Thread):
    def __init__(self, func, args):
        super(MyThread, self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result
        except Exception:
            return None


# 获取三个交易所的k线数据，返回不为空的那个
def get_all_klinedata(symbol, granularity):
    t1 = MyThread(get_klinedata, args=("okex", symbol, granularity))
    t2 = MyThread(get_klinedata, args=("binance", symbol, granularity))
    t3 = MyThread(get_klinedata, args=("huobi", symbol, granularity))
    t1.start()
    t2.start()
    t3.start()
    t1.join(timeout=1)
    t1.join(timeout=1)
    t1.join(timeout=1)
    klinedata1 = t1.get_result()
    klinedata2 = t2.get_result()
    klinedata3 = t3.get_result()
    if klinedata1 != []:
        # print("okex--K线数据")
        return "okex", klinedata1
    else:
        if klinedata2 != []:
            # print("binance--K线数据")
            return "binance", klinedata2
        else:
            if klinedata3 != []:
                # print("huobi--K线数据")
                return "huobi", klinedata3
            else:
                return "", []


def MA_signal(klinedata):
    dataframe = dict({'time': [i[0] for i in klinedata],
                      'close': [float(i[4]) for i in klinedata]})
    data = pd.DataFrame(dataframe)
    data['MA10'] = data['close'].shift().rolling(10).mean()
    data['MA30'] = data['close'].shift().rolling(30).mean()
    MA10, MA30 = list(data['MA10'])[-1], list(data['MA30'])[-1]
    if MA10 > MA30:
        return 1
    elif MA10 < MA30:
        return -1


def MA_buy():
    userUuid = "6e7c88272f554956a35d8ed2cf833201"
    apiAccountId = 10156
    platform = "befinx"
    current_price = get_currentprice0(platform, symbol)
    buy_price = round(current_price * 1.001, pricelimit[symbol][platform])
    total_amount = 200
    buy_amount = total_amount / buy_price
    if buy_amount != 0:
        try:
            x, y = str(buy_amount).split(".")
            buy_amout = float(x + '.' + y[0:amountlimit[symbol][platform]])
        except:
            pass
    print(buy_amout)
    tradeparams = {"direction": 1, "amount": buy_amout, "symbol": symbol, "platform": platform, "price": buy_price,
                   "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 1, "strategyId": 1}
    res = requests.get(Remain_url, params={'userUuid': userUuid, 'apiAccountid': apiAccountId})
    print(res.json())
    traderes = requests.post(Trade_url, data=tradeparams)
    trade_dict=json.loads(traderes.content.decode())
    print(trade_dict)
    code=trade_dict['code']  #获取下单状态
    orderid=trade_dict['response']['orderid']  #获取订单id
    time.sleep(2)
    queryparams={'direction':1,'symbol':symbol,'platform':'huobi'}






def MA_sell():
    return


if __name__ == "__main__":
    while True:
        flag = 0
        try:
            granularity = 86400
            symbol = 'btc_usdt'
            # if int(time.time()) % granularity >= (granularity - 1):
            platform, klinedata = get_all_klinedata(symbol, granularity)
            signal = MA_signal(klinedata)
            print(signal)
            if signal == 1 and flag == 0:
                currentprice = float(klinedata[-1][4])
                info = "{}-{}，K线级别{}，MA【金叉】，当前价{}，当前时间{}".format(platform, symbol,
                                                                 "日线", currentprice,
                                                                 time.strftime("%Y-%m-%d %H:%M:%S",
                                                                               time.localtime()))
                # sendEmail(info)
                print(info)
                strategy_list = r2.hvals("MACD_strategy")
                strategy_list = [json.loads(i) for i in strategy_list]
                print(strategy_list)
                MA_buy()
                flag = 1
            elif signal == -1 and flag == 1:
                currentprice = float(klinedata[-1][4])
                info = "{}-{},K线级别{}，MA【死叉】，当前价{}，当前时间{}".format(platform, symbol,
                                                                 "日线", currentprice,
                                                                 time.strftime("%Y-%m-%d %H:%M:%s",
                                                                               time.localtime()))
                # sendEmail(info)
                print(info)
                # MA_sell()
                flag = 0
        except Exception as e:
            pass
        time.sleep(1)

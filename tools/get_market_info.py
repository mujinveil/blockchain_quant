# encoding=utf-8
import json
import random
import threading
import time
import pandas as pd
import requests
from tools.Config import symbols, pricelimit0, befinx_base_url, huobi_api_url, binance_api_url, okex_api_url, \
    bitget_base_url, T8ex_base_url
from tools.User_agent_list import USER_AGENT_LIST


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


"""bw行情"""


def bwfunc(symbol):
    try:
        bwdict = {"platform": "bw", "symbol": symbol}
        symbol = symbol.upper()
        res = requests.get("https://kline.bw.com/api/data/v1/entrusts", params={"marketName": symbol, "dataSize": 5},
                           headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
        dict = json.loads(res.content.decode())
        ask = dict["datas"]["asks"][-1]
        bid = dict["datas"]["bids"][0]
        bwdict["sellprice"] = float(ask[0])
        bwdict["sellquantity"] = float(ask[-1])
        bwdict["buyprice"] = float(bid[0])
        bwdict["buyquantity"] = float(bid[-1])
        return bwdict
    except:
        return {'platform': 'bw', 'symbol': symbol,
                'sellprice': 0, 'sellquantity': 0, 'buyprice': 0, 'buyquantity': 0}


"""ZB行情"""


def zbfunc(symbol):
    try:
        res = requests.get("http://api.zb.live/data/v1/depth", params={"market": symbol, "size": 3},
                           headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
        dict = json.loads(res.content.decode())
        zbdict = {"platform": "zb", "symbol": symbol}
        ask = dict["asks"][-1]
        bid = dict["bids"][0]
        zbdict["sellprice"] = ask[0]
        zbdict["sellquantity"] = ask[-1]
        zbdict["buyprice"] = bid[0]
        zbdict["buyquantity"] = bid[-1]
        return zbdict
    except:
        return {'platform': 'zb', 'symbol': symbol, 'sellprice': 0, 'sellquantity': 0, 'buyprice': 0, 'buyquantity': 0}


"""gate行情"""


def gatefunc(symbol):
    try:
        res = requests.get("https://data.gateio.co/api2/1/orderBook/{}".format(symbol),
                           headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
        dict = json.loads(res.content.decode())
        gatedict = {"platform": "gate", "symbol": symbol}
        ask = dict["asks"][-1]
        bid = dict["bids"][0]
        gatedict["sellprice"] = float(ask[0])
        gatedict["sellquantity"] = float(ask[-1])
        gatedict["buyprice"] = float(bid[0])
        gatedict["buyquantity"] = float(bid[-1])
        return gatedict
    except:
        return {'platform': 'gate', 'symbol': symbol, 'sellprice': 0, 'sellquantity': 0, 'buyprice': 0,
                'buyquantity': 0}


"""bitz行情"""


def bitzfunc(symbol):
    try:
        bitzres = requests.get("https://api.bitzapi.com/Market/depth?symbol={}".format(symbol),
                               headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
        dict = json.loads(bitzres.content.decode())
        bitzdict = {"platform": "bitz", "symbol": symbol}
        ask = dict["data"]["asks"][-1]
        bid = dict["data"]["bids"][0]
        bitzdict["sellprice"] = float(ask[0])
        bitzdict["sellquantity"] = float(ask[1])
        bitzdict["buyprice"] = float(bid[0])
        bitzdict["buyquantity"] = float(bid[1])
        return bitzdict
    except:
        return {'platform': 'bitz', 'symbol': symbol, 'sellprice': 0, 'sellquantity': 0, 'buyprice': 0,
                'buyquantity': 0}


"""okex行情"""


def okexfunc(symbol):
    try:
        res = requests.get(
            "https://{}/api/spot/v3/instruments/{}/book?size=10&depth=0.001".format(okex_api_url, symbol.upper()),
            headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
        dict = json.loads(res.content.decode())
        okexdict = {"platform": "okex", "symbol": symbol}
        ask = dict["asks"][0]
        bid = dict["bids"][0]
        okexdict["sellprice"] = float(ask[0])
        okexdict["sellquantity"] = float(ask[1])
        okexdict["buyprice"] = float(bid[0])
        okexdict["buyquantity"] = float(bid[1])
        return okexdict
    except:
        return {'platform': 'okex', 'symbol': symbol, 'sellprice': 0, 'sellquantity': 0, 'buyprice': 0,
                'buyquantity': 0}


"""binance行情"""


def binancefunc(symbol):
    try:
        res = requests.get(
            "https://{}/api/v1/depth?symbol={}&limit=5".format(binance_api_url, symbol.replace("_", "").upper()),
            headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
        dict = json.loads(res.content.decode())
        binancedict = {"platform": "binance", "symbol": symbol}
        ask = dict["asks"][0]
        bid = dict["bids"][0]
        binancedict["sellprice"] = float(ask[0])
        binancedict["sellquantity"] = float(ask[1])
        binancedict["buyprice"] = float(bid[0])
        binancedict["buyquantity"] = float(bid[1])
        return binancedict
    except:
        return {'platform': 'binance', 'symbol': symbol, 'sellprice': 0, 'sellquantity': 0, 'buyprice': 0,
                'buyquantity': 0}


"""huobi行情"""


def huobifunc(symbol):
    try:
        res = requests.get(
            "https://{}/market/depth?symbol={}&type=step0".format(huobi_api_url, symbol.replace("_", "")),
            headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
        dict = json.loads(res.content.decode())
        huobidict = {"platform": "huobi", "symbol": symbol}
        ask = dict["tick"]["asks"][0]
        bid = dict["tick"]["bids"][0]
        huobidict["sellprice"] = float(ask[0])
        huobidict["sellquantity"] = float(ask[1])
        huobidict["buyprice"] = float(bid[0])
        huobidict["buyquantity"] = float(bid[1])
        return huobidict
    except:
        return {'platform': 'huobi', 'symbol': symbol, 'sellprice': 0, 'sellquantity': 0, 'buyprice': 0,
                'buyquantity': 0}


"""aifive行情"""


def aifivefunc(symbol):
    try:
        res = requests.get(
            "https://api.fubt.co/v1/market/depth", params={"symbol": symbol.replace("_", "").upper(),
                                                           "accessKey": "JvWI3i9HxUB/nm5yMoh0RUUbx6GHvlanaA4/InXEcSc=",
                                                           "step": "STEP0"},
            headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
        dict = json.loads(res.content.decode())
        aifivedict = {"platform": "aifive", "symbol": symbol}
        ask = dict["data"]["sell"]
        bid = dict["data"]["buy"]
        aifivedict["sellprice"] = float(ask[0]["price"])
        aifivedict["sellquantity"] = float(ask[0]["amount"])
        aifivedict["buyprice"] = float(bid[0]["price"])
        aifivedict["buyquantity"] = float(bid[0]["amount"])
        return aifivedict
    except:
        return {'platform': 'aifive', 'symbol': symbol, 'sellprice': 0, 'sellquantity': 0, 'buyprice': 0,
                'buyquantity': 0}


"""befinx行情"""


def befinxfunc(symbol):
    try:
        url = "{}/open-api/open/trade_plate".format(befinx_base_url)
        res = requests.post(url, data={"symbol": symbol.replace("_", "/").upper(), "size": 30})
        dict = json.loads(res.content.decode())
        befinxdict = {"platform": "befinx", "symbol": symbol}
        ask = dict["data"]["ask"]
        bid = dict["data"]["bid"]
        befinxdict["sellprice"] = float(ask[0]["price"])
        befinxdict["sellquantity"] = float(ask[0]["amount"])
        befinxdict["buyprice"] = float(bid[0]["price"])
        befinxdict["buyquantity"] = float(bid[0]["amount"])
        return befinxdict
    except:
        return {'platform': 'befinx', 'symbol': symbol, 'sellprice': 0, 'sellquantity': 0, 'buyprice': 0,
                'buyquantity': 0}


"""获取单个平台现价"""


def getcurretprice_zb(symbol):
    try:
        res1 = requests.get("http://api.zb.live/data/v1/ticker", params={"market": symbol},
                            headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
        res = json.loads(res1.content.decode())
        currentprice = float(res["ticker"]["last"])
        return {"platform": "zb", "symbol": symbol, "currentprice": currentprice}
        # r.set("currentprice:zb:" + symbol, currentprice)
    except:
        return {"platform": "zb", "symbol": symbol, "currentprice": 0}


def getcurretprice_bitz(symbol):
    try:
        res1 = requests.get("https://apiv2.bitz.com/Market/ticker?symbol={}".format(symbol),
                            headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
        res = json.loads(res1.content.decode())
        currentprice = float(res["data"]["now"])
        return {"platform": "bitz", "symbol": symbol, "currentprice": currentprice}
        # r.set("currentprice:bitz:" + symbol, currentprice)
    except:
        return {"platform": "bitz", "symbol": symbol, "currentprice": 0}


def getcurretprice_bw(symbol):
    try:
        res1 = requests.get("https://www.bw.com/api/data/v1/ticker", params={"marketName": symbol.upper()},
                            headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
        dict1 = json.loads(res1.content.decode())
        currentprice = float(dict1["datas"][1])
        return {"platform": "bw", "symbol": symbol, "currentprice": currentprice}
    except:
        return {"platform": "bw", "symbol": symbol, "currentprice": 0}


def getcurretprice_gate(symbol):
    try:
        res1 = requests.get("https://data.gateio.co/api2/1/ticker/{}".format(symbol),
                            headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
        dict1 = json.loads(res1.content.decode())
        currentprice = float(dict1["last"])
        return {"platform": "gate", "symbol": symbol, "currentprice": currentprice}
        # r.set("currentprice:gate:" + symbol, currentprice)
    except:
        return {"platform": "gate", "symbol": symbol, "currentprice": 0}


def getcurretprice_okex(symbol):
    try:
        res1 = requests.get("https://{}/api/spot/v3/instruments/{}/ticker".format(okex_api_url, symbol.upper()),
                            headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
        dict1 = json.loads(res1.content.decode())
        currentprice = float(dict1["last"])
        return {"platform": "okex", "symbol": symbol, "currentprice": currentprice}
        # r.set("currentprice:okex:" + symbol, currentprice)
    except:
        return {"platform": "okex", "symbol": symbol, "currentprice": 0}


def getcurretprice_binance(symbol):
    try:
        res1 = requests.get(
            "https://{}/api/v1/ticker/price?symbol={}".format(binance_api_url, symbol.replace("_", "").upper()),
            headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
        dict1 = json.loads(res1.content.decode())
        currentprice = float(dict1["price"])
        return {"platform": "binance", "symbol": symbol, "currentprice": currentprice}
        # r.set("currentprice:binance:" + symbol, currentprice)
    except:
        return {"platform": "binance", "symbol": symbol, "currentprice": 0}


def getcurretprice_huobi(symbol):
    try:
        res1 = requests.get(
            "https://{}/market/trade?symbol={}".format(huobi_api_url, symbol.replace("_", "")),
            headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
        dict1 = json.loads(res1.content.decode())
        currentprice = dict1["tick"]["data"][0]["price"]
        return {"platform": "huobi", "symbol": symbol, "currentprice": currentprice}
        # r.set("currentprice:huobi:" + symbol, currentprice)
    except:
        return {"platform": "huobi", "symbol": symbol, "currentprice": 0}


def getcurretprice_aifive(symbol):
    try:
        res1 = requests.get(
            "https://api.fubt.co/v1/market/ticker",
            params={"symbol": symbol.replace("_", "").upper(),
                    "accessKey": "JvWI3i9HxUB/nm5yMoh0RUUbx6GHvlanaA4/InXEcSc="},
            headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
        dict1 = json.loads(res1.content.decode())
        currentprice = dict1["data"]["last"]
        return {"platform": "aifive", "symbol": symbol, "currentprice": currentprice}
        # r.set("currentprice:aifive:" + symbol, currentprice)
    except:
        return {"platform": "aifive", "symbol": symbol, "currentprice": 0}


# 获取befinx交易所最新成交价
def getcurrentprice_befinx(symbol):
    try:
        url = "{}/open-api/open/trade_history".format(befinx_base_url)
        res = requests.post(url, data={"symbol": symbol.replace("_", "/").upper(), "size": 1})
        resdict = json.loads(res.content.decode())
        currentprice = resdict["data"][0]["price"]
        return {"platform": "befinx", "symbol": symbol, "currentprice": currentprice}
    except:
        return {"platform": "befinx", "symbol": symbol, "currentprice": 0}


# 根据行情价修改网格间距
def updateGridspace():
    currentpricelist = get_all_currentprice()
    btcusdtlist = []
    ethusdtlist = []
    xrpusdtlist = []
    eosusdtlist = []
    ethbtclist = []
    eosbtclist = []
    xrpbtclist = []
    for item in currentpricelist:
        if item["symbol"] == "btc_usdt" and item["currentprice"] != 0:
            btcusdtlist.append(item)
        if item["symbol"] == "eth_usdt" and item["currentprice"] != 0:
            ethusdtlist.append(item)
        if item["symbol"] == "xrp_usdt" and item["currentprice"] != 0:
            xrpusdtlist.append(item)
        if item["symbol"] == "eos_usdt" and item["currentprice"] != 0:
            eosusdtlist.append(item)
        if item["symbol"] == "xrp_usdt" and item["currentprice"] != 0:
            xrpusdtlist.append(item)
        if item["symbol"] == "eth_btc" and item["currentprice"] != 0:
            ethbtclist.append(item)
        if item["symbol"] == "eos_btc" and item["currentprice"] != 0:
            eosbtclist.append(item)
        if item["symbol"] == "xrp_btc" and item["currentprice"] != 0:
            xrpbtclist.append(item)

    btcusdtprice = sum([i["currentprice"] for i in btcusdtlist]) / len(btcusdtlist)
    ethusdtprice = sum([i["currentprice"] for i in ethusdtlist]) / len(ethusdtlist)
    eosusdtprice = sum([i["currentprice"] for i in eosusdtlist]) / len(eosusdtlist)
    xrpusdtprice = sum([i["currentprice"] for i in xrpusdtlist]) / len(xrpusdtlist)
    # ethbtcprice = sum([i["currentprice"] for i in ethbtclist]) / len(ethbtclist)
    # eosbtcprice = sum([i["currentprice"] for i in eosbtclist]) / len(eosbtclist)
    # xrpbtcprice = sum([i["currentprice"] for i in xrpbtclist]) / len(xrpbtclist)
    print(btcusdtprice, ethusdtprice, eosusdtprice, xrpusdtprice)
    # 网格默认间距
    btcusdtgap = round(btcusdtprice * 0.0025, pricelimit0["btc_usdt"]) * 2
    ethusdtgap = round(ethusdtprice * 0.0025, pricelimit0["eth_usdt"]) * 2
    eosusdtgap = round(eosusdtprice * 0.0025, pricelimit0["eos_usdt"]) * 2
    xrpusdtgap = round(xrpusdtprice * 0.0025, pricelimit0["xrp_usdt"]) * 2
    # ethbtcgap = round(ethbtcprice * 0.0025, pricelimit0["eth_btc"]) * 2
    # eosbtcgap = round(eosbtcprice * 0.0025, pricelimit0["eos_btc"]) * 2
    # xrpbtcgap = round(xrpbtcprice * 0.0025, pricelimit0["xrp_btc"]) * 2
    # 长线默认间距
    btcusdt_tracegap = round(btcusdtprice * 0.015, pricelimit0["btc_usdt"])
    ethusdt_tracegap = round(ethusdtprice * 0.015, pricelimit0["eth_usdt"])
    eosusdt_tracegap = round(eosusdtprice * 0.015, pricelimit0["eos_usdt"])
    xrpusdt_tracegap = round(xrpusdtprice * 0.015, pricelimit0["xrp_usdt"])

    print(btcusdtgap, ethusdtgap, eosusdtgap, xrpusdtgap, btcusdt_tracegap, ethusdt_tracegap, eosusdt_tracegap,
          xrpusdt_tracegap)

    return {"btcusdtgap": btcusdtgap, "ethusdtgap": ethusdtgap, "eosusdtgap": eosusdtgap,
            "xrpusdtgap": xrpusdtgap, "btcusdt_tracegap": btcusdt_tracegap, "ethusdt_tracegap": ethusdt_tracegap,
            "eosusdt_tracegap": eosusdt_tracegap, "xrpusdt_tracegap": xrpusdt_tracegap}


# 获取单个平台现价
def get_currentprice0(platform, symbol):
    try:
        if platform == "zb":
            res1 = requests.get("http://api.zb.live/data/v1/ticker", params={"market": symbol},
                                headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
            res = json.loads(res1.content.decode())
            currentprice = float(res["ticker"]["last"])
            return currentprice
        elif platform == "bitz":
            res1 = requests.get("https://apiv2.bitz.com/Market/ticker?symbol={}".format(symbol),
                                headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
            res = json.loads(res1.content.decode())
            currentprice = float(res["data"]["now"])
            return currentprice
        elif platform == "bw":
            res1 = requests.get("https://www.bw.com/api/data/v1/ticker", params={"marketName": symbol.upper()},
                                headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
            dict1 = json.loads(res1.content.decode())
            currentprice = float(dict1["datas"][1])
            return currentprice
        elif platform == "gate":
            res1 = requests.get("https://data.gateio.co/api2/1/ticker/{}".format(symbol),
                                headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
            dict1 = json.loads(res1.content.decode())
            currentprice = float(dict1["last"])
            return currentprice
        elif platform == "okex":
            res1 = requests.get("https://{}/api/spot/v3/instruments/{}/ticker".format(okex_api_url, symbol.upper()),
                                headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
            dict1 = json.loads(res1.content.decode())
            currentprice = float(dict1["last"])
            return currentprice
        elif platform == "binance":
            for i in range(3):
                try:
                    res1 = requests.get(
                        "https://{}/api/v1/ticker/price?symbol={}".format(binance_api_url, symbol.replace("_", "").upper()),
                        headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
                    if res1.status_code == 200:
                        dict1 = json.loads(res1.content.decode())
                        currentprice = float(dict1["price"])
                        break
                except:
                    pass
            return currentprice
        elif platform == "huobi":
            for _ in range(3):
                try:
                    res1 = requests.get(
                        "https://{}/market/trade?symbol={}".format(huobi_api_url, symbol.replace("_", "")),
                        headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
                    dict1 = json.loads(res1.content.decode())
                    currentprice = dict1["tick"]["data"][0]["price"]
                    return currentprice
                except:
                    pass
        elif platform == "aifive":
            res1 = requests.get(
                "https://api.fubt.co/v1/market/ticker",
                params={"symbol": symbol.replace("_", "").upper(),
                        "accessKey": "JvWI3i9HxUB/nm5yMoh0RUUbx6GHvlanaA4/InXEcSc="},
                headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
            dict1 = json.loads(res1.content.decode())
            currentprice = dict1["data"]["last"]
            return currentprice

        elif platform == "befinx":
            url = "{}/open-api/open/trade_history".format(befinx_base_url)
            res = requests.post(url, data={"symbol": symbol.replace("_", "/").upper(), "size": 1})
            resdict = json.loads(res.content.decode())
            lastprice = resdict["data"][0]["price"]
            return lastprice

        elif platform == 'bitget':
            url = "{}/open-api/open/trade_history".format(bitget_base_url)
            res = requests.post(url, data={'symbol': symbol.replace("_", '/').upper(), 'size': 1})
            resdict = json.loads(res.content.decode())
            lastprice = resdict['data'][0]['price']
            return lastprice
        elif platform == 'T8ex':
            url = "{}/open-api/open/trade_history".format(T8ex_base_url)
            res = requests.post(url, data={'symbol': symbol.replace("_", '/').upper(), 'size': 1})
            resdict = json.loads(res.content.decode())
            lastprice = resdict['data'][0]['price']
            return lastprice
    except Exception as e:
        print("获取{}现价失败,原因{}".format(platform, e))
        return 0


# 如果获取失败，从币安获和okex取
def get_currentprice1(platform, symbol):
    currentprice = get_currentprice0(platform, symbol)
    if currentprice == 0:
        # print("{}获取{}现价失败，尝试从okex获取".format(platform, symbol))
        try:
            res1 = requests.get("https://{}/api/spot/v3/instruments/{}/ticker".format(okex_api_url, symbol.upper()),
                                headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
            dict1 = json.loads(res1.content.decode())
            currentprice = float(dict1["last"])
        except:
            currentprice = 0
    if currentprice == 0:
        # print("{}获取{}现价失败，尝试从huobi获取".format(platform, symbol))
        try:
            res1 = requests.get(
                "https://{}/market/trade?symbol={}".format(huobi_api_url, symbol.replace("_", "")),
                headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
            dict1 = json.loads(res1.content.decode())
            currentprice = dict1["tick"]["data"][0]["price"]
        except:
            currentprice = 0
    if currentprice == 0:
        # print("{}获取{}现价失败，尝试从币安获取".format(platform, symbol))
        try:
            res1 = requests.get(
                "https://{}/api/v1/ticker/price?symbol={}".format(binance_api_url, symbol.replace("_", "").upper()),
                headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
            dict1 = json.loads(res1.content.decode())
            currentprice = float(dict1["price"])
        except:
            currentprice = 0
    if currentprice == 0:
        for i in range(3):
            currentprice = huobifunc(symbol)['sellprice']
            if currentprice:
                break
    return currentprice


# 获取某个交易所的深度
def get_market_depth(platform, symbol):
    try:
        if platform == "bw":
            bwdict = {"platform": "bw", "symbol": symbol}
            symbol = symbol.upper()
            res = requests.get("https://kline.bw.com/api/data/v1/entrusts",
                               params={"marketName": symbol, "dataSize": 5},
                               headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
            dict = json.loads(res.content.decode())
            ask = dict["datas"]["asks"][-1]
            bid = dict["datas"]["bids"][0]
            bwdict["sellprice"] = float(ask[0])
            bwdict["sellquantity"] = float(ask[-1])
            bwdict["buyprice"] = float(bid[0])
            bwdict["buyquantity"] = float(bid[-1])
            return bwdict

        if platform == "zb":
            res = requests.get("http://api.zb.live/data/v1/depth", params={"market": symbol, "size": 3},
                               headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
            dict = json.loads(res.content.decode())
            zbdict = {"platform": "zb", "symbol": symbol}
            ask = dict["asks"][-1]
            bid = dict["bids"][0]
            zbdict["sellprice"] = ask[0]
            zbdict["sellquantity"] = ask[-1]
            zbdict["buyprice"] = bid[0]
            zbdict["buyquantity"] = bid[-1]
            return zbdict

        if platform == "gate":
            res = requests.get("https://data.gateio.co/api2/1/orderBook/{}".format(symbol),
                               headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
            dict = json.loads(res.content.decode())
            gatedict = {"platform": "gate", "symbol": symbol}
            ask = dict["asks"][-1]
            bid = dict["bids"][0]
            gatedict["sellprice"] = float(ask[0])
            gatedict["sellquantity"] = float(ask[-1])
            gatedict["buyprice"] = float(bid[0])
            gatedict["buyquantity"] = float(bid[-1])
            return gatedict

        if platform == "bitz":
            bitzres = requests.get("https://api.bitzapi.com/Market/depth?symbol={}".format(symbol),
                                   headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
            dict = json.loads(bitzres.content.decode())
            bitzdict = {"platform": "bitz", "symbol": symbol}
            ask = dict["data"]["asks"][-1]
            bid = dict["data"]["bids"][0]
            bitzdict["sellprice"] = float(ask[0])
            bitzdict["sellquantity"] = float(ask[1])
            bitzdict["buyprice"] = float(bid[0])
            bitzdict["buyquantity"] = float(bid[1])
            return bitzdict

        if platform == "okex":
            res = requests.get(
                "https://{}/api/spot/v3/instruments/{}/book?size=10&depth=0.001".format(okex_api_url, symbol.upper()),
                headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
            dict = json.loads(res.content.decode())
            okexdict = {"platform": "okex", "symbol": symbol}
            ask = dict["asks"][0]
            bid = dict["bids"][0]
            okexdict["sellprice"] = float(ask[0])
            okexdict["sellquantity"] = float(ask[1])
            okexdict["buyprice"] = float(bid[0])
            okexdict["buyquantity"] = float(bid[1])
            return okexdict

        if platform == "binance":
            res = requests.get(
                "https://{}/api/v1/depth?symbol={}&limit=5".format(binance_api_url, symbol.replace("_", "").upper()),
                headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
            dict = json.loads(res.content.decode())
            binancedict = {"platform": "binance", "symbol": symbol}
            ask = dict["asks"][0]
            bid = dict["bids"][0]
            binancedict["sellprice"] = float(ask[0])
            binancedict["sellquantity"] = float(ask[1])
            binancedict["buyprice"] = float(bid[0])
            binancedict["buyquantity"] = float(bid[1])
            return binancedict

        if platform == "huobi":
            res = requests.get(
                "https://{}/market/depth?symbol={}&type=step0".format(huobi_api_url, symbol.replace("_", "")),
                headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
            dict = json.loads(res.content.decode())
            huobidict = {"platform": "huobi", "symbol": symbol}
            ask = dict["tick"]["asks"][0]
            bid = dict["tick"]["bids"][0]
            huobidict["sellprice"] = float(ask[0])
            huobidict["sellquantity"] = float(ask[1])
            huobidict["buyprice"] = float(bid[0])
            huobidict["buyquantity"] = float(bid[1])
            return huobidict

        if platform == "aifive":
            res = requests.get(
                "https://api.fubt.co/v1/market/depth", params={"symbol": symbol.replace("_", "").upper(),
                                                               "accessKey": "JvWI3i9HxUB/nm5yMoh0RUUbx6GHvlanaA4/InXEcSc=",
                                                               "step": "STEP0"},
                headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
            dict = json.loads(res.content.decode())
            aifivedict = {"platform": "aifive", "symbol": symbol}
            ask = dict["data"]["sell"]
            bid = dict["data"]["buy"]
            aifivedict["sellprice"] = float(ask[0]["price"])
            aifivedict["sellquantity"] = float(ask[0]["amount"])
            aifivedict["buyprice"] = float(bid[0]["price"])
            aifivedict["buyquantity"] = float(bid[0]["amount"])
            return aifivedict

        if platform == "befinx":
            url = "{}/open-api/open/trade_plate".format(befinx_base_url)
            res = requests.post(url, data={"symbol": symbol.replace("_", "/").upper(), "size": 30})
            dict = json.loads(res.content.decode())
            befinxdict = {"platform": "befinx", "symbol": symbol}
            ask = dict["data"]["ask"]
            bid = dict["data"]["bid"]
            print(ask)
            print(bid)
            befinxdict["sellprice"] = float(ask[0]["price"])
            befinxdict["sellquantity"] = float(ask[0]["amount"])
            befinxdict["buyprice"] = float(bid[0]["price"])
            befinxdict["buyquantity"] = float(bid[0]["amount"])
            return befinxdict
    except:
        return {'platform': platform, 'symbol': symbol, 'sellprice': 0, 'sellquantity': 0, 'buyprice': 0,
                'buyquantity': 0}


# 获取所有交易所交易对的现价
def get_all_currentprice():
    result_list = []
    thread_list = []
    for symbol in symbols:
        for func in [getcurretprice_okex, getcurretprice_binance, getcurretprice_huobi, getcurrentprice_befinx,
                     getcurretprice_zb]:
            t = MyThread(func, (symbol,))
            thread_list.append(t)
            t.start()
    for t in thread_list:
        t.join()
        result_list.append(t.get_result())
    return result_list




# 获取所有交易所的深度
def get_all_market_depth():
    result_list = []
    thread_list = []
    for symbol in symbols:
        for func in [huobifunc, okexfunc, binancefunc, befinxfunc]:
            t = MyThread(func, (symbol,))
            thread_list.append(t)
            t.start()
    for t in thread_list:
        t.join()
        result_list.append(t.get_result())
    return result_list


def get_okex_price_and_side(symbol):
    urllist = [okex_api_url, "www.okexcn.com"]
    currentprice = 0
    side = "buy"
    for url in urllist:
        try:
            res1 = requests.get(
                "https://{}/api/spot/v3/instruments/{}/trades?limit=1".format(url, symbol.replace("_", "-").upper()),
                headers={"user-agent": random.choice(USER_AGENT_LIST)}, timeout=2)
            dict1 = json.loads(res1.content.decode())
            currentprice = float(dict1[0]["price"])
            side = dict1[0]["side"]
            break
        except:
            pass
    return currentprice, side


# 获取befinx交易所盘口买卖单
def get_befinx_orders(symbol):
    url = "{}/open-api/open/trade_plate".format(befinx_base_url)
    res = requests.post(url, data={"symbol": symbol.replace("_", "/").upper(), "size": 1000})
    resdict = json.loads(res.content.decode())
    return resdict


# 获取bitget交易所盘口买卖单
def get_bitget_orders(symbol):
    url = '{}/open-api/open/trade_plate'.format(bitget_base_url)
    res = requests.post(url, data={'symbol': symbol.replace("_", "/").upper(), 'size': 1000})
    resdict = json.loads(res.content.decode())
    return resdict


# 获取T8ex交易所盘口买卖单
def get_T8ex_orders(symbol):
    url = '{}/open-api/open/trade_plate'.format(T8ex_base_url)
    res = requests.post(url, data={'symbol': symbol.replace("_", "/").upper(), 'size': 1000})
    resdict = json.loads(res.content.decode())
    return resdict



if __name__ == '__main__':
    # a = get_currentprice0("okex","btc_usdt")
    b = get_currentprice1('huobi', "btc_usdt")
    # c = get_currentprice0("binance","btc_usdt")
    # d = get_currentprice0("befinx","btc_usdt")
    # print(a)
    # print(b)
    # print(c)
    # print(d)
    # datalist = get_market_depth("befinx","btc_usdt")
    # print(datalist)

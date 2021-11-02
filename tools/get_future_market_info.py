# encoding=utf-8
import json
import time
import pandas as pd
import requests
import sys
sys.path.append("..")
from tools.Config import huobifuture_api_url, T8ex_kline_url, binancefuture_api_url, T8ex_base_url


# 获取火币或者T8或者币安历史合约K线收盘价数据
def get_future_klinedata0(platform, symbol, granularity=50):
    if platform == 'huobi':
        for _ in range(3):
            now = int(time.time())
            start_time = now - 86400 * granularity
            contract_code = "{}-usdt".format(symbol).upper()
            url = huobifuture_api_url + '/linear-swap-ex/market/history/kline?contract_code={}&period=1day&from={}&to={}'.format(
                contract_code, start_time, now)
            try:
                res = requests.get(url)
                if res.status_code == 200:
                    resdict = json.loads(res.content.decode())
                    df = pd.DataFrame()
                    df['close'] = [i['close'] for i in resdict['data']]
                    break
            except Exception as e:
                print(e)
                df = pd.DataFrame()
        return df
    elif platform == "T8ex":
        try:
            contract_id_dict = {'BTC': 1, 'ETH': 2, 'LINK': 3, 'EOS': 4, 'FIL': 5, 'LTC': 6, 'UNI': 7, 'DOT': 8,
                                'DOGE': 9}
            contract_id = contract_id_dict[symbol.upper()]
            now = int(time.time())
            start_time = now - 86400 * granularity
            url = T8ex_kline_url + "?contractId={}&from={}&to={}&resolution={}".format(contract_id, start_time * 1000,
                                                                                       now * 1000, "1D")
            res = requests.get(url).json()
            df = pd.DataFrame()
            df['close'] = [i[4] for i in res]
        except Exception as e:
            print(e)
            df = pd.DataFrame()
        finally:
            return df

    elif platform == "binance":
        for _ in range(3):
            url = binancefuture_api_url + '/dapi/v1/continuousKlines'
            pair = "{}usd".format(symbol).upper()
            now = int(time.time())
            start_time = now - 86400 * granularity
            data = {'pair': pair, 'contractType': "PERPETUAL", 'interval': '1d', 'starttime': start_time * 1000,
                    'endtime': now * 1000}
            try:
                response = requests.get(url, params=data, timeout=1)
                if response.status_code == 200:
                    res = response.json()
                    df = pd.DataFrame()
                    df['close'] = [float(i[4]) for i in res]
                    break
            except Exception as e:
                print(e)
        return df


# 获取火币、T8、币安历史合约K线收盘价数据
def get_future_klinedata1(platform, symbol):
    df = get_future_klinedata0(platform, symbol, granularity=50)
    if df.empty:
        try:
            now = int(time.time())
            start_time = now - 86400 * 200
            contract_code = "{}-usdt".format(symbol).upper()
            url = huobifuture_api_url + '/linear-swap-ex/market/history/kline?contract_code={}&period=1day&from={}&to={}'.format(
                contract_code, start_time, now)
            res = requests.get(url)
            resdict = json.loads(res.content.decode())
            df = pd.DataFrame()
            df['close'] = [i['close'] for i in resdict['data']]
            df['high'] = [i['high'] for i in resdict['data']]
            df['low'] = [i['low'] for i in resdict['data']]
            df['open'] = [i['open'] for i in resdict['data']]
        except Exception as e:
            df = pd.DataFrame()
        if df.empty:
            try:
                contract_id_dict = {'BTC': 1, 'ETH': 2, 'LINK': 3, 'EOS': 4, 'FIL': 5, 'LTC': 6, 'UNI': 7, 'DOT': 8,
                                    'DOGE': 9}
                contract_id = contract_id_dict[symbol.upper()]
                now = int(time.time())
                start_time = now - 86400 * 200
                url = T8ex_kline_url + "?contractId={}&from={}&to={}&resolution={}".format(contract_id,
                                                                                           start_time * 1000,
                                                                                           now * 1000,
                                                                                           "1D")
                res = requests.get(url).json()
                df = pd.DataFrame()
                df['close'] = [i[4] for i in res]
                df['high'] = [i[2] for i in res]
                df['low'] = [i[3] for i in res]
                df['open'] = [i[1] for i in res]
            except Exception as e:
                df = pd.DataFrame()
        if df.empty:
            try:
                url = binancefuture_api_url + '/dapi/v1/continuousKlines'
                pair = "{}usd".format(symbol).upper()
                now = int(time.time())
                start_time = now - 86400 * 200
                data = {'pair': pair, 'contractType': "PERPETUAL", 'interval': '1d', 'starttime': start_time * 1000,
                        'endtime': now * 1000}
                res = requests.get(url, params=data, timeout=1).json()
                df = pd.DataFrame()
                df['high'] = [i[2] for i in res]
                df['low'] = [i[3] for i in res]
                df['close'] = [i[4] for i in res]
            except Exception as e:
                df = pd.DataFrame()
    return df


# 获取火币历史合约K线最高价、最低价、开盘价、收盘价数据
def get_huobifuture_klinedata(symbol):
    try:
        now = int(time.time())
        start_time = now - 86400 * 50
        contract_code = "{}-usdt".format(symbol).upper()
        url = huobifuture_api_url + '/linear-swap-ex/market/history/kline?contract_code={}&period=1day&from={}&to={}'.format(
            contract_code, start_time, now)
        res = requests.get(url)
        resdict = json.loads(res.content.decode())
        df = pd.DataFrame()
        df['close'] = [i['close'] for i in resdict['data']]
        df['high'] = [i['high'] for i in resdict['data']]
        df['low'] = [i['low'] for i in resdict['data']]
        df['open'] = [i['open'] for i in resdict['data']]
    except Exception as e:
        print(e)
        df = pd.DataFrame()
    finally:
        return df


# 获取永续合约价格
def get_perpetualprice0(platform, symbol):
    if platform == 'huobi':
        try:
            now = int(time.time())
            start_time = now - 300
            contract_code = "{}-usdt".format(symbol).upper()
            url = huobifuture_api_url + '/linear-swap-ex/market/history/kline?contract_code={}&period=5min&from={}&to={}'.format(
                contract_code, start_time, now)
            res = requests.get(url)
            resdict = json.loads(res.content.decode())
            close_price = [i['close'] for i in resdict['data']][-1]
        except:
            close_price = 0
        return close_price
    elif platform == "T8ex":
        try:
            contract_id_dict = {'BTC': 1, 'ETH': 2, 'LINK': 3, 'EOS': 4, 'FIL': 5, 'LTC': 6, 'UNI': 7, 'DOT': 8,
                                'DOGE': 9}
            contract_id = contract_id_dict[symbol.upper()]
            now = int(time.time())
            start_time = now - 300
            url = T8ex_kline_url + "?contractId={}&from={}&to={}&resolution={}".format(contract_id, start_time * 1000,
                                                                                       now * 1000, 1)
            res = requests.get(url).json()
            if res:
                close_price = res[-1][-2]
        except:
            close_price = 0
        return close_price
    elif platform == "binance":
        try:
            url = binancefuture_api_url + '/dapi/v1/ticker/price'
            data = "{}usdt".format(symbol).upper()
            res = requests.get(url, params={"symbol": data}, timeout=2).json()
            if res:
                close_price = float(res['price'])
        except:
            close_price = 0
        return close_price


# 获取永续合约价格
def get_perpetualprice(platform, symbol):
    currentprice = get_perpetualprice0(platform, symbol)
    if currentprice == 0:
        # print("{}获取{}现价失败，尝试从huobi获取".format(platform, symbol))
        try:
            now = int(time.time())
            start_time = now - 300
            contract_code = "{}-usdt".format(symbol).upper()
            url = huobifuture_api_url + '/linear-swap-ex/market/history/kline?contract_code={}&period=5min&from={}&to={}'.format(
                contract_code, start_time, now)
            res = requests.get(url)
            resdict = json.loads(res.content.decode())
            currentprice = [i['close'] for i in resdict['data']][-1]
            return currentprice
        except:
            currentprice = 0
    if currentprice == 0:
        # print("{}获取{}现价失败，尝试从T8ex获取".format(platform, symbol))
        try:
            contract_id_dict = {'BTC': 1, 'ETH': 2, 'LINK': 3, 'EOS': 4, 'FIL': 5, 'LTC': 6, 'UNI': 7, 'DOT': 8,
                                'DOGE': 9}
            contract_id = contract_id_dict[symbol.upper()]
            now = int(time.time())
            start_time = now - 300
            url = T8ex_kline_url + "?contractId={}&from={}&to={}&resolution={}".format(contract_id, start_time * 1000,
                                                                                       now * 1000, 1)
            res = requests.get(url).json()
            if res:
                currentprice = res[-1][-2]
                return currentprice
        except:
            currentprice = 0
    if currentprice == 0:
        # print("{}获取{}现价失败，尝试从币安获取".format(platform, symbol))
        try:
            url = binancefuture_api_url + '/dapi/v1/ticker/price'
            data = "{}usdt".format(symbol).upper()
            res = requests.get(url, params={"symbol": data}, timeout=2).json()
            if res:
                time.sleep(1)
                currentprice = float(res['price'])
        except:
            currentprice = 0
    return currentprice


# 获取永续合约持仓
def future_position(platform, symbol):
    if platform == "binance":
        url = binancefuture_api_url + '/futures/data/openInterestHist'
        # now = int(time.time())-86400 *29
        data = {'symbol': symbol, 'period': '1d', "pair": "{}USD".format(symbol)}
        df = pd.DataFrame()
        for _ in range(3):
            try:
                response = requests.get(url, params=data)
                if response.status_code == 200:
                    res = response.json()
                    df[symbol] = [float(i['sumOpenInterest']) for i in res]
                    # df['time']=[time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(i["timestamp"]/1000)) for i in res]
                    break
            except Exception as e:
                print(e)
        return df
    elif platform == "huobi":
        url = huobifuture_api_url + '/linear-swap-api/v1/swap_his_open_interest'
        data = {'contract_code': '{}-usdt'.format(symbol).upper(), 'period': '1day', "amount_type": 1}
        df = pd.DataFrame()
        for _ in range(3):
            try:
                response = requests.get(url, params=data)
                if response.status_code == 200:
                    res = response.json()['data']['tick']
                    df[symbol] = [float(i['value']) for i in res]
                    break
            except Exception as e:
                print(e)
        return df


# 获取大户近30日持仓多空比
def top_position_ratio(platform, symbol):
    if platform == "binance":
        url = binancefuture_api_url+'/futures/data/topLongShortPositionRatio'
        data = {'symbol': symbol, 'period': '1d', 'pair': '{}usd'.format(symbol).upper()}
        df = pd.DataFrame()
        for _ in range(3):
            try:
                res = requests.get(url, params=data)
                if res.status_code == 200:
                    res = res.json()
                    df['longratio'] = [float(i['longPosition']) for i in res]
                    df['shortratio'] = [float(i['shortPosition']) for i in res]
                    df['time'] = [time.strftime("%Y-%m-%d", time.localtime(i["timestamp"] / 1000)) for i in res]
                    break
            except Exception as e:
                print(e)
        return df

    elif platform == "huobi":
        url = huobifuture_api_url+'/linear-swap-api/v1/swap_elite_position_ratio'
        data = {'contract_code': '{}-usdt'.format(symbol).upper(), 'period': '1day'}
        df = pd.DataFrame()
        for _ in range(3):
            try:
                res = requests.get(url, params=data)
                if res.status_code == 200:
                    res = res.json()['data']['list']
                    df['longratio'] = [float(i['buy_ratio']) for i in res]
                    df['shortratio'] = [float(i['sell_ratio']) for i in res]
                    df['time'] = [time.strftime("%Y-%m-%d", time.localtime(i['ts'] / 1000)) for i in res]
                    break
            except Exception as e:
                print(e)
        return df


# 获取T8ex交易所合约盘口买卖单
def get_T8ex_contract_orders(symbol):
    url = '{}/open-api/open/contract-plate'.format(T8ex_base_url)
    contract_id_dict = {'BTC': 1, 'ETH': 2, 'LINK': 3, 'EOS': 4, 'FIL': 5, 'LTC': 6, 'UNI': 7, 'DOT': 8, 'DOGE': 9}
    contract_id = contract_id_dict[symbol.upper()]
    res = requests.post(url, data={'contractId': contract_id})
    resdict = res.json()
    return resdict


if __name__ == "__main__":
    print(get_future_klinedata1("binance", "eth"))

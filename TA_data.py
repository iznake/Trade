import ccxt
import os
import time

import numpy as np
import pandas as pd
import datetime
import json

from apikey import APIKEY, SECRET


'''***************************************************
全局变量
****************************************************'''
csvfileDir = './data/'  # csv文件存储文件夹


timeparm = {
    '1w':168,#7*24
    '3d':72,
    '2d':48,
    '1d':24,
    '4h':4,
    '2h':2,
    '1h':1,
}


'''***************************************************
函数名:timestampToUTC
函数说明:UTC时间戳改UTC+8时间
输入: 
timestamp - utc时间戳
输出: utc+8时间:%Y-%m-%d %H:%M:%S
*******************************************************'''


def timestampToUTC(timestamp):
    timestamp += 8 * 60 * 60
    timeArray = time.localtime(int(timestamp / 1000))
    readableTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    return readableTime


'''***************************************************
函数名:WriteLineToCSV
函数说明:写入csv文件
输入: 
csvFileName - csv文件名称(自动过滤交易对中的'/')
data - 交易对OHLCV数据
输出: none
*******************************************************'''

def WriteLineToCSV(csvFileName, data):
    csvFileName = csvFileName.replace('/', '')
    csvFileName = csvfileDir + csvFileName + '.csv'
    if (not os.path.exists(csvFileName)):
        # 创建文件
        file = open(csvFileName, 'w')
        file.close()

    # UTC时间戳转换为UTC+8可读时间
    top = data[0][1];
    worth = [0] * len(data);  # 净值
    for index in range(len(data)):
        data[index][0] = timestampToUTC(data[index][0]);
        worth[index] = data[index][1] / top;

    np_data = np.array(data);
    new_data = np.column_stack((np_data, worth))

    # 写数据到csv
    df = pd.DataFrame(new_data, columns=['UTC+8时间', '开盘价格', '最高价格', '最低价格', '收盘价格', '交易量', '净值'])
    df.to_csv(csvFileName, encoding='utf_8_sig', index=None)


'''*******************************************************************
函数名:GetAllOHLCVData
函数说明:抓取交易所所有交易对(USDT和BUSD交易对)的所有OHLCV数据
输入: none
输出: none

烛线数据结构:结果数组是以时间升序排列的，最早的烛线排在第一个，最新的烛线排在最后一个
[
[
1504541580000, // UTC 时间戳，单位：毫秒
4235.4, // (O)开盘价格, float
4240.6, // (H)最高价格, float
4230.0, // (L)最低价格, float
4230.7, // (C)收盘价格, float
37.72941911 // (V)交易量，以基准货币计量， float
],
...
]
********************************************************************'''


def GetAllOHLCVData(symbolList,star_time_str,time_interval):

    end = cex_binance.milliseconds() - 60 * 60 * 1000 * timeparm[time_interval];  # Now 60 * 60 * 24 * 1000
    # symbol = cex_binance.markets;
    # print(symbol)
    if cex_binance.has['fetchOHLCV']:
        # for symbol in cex_binance.markets:
        for symbol in symbolList:
            print(symbol);
            since = cex_binance.parse8601(star_time_str);

            time.sleep(cex_binance.rateLimit / 1000)  # time.sleep wants seconds
            all_data = [];
            while since < end:
                data = cex_binance.fetch_ohlcv(symbol, timeframe=time_interval, since=since, limit=500);

                if len(data):
                    # 更新获取时间
                    since = data[len(data) - 1][0] + 60 * 60 * 1000*timeparm[time_interval] ; #60 * 60 * 24 * 1000
                    print(since);
                    all_data += data;
                else:
                    break

            # 存入csv
            if (symbol.find('USDT') > -1 or symbol.find('BUSD') > -1):
                WriteLineToCSV(symbol, all_data);


'''*****************************************************************************
逻辑部分
******************************************************************************'''

# 初始化binance api
cex_binance = ccxt.binance({

    #'apiKey': APIKEY,
    #'secret': SECRET,
    # 'password' : okex独有参数
    'timeout': 30000,
    'enableRateLimit': True,
    'proxies': {
                "http": "http://127.0.0.1:7890",
                "https": "http://127.0.0.1:7890",
            },
})

#symbolList = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'MATICUSDT', 'LTCUSDT', 'DOGEUSDT', 'XRPUSDT', 'LINKUSDT', 'ATOMUSDT', 'ETCUSDT'];
symbolList =['BTCUSDT', 'ETHUSDT', 'BNBUSDT','EOSUSDT', 'MATICUSDT','DOGEUSDT','NEARUSDT' ,'SOLUSDT','LTCUSDT', 'BCHUSDT', 'XRPUSDT', 'LINKUSDT', 'ATOMUSDT',
#              'LUNAUSDT','FTTUSDT','XEMUSDT','SOLUSDT','DCRUSDT','ICPUSDT','LRCUSDT','QTUMUSDT','XMRUSDT','ADAUSDT','DOTUSDT','SHIBUSDT','TRXUSDT','UNIUSDT',
#              'AVAXUSDT','APTUSDT','BCHUSDT','QNTUSDT','APEUSDT','ALGOUSDT','FILUSDT','VETUSDT','EOSUSDT','TWTUSDT','FLOWUSDT','XTZUSDT','AXSUSDT','SANDUSDT',
#              'AAVEUSDT','THETAUSDT','LDOUSDT','CHZUSDT','MANAUSDT','CAKEUSDT','FTMUSDT','NEOUSDT','DASHUSDT','ARUSDT','CRVUSDT',
#              'ARDRUSDT','OMGUSDT','SCUSDT','EOSUSDT','IOTXUSDT',
    'ETCUSDT'];

# 加载市场数据
cex_binance.load_markets()


# 获取所有数据并存储
GetAllOHLCVData(symbolList,'2018-12-31T16:00:00Z','1d');


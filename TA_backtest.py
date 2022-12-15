import time
import numpy
import pandas as pd
import json
import datetime
import matplotlib.pyplot as plt
import random

from itertools import combinations

'''***************************************************
函数名:readCsv
函数说明:读取对应交易对的CSV文件
输入: 
symbol - 交易对名,比如BTCUSDT
输出: pandas df
*******************************************************'''

DATAPATH = "./data/";


def readCsv(symbol):
    csvFile = DATAPATH + symbol + ".csv"
    df = pd.read_csv(csvFile)
    return df


'''***************************************************
函数名:getChange
函数说明:获取涨跌幅
输入: 
None
输出: pandas change_dict
*******************************************************'''

def getChange(listToken, N):
    # 集合存储结果
    change_dict = {};

    # 由于一些币上架时间晚，需要补齐前面的0数据，以便后面统一处理
    maxdatanum = 0;#记录最大的日期数，即最早上架的币的时间
    datanum = {};#每个币种的日期
    maxdatasymbol = 0;#记录这一批币中最早上架比的名称
    for symbol in listToken:
        # 获取数据
        df = readCsv(symbol)

        # 获取所有N天涨跌幅  也可以直接用   df['最近N天涨跌幅'] = df['开盘价格'].pct_change(N)，差别不大
        for index in range(N, len(df)):
            df.at[index, '最近N天涨跌幅'] = (df.iloc[index - 1]['收盘价格'] / df.iloc[index - N]['开盘价格']) - 1

        change_dict[symbol] = df[['UTC+8时间', '开盘价格', '收盘价格', '最近N天涨跌幅']]

       #设置时间为索引，并在DF里生效
        change_dict[symbol].set_index('UTC+8时间', inplace=True);

        # 找到数据最多的交易对，即最早上市的币种，并记录下名称
        datanum[symbol] = len(df);
        if maxdatanum < datanum[symbol]:
            maxdatanum = datanum[symbol];
            maxdatasymbol = symbol;
    # print(change_dict);

    org = change_dict[maxdatasymbol];#日期最早的那个币的数据

    # 按照数据最多的交易对 来填补数据不足的交易对
    for symbol in listToken:
        if (symbol != maxdatasymbol) and (datanum[symbol] < maxdatanum):
            left, change_dict[symbol] = org.align(change_dict[symbol], join="outer", axis=0);

    # 恢复默认索引
    for symbol in listToken:
        change_dict[symbol].reset_index(inplace=True);
        #for debug
        # filename = "./data/T_" + symbol+"_0_"+str(N)+".csv";
        # change_dict[symbol].to_csv(filename, encoding='utf_8_sig', index=None)

    return change_dict;


'''***************************************************
函数名:calHold
函数说明:根据涨跌幅计算持仓
输入: change_dist
输出: pandas hold_result
*******************************************************'''

def calHold(change_dict, listToken, selectedNum):
    # 计算持仓
    hold_result = {};

    for index in range(len(change_dict[listToken[0]])):

        date = change_dict[listToken[0]].iloc[index]['UTC+8时间'];

        symboldata = [];
        for symbol in listToken:
            symboldata.append([symbol, change_dict[symbol].iloc[index]['最近N天涨跌幅']]);

        # 对涨幅降序排列，方便后续处理
        df = pd.DataFrame(symboldata, columns=['交易对', '涨幅']);
        df.sort_values(by='涨幅', ascending=False, inplace=True);

        selectedSymbol = [];
        for i in range(selectedNum):
            if numpy.isnan(df.iloc[i]['涨幅']):
                selectedSymbol.append('USDT');
            elif df.iloc[i]['涨幅'] > 0:
                selectedSymbol.append(df.iloc[i]['交易对']);
            else:
                selectedSymbol.append('USDT');

        hold_result[index] = [date] + selectedSymbol;

    # print('calhold finish');
    # filename = "./data/T_" + symbol + "_MA.csv";
    # df.to_csv(filename, encoding='utf_8_sig', index=None)
    return hold_result


'''***************************************************
函数名:backTest
函数说明:根据持仓回测收益
输入: init_U, listToken, N
输出: pandas hold_result
*******************************************************'''


def backTest(init_U, listToken, selectedNum, csv, gasfee):
    market = {};

    rate = [1/selectedNum for i in range(selectedNum)];# rate = [0.333,0.333,0.333] selectdNum = 3
    #rate = [0.5, 0.3, 0.2];  # rate = [0.333,0.333,0.333] selectdNum = 3
    for symbol in listToken:
        market[symbol] = readCsv(symbol);
        market[symbol].set_index('UTC+8时间', inplace=True);

    hold = pd.read_csv(csv);

    for i in range(selectedNum):
        numString = "持仓" + str(i) + "数量";
        symbolValueStr = "持仓" + str(i) + "价值";  # 持仓0价值
        hold[numString] = 0;
        hold.at[0, symbolValueStr] = init_U * rate[i];

    hold['持仓U'] = 0;  # 持仓U 含义为 所有持仓价值的U计价

    # 首日
    hold.at[0, '持仓U'] = init_U;
    hold.at[0, '持仓净值'] = 1;

    for index in range(1, len(hold)):

        hold.at[index, '持仓U'] = 0;
        count = 0;  # 计算有几个是USDT，用于统计持币后的卖出行为，再次分配资金时使用
        timeindex = hold.at[index, 'UTC+8时间'];
        # print(str(index) +"  "+ timeindex);

        # if index == 33:
        #     print ("start to debug");

        for i in range(selectedNum):
            numStr = "持仓" + str(i) + "数量";  # 持仓N数量
            symbolStr = "持仓" + str(i);  # 持仓N 种类
            symbolValueStr = "持仓" + str(i) + "价值";  # 持仓N价值
            curType = hold[symbolStr][index];  # 当前持仓的币种 hold['持仓'][index]
            preType = hold[symbolStr][index - 1];  ##上一时间段持仓的币种

            # cur_price  = market[curType].loc[timeindex]['开盘价格']; 当前日期持仓币种的开盘价格
            # pre_price  = market[preType].loc[timeindex]['开盘价格']; 前一个日期的持仓币种的开盘价格

            if (curType == preType):
                hold.at[index, numStr] = hold[numStr][index - 1];
                if (curType == 'USDT'):
                    hold.at[index, symbolValueStr] = hold[symbolValueStr][index - 1];
                    # hold.at[index, '持仓净值'] = hold['持仓净值'][index - 1];
                    count = count + 1;
                else:
                    cur_price = market[curType].loc[timeindex]['开盘价格'];
                    hold.at[index, symbolValueStr] = hold[numStr][index] * cur_price;
                    # hold.at[index, '持仓净值'] = hold['持仓U'][index] / init_U;

            # 需要换仓，则交易
            elif (curType == 'USDT' and preType != 'USDT'):
                # 本轮持仓U,上轮持仓代币:直接卖出代币
                pre_price = market[preType].loc[timeindex]['开盘价格'];
                hold.at[index, symbolValueStr] = hold[numStr][index - 1] * pre_price;  #
                hold.at[index, symbolValueStr] *= (1 - gasfee);
                # hold.at[index, '持仓净值'] = hold['持仓U'][index] / init_U;
                hold.at[index, numStr] = 0;

                count = count + 1;
            elif (curType != 'USDT' and preType == 'USDT'):
                # 本轮持仓代币,上轮持仓U:直接买入代币
                cur_price = market[curType].loc[timeindex]['开盘价格'];

                hold.at[index, symbolValueStr] = hold[symbolValueStr][index - 1] * (1 - gasfee);
                hold.at[index, numStr] = hold[symbolValueStr][index] / cur_price;
                # hold.at[index, '持仓净值'] = hold['持仓U'][index] / init_U;
            elif (curType != 'USDT' and preType != 'USDT'):
                # 本轮持仓A,上轮持仓B:先卖出B再买入A
                cur_price = market[curType].loc[timeindex]['开盘价格'];
                pre_price = market[preType].loc[timeindex]['开盘价格'];

                hold.at[index, symbolValueStr] = hold[numStr][index - 1] * pre_price;  #
                hold.at[index, symbolValueStr] *= (1 - gasfee);
                # hold.at[index, '持仓净值'] = hold['持仓U'][index] / init_U;
                # 以当日开盘价格购入BTC
                hold.at[index, symbolValueStr] *= (1 - gasfee);
                hold.at[index, numStr] = hold[symbolValueStr][index] / cur_price;

            # 分别加到持仓价值中
            hold.at[index, '持仓U'] = hold.at[index, '持仓U'] + hold.at[index, symbolValueStr];

        # 更新此时的净值
        hold.at[index, '持仓净值'] = hold['持仓U'][index] / init_U;
        # 当全部卖出时，更新一下每一个持仓的价值，即按比例重新分配每个币种的资金
        if count == selectedNum:
            for i in range(selectedNum):
                symbolValueStr = "持仓" + str(i) + "价值";  # 持仓0价值
                hold.at[index, symbolValueStr] = hold['持仓U'][index] * rate[i];

        # print(hold)

    # print(index);
    return hold;


'''***************************************************
函数名:drawPlot
函数说明:根据回测收益绘制折线图
输入: hold_backtest, backtest, listToken
输出: None
*******************************************************'''


def drawPlot(hold_backtest, back_sum, N, listToken):
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

    # 创建图形
    plt.figure();
    # 设置网格，途中红色虚线
    plt.grid(linestyle=":", color="red")

    # 持仓收益变化
    x = hold_backtest.loc[:, 'UTC+8时间'];
    y = hold_backtest.loc[:, '持仓净值'];
    plt.plot(x, y);
    l = plt.plot(x, y, 'g--', label=N);


    ylen = len(y);

    # btc/eth净值
    token = {};
    color = ['blue', 'coral', 'gold', 'green','aqua','beige','brown','cyan','pink','olive','yellow'];
    index = 0;
    drawlist = ['BTCUSDT', 'ETHUSDT'];
    for symbol in drawlist:
        print(symbol);
        # 获取数据
        df = readCsv(symbol);
        # 获取所有1天涨跌幅
        #x_n = df.loc[:, 'UTC+8时间'];
        y_n = df.loc[:, '净值'];

        #####数据不全的情况，需要补全
        if len(y_n)<ylen:
            org = hold_backtest[['UTC+8时间','持仓净值']];
            org.columns = ['UTC+8时间','净值'];
            new = df[['UTC+8时间','净值']];
            new['UTC+8时间'] = pd.to_datetime(new['UTC+8时间']);
            org.set_index('UTC+8时间', inplace=True);
            new.set_index('UTC+8时间', inplace=True);
            left,right = org.align(new, join="outer", axis=0);
            y_n = right.loc[:, '净值'];
            #print("lack of data");
        print(y_n);
        plt.plot(x, y_n);
        ln = plt.plot(x, y_n, color[index], label=symbol);
        index += 1;

    # 最大回撤注释
    # 转换为日期索引
    hold_backtest['UTC+8时间'] = pd.to_datetime(hold_backtest['UTC+8时间'])
    hold_backtest.set_index('UTC+8时间', inplace=True);

    x_back = back_sum['历史最大回撤日期'];
    y_back = hold_backtest.loc[x_back]['持仓净值'];
    back_max = '最大回撤' + str(back_sum['历史最大回撤比例']);
    plt.annotate(text=back_max, xy=(x_back, y_back), xytext=(x_back, y_back + 10), fontsize=6,
                 arrowprops={'arrowstyle': '->'});

    plt.title(u'回测最优解');
    plt.xlabel(u'UTC+8时间');
    plt.ylabel('持仓净值');
    plt.legend();
    plt.show();


'''***************************************************
函数名:getSumData
函数说明:根据回测收益计算每年涨幅/年华收益率/最大回撤/净值(截止目前)
输入: hold_backtest
输出: backtest
*******************************************************'''


def getSumData(hold_backtest):
    backtest = {};
    # 转换为日期索引
    hold_backtest['UTC+8时间'] = pd.to_datetime(hold_backtest['UTC+8时间']);
    hold_backtest.set_index('UTC+8时间', inplace=True);

    # 计算每年涨幅/年华收益率
    years = ['2019', '2020', '2021', '2022'];
    #years = [ '2022'];
    for year in years:
        head = hold_backtest.loc[year].head(1)['持仓U'][0];
        tail = hold_backtest.loc[year].tail(1)['持仓U'][0];
        backtest['N'] = N;
        backtest[year + '年涨幅'] = '%.2f%%' % (tail / head * 100);
        backtest[year + '年化收益率'] = '%.2f%%' % ((tail - head) / head * 100);
        backtest[year + '年度收益值'] = tail - head;
        backtest[year + '年度最大回撤'] = hold_backtest.loc[year]['持仓U'].diff(1).min();

    date = hold_backtest['持仓U'].diff(1).idxmin();
    backtest['总收益值'] = hold_backtest.tail(1)['持仓U'][0] - hold_backtest.head(1)['持仓U'][0];
    backtest['总净值'] = hold_backtest.tail(1)['持仓U'][0];
    backtest['总涨幅'] = '%.2f%%' % (backtest['总收益值'] / hold_backtest.head(1)['持仓U'][0] * 100);
    backtest['历史最大回撤日期'] = date;

    hold_U_day = float(hold_backtest.loc[date]['持仓U']);
    back_U_day = float(hold_backtest['持仓U'].diff(1).min());
    backtest['历史最大回撤'] = back_U_day;
    backtest['历史最大回撤比例'] = '%.2f%%' % (back_U_day / hold_U_day * 100);

    return backtest;


'''*****************************************************************************
逻辑部分
******************************************************************************'''
# Start
print('Start');
#symbolList = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'MATICUSDT', 'LTCUSDT', 'DOGEUSDT', 'XRPUSDT', 'LINKUSDT', 'ATOMUSDT', 'ETCUSDT']; #2000倍的那个
symbolList = ['BTCUSDT', 'ETHUSDT']; #
#symbolList = ['BTCUSDT','ETHUSDT' ,'MATICUSDT',];
# symbolList = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'BCHUSDT', 'LTCUSDT', 'BSVUSDT', 'XRPUSDT', 'DASHUSDT', 'ZECUSDT', 'ETCUSDT']; #2000倍的那个
# listToken = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'MATICUSDT', 'LTCUSDT', 'DOGEUSDT', 'XRPUSDT', 'LINKUSDT', 'ATOMUSDT', 'FTTUSDT'];
# listToken = ['LUNAUSDT','FTTUSDT','BTCUSDT','SOLUSDT','DCRUSDT','ICPUSDT','LRCUSDT','QTUMUSDT','XMRUSDT','DOGEUSDT'];
#symbolList =['BTCUSDT', 'ETHUSDT', 'BNBUSDT','EOSUSDT', 'MATICUSDT','DOGEUSDT','NEARUSDT',  'ZECUSDT',  'LTCUSDT', 'BCHUSDT',
             # 'LUNAUSDT','FTTUSDT', 'XEMUSDT','SOLUSDT', 'ICPUSDT',  'ADAUSDT',  'DOTUSDT',  'SHIBUSDT','TRXUSDT','UNIUSDT',
             # 'AVAXUSDT','APTUSDT','BCHUSDT','APEUSDT','ALGOUSDT','FILUSDT','EOSUSDT','FLOWUSDT','XTZUSDT','AXSUSDT','SANDUSDT',
             # 'AAVEUSDT','CHZUSDT','MANAUSDT','CAKEUSDT','FTMUSDT','CRVUSDT','XRPUSDT', 'LINKUSDT', 'ATOMUSDT', 'ETCUSDT'];
# symbolList = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'EOSUSDT', 'MATICUSDT', 'DOGEUSDT', 'NEARUSDT', 'LTCUSDT', 'CRVUSDT',
#               'LINKUSDT', 'ATOMUSDT', 'ETCUSDT'];#66 次
#symbolList = ['BTCUSDT', 'ETHUSDT'];
selectedNum = 1;#选几个币，适用于如 10个币选3个的情况

selectlist = list(combinations(symbolList, len(symbolList)));##在其中选几个币。比如40个币中选任意10个，数据会非常大排列C40 10，
#print(len(selectlist));

for i in range(0, len(selectlist)):

    market = {};
    hold_backtest = {};
    back_sum = {};
    max_N = {};
    gasfee = 0.0002;
    listToken = selectlist[i];

    # 回测收益
    for N in range(1, 21):
        # 计算涨跌幅
        change = getChange(listToken, N);

        # 根据涨跌幅计算持仓
        hold = calHold(change, listToken, selectedNum);

        # 写数据到csv
        csv = DATAPATH + '持仓明细_' + str(N) + '.csv';
        df = pd.DataFrame(hold).T
        # holdlist = [f"持仓{i}" for i in range(1,selectedNum+1)];
        df.columns = ['UTC+8时间'] + [f"持仓{i}" for i in range(selectedNum)];
        df.to_csv(csv, encoding='utf_8_sig', index=None)  # 不存数据

        # 回测
        init_U = 10000;  # 初始资金
        key = "N=" + str(N);
        hold_backtest[key] = backTest(init_U, listToken, selectedNum, csv, gasfee);

        # 存入数据到csv
        csv = DATAPATH + '持仓明细_回测收益_' + str(N) + '.csv';
        df = pd.DataFrame(hold_backtest[key]);
        for i in range(selectedNum):
            numStr = "持仓" + str(i) + "数量";  # 持仓0数量
            symbolStr = "持仓" + str(i);  # 持仓N 种类
            symbolValueStr = "持仓" + str(i) + "价值";  # 持仓0价值
        numStr = [f"持仓{i}数量" for i in range(selectedNum)];
        symbolStr = [f"持仓{i}" for i in range(selectedNum)];
        symbolValueStr = [f"持仓{i}价值" for i in range(selectedNum)];  # 持仓0价值

        # df.columns = ['UTC+8时间', '持仓', '持仓数量', '持仓U', '持仓净值'];
        df.columns = ['UTC+8时间'] + symbolStr + numStr + symbolValueStr + ['持仓U', '持仓净值'];
        df.to_csv(csv, encoding='utf_8_sig', index=None);  # 存数据

        # 计算每年涨幅/年华收益率/最大回撤/净值(截止目前)
        back_sum[key] = getSumData(df);
        print(back_sum[key]);

        # 存入数据到csv
        csv = DATAPATH + '回测收益总计' + str(N) + '.csv';
        df = pd.DataFrame(back_sum[key], index=[0]).T;
        # df.columns = ['分项','数据'];
        df.to_csv(csv, encoding='utf_8_sig');  # 存数据

        # # 统计N的序列
        # ncsv = DATAPATH + str(N) + '_币对涨幅' + '.csv';
        # Stotal = round(back_sum[key]['总净值'] / 10000, 3);
        # S2019 = back_sum[key]['2019年化收益率'];
        # S2020 = back_sum[key]['2020年化收益率'];
        # S2021 = back_sum[key]['2021年化收益率'];
        # S2022 = back_sum[key]['2022年化收益率'];
        # nlist = [Stotal, S2019, S2020, S2021, S2022, listToken];
        # ndf = pd.DataFrame(nlist).T;
        # ndf.to_csv(ncsv, mode='a',index= True,header=False, encoding='utf_8_sig');  #

        if ((not max_N) or (max_N['最大总收益值'] < back_sum[key]['总收益值'])):
            max_N['最大总收益值'] = back_sum[key]['总收益值'];
            max_N['最优N'] = key;

# 绘制最优收益的回测收益折线图
drawPlot(hold_backtest[max_N['最优N']], back_sum[max_N['最优N']], max_N['最优N'], listToken);

# Finish
print('Finish');

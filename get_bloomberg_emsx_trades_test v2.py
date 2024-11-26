# -*- coding: utf-8 -*-
#Working python 311
"""
Created on Wed May  6 12:43:18 2020

@author: blau_zim
"""
from turtle import back
import activate_env
import sys 
import datetime
import pandas as pd
import os
import time
import shutil
import ops_utils as ou
import re
import xlwt
import numpy as np
import csv
import pysftp

settle_day = pd.DataFrame( pd.Series({'HK' :2, 'JP' :2, 'SG' : 2, 'AU': 2, 'CN': 0, 'ID': 2, 'IN': 1, 'KR': 2, 'MY': 2, 'NZ':2, 'PH': 3, 'TH': 2, 'TW': 2, 'US':2, 'PK':2, 'VN':2},name='days'))

                
def refresh_dashboard(return_loc = False, db = True):
    workingpath =  "//zimnashk/sd.zentific-im.com$/bbgloader/WORKING/"
    pendingpath =  "//zimnashk/sd.zentific-im.com$/bbgloader/PENDING/"
    dailyorderspath = "//zimnashk/sd.zentific-im.com$/bbgloader/todays_orders/Done/"
    daystr = datetime.datetime.strftime(datetime.datetime.today().date(),'%Y%m%d')
    senttime = datetime.datetime.strftime(datetime.datetime.now(),'%H%M%S')    
    blotter = pd.DataFrame(columns=['API_SEQ_NUM','EMSX_ACCOUNT','EMSX_AMOUNT','EMSX_ARRIVAL_PRICE','EMSX_ASSET_CLASS','EMSX_ASSIGNED_TRADER','EMSX_AVG_PRICE','EMSX_BASKET_NAME','EMSX_BASKET_NUM','EMSX_BROKER','EMSX_BROKER_COMM','EMSX_BSE_AVG_PRICE','EMSX_BSE_FILLED','EMSX_CFD_FLAG','EMSX_CLEARING_ACCOUNT','EMSX_COMM_DIFF_FLAG','EMSX_COMM_RATE','EMSX_CURRENCY_PAIR','EMSX_CUSTOM_NOTE1','EMSX_CUSTOM_NOTE2','EMSX_CUSTOM_NOTE3','EMSX_CUSTOM_NOTE4','EMSX_CUSTOM_NOTE5','EMSX_DATE','EMSX_DAY_AVG_PRICE','EMSX_DAY_FILL','EMSX_DIR_BROKER_FLAG','EMSX_EXCHANGE','EMSX_EXCHANGE_DESTINATION','EMSX_EXEC_INSTRUCTION','EMSX_FILL_ID','EMSX_FILLED','EMSX_GTD_DATE','EMSX_HAND_INSTRUCTION','EMSX_IDLE_AMOUNT','EMSX_INVESTOR_ID','EMSX_ISIN','EMSX_LIMIT_PRICE','EMSX_LOCATE_BROKER','EMSX_LOCATE_REQ','EMSX_NOTES','EMSX_NSE_AVG_PRICE','EMSX_NSE_FILLED','EMSX_ORD_REF_ID','EMSX_ORDER_TYPE','EMSX_ORIGINATE_TRADER','EMSX_ORIGINATE_TRADER_FIRM','EMSX_PERCENT_REMAIN','EMSX_PM_UUID','EMSX_PORT_MGR','EMSX_PORT_NAME','EMSX_PORT_NUM','EMSX_POSITION','EMSX_PRINCIPLE','EMSX_PRODUCT','EMSX_QUEUED_DATE','EMSX_QUEUED_TIME','EMSX_REASON_CODE','EMSX_REASON_DESC','EMSX_REMAIN_BALANCE','EMSX_ROUTE_ID','EMSX_ROUTE_PRICE','EMSX_ROUTE_REF_ID','EMSX_SEC_NAME','EMSX_SEDOL','EMSX_SEQUENCE','EMSX_SETTLE_AMOUNT','EMSX_SETTLE_DATE','EMSX_SIDE','EMSX_START_AMOUNT','EMSX_STATUS','EMSX_STEP_OUT_BROKER','EMSX_STOP_PRICE','EMSX_STRATEGY_END_TIME','EMSX_STRATEGY_PART_RATE1','EMSX_STRATEGY_PART_RATE2','EMSX_STRATEGY_STYLE','EMSX_STRATEGY_TYPE','EMSX_TICKER','EMSX_TIF','EMSX_TIME_STAMP','EMSX_TRAD_UUID','EMSX_TRADE_DESK','EMSX_TRADER','EMSX_TRADER_NOTES','EMSX_TS_ORDNUM','EMSX_TYPE','EMSX_UNDERLYING_TICKER','EMSX_USER_COMM_AMOUNT','EMSX_USER_COMM_RATE','EMSX_USER_FEES','EMSX_USER_NET_MONEY','EMSX_USER_WORK_PRICE','EMSX_WORKING','EMSX_YELLOW_KEY'])
    blotter_file = None
    try : 
        blotter_update_time = ou.exec_sql ("select last_user_update from sys.dm_db_index_usage_stats where object_id = (select OBJECT_ID('zimdb..blotter_live'))", conn=ou.get_db_conn(reuse=False))
        if len(blotter_update_time) > 0 and type(blotter_update_time.iloc[0,0]) == pd.tslib.Timestamp : 
            blotter_update_time = blotter_update_time.iloc[0,0]
        else : 
            blotter_update_time = (datetime.datetime.now() - pd.offsets.BDay(1))
    except : 
        blotter_update_time = (datetime.datetime.now())
    if ou.sql_table_exists('blotter_live', db = 'zimdb') == True and blotter_update_time.date() == datetime.datetime.now().date() :  
        print("use live blotter from db blotter_live table last updated : " + str(blotter_update_time))
        retries = 30 
        tmp = pd.DataFrame()
        while len(tmp) == 0 and retries > 0 : 
            tmp = ou.exec_sql("select * from zimdb..blotter_live", conn=ou.get_db_conn(reuse=False))
            retries = retries - 1
            if len(tmp) == 0 :
                time.sleep(1)
        blotter = tmp.loc[tmp['MSG_SUB_TYPE'] == 'O']
        routes = tmp.loc[tmp['MSG_SUB_TYPE'] == 'R']
    recvtime = datetime.datetime.strftime(datetime.datetime.now(),'%H%M%S')    
    orderfile = 'day' + daystr + '_s.' + recvtime + '_r.' + recvtime + '.csv'
    
    blotter.to_csv(dailyorderspath+orderfile, quoting=csv.QUOTE_NONNUMERIC, index = False)
    blotter_file = orderfile
    return blotter_file


if False : 
    alloc_str = 'ZEN_MST:2000;ZEN_CNG:1000;ZEN_MS2:3000'
    trade_shares = 7000
    trade_fills = np.trunc(trade_shares * 0.75)
    lot_size = 100
def get_allocation(alloc_str, trade_shares=None, trade_fills=None, lot_size=None) :
    alloc = pd.DataFrame([x.split(":") for x in alloc_str.split(";")], columns=['account','shares'])
    alloc['account'] = [x if "ZEN_" in x else "ZEN_" + x for x in alloc['account']]
    alloc['shares'] = [float(x) for x in alloc['shares']]
    # if allocation shares not same as total target trade shares 
    if trade_shares is not None and sum(alloc['shares']) != trade_shares  :
        # handle case where requestesd trade amount != sum of allocation amounts and also when trade amount > allocation sum
        alloc['pro_rate'] = alloc['shares'] / sum(alloc['shares'])
        alloc['shares_tgt'] = alloc['pro_rate'] * trade_shares 
        alloc['shares_new'] = np.trunc(alloc['shares_tgt'] / lot_size) * lot_size
        alloc['residual'] = alloc['shares_tgt'] - alloc['shares_new']
        # allocate residual
        alloc = ou.sort_dataframe(alloc,['residual','shares_new'],ascending=False)
        residual = trade_shares - sum(alloc['shares_new'])
        alloc_pos = 0
        while residual > 0 and alloc_pos < len(alloc) :
            increment = min(np.ceil(alloc.loc[alloc.index[alloc_pos], 'residual']/lot_size)*lot_size, residual)
            alloc.loc[alloc.index[alloc_pos], 'shares_new'] = alloc.loc[alloc.index[alloc_pos], 'shares_new'] + increment
            residual = residual - increment
        if residual > 0  :
            raise Exception ('Error Allocations for trade size greater than the number of shares for ')
        alloc['shares'] = alloc['shares_new']
    # if fully filled just use allocation
    if trade_fills is not None : 
        if sum(alloc['shares']) == trade_fills  :
            alloc['filled'] = alloc['shares']
        # partial fill need to pro-rate
        else :
            # handle case where requestesd Filled amount != sum of allocation amounts
            alloc['pro_rate'] = alloc['shares'] / sum(alloc['shares'])
            alloc['filled'] = np.trunc(alloc['pro_rate'] * trade_fills / lot_size) * lot_size
            alloc['residual'] = alloc['shares'] - alloc['filled']
            # allocate residual
            alloc = ou.sort_dataframe(alloc,['residual','filled', 'shares','account'],ascending=[False, True, False, True])
            residual = trade_fills - sum(alloc['filled'])
            alloc_pos = 0
            while residual > 0 and alloc_pos < len(alloc) :
                alloc.loc[alloc.index[alloc_pos], 'filled'] = alloc.loc[alloc.index[alloc_pos], 'filled'] + min(alloc.loc[alloc.index[alloc_pos], 'residual'], residual)
                residual = residual - min(alloc.loc[alloc.index[alloc_pos], 'residual'], residual)
                alloc_pos = alloc_pos + 1
            if residual > 0  :
                raise Exception ('Error fill allocations greater than the number of shares traded ')
    return alloc

if False : 
    this_date = datetime.date(2024,10,8)
def get_live_trades(this_date = datetime.datetime.today().date(), force_refresh=False, backtest = False):
    #if backtest mode, enter when do you wanna backtest the rec
    now_time = this_datetime()
    
    global backtest_params
    backtest_params = {
        'fake_now_time' : datetime.datetime(2024, 11, 13, 15, 30, 0)
    }
    
    dailyorderspath = "//zimnashk/sd.zentific-im.com$/bbgloader/todays_orders/Done/"
    actions_map = {'BUY' : 'B', 'SELL' : 'S', 'SHRT' : 'H', 'COVR' : 'C', 'SHORT' : 'H', 'COVER' : 'C'}   
    exchange_map = {'C1' : 'CN', 'C2' : 'CN', 'CH' : 'CN', 'IS' : 'IN', 'KS' : 'KR', 'JP' : 'JP', 'AU' : 'AU', 'NZ' : 'NZ', 'TT' : 'TW', 'TB' : 'TH', 'IJ' : 'ID', 'HK' : 'HK', 'SP' : 'SG', 'MK' : 'MY', 'PM' : 'PH', 'US': 'US', "NO" : "NO"}
    # broker mappings 
    broker_map = pd.DataFrame([{'account' : 'BOAML'   ,'stock' : 'MLAP', 'future' : ''},
                               {'account' : 'BOAML-HT','stock' : 'MLAP', 'future' : ''},
                               {'account' : 'BOAML-PT','stock' : 'MLPT', 'future' : 'MLFP'},
                               {'account' : 'CIMB'    ,'stock' : 'CIMA', 'future' : ''},
                               {'account' : 'CS'      ,'stock' : 'AESA', 'future' : ''},
                               {'account' : 'CS-PT'   ,'stock' : 'CSAP', 'future' : 'CS'},
                               {'account' : 'GS'      ,'stock' : 'GSDA', 'future' : 'GSFT'},
                               {'account' : 'GS'      ,'stock' : 'GSGL', 'future' : '', 'countries' : ['US']},
                               {'account' : 'GS-HT'   ,'stock' : 'GSDA', 'future' : ''},
                               {'account' : 'GS-PT'   ,'stock' : 'GSDA', 'future' : 'GSFT'},
                               {'account' : 'HAIT'    ,'stock' : 'HTDS', 'future' : ''},
                               {'account' : 'HAIT-PT' ,'stock' : 'HTIL', 'future' : ''},
                               {'account' : 'JPM'     ,'stock' : 'JPGA', 'future' : ''},
                               {'account' : 'JPM-PT'  ,'stock' : 'JPML', 'future' : ''},
                               {'account' : 'MS'      ,'stock' : 'MSET', 'future' : ''},
                               {'account' : 'TEST'    ,'stock' : 'BMTB', 'future' : ''},
                               {'account' : 'UBS'     ,'stock' : 'UBSB', 'future' : 'UBSD'},
                               {'account' : 'UBS-PT'  ,'stock' : 'UBSL', 'future' : 'UBSD'}])
    
    # blotter snaps 
    snap_times = {'0_161000' : ['NZ','AU','TW', 'JP', 'KR'], 
                  '0_211000' : ['CN','HK', 'ID','MY', 'HK', 'PH', 'SG', 'IN', 'TH'],
                  '1_051000' : ['NO', 'EU', 'FR', 'GB', 'DE'],
                  '1_064500' : ['US']} #containing US trades, please run before 064500 at t+1 hk time, after Us market close
    
    # make sure this_Date is a weekday if weekend make it the last day business day
    # New logic using time to filter and merge overnight US stock trading to the last days bbgfile
    # eg. Sat hk morning 6am running, should retrieve all market trade including the US ones.
    if backtest == True:
        now_time = backtest_params['fake_now_time']
        if now_time <= datetime.datetime.combine(now_time.date(), datetime.time(7, 0, 0)):
            this_date = now_time - pd.offsets.BDay(1)
        elif this_date.isoweekday() > 5 :
            this_date = now_time - pd.offsets.BDay(1)
            
    this_date = now_time.date()
    print(this_date)
    # if today or yesterday (overnight markets) then it's live! so refresh the blotter
    
    # with force_refresh, get the newest trades for the 
    if force_refresh  and this_date in [now_time.date(), (now_time - pd.offsets.Day(1)).date()] :
        print('Blotter force refresh is enabled, please check if the overnight trades are captured')
        refresh_dashboard(return_loc = True)

    # use the files to get the latest 
    max_date = (this_date + pd.offsets.Day(1)).date()
    filelist = pd.DataFrame([x for x in os.listdir(dailyorderspath) if datetime.datetime.strptime(x[3:11], "%Y%m%d").date() >= this_date and datetime.datetime.strptime(x[3:11], "%Y%m%d").date() <= max_date], columns = ['filename'])
    filelist['date'] = ['0' if datetime.datetime.strptime(x[3:11], "%Y%m%d").date() == this_date else '1' for x in filelist['filename']]
    filelist['time'] = [x[14:20] for x in filelist['filename']]
    filelist['key'] = [x + "_" + y for x,y in zip(filelist['date'], filelist['time'])]
    filelist = filelist.set_index('key')

    #this_key = '0_161000'
    trades = pd.DataFrame()
    for this_key in snap_times.keys() :
        this_markets = snap_times[this_key]
        latest_key = max([x for x in filelist.index if x < this_key])
        this_file = filelist.loc[latest_key, 'filename']
        this_trades = pd.read_csv(dailyorderspath + this_file)
        filt = [exchange_map[x] in this_markets for x in this_trades.EMSX_EXCHANGE]
        this_trades = this_trades.loc[filt]
        this_trades = this_trades.loc[this_trades['MSG_SUB_TYPE'] == 'O']
        trades = trades._append(this_trades)
    # clean up the trades
    trades.index = range(len(trades.index))
    trade_cols = list(trades.columns)
    # print(trades)         #collapse output
    if len(trades) > 0 : 
        # read in any allocations from blotter_alloc table
        alloc_table = ou.exec_sql("select emsx_notes, alloc from zimdb..blotter_alloc where date = '" + str(this_date) + "'")
        if len(alloc_table) > 0 : 
            alloc_table = alloc_table.set_index('emsx_notes')['alloc']
            trades['EMSX_CUSTOM_NOTE1'] = [y if 'MULTI' in x else x for x,y in zip(trades['EMSX_CUSTOM_NOTE1'], alloc_table[trades['EMSX_NOTES']])]

        # get currency information
        india_fut_mult = ou.exec_sql("select bb_code + ' Equity' as bb_code_traded , z_bb_code + ' Equity' as z_bb_code, multiplier from zimdb_ops..india_ssf_multiplier")
        india_fut_mult = india_fut_mult.drop_duplicates('bb_code_traded')
        india_fut_mult = india_fut_mult.set_index('bb_code_traded')
        xrate = ou.exec_sql("select XRATE_USDAUD as AUD, XRATE_USDCNY as CNY, XRATE_USDHKD as HKD, XRATE_USDIDR as IDR, XRATE_USDINR as INR, XRATE_USDJPY as JPY, XRATE_USDKRW as KRW, XRATE_USDMYR as MYR, XRATE_USDNZD as NZD, XRATE_USDPHP as PHP, XRATE_USDSGD as SGD, XRATE_USDTHB as THB, XRATE_USDTWD as TWD, XRATE_USDCNH as CNH, 1.0 as USD from zimdb_ops..ref_rates where date = (select max(date) from zimdb_ops..ref_rates)")    
        xrate = xrate.transpose().iloc[:,0]
        ticker_map = ou.exec_sql("select bb_code as bb_code_traded, sedol, z_isin, z_sedol, z_AxiomaID as AxiomaID, z_bb_code , currency, lot_size, name from zimdb_ops..trade_ticker_map")
        ticker_map['xrate'] = list(xrate.reindex(ticker_map['currency']))
        ticker_map = ticker_map.drop_duplicates('bb_code_traded')
        ticker_map = ticker_map.set_index('bb_code_traded')
        
        # columns we want to add to the trades file
        # batch, account, shares, filled, avg_price_local, principle_local, principle_usd, side, strategy, prime_broker, exec_broker
        # map in key information z_sedol, AxiomaID, currency, lot_size, multiplier, xrate
        new_cols = ['bb_code_traded', 'z_bb_code', 'z_sedol', 'AxiomaID', 'name', 'account', 'exec_broker', 'prime_broker', 'swap', 'side', 'shares', 'filled', 'avg_price_local', 'principle_local', 'currency', 'lot_size', 'multiplier', 'strategy', 'xrate', 'basket', 'country_exchange']
        trades['bb_code_traded'] = trades['EMSX_TICKER']
        trades['basket'] = trades['EMSX_BASKET_NAME']

        # Fitler out Canceled orders
        filt = [x  != 'CANCEL' for x in trades['EMSX_STATUS']]
        trades = trades.loc[filt]  

        trades['country_exchange'] = [exchange_map[x] for x in trades['EMSX_EXCHANGE']]
        for this_key in ['z_sedol', 'z_bb_code', 'AxiomaID', 'lot_size', 'currency', 'xrate', 'name'] :
            trades[this_key] = list(ticker_map[this_key][list(trades['bb_code_traded'])])
        trades['multiplier'] = list(india_fut_mult['multiplier'].reindex(list(trades['bb_code_traded'])))
        trades['multiplier'] = trades['multiplier'].fillna(1)
        # for any missing entries assume it's a india futures roll and map in the z_bb_code first
        filt = [type(x) != str for x in trades.z_bb_code]
        missing = trades.loc[filt].copy()
        if len(missing) > 0 :
            missing['z_bb_code'] = list(india_fut_mult['z_bb_code'][list(missing['bb_code_traded'])])
            missing['multiplier'] = list(india_fut_mult['multiplier'][list(missing['bb_code_traded'])])
            tmp = ticker_map.reset_index().drop_duplicates('z_bb_code').set_index('z_bb_code')        
            for this_key in ['z_sedol', 'AxiomaID', 'lot_size', 'currency', 'xrate'] :
                missing[this_key] = list(ticker_map[this_key][list(missing.z_bb_code)])
            trades = pd.concat([trades[[not x for x in filt]], missing])
    
        # now loop though trades and expand out multi entries
        #i = 65
        #this_trade = trades.iloc[i]
        trades_out = pd.DataFrame()
        for i in range(len(trades)) : 
            this_trade = trades.iloc[[i]].copy()
            this_trade['account'] = this_trade.EMSX_ACCOUNT if type(this_trade.EMSX_ACCOUNT.values[0]) == str else this_trade.EMSX_PORT_NAME
            this_trade['filled'] = this_trade['EMSX_DAY_FILL'].values[0]
            this_trade['shares'] = this_trade['EMSX_AMOUNT'].values[0]
            this_trade['avg_price_local'] = this_trade['EMSX_DAY_AVG_PRICE'].values[0]
            this_trade['swap'] = this_trade['EMSX_CFD_FLAG'].values[0]


            # do overides if data exists in EMSX
            if 'EMSX_CUSTOM_NOTE2' in this_trade.columns and type(this_trade['EMSX_CUSTOM_NOTE2'].values[0]) == str :
                tmp_str = this_trade['EMSX_CUSTOM_NOTE2'].iloc[0] 
                # new version all the parameters are in EMSX_CUSTOM_NOTE2
                if "|" in tmp_str :
                    tmp = tmp_str.split("|")
                    this_trade['prime_broker'] = tmp[0]
                    this_trade['exec_broker'] = tmp[1]
                    this_trade['side'] = actions_map[tmp[2]]
                else : 
                    if this_trade['EMSX_CUSTOM_NOTE3'].values[0] in actions_map.keys() :
                        this_trade['side'] = actions_map[this_trade['EMSX_CUSTOM_NOTE3'].values[0]]
                    else : 
                        this_trade['side'] = actions_map[this_trade['EMSX_SIDE'].values[0]]        
                    if 'EMSX_CUSTOM_NOTE4' in this_trade.columns and type(this_trade['EMSX_CUSTOM_NOTE4'].values[0]) == str :
                        this_trade['exec_broker'] = this_trade['EMSX_CUSTOM_NOTE4'] 
                    elif this_trade['EMSX_BROKER'].values[0] in list(broker_map['stock']) :
                        this_trade['exec_broker'] = broker_map.set_index('stock')['account'][this_trade['EMSX_BROKER'].values[0]]
                    else :
                        this_trade['exec_broker'] = this_trade['EMSX_BROKER']                
                    if 'EMSX_CUSTOM_NOTE2' in this_trade.columns and type(this_trade['EMSX_CUSTOM_NOTE2'].values[0]) == str :
                        this_trade['prime_broker'] = this_trade['EMSX_CUSTOM_NOTE2'] 
                    elif this_trade['EMSX_BROKER'].values[0] in list(broker_map['stock']) :
                        this_trade['prime_broker'] = broker_map.set_index('stock')['account'][this_trade['EMSX_BROKER'].values[0]]
                    else :
                        this_trade['prime_broker'] = this_trade['EMSX_BROKER']                
            else :
                this_trade['prime_broker'] = this_trade['EMSX_CUSTOM_NOTE2'] if 'EMSX_CUSTOM_NOTE2' in this_trade.columns and type(this_trade['EMSX_CUSTOM_NOTE2'].values[0]) == str else this_trade['EMSX_BROKER']
            this_trade['strategy'] = this_trade['prime_broker']
            # use allocation from CUSTOM_NOTE1
            if 'EMSX_CUSTOM_NOTE1' in this_trade.columns :
                alloc_str=this_trade['EMSX_CUSTOM_NOTE1'].values[0]
                if ';' in alloc_str or this_trade['EMSX_ACCOUNT'].values[0] == 'MULTI':
                    alloc = get_allocation(alloc_str = alloc_str, 
                        trade_shares = this_trade['shares'].values[0],
                        trade_fills = this_trade['filled'].values[0],
                        lot_size = this_trade['lot_size'].values[0])
                    tmp = pd.DataFrame()
                    for j, this_ob in alloc.iterrows() : 
                        tmp_trade = this_trade.copy()
                        tmp_trade['account'] = this_ob['account']
                        tmp_trade['filled'] = this_ob['filled']
                        tmp_trade['shares'] = this_ob['shares'] 
                        tmp = pd.concat([tmp, tmp_trade.copy()])
                    this_trade=tmp
                trades_out = pd.concat([trades_out, this_trade])
            else : 
                trades_out = pd.concat([trades_out,this_trade])
    
        if len(trades_out) > 0 : 
            trades_out['principle_local'] = trades_out['filled'] * trades_out['avg_price_local'] * trades_out['multiplier'] 
            trades_out = trades_out[new_cols + trade_cols]
            # fix types :
            trades_out['EMSX_SEDOL'] = [str(x) for x in trades_out['EMSX_SEDOL']]
        else :
            trades_out = pd.DataFrame(columns=['bb_code_traded', 'z_bb_code', 'z_sedol', 'AxiomaID', 'name', 'account', 'exec_broker', 'prime_broker', 'swap', 'side', 'shares', 'filled', 'avg_price_local', 'principle_local', 'currency', 'lot_size', 'multiplier', 'strategy', 'xrate', 'basket', 'country_exchange', 'API_SEQ_NUM', 'EMSX_ACCOUNT', 'EMSX_AMOUNT', 'EMSX_ARRIVAL_PRICE', 'EMSX_ASSET_CLASS', 'EMSX_ASSIGNED_TRADER', 'EMSX_AVG_PRICE', 'EMSX_BASKET_NAME', 'EMSX_BASKET_NUM', 'EMSX_BROKER', 'EMSX_BROKER_COMM', 'EMSX_BSE_AVG_PRICE', 'EMSX_BSE_FILLED', 'EMSX_CFD_FLAG', 'EMSX_CLEARING_ACCOUNT', 'EMSX_COMM_DIFF_FLAG', 'EMSX_COMM_RATE', 'EMSX_CURRENCY_PAIR', 'EMSX_CUSTOM_NOTE1', 'EMSX_CUSTOM_NOTE2', 'EMSX_CUSTOM_NOTE3', 'EMSX_CUSTOM_NOTE4', 'EMSX_CUSTOM_NOTE5', 'EMSX_DATE', 'EMSX_DAY_AVG_PRICE', 'EMSX_DAY_FILL', 'EMSX_DIR_BROKER_FLAG', 'EMSX_EXCHANGE', 'EMSX_EXCHANGE_DESTINATION', 'EMSX_EXEC_INSTRUCTION', 'EMSX_FILL_ID', 'EMSX_FILLED', 'EMSX_GTD_DATE', 'EMSX_HAND_INSTRUCTION', 'EMSX_IDLE_AMOUNT', 'EMSX_INVESTOR_ID', 'EMSX_ISIN', 'EMSX_LIMIT_PRICE', 'EMSX_LOCATE_BROKER', 'EMSX_LOCATE_REQ', 'EMSX_NOTES', 'EMSX_NSE_AVG_PRICE', 'EMSX_NSE_FILLED', 'EMSX_ORD_REF_ID', 'EMSX_ORDER_TYPE', 'EMSX_ORIGINATE_TRADER', 'EMSX_ORIGINATE_TRADER_FIRM', 'EMSX_PERCENT_REMAIN', 'EMSX_PM_UUID', 'EMSX_PORT_MGR', 'EMSX_PORT_NAME', 'EMSX_PORT_NUM', 'EMSX_POSITION', 'EMSX_PRINCIPLE', 'EMSX_PRODUCT', 'EMSX_QUEUED_DATE', 'EMSX_QUEUED_TIME', 'EMSX_REASON_CODE', 'EMSX_REASON_DESC', 'EMSX_REMAIN_BALANCE', 'EMSX_ROUTE_ID', 'EMSX_ROUTE_PRICE', 'EMSX_ROUTE_REF_ID', 'EMSX_SEC_NAME', 'EMSX_SEDOL', 'EMSX_SEQUENCE', 'EMSX_SETTLE_AMOUNT', 'EMSX_SETTLE_DATE', 'EMSX_SIDE', 'EMSX_START_AMOUNT', 'EMSX_STATUS', 'EMSX_STEP_OUT_BROKER', 'EMSX_STOP_PRICE', 'EMSX_STRATEGY_END_TIME', 'EMSX_STRATEGY_PART_RATE1', 'EMSX_STRATEGY_PART_RATE2', 'EMSX_STRATEGY_STYLE', 'EMSX_STRATEGY_TYPE', 'EMSX_TICKER', 'EMSX_TIF', 'EMSX_TIME_STAMP', 'EMSX_TRAD_UUID', 'EMSX_TRADE_DESK', 'EMSX_TRADER', 'EMSX_TRADER_NOTES', 'EMSX_TS_ORDNUM', 'EMSX_TYPE', 'EMSX_UNDERLYING_TICKER', 'EMSX_USER_COMM_AMOUNT', 'EMSX_USER_COMM_RATE', 'EMSX_USER_FEES', 'EMSX_USER_NET_MONEY', 'EMSX_USER_WORK_PRICE', 'EMSX_WORKING', 'EMSX_YELLOW_KEY'])
    else :
        trades_out = pd.DataFrame(columns=['bb_code_traded', 'z_bb_code', 'z_sedol', 'AxiomaID', 'name', 'account', 'exec_broker', 'prime_broker', 'swap', 'side', 'shares', 'filled', 'avg_price_local', 'principle_local', 'currency', 'lot_size', 'multiplier', 'strategy', 'xrate', 'basket', 'country_exchange', 'API_SEQ_NUM', 'EMSX_ACCOUNT', 'EMSX_AMOUNT', 'EMSX_ARRIVAL_PRICE', 'EMSX_ASSET_CLASS', 'EMSX_ASSIGNED_TRADER', 'EMSX_AVG_PRICE', 'EMSX_BASKET_NAME', 'EMSX_BASKET_NUM', 'EMSX_BROKER', 'EMSX_BROKER_COMM', 'EMSX_BSE_AVG_PRICE', 'EMSX_BSE_FILLED', 'EMSX_CFD_FLAG', 'EMSX_CLEARING_ACCOUNT', 'EMSX_COMM_DIFF_FLAG', 'EMSX_COMM_RATE', 'EMSX_CURRENCY_PAIR', 'EMSX_CUSTOM_NOTE1', 'EMSX_CUSTOM_NOTE2', 'EMSX_CUSTOM_NOTE3', 'EMSX_CUSTOM_NOTE4', 'EMSX_CUSTOM_NOTE5', 'EMSX_DATE', 'EMSX_DAY_AVG_PRICE', 'EMSX_DAY_FILL', 'EMSX_DIR_BROKER_FLAG', 'EMSX_EXCHANGE', 'EMSX_EXCHANGE_DESTINATION', 'EMSX_EXEC_INSTRUCTION', 'EMSX_FILL_ID', 'EMSX_FILLED', 'EMSX_GTD_DATE', 'EMSX_HAND_INSTRUCTION', 'EMSX_IDLE_AMOUNT', 'EMSX_INVESTOR_ID', 'EMSX_ISIN', 'EMSX_LIMIT_PRICE', 'EMSX_LOCATE_BROKER', 'EMSX_LOCATE_REQ', 'EMSX_NOTES', 'EMSX_NSE_AVG_PRICE', 'EMSX_NSE_FILLED', 'EMSX_ORD_REF_ID', 'EMSX_ORDER_TYPE', 'EMSX_ORIGINATE_TRADER', 'EMSX_ORIGINATE_TRADER_FIRM', 'EMSX_PERCENT_REMAIN', 'EMSX_PM_UUID', 'EMSX_PORT_MGR', 'EMSX_PORT_NAME', 'EMSX_PORT_NUM', 'EMSX_POSITION', 'EMSX_PRINCIPLE', 'EMSX_PRODUCT', 'EMSX_QUEUED_DATE', 'EMSX_QUEUED_TIME', 'EMSX_REASON_CODE', 'EMSX_REASON_DESC', 'EMSX_REMAIN_BALANCE', 'EMSX_ROUTE_ID', 'EMSX_ROUTE_PRICE', 'EMSX_ROUTE_REF_ID', 'EMSX_SEC_NAME', 'EMSX_SEDOL', 'EMSX_SEQUENCE', 'EMSX_SETTLE_AMOUNT', 'EMSX_SETTLE_DATE', 'EMSX_SIDE', 'EMSX_START_AMOUNT', 'EMSX_STATUS', 'EMSX_STEP_OUT_BROKER', 'EMSX_STOP_PRICE', 'EMSX_STRATEGY_END_TIME', 'EMSX_STRATEGY_PART_RATE1', 'EMSX_STRATEGY_PART_RATE2', 'EMSX_STRATEGY_STYLE', 'EMSX_STRATEGY_TYPE', 'EMSX_TICKER', 'EMSX_TIF', 'EMSX_TIME_STAMP', 'EMSX_TRAD_UUID', 'EMSX_TRADE_DESK', 'EMSX_TRADER', 'EMSX_TRADER_NOTES', 'EMSX_TS_ORDNUM', 'EMSX_TYPE', 'EMSX_UNDERLYING_TICKER', 'EMSX_USER_COMM_AMOUNT', 'EMSX_USER_COMM_RATE', 'EMSX_USER_FEES', 'EMSX_USER_NET_MONEY', 'EMSX_USER_WORK_PRICE', 'EMSX_WORKING', 'EMSX_YELLOW_KEY'])
    return trades_out

def calc_settle_date(this_date, orig_data) :
    # calc_settle_date(this_date, body[['z_sedol', 'prime_broker', 'country_exchange']].copy()) 
    # orig_data = body[['z_sedol', 'prime_broker', 'country_exchange']].copy()
    holidays = ou.exec_sql("select * from zimdb..holiday_calendar where date > '" + str(this_date) + "' and date < '" + str((this_date + pd.offsets.BDay(10)).date()) + "'")
    holidays['iso_country'] = ['CN' if x == 'CN_A' else x for x in holidays['iso_country']]
    holidays = holidays.set_index('iso_country')
    settle_day['date_settle'] = [(this_date + pd.offsets.BDay(x)).date() for x in settle_day['days']]
    for this_country in holidays.index :
        date_settle = settle_day.loc[this_country,'date_settle']
        for this_holiday in list(holidays.loc[[this_country],'date']) :
            if this_holiday <= date_settle : 
                date_settle = (date_settle + pd.offsets.BDay(1)).date()
        settle_day.loc[this_country,'date_settle'] = date_settle
    orig_data['date_settle'] = [settle_day.loc[y,'date_settle'] for y  in orig_data['country_exchange']]

    prefix_list = ['', 'cnl_', 'cna_', 'cng_', 'nht_', 'nhl_']
    broker_map = pd.Series(['GS', 'GS',  'GS',   'BBG', 'BOAML', 'BOAML',    'CS', 'CS',    'INT', 'CIMB', 'GS-HT', 'BOAML-HT', 'GS-PT',    'UBS', 'UBS',    'ACCRUAL', 'HAIT', 'HAIT'   ,'JPM', 'JPM', 'MS' ,'MS'   ],
                   index = ['GS', 'GSI', 'GSCO', 'BBG', 'BOAML', 'BOAML-PT', 'CS', 'CS-PT', 'INT', 'CIMB', 'GS-HT', 'BOAML-HT', 'GS-PT', 'UBS', 'UBS-PT', '2GS',     'HAIT', 'HAIT-PT','JPM', 'JPM-PT', 'MS' ,'MS-PT'])
    orig_data['prime_broker'] = list(broker_map[list(orig_data['prime_broker'])])
    broker_settle = pd.DataFrame()
    for this_prefix in prefix_list :
        tmp = ou.exec_sql("select distinct z_sedol, bb_code, prime, date_settle from zimdb_ops.."+this_prefix+"reconciled_trades where date = '"+str(this_date)+"'")
        broker_settle = broker_settle.append(tmp)
    broker_settle = broker_settle.drop_duplicates(['z_sedol', 'prime']).set_index(['z_sedol', 'prime'])
    keys = orig_data.set_index(['z_sedol', 'prime_broker']).index
    if len(broker_settle) > 0 :
        result = [broker if type(broker) == datetime.date else orig  for orig, broker in zip(orig_data.date_settle, list(broker_settle.loc[keys]['date_settle']))]
    else : 
        result = list(orig_data.date_settle)
    return result

def generate_boaml_trade_files(all_trades_main=None, this_date=datetime.datetime.now().date()) :
    # get the trades
    this_datetime = this_datetime()
    this_date = this_datetime.date()
    
    print('downloading emsx trade blotter ')
    if all_trades_main is None :
        all_trades_main  = get_live_trades()
        all_trades_main = all_trades_main.loc[all_trades_main.filled> 0]
    outdir = 'S:/Operations/Workflow/zen_agg/broker_day_end_files/BOAML/'
    datestamp = datetime.datetime.strftime(this_date, "%Y%m%d%H%M%S")
    batch_code=1
    client_code = 'YZNT'
    account_map = pd.Series({'ZEN_MST' :'S11', 'ZEN_CNA' :'S21', 'ZEN_CNG' :'S41'})
    body_header = ['Prime Broker Account Number',	'Unused Field',	'Trading Unit',	'Trading Sub-Unit (Deal Id)',	'Record Type',	'Transaction Type',	'Client Transaction ID',	'Client Block Id',	'Client Original Transaction Id',	'Client Asset Type',	'Client Product Id Type',	'Client Product Id',	'Country of Trading',	'Client Product Description',	'Client Executing Broker',	'Trade Date',	'Contractual Settlement Date',	'Spot Date',	'Price',	'Issue Currency',	'Settlement Currency',	'Cost Basis FX Rate',	'Quantity',	'Commission Amount',	'Commission Rate',	'Commission Type',	'SEC Fee',	'Fee Type 1',	'Fee Amount 1',	'Fee Type 2',	'Fee Amount 2',	'Fee Type 3',	'Fee Amount 3',	'Fee Type 4',	'Fee Amount 4',	'Fee Type 5',	'Fee Amount 5',	'Accrued Interest',	'Net Amount']
    filename = client_code + "_EQTradeFile_" + datestamp + str(int(batch_code)) + ".txt"
    broker_map = pd.Series(['GS', 'GS',  'GS',   'BBG', 'BOAML', 'BOAML',    'CS', 'CS',    'INT', 'CIMB', 'GS-HT', 'BOAML-HT', 'GS-PT',    'UBS', 'UBS',    'ACCRUAL', 'HAIT', 'HAIT'   ,'JPM', 'JPM', 'MS' ,'MS'   ],
                   index = ['GS', 'GSI', 'GSCO', 'BBG', 'BOAML', 'BOAML-PT', 'CS', 'CS-PT', 'INT', 'CIMB', 'GS-HT', 'BOAML-HT', 'GS-PT', 'UBS', 'UBS-PT', '2GS',     'HAIT', 'HAIT-PT','JPM', 'JPM-PT', 'MS' ,'MS-PT'])
    actions_map = {'B' : 'BY', 'S' : 'SL', 'H' : 'SS', 'C' : 'CS'} 
    query = ("select country_exchange, type, short_finance, long_finance, commission, fees_buy, fees_sell \n"
             "from zimdb_ops..broker_rates \n"
             "where date = (select max(date) from zimdb_ops..broker_rates) \n"
             "and account = 'DEFAULT' \n"
             "and broker = 'BOAML' and type = 'stock' ")
    rates = ou.exec_sql(query)
    rates = rates.set_index('country_exchange')

    body = all_trades_main.loc[[type(x) == str and broker_map[x] == 'BOAML' and y[0] == 'N' for x,y in zip(all_trades_main.prime_broker, all_trades_main.swap)]].copy()
    body['Prime Broker Account Number'] = list(client_code + account_map[body['account']])
    body['Record Type'] = 'N'
    body['Client Asset Type'] = 'EQ'
    body['Client Product Id Type'] = 'SEDOL'
    body['Client Product Description'] = [x.split(" ")[0] for x in body['bb_code_traded']]
    body['Commission Type'] = 'A'
    body['SEC Fee'] = 0
    body['Accrued Interest'] = 0
    body['Transaction Type'] = [actions_map[x] for x in body['side']]
    body['Client Transaction ID'] = [str(int(x)) for x in body['EMSX_SEQUENCE']]
    body['Client Product Id'] = body['EMSX_SEDOL']
    body['Client Executing Broker'] = body['exec_broker']
    body['Trade Date'] = [str(int(x)) for x in body['EMSX_DATE']]
    body['Contractual Settlement Date'] = [str(y).replace("-","") for y  in calc_settle_date(this_date, body[['z_sedol', 'prime_broker', 'country_exchange']].copy()) ]
    body['Price'] = body['avg_price_local']
    body['Issue Currency'] = body['currency']
    body['Settlement Currency'] = body['currency']
    body['Quantity'] = body['filled']
    body['Commission Amount'] = [principle * comms for principle, comms in zip(body['principle_local'], rates.commission[list(body['country_exchange'])])]
    body['Fee Type 2'] = 'LEVY'
    body['Fee Amount 2'] = [principle *(fees_buy if side in['B', 'BUY', 'COVER', 'COVR', 'C'] else fees_sell)  for principle, side, fees_buy, fees_sell, comm in zip(body['principle_local'], body['side'],rates.fees_buy[list(body['country_exchange'])], rates.fees_sell[list(body['country_exchange'])], rates.commission[list(body['country_exchange'])])]
    body['Fee Type 3'] = 'STAMP'
    body['Fee Amount 3'] = 0
    body['Fee Type 5'] = 'OTHER'
    body['Fee Amount 5'] = 0
    body['Net Amount'] = [principle+fees+comm if side in['B', 'BUY', 'COVER', 'COVR', 'C'] else principle-fees-comm for principle, side, fees, comm in zip(body['principle_local'], body['side'], body['Fee Amount 2'],body['Commission Amount'])]
    for x in set(body_header) - set(body.columns) :
        body[x] = np.nan
    # body[body_header].to_clipboard()
    header = pd.DataFrame(["TRADE", batch_code, datestamp, client_code]).transpose()
    trailer = pd.DataFrame([client_code, batch_code, len(body)]).transpose()
    header.to_csv(outdir + filename, sep = '|', header=False, index=False)
    body[body_header].to_csv(outdir + filename, sep = '|', header=False, mode = 'a', index=False)
    trailer.to_csv(outdir + filename, sep = '|', header=False, mode = 'a', index=False)

def generate_gs_trade_files(all_trades_main=None, this_date=datetime.datetime.now().date()) :
    # get the trades
    this_datetime = this_datetime()
    this_date = this_datetime.date()
    print('downloading emsx trade blotter ')
    if all_trades_main is None :
        all_trades_main = get_live_trades()
        all_trades_main = all_trades_main.loc[all_trades_main.filled> 0]
    outdir = 'S:/Operations/Workflow/zen_agg/broker_day_end_files/GS/'
    datestamp = datetime.datetime.strftime(this_date, "%Y%m%d")
    gs_code = { 'ZEN_MST_N' : "054552179",  #     ||   ZENTIFIC ASIA OPPORTUNITES OFFSHORE MASTER FUND LIMITED
                'ZEN_MST_Y' : "054552187",  #     ||   ZENTIFIC ASIA OPPORTUNITES OFFSHORE MASTER FUND LIMITED – CFD
                'ZEN_CNA_N' : "054797162",  #     ||   ZENTIFIC CHINA A INVESTMENT COMPANY LIMITED
                'ZEN_CNA_Y' : "054797170",  #     ||   ZENTIFIC CHINA A INVESTMENT COMPANY LIMITED REF: CFD
                'ZEN_CNL_N' : "054966841",  #     ||   ZENTIFIC CH A LG ONLY F L REF: DIRECT ACCOUNT
                'ZEN_CNL_Y' : "054966858",  #     ||   ZENTIFIC CH A LG ONLY F L REF: CFD ACCOUNT
                'ZEN_CNT_N' : "065059198",  #     ||   TOPWATER CHINA
                'ZEN_CNT_Y' : "063838445",  #     ||   TOPWATER CHINA : CFD
                'ZEN_CNG_N': "054470190",   # ||   Zentific China Opportunities Offshore Fund Limited
                'ZEN_CNG_Y': "054470208",   # ||   Zentific China Opportunities Offshore Fund Limited – CFD
                }
    body_header = ['Order number','Activity','Account','Security','Broker','Custodian','Transaction type','Settlement Ccy','Trade date','Settle date','Quantity','Commission','Price','Accrued interest','Sec Fee','Trade tax','Misc money','Net amount','Principal','Description','Reserved1','Reserved2','Reserved3','Is_CFD','Clearing Agent']
    filename = "tradefile.csv." + datestamp 
    broker_map = pd.Series(['GS', 'GS',  'GS',   'BBG', 'BOAML', 'BOAML',    'CS', 'CS',    'INT', 'CIMB', 'GS-HT', 'BOAML-HT', 'GS-PT',    'UBS', 'UBS',    'ACCRUAL', 'HAIT', 'HAIT'   ,'JPM', 'JPM', 'MS' ,'MS'   ],
                   index = ['GS', 'GSI', 'GSCO', 'BBG', 'BOAML', 'BOAML-PT', 'CS', 'CS-PT', 'INT', 'CIMB', 'GS-HT', 'BOAML-HT', 'GS-PT', 'UBS', 'UBS-PT', '2GS',     'HAIT', 'HAIT-PT','JPM', 'JPM-PT', 'MS' ,'MS-PT'])
    actions_map = {'B' : 'B', 'S' : 'S', 'H' : 'SS', 'C' : 'BC'} 
    query = ("select broker, country_exchange, type, short_finance, long_finance, commission, fees_buy, fees_sell \n"
             "from zimdb_ops..broker_rates \n"
             "where date = (select max(date) from zimdb_ops..broker_rates) \n"
             "and account = 'DEFAULT' \n")
    rates = ou.exec_sql(query)
    rates = rates.set_index('country_exchange')
    rates['Misc money'] = np.nan
    rates['trade_tax'] = np.nan
    rates.loc['HK', 'trade_tax'] = 0.0010
    rates.loc['HK', 'Misc money'] = 0.00005 + 0.000027
    rates.loc['SG', 'Misc money'] = 0.000325 + 0.000075
    rates = rates.reset_index().set_index(['broker', 'country_exchange'])

    body = all_trades_main.loc[[broker_map[x] == 'GS' and z not in ['IN'] for x,y,z in zip(all_trades_main.prime_broker, all_trades_main.swap, all_trades_main.country_exchange)]].copy()
    if len(body)  > 0 : 
        body['Account'] = [gs_code[x+"_"+y] for x,y in zip(body.account, body.swap)]
        body['Security'] = [x.strip() if x.strip() != '' else y for x, y in zip(body['EMSX_SEDOL'], body['bb_code_traded'])]
        body['Broker'] = body['exec_broker']
        body['Custodian'] = body['prime_broker']
        body['Transaction type'] = [actions_map[x] for x in body['side']]
        body['Trade date'] = [str(int(x)) for x in body['EMSX_DATE']]
        body['Quantity'] = body['filled']
        body['Price'] = body['avg_price_local']
        body['Is_CFD'] = [x[0] for x in body['swap']]
        body['Commission'] = [this_quantity * this_price * rates.loc[broker].loc[country]['commission'] if swap == 'N' else 0 for this_price, this_quantity, swap, country, broker in zip(body['avg_price_local'], body['filled'], body['swap'], body['country_exchange'], body['exec_broker'])]
        body['Misc money'] = [this_quantity * this_price * rates.loc[broker].loc[country]['Misc money'] if swap == 'N' else 0 for this_price, this_quantity, swap, country, broker in zip(body['avg_price_local'], body['filled'], body['swap'], body['country_exchange'], body['exec_broker'])]
        body['Trade tax']  = [this_quantity * this_price * rates.loc[broker].loc[country]['trade_tax']  if swap == 'N' else 0 for this_price, this_quantity, swap, country, broker in zip(body['avg_price_local'], body['filled'], body['swap'], body['country_exchange'], body['exec_broker'])]
        body['Order number'] = [x.split("|")[0].replace("#","") for x in body['EMSX_NOTES']]
        body['Order number'] = [y if x == "" else x for x,y in zip(body['Order number'], body['EMSX_SEQUENCE'])]
        body.loc[body['Order number'].duplicated(), 'Order number'] = body.loc[body['Order number'].duplicated(), 'Order number'] +"b"
        body['Activity'] = 'N'
        body['Settlement Ccy'] = ['USD' if y=='Y' else x for x,y in zip(body['currency'], body['Is_CFD'])]
        body['prime_broker'] = ['GSI' if x =='GS' and y == 'N' else x for x,y in zip(body['prime_broker'],body['Is_CFD'])]
        body['Settle date'] = [str(y).replace("-","") for y  in calc_settle_date(this_date, orig_data=body[['z_sedol', 'prime_broker', 'country_exchange']].copy()) ]
        for x in set(body_header) - set(body.columns) :
            body[x] = np.nan
        body[body_header].to_csv(outdir + filename, sep = ',', header=True, index=False, quoting=csv.QUOTE_NONNUMERIC)

def generate_bbg_files(all_trades_main = None, this_date = datetime.datetime.today().date(), cancel_assign = True, backtest = True, pfs = ['ZEN_MST','ZEN_CNA','ZEN_CNL','ZEN_CNG','ZEN_NHT','ZEN_NHL','ZEN_SEA']):
    rncls = {"bb_code_traded":"Ticker & Exc","EMSX_SEQUENCE":"Order Num","swap":"CFD","exec_broker":"Broker ID",
             "EMSX_DATE":"As of Dt","avg_price_local":"Price","filled":"Amount","EMSX_NOTES":"emsx_notes",
             "principle_local":"Full Net Amt","EMSX_SETTLE_DATE":"Stl Date","prime_broker":"Prime","name":"Long Description", 
             "side" : "EMSX_SIDE", "EMSX_STATUS":"status", "basket":"basket","country_exchange":"country_exchange", 
             "EMSX_CUSTOM_NOTE2":"custom_note","EMSX_CUSTOM_NOTE1":"allocation"} 
    
    global backtest_params
    if backtest == True:
        dt = datetime.datetime.strftime(backtest_params['fake_now_time'].date(),'%Y%m%d')
        debug_prefix = '_test'
    else:
        dt = datetime.datetime.strftime(this_date,'%Y%m%d')
        debug_prefix = ''
        
    locs = {'ZEN_MST': '//zimnashk/sd.zentific-im.com$/Operations/Workflow/Trades/'+dt + '/Bloomberg_new/MST' + debug_prefix +'.xlsx',
            'ZEN_CNA' : '//zimnashk/sd.zentific-im.com$/Operations/Workflow/zen_cna/Trades/'+dt + '/Bloomberg_new/CNA'+ debug_prefix +'.xlsx',
            'ZEN_CNL' : '//zimnashk/sd.zentific-im.com$/Operations/Workflow/zen_cnl/Trades/'+dt + '/Bloomberg_new/CNL'+ debug_prefix +'.xlsx',
            'ZEN_CNT' : '//zimnashk/sd.zentific-im.com$/Operations/Workflow/zen_cnt/Trades/'+dt + '/Bloomberg_new/CNT'+ debug_prefix +'.xlsx',
            'ZEN_CNG': '//zimnashk/sd.zentific-im.com$/Operations/Workflow/zen_cng/Trades/' + dt + '/Bloomberg_new/CNG'+ debug_prefix +'.xlsx',
            'ZEN_NHT': '//zimnashk/sd.zentific-im.com$/Operations/Workflow/zen_nht/Trades/' + dt + '/Bloomberg_new/NHT'+ debug_prefix +'.xlsx',
            'ZEN_NHL': '//zimnashk/sd.zentific-im.com$/Operations/Workflow/zen_nhl/Trades/' + dt + '/Bloomberg_new/NHL'+ debug_prefix +'.xlsx',
            'ZEN_SEA': '//zimnashk/sd.zentific-im.com$/Operations/Workflow/zen_sea/Trades/' + dt + '/Bloomberg_new/SEA'+ debug_prefix +'.xlsx'
            }
    ts = datetime.datetime.strftime(datetime.datetime.now(),'%H%M')
    actions_map = {'BUY' : 'B', 'SELL' : 'S', 'SHRT' : 'H', 'COVR' : 'C', 'SHORT' : 'H', 'COVER' : 'C'}   
   
    # get the trades
    print('downloading emsx trade blotter ')
    if all_trades_main is None :
        all_trades_main = get_live_trades()
        all_trades_main = all_trades_main.loc[all_trades_main.filled> 0]
    missing_trades = all_trades_main.loc[[x not in pfs for x in all_trades_main.account]]
    if len(missing_trades) > 0 : 
        print("MISSING TRADE ALLOCATIONS\n==============")
        print(missing_trades[['bb_code_traded', "account", 'basket', "filled", 'avg_price_local', "EMSX_NOTES", 'EMSX_TRADER_NOTES']])
        print("==============")

    # work out settle dates
    holidays = ou.exec_sql("select * from zimdb..holiday_calendar where date > '" + str(this_date) + "' and date < '" + str((this_date + pd.offsets.BDay(10)).date()) + "'")
    holidays['iso_country'] = ['CN' if x == 'CN_A' else x for x in holidays['iso_country']]
    holidays = holidays.set_index('iso_country')
    settle_day['date_settle'] = [(this_date + pd.offsets.BDay(x)).date() for x in settle_day['days']]
    for this_country in holidays.index :
        date_settle = settle_day.loc[this_country,'date_settle']
        for this_holiday in list(holidays.loc[[this_country],'date']) :
            if this_holiday <= date_settle : 
                date_settle = (date_settle + pd.offsets.BDay(1)).date()
        settle_day.loc[this_country,'date_settle'] = date_settle
    
    # read in the ticketed trades
    ticket_book = ou.exec_sql("select * from zimdb..ticketbook where date = '" + str(this_date) + "'")    
    if len(ticket_book) > 0 : 
        # clean up ticketbook
        ticket_book = ticket_book.apply(lambda x : x.replace("\r\n","") if type(x) == str else x)
        # clean up the ticker just in case
        ticket_book['bb_code'] = [" ".join(x.split(" ")[:-1]) + " " + x.split(" ")[-1].capitalize() for x in ticket_book['bb_code']]    
    # p = 'ZEN_MST'
    for p in pfs:
        ticket_fills = ticket_book[ticket_book.portfolio == p]
        bbg_fills = all_trades_main.loc[all_trades_main.account == p][list(rncls.keys())].copy()
        bbg_fills['in_emsx'] = True
        bbg_fills['bb_code_traded'] = [x.replace(" Equity","") for x in bbg_fills['bb_code_traded']]
        bbg_fills['EMSX_DATE'] = this_date #[datetime.datetime.strftime(datetime.datetime.strptime(str(x),'%Y%m%d'),'%d/%m/%Y') for x in bbg_fills['EMSX_DATE']]
        bbg_fills['EMSX_SETTLE_DATE'] = [settle_day.loc[y,'date_settle'] for y  in bbg_fills['country_exchange']]
        bbg_fills["Setl Total in Setl C"] = bbg_fills["principle_local"]
        bbg_fills['Type'] = 'OA'
        bbg_fills['B/S'] = bbg_fills['side']
        bbg_fills = bbg_fills.rename(columns=rncls)
        bbg_fills['Order Num'] = bbg_fills['Order Num'].astype(int)
        bbg_fills['Commission'] =  0
        bbg_fills['Short Note1'] = ""
        bbg_fills['Tkt #'] = np.nan
        bbg_fills['trader'] = [x.split("|")[3] for x in bbg_fills['custom_note']]
        
        bbg_fills = bbg_fills[['CFD',	'Ticker & Exc',	'Long Description',	'B/S',	'Amount',	'Price',	'Broker ID',	'Prime',	'As of Dt',	'Stl Date',	'Commission',	'Tkt #',	'Type',	'Full Net Amt',	'Setl Total in Setl C',	'Short Note1',	'Order Num', 'basket', 'trader', 'emsx_notes', 'allocation']]    
        # t = ticket_fills.loc[0]
        for i,t in ticket_fills.iterrows():
            tmp = pd.Series(index=['CFD',	'Ticker & Exc',	'Long Description',	'B/S',	'Amount',	'Price',	'Broker ID',	'Prime',	'As of Dt',	'Stl Date',	'Commission',	'Tkt #',	'Type',	'Full Net Amt',	'Setl Total in Setl C',	'Short Note1',	'Order Num', 'basket', 'trader', 'emsx_notes', 'allocation'],dtype=object)
            tmp['CFD'] = t['cfd'].strip()
            tmp['Ticker & Exc'] = t['bb_code'].strip().replace(" Equity","")
            # blank out currency trades
            tmp['Ticker & Exc'] = "" if ' Curncy' in tmp['Ticker & Exc'] else tmp['Ticker & Exc']
            tmp['Long Description'] = t['long_desc'].strip() if type(t['long_desc']) == str else ''
            tmp['B/S'] = t['action'].strip()
            tmp['Amount'] = t['quantity']
            tmp['Price'] = t['price']
            tmp['Broker ID'] = t['broker'].strip()
            tmp['Prime'] = t['prime_broker'].strip()
            tmp['As of Dt'] = t['date']
            tmp['Stl Date'] = t['settle_date']
            tmp['Commission'] = 0
            tmp['basket'] = 'ticket'
            if " curncy" in t['bb_code'].lower() :
                tmp['Full Net Amt'] = t['quantity']
                tmp['Setl Total in Setl C'] = t['quantity'] * t['price']
            else :
                tmp['Full Net Amt'] = t['price'] * t['quantity']
                tmp['Setl Total in Setl C'] = t['price'] * t['quantity']
            tmp['Short Note1'] = "" if type(t['short_notes']) != str else t['short_notes'].strip()
            tmp['Order Num'] = i
            tmp['Type'] = t['type']
            tmp['trader'] = t['request_ip']
            tmp['emsx_notes'] = 'ticket-ts' + str(t['timestamp'])
            tmp['allocation'] = p + ":" + str(int(t['quantity']))
            bbg_fills = bbg_fills._append(tmp, ignore_index=True)

        # save the file     
        if len(bbg_fills) > 0 :
            # check that the directory exists
            this_loc = locs[p]
            print(this_loc)
            this_dir = "/".join(this_loc.split('/')[:-2])
            if (os.path.isdir(this_dir) == False) :
                os.mkdir(this_dir)
            this_dir = "/".join(this_loc.split('/')[:-1])
            if (os.path.isdir(this_dir) == False) :
                os.mkdir(this_dir)
            bbg_fills.to_excel(locs[p], index=False)
            all_trades_main.to_csv(locs[p].replace(".xlsx", "_debug.csv"))
            # generate OI to load trades via OI
            knowncols = ['bb_code_traded','exec_broker','account','side','trade_shares','price','prime_broker','strategy','cfd_flag','order_type','limit_price','emsx_notes','tif','algo','parameters']
            tmp = all_trades_main[['bb_code_traded', 	'exec_broker', 'account', 'side', 'shares', 'prime_broker','swap', 'EMSX_NOTES','filled', 'avg_price_local', 'EMSX_CUSTOM_NOTE3']].copy()
            tmp = tmp.loc[tmp.account == p]
            tmp['trade_shares'] = tmp['shares']
            tmp['strategy'] = [x.replace("GSI","GS") for x in tmp['prime_broker']]
            tmp['prime_broker'] = ['GSI' if swap=='N' and prime =='GS' and '=' not in bb_code else prime for bb_code, swap, prime in zip(tmp['bb_code_traded'], tmp['swap'], tmp['prime_broker'])]
            tmp['cfd_flag'] = tmp['swap']
            tmp['order_type'] = 'MKT'
            tmp['limit_price'] = np.nan
            tmp['price'] = tmp['avg_price_local']
            tmp['trade_shares'] = tmp['filled']
            tmp['emsx_notes'] = tmp['EMSX_NOTES']
            tmp['tif'] = 'DAY'
            tmp['algo'] = np.nan
            tmp['parameters'] = np.nan
            tmp[knowncols].to_csv(locs[p].replace(".xlsx", "_all_account_oi_loadfile.csv"), index=False, header=False)

    # load ticketed trades from ticketbook
    # For Coroprate Actions, Currency and Forward Tickets
    # Also for column O (Setl Total in Setl C), the amount should equal to column N. They are only different when it's a currency trade where column O = local ccy, column N = USD

    return

def generate_tw_trade_files(all_trades_main=None, this_date=datetime.datetime.now().date()) :
    this_datetime = this_datetime()
    this_date = this_datetime.date()
    # get the trades
    print('downloading emsx trade blotter ')
    if all_trades_main is None :
        all_trades_main  = get_live_trades()
        all_trades_main = all_trades_main.loc[all_trades_main.filled> 0]
    outdir = 'S:/Operations/Workflow/zen_agg/broker_day_end_files/TW/'
    datestamp = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d%H%M%S")
    body_header = ["ClientOrderID", "Security Type", "Security Currency", "Security Description", "Buy/Sell/Short/Cover", "ID Source", "Security Id", "Fill Qty", "Fill Price", "Trade Date", "Settlement Date", "Execution Broker", "Clearing Agent", "FX Rate", "Commission", "Expenses", "Settle Currency", "Fund"]
    filename = "ZEN_CNT_Trade_File_" + datestamp + ".csv"
    broker_map = pd.Series(['GS', 'GS',  'GS',   'BBG', 'BOAML', 'BOAML',    'CS', 'CS',    'INT', 'CIMB', 'GS-HT', 'BOAML-HT', 'GS-PT',    'UBS', 'UBS',    'ACCRUAL', 'HAIT', 'HAIT'   ,'JPM', 'JPM', 'MS' ,'MS'   ],
                   index = ['GS', 'GSI', 'GSCO', 'BBG', 'BOAML', 'BOAML-PT', 'CS', 'CS-PT', 'INT', 'CIMB', 'GS-HT', 'BOAML-HT', 'GS-PT', 'UBS', 'UBS-PT', '2GS',     'HAIT', 'HAIT-PT','JPM', 'JPM-PT', 'MS' ,'MS-PT'])
    actions_map = {'B' : 'B', 'S' : 'SS', 'H' : 'SS', 'C' : 'BC'} 
    query = ("select country_exchange, type, short_finance, long_finance, commission, fees_buy, fees_sell \n"
             "from zimdb_ops..broker_rates \n"
             "where date = (select max(date) from zimdb_ops..broker_rates) \n"
             "and account = 'DEFAULT' \n"
             "and broker = 'GS' and type = 'stock' ")
    rates = ou.exec_sql(query)
    rates = rates.set_index('country_exchange')

    holidays = ou.exec_sql("select * from zimdb..holiday_calendar where date > '" + str(this_date) + "' and date < '" + str((this_date + pd.offsets.BDay(10)).date()) + "'")
    holidays['iso_country'] = ['CN' if x == 'CN_A' else x for x in holidays['iso_country']]
    holidays = holidays.set_index('iso_country')
    settle_day['date_settle'] = [(this_date + pd.offsets.BDay(x)).date() for x in settle_day['days']]
    for this_country in holidays.index :
        date_settle = settle_day.loc[this_country,'date_settle']
        while date_settle in list(holidays.loc[[this_country],'date']) :
            date_settle = (date_settle + pd.offsets.BDay(1)).date()
        settle_day.loc[this_country,'date_settle'] = date_settle
    sqlcmd = 'select 1 as USD, XRATE_USDAUD as AUD, XRATE_USDCNY as CNY, XRATE_USDHKD as HKD, XRATE_USDIDR as IDR, XRATE_USDINR as INR, XRATE_USDJPY as JPY, XRATE_USDKRW as KRW, XRATE_USDMYR as MYR, XRATE_USDNZD as NZD, XRATE_USDPHP as PHP, XRATE_USDSGD as SGD, XRATE_USDTHB as THB, XRATE_USDTWD as TWD, XRATE_USDCNH as CNH, XRATE_USDEUR as EUR from zimdb_ops..ref_rates where date = (select max(date) from zimdb_ops..ref_rates)'
    fx_rate = ou.exec_sql(sqlcmd).transpose()[0]
    trade_ticker_map = ou.exec_sql("select * from zimdb_ops..trade_ticker_map")
    trade_ticker_map.set_index('bb_code')['currency']
    curr_map = trade_ticker_map.set_index('bb_code')['currency']
    exchange_map = trade_ticker_map.set_index('bb_code')['country_exchange']

    account = 'ZEN_MST'
    body = all_trades_main.loc[[type(x) == str and x == account for x in all_trades_main.account]].copy()
    body['ClientOrderID'] = [str(int(x)) for x in body['EMSX_SEQUENCE']]
    body['Security Type'] = [('FUT' if '=' in bb_code_traded else ('EQSWAP' if swap == 'Y' else 'CS')) for swap,bb_code_traded in zip(body['swap'], body['bb_code_traded'])]
    body['ID Source'] = 'Bloomberg Ticker'
    body["Security Currency"] = body['currency']
    body["Settle Currency"] = ['USD' if swap == 'Y' else currency for swap,currency in zip(body['swap'], body['Security Currency'])]
    body["Security Description"] = body['name']
    body["Security Id"] = body['bb_code_traded']
    body["Buy/Sell/Short/Cover"] = [actions_map[x] for x in body['side']]
    body['Fill Price'] = body['avg_price_local']
    body['Fill Qty'] = body['filled']
    body['Trade Date'] = str(this_date).replace('-','')
    body['Settlement Date'] = [str(settle_day.loc[y,'date_settle']).replace("-","") for y  in body['country_exchange']]
    body['Execution Broker'] = body['exec_broker']
    body['Clearing Agent'] = body['prime_broker']
    body['Fund'] = account
    body['FX Rate'] = [fx_rate[x] for x in body['currency']]
    body['Commission'] = [principle * comms for principle, comms in zip(body['principle_local'], rates.commission[list(body['country_exchange'])])]
    body['Expenses'] = [principle *(fees_buy if side in['B', 'BUY', 'COVER', 'COVR'] else fees_sell)  for principle, side, fees_buy, fees_sell, comm in zip(body['principle_local'], body['side'],rates.fees_buy[list(body['country_exchange'])], rates.fees_sell[list(body['country_exchange'])], rates.commission[list(body['country_exchange'])])]
    for x in set(body_header) - set(body.columns) :
        body[x] = np.nan

    # now do ticketed trades....
    ticket_fills = ou.exec_sql("select * from zimdb..ticketbook where date = '" + str(this_date) + "' and portfolio = '" + account + "'")    
    #ticket_fills = ou.exec_sql("select * from zimdb..ticketbook where portfolio = '" + account + "'")    
    ticket_fills["country_exchange"] = list(exchange_map[list(ticket_fills.bb_code)])
    ticket_fills['country_exchange'].fillna('HK', inplace=True)
    ticket_fills['principle_local'] = ticket_fills['price'] * ticket_fills['quantity']
    ticket_fills['ClientOrderID'] = [str(x).replace("-","") + str(y).zfill(3) for x,y in zip(ticket_fills['date'], ticket_fills.index)]
    ticket_fills['Security Type'] = ['FWDFX' if 'FWD' in note else ('FUT' if '=' in bb_code_traded else ('EQSWAP' if swap == 'Y' else 'CS')) for swap,bb_code_traded, note in zip(ticket_fills['cfd'], ticket_fills['bb_code'], ticket_fills['short_notes'])]
    ticket_fills['ID Source'] = 'Bloomberg Ticker'
    ticket_fills["Security Currency"] = list(curr_map[list(ticket_fills.bb_code)])
    ticket_fills["Settle Currency"] = ['USD' if swap == 'Y' or type(currency) != str else currency for swap,currency in zip(ticket_fills['cfd'], ticket_fills['Security Currency'])]
    ticket_fills["Security Description"] = ticket_fills['long_desc']
    ticket_fills["Security Id"] = ticket_fills['bb_code']
    ticket_fills["Buy/Sell/Short/Cover"] = [actions_map[x] for x in ticket_fills['action']]
    ticket_fills['Fill Price'] = ticket_fills['price']
    ticket_fills['Fill Qty'] = ticket_fills['quantity']
    ticket_fills['Trade Date'] = str(this_date).replace('-','')
    ticket_fills['Settlement Date'] = [str(settle_day.loc[y,'date_settle']).replace("-","") for y  in ticket_fills['country_exchange']]
    ticket_fills['Execution Broker'] = ticket_fills['broker']
    ticket_fills['Clearing Agent'] = ticket_fills['prime_broker']
    ticket_fills['Fund'] = account
    ticket_fills['FX Rate'] = [fx_rate[x] if type(x) == str else np.nan for x in ticket_fills['Security Currency']]
    ticket_fills['Commission'] = [principle * comms for principle, comms in zip(ticket_fills['principle_local'], rates.commission[list(ticket_fills['country_exchange'])])]
    ticket_fills['Expenses'] = [principle *(fees_buy if side in['B', 'BUY', 'COVER', 'COVR'] else fees_sell)  for principle, side, fees_buy, fees_sell, comm in zip(ticket_fills['principle_local'], ticket_fills['action'],rates.fees_buy[list(body['country_exchange'])], rates.fees_sell[list(ticket_fills['country_exchange'])], rates.commission[list(ticket_fills['country_exchange'])])]
    for x in set(body_header) - set(ticket_fills.columns) :
        ticket_fills[x] = np.nan
    result = body[body_header].append(ticket_fills[body_header])
    result.to_csv(outdir + filename, index=False)
    return outdir + filename

def send_tw_trade_files(file) :
    host = "secure.paladyne.us"
    username = "tpw-zentific"
    private_key = "S:\Operations\certificates\Topwater server\ZIM_priv_key2.pem"
    password = "5ftpZ1m!!"
    server = pysftp.Connection(host=host, username=username, private_key=private_key, private_key_pass=private_key_pass)
    with pysftp.Connection(host=host, username=username, private_key=private_key, private_key_pass=private_key_pass) as server:
        #all_files = server.listdir()
        server.put(file)

####
# NH live intraday file generation
def generate_nh_intraday_fills() :
    trades = get_live_trades(this_date = datetime.datetime.today().date())
    this_datetime = this_datetime()
    this_date = this_datetime.date()
    # special enfusion data
    account = "432452"
    counterparty = 'Internal'    
    trade_map = { 'B' : 'Buy', 'S' : 'Sell', 'C' : 'BuyToCover', 'H' : 'SellShort'}
    today = datetime.datetime.now().date()
    trades['date'] = [datetime.datetime.strptime(str(int(x)), "%Y%m%d").date() for x in trades['EMSX_DATE']]
    filt = [x == today and y in ('ZEN_NHT', 'ZEN_NHL') for x,y in zip(trades['date'], trades['account'])]
    trades = trades.loc[filt]
    cols = ['Trade Date',	'Account Id',	'Counterparty Code',	'Transaction Type',	'Identifier',	'Identifier Type',	'Quantity',	'Trade CCY',	'Trade Price',	'Settle CCY', 'TRS ID']
    trades['Trade Date'] = [datetime.datetime.strftime(x,"%m/%d/%y") for x in trades['date']]
    trades['Account Id'] = account
    trades['Counterparty Code'] = counterparty
    trades['Transaction Type'] = [trade_map[x] for x in trades['side']]
    trades['Identifier'] = trades['bb_code_traded']
    trades['Identifier Type'] = 'Bloomberg Yellow'
    trades['Quantity'] = trades['shares']
    trades['Trade Price'] = trades['avg_price_local']
    trades['Trade CCY'] = trades['currency']   
    trades['Settle CCY'] = ['USD' if x == 'Y' else y for x,y in zip(trades['swap'], trades['currency'])]   
    trades['TRS ID'] = [int(y) if x == 'Y' else '' for x,y in zip(trades['swap'], trades['EMSX_TS_ORDNUM'])]
    filename = 'output/Zentific_' + datetime.datetime.strftime(datetime.datetime.now(),"%Y%m%d%H%M") + ".csv"
    trades[cols].to_csv(filename, index=False)



def backup_eod_blotter (src_dir = 'S:/bbgloader/todays_orders/Done/', dest_dir = 'S:/Operations/Workflow/zen_agg/eod_blotter/') :
    dest_list = os.listdir(dest_dir)
    today_str = str((datetime.datetime.now()).date()).replace("-","")
    src_list = pd.DataFrame(os.listdir(src_dir), columns = ['filename'])
    filt = [x.startswith('day') and x.endswith('.csv') for x in  src_list['filename'] ]
    src_list = src_list.loc[filt]
    src_list['date_str'] = [x[3:11] for x in  src_list['filename'] ]
    src_list['time_str'] = [x[14:18] for x in  src_list['filename'] ]
    filt = [x < '1830' for x in  src_list['time_str'] ]
    src_list = src_list.loc[filt]
    src_list = src_list.groupby('date_str')['filename'].max()
    src_list = [x for x in  src_list  if x[3:11] != today_str]

    copy_files = list(set(src_list) - set(dest_list))
    copy_files.sort()
    for file_name in copy_files:
        full_file_name = os.path.join(src_dir, file_name)
        if os.path.isfile(full_file_name):
            shutil.copy(full_file_name, dest_dir)    

def this_datetime(this_datetime = None):    #US fix trade
    if this_datetime == None:
        this_datetime = datetime.datetime.now()
        if this_datetime <= datetime.datetime.combine(this_datetime.date(), datetime.time(6, 25, 0)):
            this_datetime = this_datetime - pd.offsets.BDay(1)
        elif this_datetime.isoweekday() > 5 :
            this_datetime = this_datetime - pd.this_datetime.BDay(1)
        else:
            this_datetime = this_datetime
    return this_datetime
        

# argv = ['2024-10-18']
def main (argv):
    # get the trades
    print('downloading emsx trade blotter ')
    backtest = False
    if len(argv) > 0 :
        this_date = datetime.datetime.strptime(argv[0], '%Y-%m-%d').date()
    else :
        #this_date = datetime.datetime.today().date()
        this_date = this_datetime()
    # for debugging
    all_trades_main = get_live_trades(this_date=this_date, force_refresh=True, backtest=backtest)
    all_trades_main = all_trades_main.loc[all_trades_main.filled> 0]
    # all_trades_main.to_clipboard()
    # make sure emsx trade date is today:
    #if this_date == datetime.date(2021,11,8) :
    #    all_trades_main['EMSX_DATE'] = 20211108.0
    if len(argv) >= 1 and argv[0] == 'post_trade' :
        generate_gs_trade_files(all_trades_main, this_date=this_date)
        generate_boaml_trade_files(all_trades_main, this_date=this_date)
    else :
        generate_bbg_files(all_trades_main, this_date, backtest=backtest)
        #generate_gs_trade_files(all_trades_main, this_date=this_date)
        #generate_boaml_trade_files(all_trades_main, this_date=this_date)

    # now back up the end of day blotter file from previous day
    backup_eod_blotter()

if (__name__ == '__main__') :
    main(argv=sys.argv[1:])

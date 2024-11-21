import os
import datetime
import pandas as pd
from pandas.tseries.offsets import BDay
import sys
import numpy as np
pd.options.display.max_columns

code_path = r'S:\\Operations\Workflow\zim_ops'
output_path = r'S:\Operations\Workflow\tradefillsmacro\cash_rec\cash_tmp\\'
sys.path.append(code_path)
import ops_utils as ou

ticker_map = ou.exec_sql("""SELECT
                            CASE
                                WHEN bb_code_traded LIKE '%Equity' THEN SUBSTRING(bb_code_traded, 1, CHARINDEX('Equity',bb_code_traded)-2 )
                                ELSE bb_code_traded
                            END AS bb_code,
                            sedol_traded AS sedol,
                            isin,
                            CASE
                                WHEN bb_code_traded LIKE '%/F TB%' AND CHARINDEX('.BK',ric) > 0 
                                    THEN SUBSTRING(ric, 1, CHARINDEX('.BK',ric)-1) + '_f.BK'
                                WHEN bb_code_traded LIKE '%-R TB%' AND CHARINDEX('.BK',ric) > 0
                                    THEN SUBSTRING(ric, 1, CHARINDEX('.BK',ric)-1) + '_n.BK'
                                ELSE ric
                            END AS ric
                            FROM zimdb..trade_ticker_map
                            """)


def filter_files(filepath, includes=[], excludes=[]):
    return [x for x in os.listdir(filepath)
            if all(i in x for i in includes)
            and not any(e in x for e in excludes)]


def get_zen_unwind_performance(broker, date, ops_param):
    sql = """SELECT *
        FROM zimdb_ops..{0}
        WHERE date_settle = '{1}'
        AND prime = '{2}'
        AND event = 'close trade'
    """.format(ops_param['cash_tickets_table'], date, broker)
    df = ou.exec_sql(sql)
    return df

def get_swap_settlement_report_path(broker, date, ops_param):
    filepath = ops_param['workflow_path'] + r'Archive\{}\{}'.format(date.strftime('%Y%m%d'),broker)
    if broker == 'GS':
        includes = ['Custody_Settle_D_301701']
        excludes = ['AP','APE']
    elif broker == 'BOAML':
        includes = ['Lawrence']
        excludes = []
    elif broker == 'UBS':
        includes = ['PRTTerminationsISIN.GRPZENTI']
        excludes = []
    elif broker == 'JPM':
        includes = ['Blue_Portfolio_Swap_Settlement_Enhanced_Report']
        excludes = []
    elif broker == 'MS':
        includes = ['ZIM-EQSWAP24MX']
        excludes = []
    files = filter_files(filepath, includes, excludes)
    files.sort(key=lambda x: os.path.getmtime(os.path.join(filepath, x)), reverse=True)
    if len(files) > 0:
        filename = files[0]
        return os.path.join(filepath, filename)
    else:
        print('Cannot find {} report for {} in {]'.format(includes[0],broker,filepath))
        sys.exit()

def get_swap_activity_report_path(broker, date, ops_param):
    filepath = ops_param['workflow_path'] + r'Archive\{}\{}'.format(date.strftime('%Y%m%d'),broker)
    if broker == 'GS':
        includes = ['CFD_Daily_Activi_287575']
        excludes = ['AP','APE']
    elif broker == 'BOAML':
        includes = []
        excludes = []
    elif broker == 'UBS':
        includes = []
        excludes = []
    elif broker == 'JPM':
        includes = []
        excludes = []
    elif broker == 'MS':
        includes = []
        excludes = []
    files = filter_files(filepath, includes, excludes)
    files.sort(key=lambda x: os.path.getmtime(os.path.join(filepath, x)), reverse=True)
    if len(files) > 0:
        filename = files[0]
        return os.path.join(filepath, filename)
    else:
        print('Cannot find {} report for {} in {]'.format(includes[0],broker,filepath))
        sys.exit()

def load_broker_swap_settlement_cashflow(broker, date, ops_param):
    '''
    GS: financing paid at ME
    BOAML: financing paid when unwind
    UBS: financing paid at ME
    JPM: financing paid when unwind
    MS: financing paid when unwind
    '''
    
    trade_dates = set(get_zen_unwind_performance(broker, date, ops_param).loc[:,'date'])
    if broker == 'GS':
        dfs = []
        for trade_date in trade_dates:
            filepath = get_swap_activity_report_path(broker,trade_date,ops_param)
            df_temp = pd.read_excel(filepath, skiprows=7)
            dfs.append(df_temp)
        df = ou.merge_df_list(dfs)
        df = df[df['Open/Close'] == 'Close']
    elif broker == 'BOAML':
        dfs = []
        for trade_date in trade_dates:
            filepath = get_swap_settlement_report_path(broker, trade_date, ops_param)
            df_temp = pd.read_excel(filepath, skiprows=[0,2])
            df_temp = df_temp[df_temp['Report Date'] == trade_date.strftime('%Y-%m-%d')]
            dfs.append(df_temp)
        df = ou.merge_df_list(dfs)
    elif broker == 'UBS':
        dfs = []
        for trade_date in trade_dates:
            filepath = get_swap_settlement_report_path(broker, trade_date, ops_param)
            df_temp = pd.read_csv(filepath)
            dfs.append(df_temp)
        df = ou.merge_df_list(dfs)
        df = df[df['Account Name'] == 'BLUEHARBOUR MAP I LP-ZENTIFIC']
    elif broker == 'JPM':
        filepath = get_swap_settlement_report_path(broker, date, ops_param)
        df = pd.read_csv(filepath)
        df = df[df['Level']=='Trade']
        df = df[df['Swap Pay Date']== date.strftime('%Y-%m-%d')]
    elif broker == 'MS':
        filepath = get_swap_settlement_report_path(broker, date, ops_param)
        df = pd.read_csv(filepath,skiprows=1)
        df = df[df['Account Number'] == '038CAFIQ0']
        df = df[df['Payment Date'] == date.strftime('%Y-%m-%d')]
    return df

def reconcile_broker_swap_settlement_cashflow(broker, date, ops_param):

    def _helper(df_zen, df_broker, broker, bb_code_col_name, performance_col_name):
        perf_dict1 = dict(zip(df_zen['bb_code'], df_zen['accrued_fin']+df_zen['cash_local']))
        perf_dict2 = dict(zip(df_broker[bb_code_col_name], df_broker[performance_col_name]))
        all_bb_codes = set(perf_dict1) | set(perf_dict2)
        result = []
        for bb in all_bb_codes:
            p1 = perf_dict1.get(bb)
            p2 = float(perf_dict2.get(bb))
            if p1 is None:
                flag = 'break - Zen missing'
            elif p2 is None:
                flag = 'break - {} missing'.format(broker)
            elif abs(p1 - p2) > 1:
                flag = 'break - diff > 1'
            else:
                flag = ''
            result.append({
                'break': flag,
                'bb_code': bb,
                '{}_NetAmount'.format(broker): p2,
                'Z_NetAmount': p1,
                'diff': p1 - p2 if p1 is not None and p2 is not None else None,
            })
        result_df = pd.DataFrame(result)[['break','bb_code','{}_NetAmount'.format(broker),'Z_NetAmount','diff']]
        return result_df

    df_zen = get_zen_unwind_performance(broker, date, ops_param)
    df_broker = load_broker_swap_settlement_cashflow(broker, date, ops_param)
    if broker == 'GS':
        df_break = _helper(df_zen=df_zen,
                           df_broker=(
                               df_broker
                               .merge(ticker_map[['ric','bb_code']], left_on='Underlyer RIC', right_on='ric', how='left')
                           ),
                           broker=broker,
                           bb_code_col_name='bb_code',
                           performance_col_name='Net Amount (Settle CCY)'
                           )
        # to align df_broker with legacy V2 file
        df_broker.rename(columns={'Net Amount (Settle CCY)':'GS_NetAmount'}, inplace=True)
        df_broker.insert(33, 'bb_code',ticker_map.set_index('ric',drop=True).loc[df_broker['Underlyer RIC'],'bb_code'])

    elif broker == 'BOAML':
        df_break = _helper(df_zen=df_zen,
                           df_broker=(
                               df_broker[df_broker['Reset Record Code']=='REQY']
                           ),
                           broker=broker,
                           bb_code_col_name='Underlying Primary Bloomberg Code',
                           performance_col_name='Total Pay CCY'
                           )
        # to align df_broker with legacy V2 file
        df_broker.columns = [col.replace('-', '').replace(' ', '') for col in df_broker.columns]
        df_broker = df_broker[['ResetRecordDescription','MLReferenceNumber','DateClosed','SwapResetDate','PaymentDate',
                               'Action','SwapDescription','UnderlyingShortProductDescription','UnderlyingInternalID','UnderlyingPrimaryBloombergCode',
                               'UnderlyingISIN','UnderlyingSEDOL', 'Position','TotalPayCCY','SettlementCCY',
                               'PositionCostBasis','MarkPriceLocalCCY','Comments']]
        df_broker.rename(columns={'TotalPayCCY':'BOAML_NetAmount'},inplace=True)


    elif broker == 'UBS':
        df_break = _helper(df_zen=df_zen,
                           df_broker=(
                               df_broker
                               .merge(ticker_map[['ric', 'bb_code']], left_on='RIC', right_on='ric', how='left')
                               .apply(lambda x: x.apply(ou.text2no) if x.name == 'Net PnL' else x)
                           ),
                           broker=broker,
                           bb_code_col_name='bb_code',
                           performance_col_name='Net PnL'
                           )
        # to align df_broker with legacy V2 file
        df_broker.rename(columns={'Net PnL': 'UBS_NetAmount'}, inplace=True)
        df_broker.insert(21, 'bb_code', ticker_map.set_index('ric', drop=True).loc[df_broker['RIC'], 'bb_code'])



    elif broker == 'JPM':
        df_break = _helper(df_zen=df_zen,
                           df_broker=df_broker,
                           broker=broker,
                           bb_code_col_name='Bloomberg Code',
                           performance_col_name='Equity Amount (Pay Currency)'
                           )
        # to align df_broker with legacy V2 file
        df_broker.columns = [col.replace('-', '').replace(' ', '') for col in df_broker.columns]
        df_broker.rename(columns={'EquityAmount(PayCurrency)': 'JPM_NetAmount'}, inplace=True)


    elif broker == 'MS':
        df_break = _helper(df_zen=df_zen,
                           df_broker=df_broker,
                           broker=broker,
                           bb_code_col_name='Bloomberg ID',
                           performance_col_name='Amount'
                           )
        # to align df_broker with legacy V2 file
        df_broker.columns = [col.replace('-', '').replace(' ', '') for col in df_broker.columns]
        df_broker.rename(columns={'Amount': 'MS_NetAmount'}, inplace=True)
        df_broker.insert(53, 'MS_Total', df_broker['MS_NetAmount']+df_broker['FinancingAmount'])


    #to align df_zen with legacy V2 file
    df_zen.insert(18,'pre_NetAmount',-(df_zen['accrued_fin']+df_zen['cash_local']))
    df_zen.insert(19, 'Z_NetAmount', (df_zen['accrued_fin'] + df_zen['cash_local']))
    df_zen.insert(28, 'updated fx', '')
    df_zen.insert(29, 'BBG_CashEvent', 'TRUE')
    df_zen.insert(30, 'cash_USD', df_zen['cash_local'])
    df_zen.drop(labels='curr_underlying',axis=1, inplace=True)

    #output to align with legacy design
    df_zen.to_csv(output_path+'z_sea_{}_tmp.csv'.format(broker.lower()),index=False)
    df_broker.to_csv(output_path + '{b}_sea_{b}_tmp.csv'.format(b=broker.lower()),index=False)
    df_break.to_csv(output_path+'BREAK_sea_{}.csv'.format(broker.lower()),index=False)
    return


def main(argv):
    ops_param = ou.get_ops_param('ZEN_SEA')
    print ("called with " + str(len(argv)) + " paramenters " + argv[0])
    if (len(argv) > 0) and (type(ou.text2date(argv[0])) == datetime.date) :
        print ("paramenters 0 " + argv[0])
        date = ou.text2date(argv[0])
    if (len(argv) > 1)  and argv[1] in ['GS','BOAML','UBS','JPM','MS'] :
        print ("paramenters 1 " + argv[1])
        broker = argv[1]

    reconcile_broker_swap_settlement_cashflow(broker, date, ops_param)


if (__name__ == '__main__') :
   main(sys.argv[1:])
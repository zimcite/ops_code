import os
import datetime
import pandas as pd
from pandas.tseries.offsets import BDay
import sys


template_path = r'S:\Operations\Workflow\zim_ops\ops_code_py311\sea_broker_report_template\\'
output_path = r'S:\Operations\Workflow\tradefillsmacro\cash_rec\cash_tmp\\'
log_file = os.path.join(output_path,'logs.txt')
import ops_utils as ou

def merge_txt_to_csv(csv_path, txt_path, column_name):
    df = pd.read_csv(csv_path)
    original_columns = df.columns  # Store original column order
    new_rows = []

    with open(txt_path, 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            new_row = {col: '' for col in original_columns}  # Initialize with original columns
            new_row[column_name] = line
            new_rows.append(new_row)

    # Append all new rows and reorder columns
    df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    df = df[original_columns]  # Restore original column order
    df.to_csv(csv_path, index=False)
    return df


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

def load_zen_unwind_performance(broker, date, ops_param):
    sql = """SELECT *
        FROM zimdb_ops..{0}
        WHERE date_settle = '{1}'
        AND prime = '{2}'
        AND event = 'close trade'
        AND on_swap = 'SWAP'
    """.format(ops_param['cash_tickets_table'], date, broker)
    df = ou.exec_sql(sql)
    if df.empty:
        logger.warning('There is no ZEN swap unwind cashflow on settle date = {} for broker {}'.format(date, broker))
    return df

def get_swap_settlement_report_path(broker, date, ops_param, verbose=True):
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
    files = ou.filter_files(filepath, includes, excludes)
    files.sort(key=lambda x: os.path.getmtime(os.path.join(filepath, x)), reverse=True)
    if len(files) > 0:
        filename = files[0]
        return os.path.join(filepath, filename)
    else:
        if verbose:
            logger.warning('Cannot find {} report for {} in {}'.format(includes[0],broker,filepath))
        return

def get_swap_activity_report_path(broker, date, ops_param, verbose=True):
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
    files = ou.filter_files(filepath, includes, excludes)
    files.sort(key=lambda x: os.path.getmtime(os.path.join(filepath, x)), reverse=True)
    if len(files) > 0:
        filename = files[0]
        return os.path.join(filepath, filename)
    else:
        if verbose:
            logger.warning('Cannot find {} report for {} in {}'.format(includes[0],broker,filepath))
        return

def load_broker_swap_settlement_cashflow(broker, date, ops_param, column_header, trade_dates):
    '''
    GS: unwind financing paid at ME along with reset financing
    BOAML: unwind financing paid when unwind
    UBS: unwind financing paid after ME along with reset financing
    JPM: unwind financing paid when unwind
    MS: unwind financing paid when unwind

    date is T0
    GS/BOAML/UBS:
        - retrieve trade date from zen cash tickets
        - loop through trade date activity report to load unwind performance cashflow
    JPM/MS:
        - first try T0 settlement report
        - if not found, goes back to T-1 settlement report
    '''
    if broker == 'GS':
        if len(trade_dates) > 0:
            dfs = []
            for trade_date in trade_dates:
                filepath = get_swap_activity_report_path(broker,trade_date,ops_param)
                if filepath is not None:
                    df_temp = pd.read_excel(filepath, skiprows=7)
                    df_temp.columns = column_header
                    df_temp = df_temp[df_temp['Open/Close'] == 'Close']
                    if df_temp.empty:
                        logger.warning('There is no {} swap unwind cashflow on settle date = {} as sourced from {}'.format(broker,date, filepath))
                else:
                    df_temp = pd.DataFrame(columns=column_header)
                dfs.append(df_temp)
            df = ou.merge_df_list(dfs,'v')
        else:
            df = pd.DataFrame(columns=column_header)
    elif broker == 'BOAML':
        if len(trade_dates) > 0:
            dfs = []
            for trade_date in trade_dates:
                filepath = get_swap_settlement_report_path(broker, trade_date, ops_param)
                if filepath is not None:
                    df_temp = pd.read_excel(filepath, skiprows=[0,2])
                    df_temp.columns = column_header
                    #df_temp = df_temp[df_temp['Report Date'] == trade_date.strftime('%Y-%m-%d')]
                    df_temp = df_temp[df_temp['Payment Date'] == date]
                    if df_temp.empty:
                        logger.warning('There is no {} swap unwind cashflow on settle date = {} as sourced from {}'.format(broker,date, filepath))
                else:
                    df_temp = pd.DataFrame(columns=column_header)
                dfs.append(df_temp)
            df = ou.merge_df_list(dfs,'v')

        else:
            df = pd.DataFrame(columns=column_header)
    elif broker == 'UBS':
        if len(trade_dates) > 0:
            dfs = []
            for trade_date in trade_dates:
                filepath = get_swap_settlement_report_path(broker, trade_date, ops_param)
                if filepath is not None:
                    df_temp = pd.read_csv(filepath)
                    df_temp.columns = column_header
                    df_temp = df_temp[df_temp['Account Name'] == 'BLUEHARBOUR MAP I LP-ZENTIFIC']
                    df_temp = df_temp[df_temp['Settle Date'] == date.strftime('%m/%d/%Y')]
                    if df_temp.empty:
                        logger.warning('There is no {} swap unwind cashflow on settle date = {} as sourced from {}'.format(broker,date, filepath))
                else:
                    df_temp = pd.DataFrame(columns=column_header)
                dfs.append(df_temp)
            df = ou.merge_df_list(dfs,'v')

        else:
            df = pd.DataFrame(columns=column_header)
    elif broker == 'JPM':
        df = pd.DataFrame(columns=column_header)
        for date_tmp in [date,date-BDay(1)]:
            filepath = get_swap_settlement_report_path(broker, date_tmp, ops_param, verbose=False if date_tmp==date else True)
            if filepath is not None:
                df = pd.read_csv(filepath)
                df.columns = column_header
                df = df[df['Level']=='Trade']
                df = df[df['Swap Pay Date']== date.strftime('%Y-%m-%d')]
                if df.empty:
                    logger.warning('There is no {} swap unwind cashflow on settle date = {} as sourced from {}'.format(broker, date, filepath))
                break
            else:
                continue
    elif broker == 'MS':
        df = pd.DataFrame(columns=column_header)
        for date_tmp in [date,date-BDay(1)]:
            filepath = get_swap_settlement_report_path(broker, date_tmp, ops_param, verbose=False if date_tmp==date else True)
            if filepath is not None:
                df = pd.read_csv(filepath,skiprows=1)
                df.columns = column_header
                df = df[df['Account Number'] == '038CAFIQ0']
                df = df[df['Payment Date'] == date.strftime('%Y-%m-%d')]
                if df.empty:
                    logger.warning('There is no {} swap unwind cashflow on settle date = {} as sourced from {}'.format(broker, date, filepath))
                break
            else:
                continue

    return df

def reconcile_broker_swap_settlement_cashflow(broker, date, ops_param, column_header):

    def _helper(df_zen, df_broker, broker, bb_code_col_name, performance_col_name):
        perf_dict1 = (df_zen.groupby('bb_code')['accrued_fin'].sum() + df_zen.groupby('bb_code')['cash_local'].sum()).to_dict()
        perf_dict2 = df_broker.groupby(bb_code_col_name)[performance_col_name].sum().to_dict()

        all_bb_codes = set(perf_dict1) | set(perf_dict2)
        result = []
        for bb in all_bb_codes:
            p1 = perf_dict1.get(bb)
            p2 = float(perf_dict2.get(bb)) if perf_dict2.get(bb) is not None else None
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
        columns_to_keep =  ['break', 'bb_code', '{}_NetAmount'.format(broker), 'Z_NetAmount', 'diff']
        result_df = pd.DataFrame(columns=columns_to_keep)
        if result:
            result_df = pd.DataFrame(result)[columns_to_keep]
        return result_df

    df_zen = load_zen_unwind_performance(broker, date, ops_param)
    df_broker = load_broker_swap_settlement_cashflow(broker, date, ops_param, column_header, trade_dates=set(df_zen.loc[:,'date']))
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
        if df_broker.empty:
            df_broker['Underlyer RIC'] = None
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
        df_broker.insert(21, 'bb_code', ticker_map.set_index('ric', drop=True).loc[df_broker['RIC'], 'bb_code'].values)

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

    #output to align with legacy V2 file
    df_zen.to_csv(output_path+'z_sea_{}_tmp.csv'.format(broker.lower()),index=False)
    df_broker.to_csv(output_path + '{b}_sea_{b}_tmp.csv'.format(b=broker.lower()),index=False)
    df_break.to_csv(output_path+'BREAK_sea_{}.csv'.format(broker.lower()),index=False)

    #add log to break file
    merge_txt_to_csv(output_path+'BREAK_sea_{}.csv'.format(broker.lower()), log_file,'break')
    return

def main(argv):
    global logger
    logger = ou.Logger('INFO',log_file)
    column_headers = {
        'GS': list(pd.read_excel(os.path.join(template_path, ou.filter_files(template_path, includes=['GS'])[0]),skiprows=7).columns),
        'BOAML': list(pd.read_excel(os.path.join(template_path, ou.filter_files(template_path, includes=['BOAML'])[0]),skiprows=[0, 2]).columns),
        'UBS': list(pd.read_csv(os.path.join(template_path, ou.filter_files(template_path, includes=['UBS'])[0])).columns),
        'JPM': list(pd.read_csv(os.path.join(template_path, ou.filter_files(template_path, includes=['JPM'])[0])).columns),
        'MS': list(pd.read_csv(os.path.join(template_path, ou.filter_files(template_path, includes=['MS'])[0]),skiprows=1).columns)
    }

    ops_param = ou.get_ops_param('ZEN_SEA')
    print ("called with " + str(len(argv)) + " paramenters " + argv[0])
    if (len(argv) > 0) and (type(ou.text2date(argv[0])) == datetime.date) :
        print ("paramenters 0 " + argv[0])
        #input date from v2 is T-1 so plus 1 bd to get T
        date = ou.text2date(argv[0])+BDay(1)
    if (len(argv) > 1)  and argv[1] in ['GS','BOAML','UBS','JPM','MS'] :
        print ("paramenters 1 " + argv[1])
        broker = argv[1]

    reconcile_broker_swap_settlement_cashflow(broker, date, ops_param, column_headers[broker])


if (__name__ == '__main__') :
   main(sys.argv[1:])
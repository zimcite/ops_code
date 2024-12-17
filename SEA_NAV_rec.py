import pandas as pd
import sys
import datetime
from pandas.tseries.offsets import BDay
import ops_utils as ou

to_date = lambda x: x.date()

def is_month_end(d):
	"""d is datetime.date object"""
	next_day = d + datetime.timedelta(days=1)
	return d.month != next_day.month

def get_previous_month(d):
	"""d is datetime.date object"""
	MS_date = datetime.date(d.year, d.month, 1)
	last_ME_date = MS_date - datetime.timedelta(days=1)
	return last_ME_date.strftime("%b %y")

def get_past_bdays(d, num=5):
	"""d is datetime.date object"""
	date_set = set()
	for i in range(1, num+1):
		date_set.add(to_date(d-BDay(i)))
	return date_set

def get_ticker_map():
    ticker_map = ou.exec_sql(
        """SELECT
            CASE
                WHEN bb_code_traded LIKE '%Equity' THEN SUBSTRING(bb_code_traded, 1, CHARINDEX('Equity',bb_code_traded)-2 )
                ELSE bb_code_traded
            END AS bb_code,
            sedol_traded AS sedol,
            isin,
            name,
            CASE
                WHEN bb_code_traded LIKE '%/F TB%' AND CHARINDEX('.BK',ric) > 0 
                    THEN SUBSTRING(ric, 1, CHARINDEX('.BK',ric)-1) + '_f.BK'
                WHEN bb_code_traded LIKE '%-R TB%' AND CHARINDEX('.BK',ric) > 0
                    THEN SUBSTRING(ric, 1, CHARINDEX('.BK',ric)-1) + '_n.BK'
                ELSE ric
            END AS ric
            FROM zimdb..trade_ticker_map
            """)
    return ticker_map

def get_fa_broker_map(product_type):
    if product_type == 'swap':
        return {'GS':'067015040CF',
                "BOAML":'BHARBZENTIFIC_MLICF',
                "UBS":'P310129645CF',
                "JPM":'31760807CF',
                "MS":'061788400CF'}
    elif product_type == 'physical':
        return {'GS':'065470858',
                "BOAML":'52N03515',
                "UBS":'75208584',
                "JPM":'31228674',
                "MS":'038CAFIQ0'}

def get_broker_list():
    return ('GS','BOAML','UBS','JPM','MS')

def swap_position_rec():
    def get_fa_swap_position():
        dfs = []
        for broker in get_broker_list():
            df = pd.read_excel(dirs['nav_pack_path'],sheet_name=f"EQD_ZENMB1_{get_fa_broker_map('swap')[broker]}")
            mask = df.iloc[:,0].astype(str).str.count('\.') == 2
            start_row = mask.idxmax()
            headers = df.iloc[start_row-1]
            null_mask = df.iloc[:, 0].isna()
            null_rows = null_mask[start_row:]
            end_row = null_rows.idxmax()
            df = pd.DataFrame(df.iloc[start_row:end_row].values,columns=headers)
            df.insert(1,'Broker',broker)
            dfs.append(df)
        df = pd.concat(dfs, ignore_index=True)
        df['Security'] = (df['Security']
                          .str.replace('.USDDEC30', '', regex=False)
                          .str.replace('_N', '_n', regex=False)
                          .str.replace('F.BK', '_f.BK', regex=False)
                          .str.replace('N.BK', '_n.BK', regex=False)
                          )
        df = df.merge(get_ticker_map()[['ric', 'bb_code']], left_on='Security', right_on='ric', how='left').drop('ric',axis=1)
        manual_map = {
            'AMAT_f.BK': 'AMATA/F TB',
            'CELC.KL': 'CDB MK',
            'CUAN.JK': 'CUAN IJ',
            'DIGT.SIDEC30': 'DCREIT SP',
            'IVLZ.BK': 'IVL/F TB',
            'PLUS.PS': 'PLUS PM',
            'UMSI.SI': 'UMSH SP',
            'ESRL.SI':'2501822D SP',
            "PMBT_R.KL":'PMBTR1 MK'
        }
        df.loc[df['Security'].isin(manual_map.keys()), 'bb_code'] = df['Security'].map(manual_map)
        if df['bb_code'].isna().any():
            logger.debug(f"Securities with missing bb_code after manual map, program exit: \n{df[df['bb_code'].isna()][['Security','Description']]}")
            sys.exit()
        df = df[['bb_code','Broker','SSNC Contracts','SSNC OTE','SSNC Closing Price','Adjusted OTE Difference']]
        df = df.rename(columns={'SSNC Contracts':"FA: Quantity",
                                'SSNC OTE':'FA: MTM Value',
                                'SSNC Closing Price':'FA: Close Price',
                                'Adjusted OTE Difference':"FA: Cash Adjustment"})
        return df

    def get_zen_swap_position():
        def get_zen_swap_cash_adj():
            dfs = []
            for broker in get_broker_list():
                folder_path = dirs['cash_tickets_path'] + rf'{broker}\\'
                df = pd.read_excel(folder_path+ou.filter_files(folder_path,includes=['Zentific']), sheet_name='summary',header=None, names=['bb_code','amount']).dropna()
                df['prime'] = broker
                if df.empty:
                    logger.info(f"No cash adjustment found in ZEN cash ticket: {broker}")
                dfs.append(df)
            df = pd.concat(dfs, ignore_index=True)
            return df

        sqlcmd = f"""WITH breakdown AS (SELECT 
                        [bb_code],
                        [name],
                        [prime],
                        [quantity_adj],
                        [price_local_adj]/[fx_rate_adj] AS [price_usd_adj],
                        [price_local_now]/[fx_rate_now] AS [price_usd_now],
                        [quantity_adj]*([price_local_now]/[fx_rate_now] - [price_local_adj]/[fx_rate_adj]) AS [MTM]
                    FROM zimdb_ops..{ops_param['holdings_table']}
                    WHERE 
                        date = '{date}'
                        AND on_swap = 'SWAP'
                    ) 
                    
                    SELECT
                        [bb_code],
                        [name],
                        [prime],
                        SUM([quantity_adj]) AS [quantity_adj],
                        SUM([price_usd_adj]*[quantity_adj])/SUM([quantity_adj]) AS [price_usd_adj],
                        [price_usd_now],
                        sum([MTM]) AS [MTM]
                    FROM breakdown
                    GROUP BY [bb_code], [name], [prime], [price_usd_now]
        """
        df_cash_adj = get_zen_swap_cash_adj()
        df = ou.exec_sql(sqlcmd)
        df = df.merge(df_cash_adj, on=['bb_code','prime'], how='outer')
        df = df.rename(columns={'quantity_adj':'ZEN: Quantity',
                                'price_usd_adj':'ZEN: Open Price',
                                'price_usd_now':'ZEN: Close Price',
                                'MTM':'ZEN: MTM Value',
                                'amount':"ZEN: Cash Adjustment"})
        return df

    df_fa = get_fa_swap_position()
    df_zen = get_zen_swap_position()
    df = pd.merge(df_fa, df_zen, left_on=['bb_code', 'Broker'],right_on=['bb_code', 'prime'], how='outer')
    # fill missing prime
    df['prime'] = df['prime'].fillna(df['Broker'])
    df = df.drop('Broker',axis=1)
    # fill missing name
    df['name'] = df['name'].fillna(df[['bb_code']].merge(get_ticker_map(), on='bb_code')['name'])
    # fillna
    df = df.fillna(0)
    df['USD Difference'] = df['ZEN: MTM Value'] - df['FA: MTM Value'] - df['FA: Cash Adjustment']
    df = df[['bb_code','name','prime','ZEN: Quantity','FA: Quantity','ZEN: Close Price','FA: Close Price','ZEN: MTM Value','FA: MTM Value','USD Difference','ZEN: Cash Adjustment','FA: Cash Adjustment','ZEN: Open Price']]
    return df

def physical_position_rec():
    return

def swap_accrual_rec():
    """Only for USD since USD is settle date balance"""
    sqlcmd = f"""
    SELECT prime,
    SUM(accrued_fin+cash_local) 
    FROM zimdb_ops..sea_cash_tickets
    WHERE event = 'close trade'
    AND currency = 'USD'
    AND date_settle > '{date}'
    AND date <= '{date}'
    GROUP BY prime
    """
    df = ou.exec_sql(sqlcmd)
    df.columns = ['Broker','ZEN: Payables/Receivables']
    return df

def div_accrual_rec():
    def get_fa_div_accrual():

        df = pd.read_excel(dirs['nav_pack_path'],sheet_name="Accrued Dividend Break Summary", skiprows=13)
        mask = df.iloc[:,0].astype(str).str.contains('Overall')
        end_row = mask.idxmax()
        df = df.iloc[:end_row]

        #handle identifier
        df['Investment Id'] = (df['Investment Id']
                              .str.replace('.USDDEC30', '', regex=False)
                              .str.replace('_N', '_n', regex=False)
                              .str.replace('F.BK', '_f.BK', regex=False)
                              .str.replace('N.BK', '_n.BK', regex=False)
                              )
        df = df.merge(get_ticker_map()[['ric', 'bb_code']], left_on='Investment Id', right_on='ric',
                      how='left').drop('ric', axis=1).rename(columns={'bb_code': 'bb_code_ric'})
        df = df.merge(get_ticker_map()[['isin', 'bb_code']], left_on='Investment Id', right_on='isin',
                      how='left').drop('isin', axis=1).rename(columns={'bb_code': 'bb_code_isin'})
        df = df.merge(get_ticker_map()[['sedol', 'bb_code']], left_on=df['Investment Id'].str[:6], right_on='sedol',
                      how='left').drop('sedol', axis=1).rename(columns={'bb_code': 'bb_code_sedol'})
        df['bb_code'] = df['bb_code_ric'].combine_first(df['bb_code_isin']).combine_first(df['bb_code_sedol'])
        df = df.drop(['bb_code_ric', 'bb_code_isin', 'bb_code_sedol'], axis=1)

        #handle broker
        df['Broker'] = [for i in df['Cash Account']]





        # df = df[['bb_code','Broker','SSNC Contracts','SSNC OTE','SSNC Closing Price','Adjusted OTE Difference']]
        # df = df.rename(columns={'SSNC Contracts':"FA: Quantity",
        #                         'SSNC OTE':'FA: MTM Value',
        #                         'SSNC Closing Price':'FA: Close Price',
        #                         'Adjusted OTE Difference':"FA: Cash Adjustment"})
        return df

    get_fa_div_accrual()
    return

def get_cash_interest():
    return

def get_cfd_interest():
    return

def main(argv):
    global ops_param
    global date #month-end date
    global dirs
    global logger
    logger = ou.Logger('INFO')
    ops_param = ou.get_ops_param('ZEN_SEA')
    dirs = {}

    print ("called with " + str(len(argv)) + " paramenters " + argv[0])
    if (len(argv) > 0) and (type(ou.text2date(argv[0])) == datetime.date) :
        print ("paramenters 0 " + argv[0])
        date = ou.text2date(argv[0])
        dirs.update({'workspace':rf'S:\Month End Work\{date.strftime("%m - %B %Y")}\SEA\\'})
        dirs.update({'nav_pack_path':dirs['workspace'] + ou.filter_files(dirs['workspace'], includes=['eNAV'])})
        dirs.update({'cash_tickets_path':rf'S:\Operations\Reports\cash tickets - ZEN_SEA\{date.strftime("%Y%m")}\\'})
    else:
        logger.debug("No month end date was input, program exit!")
        sys.exit()

    # df_swap_position_rec = swap_position_rec()
    # df_swap_position_rec.to_excel(dirs['workspace']+'swap_position_rec.xlsx',index=False)
    # swap_accrual_rec()
    div_accrual_rec()


if (__name__ == '__main__') :
   main(sys.argv[1:])
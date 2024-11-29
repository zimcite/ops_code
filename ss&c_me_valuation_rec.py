import os
import datetime
import pandas as pd
from pandas.tseries.offsets import BDay
import sys
import numpy as np


code_path = r'S:\\Operations\Workflow\zim_ops'
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

def get_ssnc_CFD_valuation(file_path):
    df = pd.read_excel(file_path,skiprows=11)
    df.columns = [col.replace(' ', '') for col in df.columns]
    df = df[df['AssetType']=='CFD']
    return df

def get_ssnc_PHYSICAL_valuation(file_path):
    df = pd.read_excel(file_path,skiprows=11)
    df.columns = [col.replace(' ', '') for col in df.columns]
    df = df[df['AssetType']=='EQYLST']
    return df

def get_zen_CFD_valuation(ops_param, date):
    sqlcmd = """WITH breakdown AS (
    SELECT 
        [bb_code],
        [name],
        [curr_underlying],
        [quantity_adj],
        [price_local_now],
        [quantity_adj]*([price_local_now]/[fx_rate_now] - [price_local_adj]/[fx_rate_adj]) AS [Unrealised P&L in USD],
        [fx_rate_now]
    FROM zimdb_ops..{0}
    WHERE 
        date = '{1}'
        AND on_swap = 'SWAP'
    )

    SELECT 
    [bb_code], 
    [name],
    [curr_underlying],
    SUM([quantity_adj]) AS [quantity_adj],
    [price_local_now],
    SUM([Unrealised P&L in USD]) AS [Unrealised P&L in USD],
    [fx_rate_now]
    FROM breakdown
    GROUP BY 
        [bb_code], 
        [name],
        [curr_underlying],
        [price_local_now],
        [fx_rate_now]
    ORDER BY [bb_code]
    """.format(ops_param['holdings_table'], date)
    df = ou.exec_sql(sqlcmd)
    return df

def get_zen_PHYSICAL_valuation(ops_param, date):
    sqlcmd="""SELECT bb_code,
    price_local_now,
    SUM(quantity_adj) AS [quantity_adj],
    SUM(quantity_adj*price_local_now/fx_rate_now) AS [MV],
    [fx_rate_now]
    FROM zimdb_ops..{0}
    WHERE date = '{1}'
    AND on_swap = 'PHYSICAL'
    AND bb_code not LIKE '%CASH%'
    GROUP BY bb_code,price_local_now, fx_rate_now
    """.format(ops_param['holdings_table'], date)
    df = ou.exec_sql(sqlcmd)
    return df


def reconcile_PHYSICAL_valuations(file_path, date, ops_param):
    df_fa = get_ssnc_PHYSICAL_valuation(file_path)
    df_zen = get_zen_PHYSICAL_valuation(ops_param, date)
    # adding bb_code to df_fa (map on ISIN + SEDOL), force map on CUSIP)
    df_fa['Investment'] = [i[:6] if len(i)==7 else i for i in df_fa['Investment']] #cut sedol to 6 digits
    df_fa = df_fa.merge(ticker_map[['isin','bb_code']], left_on='Investment', right_on='isin', how='left')
    df_fa = df_fa.merge(ticker_map[['sedol', 'bb_code']], left_on='Investment', right_on='sedol', how='left')
    # combine two bb_code columns
    df_fa['bb_code'] = df_fa['bb_code_x'].combine_first(df_fa['bb_code_y'])
    df_fa = df_fa.drop(['bb_code_x', 'bb_code_y'], axis=1)
    manual_map = {
        'TLI/F_TB':'TLI/F TB',
        '81141R100':'SE US',
        'G11448100':'BTDR US',
        'G4124C109':'GRAB US',
    }
    df_fa.loc[df_fa['Investment'].isin(manual_map.keys()), 'bb_code'] = df_fa['Investment'].map(manual_map)
    
    df_merged = pd.merge(df_fa, df_zen, on='bb_code', how='outer')
    df_merged = df_merged[['bb_code', 'Currency', 'quantity_adj', 'price_local_now', 'FinalValue', 'MV', 'fx_rate_now']]
    df_merged.rename(
        columns={
            'quantity_adj':    'zen_quantity',
            'price_local_now': 'zen_ME_price',
            'FinalValue':      'fa_ME_mv',
            'MV':              'zen_ME_mv'
        }, inplace=True
    )
    # Add diff columns - assuming the columns are already numeric
    df_merged['ME_mv_diff'] = df_merged['fa_ME_mv'].astype(float) - df_merged['zen_ME_mv'].astype(float)
    df_merged['ME_mv_diff'] = df_merged['ME_mv_diff'].round(2)
    
    # Add comment column
    df_merged['comment'] = ''
    mask = abs(df_merged['ME_mv_diff']) > 10
    df_merged.loc[mask, 'comment'] = 'MV diff: ' + df_merged.loc[mask, 'ME_mv_diff'].round(2).astype(str)
    df_merged = df_merged.sort('ME_mv_diff', ascending=False).reset_index(drop=True)
    
    cols = ['comment', 'bb_code', 'zen_quantity', 'zen_ME_price', 'fa_ME_mv','zen_ME_mv', 'ME_mv_diff', 'fx_rate_now']
    df_merged = df_merged[cols]
    df_merged.to_excel(r'PHYSICAL valuation rec.xlsx', index=False)
    return

def reconcile_CFD_valuations(file_path,date,ops_param):
    df_fa = get_ssnc_CFD_valuation(file_path)
    df_zen = get_zen_CFD_valuation(ops_param, date)
    #adding bb_code to df_fa (map on RIC)
    df_fa['Investment'] = df_fa['Investment'].str.replace('.USDDEC30','').str.replace('_N','_n').str.replace('F.BK','_f.BK').str.replace('N.BK','_n.BK')
    df_fa = df_fa.merge(ticker_map[['ric', 'bb_code']], left_on='Investment', right_on='ric', how='left')
    manual_map = {
        'AMAT_f.BK': 'AMATA/F TB',
        'CELC.KL': 'CDB MK',
        'CUAN.JK': 'CUAN IJ',
        'DIGT.SIDEC30':'DCREIT SP',
        'IVLZ.BK':'IVL/F TB',
        'PLUS.PS':'PLUS PM',
        'UMSI.SI':'UMSH SP'
    }
    df_fa.loc[df_fa['Investment'].isin(manual_map.keys()), 'bb_code'] = df_fa['Investment'].map(manual_map)

    df_merged = pd.merge(df_fa, df_zen, on='bb_code', how='outer')
    df_merged = df_merged[['bb_code','Currency','quantity_adj','price_local_now','FinalValue','Unrealised P&L in USD','fx_rate_now']]
    df_merged.rename(columns={#'Quantity':'fa_quantity',
                              'quantity_adj':'zen_quantity',
                              #'LocalUnitPrice':'fa_ME_price',
                              'price_local_now':'zen_ME_price',
                              'FinalValue':'fa_unrealized_pnl_usd',
                              'Unrealised P&L in USD':'zen_unrealized_pnl_usd',
                              'fx_rate_now':'fx_ME'
                              }, inplace=True)
    # Add diff columns - assuming the columns are already numeric
    #df_merged['quantity_diff'] = df_merged['fa_quantity'].astype(float) - df_merged['zen_quantity'].astype(float)
    #df_merged['ME_price_diff'] = df_merged['fa_ME_price'].astype(float) - df_merged['z_ME_price'].astype(float)
    df_merged['unrealized_pnl_usd_diff'] = df_merged['fa_unrealized_pnl_usd'].astype(float) - df_merged['zen_unrealized_pnl_usd'].astype(float)
    df_merged['unrealized_pnl_usd_diff'] = df_merged['unrealized_pnl_usd_diff'].round(2)

    # Add comment column
    df_merged['comment'] = ''
    mask = abs(df_merged['unrealized_pnl_usd_diff']) > 10
    df_merged.loc[mask, 'comment'] = 'PNL diff: ' + df_merged.loc[mask, 'unrealized_pnl_usd_diff'].round(2).astype(str)
    df_merged = df_merged.sort('unrealized_pnl_usd_diff', ascending=False).reset_index(drop=True)
    
    # Organize columns
    # cols = ['comment', 'bb_code',
    #         'zen_quantity', 'fa_quantity', 'quantity_diff',
    #         'z_ME_price', 'fa_ME_price', 'ME_price_diff',
    #         'zen_unrealized_pnl_usd', 'fa_unrealized_pnl_usd', 'unrealized_pnl_usd_diff',
    #         'fx_ME']
    #
    # df_merged = df_merged[cols]
    cols = ['comment', 'bb_code','zen_quantity','zen_ME_price','zen_unrealized_pnl_usd', 'fa_unrealized_pnl_usd', 'unrealized_pnl_usd_diff','fx_ME']
    df_merged = df_merged[cols]
    df_merged.to_excel(r'CFD valuation rec.xlsx',index=False)
    return


# df_broker = (
#     df_broker
#     .merge(ticker_map[['ric', 'bb_code']], left_on='Underlyer RIC', right_on='ric', how='left')

def main():
    ops_param = ou.get_ops_param('ZEN_SEA')
    date = '2024-10-31'
    file_path = r"S:\Month End Work\10 - October 2024\SEA\Portfolio Valuation By Product.xlsx"
    reconcile_CFD_valuations(file_path, date, ops_param)
    reconcile_PHYSICAL_valuations(file_path, date, ops_param)


if (__name__ == '__main__') :
   main()
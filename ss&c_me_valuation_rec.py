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

def get_ssnc_valuation_report(file_path):
    df = pd.read_excel(file_path, skiprows=6)
    df.rename(columns={'Unnamed: 0':'Product Type'}, inplace=True)
    df.drop(labels=['Unnamed: 1', 'Unnamed: 2'], axis=1, inplace=True)
    df.columns = [col.replace(' ', '') for col in df.columns]
    df = df[df['ProductType'].notnull()]
    df = df[df['ProductType']=='Future']
    return df

def get_zen_valuation_report(ops_param, date):
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

def reconcile_valuations(df_zen, df_fa):
    #adding bb_code to df_fa
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
    df_merged = df_merged[['bb_code','InvestmentCcy','Quantity','quantity_adj','LocalUnitPrice','price_local_now','UnrealizedMarketP/L','Unrealised P&L in USD','fx_rate_now']]
    df_merged.rename(columns={'Quantity':'fa_quantity',
                              'quantity_adj':'zen_quantity',
                              'LocalUnitPrice':'fa_ME_price',
                              'price_local_now':'z_ME_price',
                              'UnrealizedMarketP/L':'fa_unrealized_pnl_usd',
                              'Unrealised P&L in USD':'zen_unrealized_pnl_usd',
                              'fx_rate_now':'fx_ME'
                              }, inplace=True)
    # Add diff columns - assuming the columns are already numeric
    df_merged['quantity_diff'] = df_merged['fa_quantity'].astype(float) - df_merged['zen_quantity'].astype(float)
    df_merged['ME_price_diff'] = df_merged['fa_ME_price'].astype(float) - df_merged['z_ME_price'].astype(float)
    df_merged['unrealized_pnl_usd_diff'] = df_merged['fa_unrealized_pnl_usd'].astype(float) - df_merged[
        'zen_unrealized_pnl_usd'].astype(float)

    # Add comment column
    df_merged['comment'] = ''
    mask = abs(df_merged['unrealized_pnl_usd_diff']) > 10
    df_merged.loc[mask, 'comment'] = 'PNL diff: ' + df_merged.loc[mask, 'unrealized_pnl_usd_diff'].round(2).astype(str)

    # Organize columns
    cols = ['comment', 'bb_code',
            'zen_quantity', 'fa_quantity', 'quantity_diff',
            'z_ME_price', 'fa_ME_price', 'ME_price_diff',
            'zen_unrealized_pnl_usd', 'fa_unrealized_pnl_usd', 'unrealized_pnl_usd_diff',
            'fx_ME']

    df_merged = df_merged[cols]




    df_merged.to_csv(r'test.csv',index=False)
    pass


# df_broker = (
#     df_broker
#     .merge(ticker_map[['ric', 'bb_code']], left_on='Underlyer RIC', right_on='ric', how='left')

def main():
    ops_param = ou.get_ops_param('ZEN_SEA')
    date = '2024-10-31'
    file_path = r"S:\Operations\Workflow\MonthEnd\fund admin position\2024-10\INNOCAP_BLUEHARBOUR- EQD_ZENMB1 -Portfolio Valuation report- COB 31 Oct 2024 - Copy.xlsx"
    df_fa = get_ssnc_valuation_report(file_path)
    df_zen = get_zen_valuation_report(ops_param, date)
    reconcile_valuations(df_zen, df_fa)


if (__name__ == '__main__') :
   main()
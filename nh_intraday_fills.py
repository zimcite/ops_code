# runs in python310 environment, python311
import activate_env
import os
import pandas as pd
import ops_utils as ou
import datetime
import pysftp

from get_bloomberg_emsx_trades import get_live_trades

type_map = {'B' : 'Buy',
    'S':'Sell',
    'C':'BuyToCover',
    'H':'SellShort'}

if False : 
    accounts = ['ZEN_NHT']
    id_type = 'Bloomberg Yellow'
    settle_ccy = 'USD'
    trade_date = datetime.datetime.now().date()
    output_dir = "S:/Operations/Workflow/NH/"
    account_id = '445246'
                   

def intraday_fills(accounts = ['ZEN_NHT'], 
                    id_type = 'Bloomberg Yellow',
                    settle_ccy = 'USD',
                    trade_date = datetime.datetime.now().date(),
                    output_dir = "S:/Operations/Workflow/NH/",
                    account_id = '445246'
                    ) :
    xrate = ou.exec_sql("select currency, fx from zimdb..fx_rates where date = (select max(date) from zimdb..fx_rates where date <= '"+str(trade_date)+"')").set_index('currency')['fx']
    ticker_map = ou.exec_sql("select bb_code as bb_code_traded, sedol, z_isin, z_sedol, z_AxiomaID as AxiomaID, z_bb_code , currency, lot_size, name from zimdb_ops..trade_ticker_map")
    ticker_map['xrate'] = list(xrate.reindex(list(ticker_map['currency'])))

    cols = ['Trade Date',	'Account Id',	'Counterparty Code',	'Transaction Type',	'Identifier',	'Identifier Type',	
            'Quantity',	'Trade CCY',	'Trade Price',	'Settle CCY']

    live_trades = get_live_trades()
    filt = [x in accounts for x in live_trades['account']] 
    this_trades = live_trades.loc[filt].copy()
    # filt = [str(int(x)) == f'{trade_date:%Y%m%d}'  for x in this_trades['EMSX_DATE']]
    filt = [str(int(x)) == '{:%Y%m%d}'.format(trade_date) for x in this_trades['EMSX_DATE']] 
    this_trades = this_trades.loc[filt].copy()
    result = pd.DataFrame(columns=cols)
    this_trades['settle_ccy'] = settle_ccy
    this_trades['id_type'] = id_type
    # this_trades['trade_date'] = f'{trade_date:%m/%d/%y}'
    this_trades['trade_date'] = '{:%m/%d/%y}'.format(trade_date)
    this_trades['trade_type'] = [type_map[x] for x in this_trades['side']]
    this_trades = this_trades[['trade_date', 'account', 'exec_broker', 'trade_type', 'bb_code_traded', 'id_type', 'filled', 'currency', 'avg_price_local', 'settle_ccy']]
    this_trades.columns = cols
    this_trades['Account Id'] = account_id
    this_trades['TRS ID'] = ''
    # strip the PT out
    # this_trades['Internal'] = [x.replace("-PT","") for x in this_trades['Counterparty Code']]
    this_trades['Counterparty Code'] = 'Internal'
    column_order = [
    'Trade Date', 'Account Id', 'Counterparty Code', 'Transaction Type',
    'Identifier', 'Identifier Type', 'Quantity', 'Trade CCY', 'Trade Price', 'Settle CCY', 'TRS ID']
    this_trades = this_trades[column_order]
    # filename = f'zentific_{datetime.datetime.now():%Y%m%d%H%M}.csv'
    filename = 'zentific_{:%Y%m%d%H%M}.csv'.format(datetime.datetime.now())
    this_trades.to_csv(os.path.join(output_dir, filename), index=False)

    # FTP THE FILE
    ftp_site = ou.get_web_account('nh_enfusion')
    local_file = os.path.join(output_dir, filename)
    target_directory = 'incoming'
    # cnopts = pysftp.CnOpts()
    # cnopts.hostkeys = None  # Disable host key checking
    # sftp = pysftp.Connection(host=ftp_site['web_site'], port=22, username=ftp_site['username'], password=ftp_site['password'], cnopts=cnopts) 
    try: 
        with pysftp.Connection(host=ftp_site['web_site'], port=22, username=ftp_site['username'], password=ftp_site['password']) as sftp:
            print("Connection established successfully.")
            # Change to the target directory on the server
            sftp.cwd(target_directory)
            # Upload the file
            sftp.put(local_file)
            print("Successfully uploaded {} to {}.".format(local_file, target_directory))
    except Exception as e:
        print("Failed to upload file: {}".format(e))

if __name__ == '__main__':
    intraday_fills()

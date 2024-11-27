import os
import datetime
import pandas as pd
from pandas.tseries.offsets import BDay
import sys

from ops_utils import Logger

template_path = r'/sea_broker_cash_balance_report_template\\'
output_path = r'S:\Operations\Workflow\tradefillsmacro\cash_rec\cash_tmp\\'
log_file = os.path.join(output_path,'logs.txt')
import ops_utils as ou


def get_broker_USD_balance_report_path(broker, date, ops_param, verbose=True):
    filepath = ops_param['workflow_path'] + r'Archive\{}\{}'.format(date.strftime('%Y%m%d'),broker)
    if broker == 'GS':
        includes = ['Custody_Settle_D_301701']
        excludes = ['AP','APE']
    elif broker == 'BOAML':
        includes = ['ValuationsandBalancesExtractIntegrated_RGxA_Zentific_BlueHarbour_Map_I_LP_RG']
        excludes = []
    elif broker == 'UBS':
        includes = ['CashBalancesFlat.KH.GRPZENTI']
        excludes = []
    elif broker == 'JPM':
        includes = ['Blue_Position_and_PL']
        excludes = []
    elif broker == 'MS':
        includes = ['ZIM-MAC002TDX-NormalizedTradeDateActivityExtra-CAED-Daily']
        excludes = []
    file = ou.filter_files(filepath, includes, excludes)
    if file is not None:
        if verbose:
            logger.info('read USD balance for {} from report {} in {}'.format(broker, file, filepath))
        return os.path.join(filepath, file)
    else:
        if verbose:
            logger.warning('Cannot find {} report for {} in {}'.format(includes[0],broker,filepath))
        return

def get_broker_nonUSD_balance_report_path(broker, date, ops_param, verbose=True):
    filepath = ops_param['workflow_path'] + r'Archive\{}\{}'.format(date.strftime('%Y%m%d'), broker)
    if broker == 'GS':
        includes = ['1200702741_Custody_Trade_Da']
        excludes = ['AP', 'APE']
    elif broker == 'BOAML':
        includes = ['ValuationsandBalancesExtractIntegrated_RGxA_Zentific_BlueHarbour_Map_I_LP_RG',date.strftime('%Y%m%d')]
        excludes = []
    elif broker == 'UBS':
        includes = ['CashBalancesFlat.KH.GRPZENTI']
        excludes = []
    elif broker == 'JPM':
        includes = ['Blue_Position_and_PL']
        excludes = []
    elif broker == 'MS':
        includes = ['ZIM-MAC002TDX-NormalizedTradeDateActivityExtra-CAED-Daily']
        excludes = []
    file = ou.filter_files(filepath, includes, excludes)
    if file is not None:
        if verbose:
            logger.info('read non-USD balance for {} from report {} in {}'.format(broker, file, filepath))
        return os.path.join(filepath, file)
    else:
        if verbose:
            logger.warning('Cannot find {} report for {} in {}'.format(includes[0], broker, filepath))
        return

def load_broker_USD_balance(broker, date, ops_param):
    USD = None
    filepath = get_broker_USD_balance_report_path(broker, date, ops_param)

    if broker=='GS':
        df = pd.read_excel(filepath, sheetname='Settle Date Summary')
        USD = df[df['Currency']=='U S DOLLAR'].loc[:,'Ending Balance'].iloc[0]

    elif broker=='BOAML':
        df = pd.read_csv(filepath)
        USD = df[df['ISO Currency Code']=='USD'].loc[:,'SD Cash Reporting CCY'].sum()

    elif broker=='UBS':
        df = pd.read_csv(filepath)
        USD = df[df['Account Name']=='BLUEHARBOUR MAP I LP-ZENTIFIC' & df['CCY']=='USD'].loc[:,'SD Cash Balance (Base)'].sum()


    elif broker=='JPM':
        pass

    elif broker=='MS':
        pass

    return USD


def load_broker_nonUSD_balance(broker, date, ops_param):
    currencies = ['HKD', 'SGD', 'IDR', 'THB', 'NOK']
    non_USD = dict.fromkeys(currencies, 0)
    filepath = get_broker_USD_balance_report_path(broker, date, ops_param)
    if broker=='GS':
        df = pd.read_excel(filepath, sheetname='Trade Date Summary')
        ISO_map = {'SINGAPORE DOLLAR':'SGD',
                   'HONG KONG DOLLAR':'HKD'}
        for currency in set(df['Currency']):
            if currency not in ISO_map:
                Logger.error('Found unidentified currency in {}'.format(filepath))
                sys.exit(0)
            non_USD[ISO_map[currency]] = df[df['Currency']==currency].loc[:,'Ending Balance'].iloc[0]

    elif broker=='BOAML':
        df = pd.read_csv(filepath)
        for currency in set(df['ISO Currency Code']):
            non_USD[currency] = df[df['ISO Currency Code']==currency].loc[:,'SD Cash Reporting CCY'].sum()

    elif broker=='UBS':
        df = pd.read_csv(filepath)
        for currency in set(df['CCY']):
            non_USD[currency] = df[df['ISO Currency Code'] == currency].loc[:, 'SD Cash Reporting CCY'].sum()
        pass

    elif broker=='JPM':
        pass

    elif broker=='MS':
        pass
    print(non_USD)
    pass


def main(argv):
    global logger
    logger = ou.Logger('INFO',log_file)
    # column_headers = {
    #     'GS': list(pd.read_excel(os.path.join(template_path, ou.filter_files(template_path, includes=['GS'])[0]),skiprows=7).columns),
    #     'BOAML': list(pd.read_excel(os.path.join(template_path, ou.filter_files(template_path, includes=['BOAML'])[0]),skiprows=[0, 2]).columns),
    #     'UBS': list(pd.read_csv(os.path.join(template_path, ou.filter_files(template_path, includes=['UBS'])[0])).columns),
    #     'JPM': list(pd.read_csv(os.path.join(template_path, ou.filter_files(template_path, includes=['JPM'])[0])).columns),
    #     'MS': list(pd.read_csv(os.path.join(template_path, ou.filter_files(template_path, includes=['MS'])[0]),skiprows=1).columns)
    # }

    ops_param = ou.get_ops_param('ZEN_SEA')
    print ("called with " + str(len(argv)) + " paramenters " + argv[0])
    if (len(argv) > 0) and (type(ou.text2date(argv[0])) == datetime.date) :
        print ("paramenters 0 " + argv[0])
        #input date from v2 is T-1
        date = ou.text2date(argv[0])
    if (len(argv) > 1)  and argv[1] in ['GS','BOAML','UBS','JPM','MS','ALL'] :
        print ("paramenters 1 " + argv[1])
        broker = argv[1]

    load_broker_USD_balance(broker,date,ops_param)
    load_broker_nonUSD_balance(broker, date, ops_param)



if (__name__ == '__main__') :
   main(sys.argv[1:])
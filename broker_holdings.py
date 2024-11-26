import activate_env
import sys
import os
import pandas as pd
import numpy as np
import datetime
from zipfile import ZipFile

# Load in zim scripts
code_path = "//zimnashk/sd.zentific-im.com$/Operations/Workflow/ZIM_ops"
workflow_path = "//zimnashk/sd.zentific-im.com$/Operations/Workflow/"
os.chdir(code_path)
import ops_utils as ou

mode = "no DEBUG"
holdings_headings = ['h_ref',
                     'z_sedol',
                     'z_bb_code',
                     'z_bb_code_short',
                     'date',
                     'sedol',
                     'ric',
                     'bb_code',
                     'isin',
                     'name',
                     'type',
                     'on_swap',
                     'quantity',
                     'net_price_local',
                     'net_price_usd',
                     'net_notional_local',
                     'net_notional_usd',
                     'fx_rate',
                     'ref_spread',
                     'ref_index',
                     'ref_rate',
                     'accrued_interest_usd',
                     'curr_swap',
                     'curr_underlying',
                     'accrued_div_usd',
                     'div_pct',
                     'src',
                     'prime',
                     'src_ref',
                     'src_file']

# manual overrides for assets
ticker_map_manual = pd.DataFrame([
    ['CSI 500 NTR', '_SWAP_CSIN0905', 'CSIN0905 Index'],
    ['CSI 500 INDEX NET RETURN INDEX CNY', '_SWAP_CSIN0905', 'CSIN0905 Index'],
    ['(GS) GOLDMAN SACHS GROUP INC.* (THE) MARKET INDEX', '_SWAP_CSIN0905', 'CSIN0905 Index'],
    ['CSI 500 NTR CSI 500 NTR', '_SWAP_CSIN0905', 'CSIN0905 Index'],
    ['CSI 300 NTR CSI 300 NTR', '_SWAP_CSIN0300', 'CSIN0300 Index'],
    # Delisted ticker: 6452 TT , 933 HK
    ['PHARMALLY INTERNATIONAL HOLDING COMPANY LIMITED CMN', 'BVTRKC', '6452 TT'],  # GS
    ['PHARMALLY INTERNATIONAL HOLD COM STK TWD 10', 'BVTRKC', '6452 TT'],  # UBS
    ['BRIGHTOIL PETROLEUM (HOLDINGS)* LTD CMN', '635207', '933 HK'],
    ['TRINA SOLAR CO LTD 0.3% BDS 13/08/27 CNY1000', 'BQ9798_', 'BQ9798936 Corp'],
    ['CHINA GEZHOUBA GROUP CO LTD COM STK CNY 1', '637721', '600068 CH'],
    ['CHENNAI SUPER KINGS CRICKET LTD UNLISTED PRODUCT-INE852S01026', 'BYM8T4', '1319044D IN'],
    ['WTS/EZION HOLDINGS LIMITED 0.2487 EXP04/16/2023', 'BFM4WX', 'EZIWSH SP'],
    ['RTS/JMT NETWORK SERVICES PCL  EXP12/20/2021 OTC RIGHT', 'BGHGSV_R', '1923817D TB'],
    ['CSI 300 NET RETURN INDEX CNY', '_SWAP_CSIN0300', 'CSIN0300 Index'],
    ['RTS/VGI PUBLIC COMPANY LIMITED  EXP03/29/2022', 'BKC5FV8_R', '1985451D TB'],
    ['ETERNAL ENERGY PCL NON RENOUNCEABLE RIGH', 'B1FZD8-R', '2001395D TB'],
    ['SJM HOLDINGS LIMITED NPV SUBS', 'B2NR3Y_R', '2123951D HK'],
    ['MERGED CO.-F', 'BYM8TY_ACQ', '1971804D TB'],
    ['AGMO HOLDINGS BHD CADUMMY X/D 17FEB2023', 'BNNTYX', 'AGMO MK'],
    ['RTS/KELSIAN GROUP LIMITED EXP04/05/2023', 'BPLGS9', 'KLSAN AU'],
    ['ESR-LOGOS REIT CADUMMY X/D 06JUN2079','B18TLR','2235734D SP'],
    ['RTS/ESR-REIT  EXP04/20/2023','B18TLR','2235734D SP'],
    ['E.SUN FINANCIAL HOLDING NON RENOUNCEABLE','643391','2230749D TT'],
    ['KIATNAKIN PHATRA BK-W5-R NEW','649108_R5','KKP-R5 TB Equity'],
    ['KIATNAKIN PHATRA BK-W6-NVDR','649108_R6','KKP-R6 TB Equity'],
    ['KIATNAKIN PHATRA BK-W5','BMXDK5','KKP-W5 TB Equity'],
    ['KIATNAKIN PHATRA BK-W6','BMXDK6','KKP-W6 TB Equity'],
    ['ZHE JIANG LI ZI YUAN FOOD -A  NON RENOUN','BMD0ZN','715337 CH Equity'],
    ['AU99999PBHO5POINTSBET HOLDINGS LIMITED','_BJYJ84','PBHAL AU'],
    ['RTS/CAPITALAND INDIA TRUST  EXP07/10/2023','B23DMQ_S','2282781D SP Equity'],
    ['CAPITALAND INDIA TRUST NON-RENOUNCEABLE','BQ7YLJ','2296532D SP Equity'],
    ['PEGAVISION NON-RENOUNCEABLE RIGHTS 17JUL','BS65ZK_R','2289846D TT'],
    ['CSI 500 Net Total Return Index CSI 500 Net', '_SWAP_CSIN0905', '.CSIN905 CH'],
    ['BANGKOK CHAIN HOSPITAL PCL COM STK THB 1','B7Z071','BCH/F TB'],
    ['PTT PCL COM STK THB 1','BD0BDH','PTT/F TB'],
    ['PTT GLOBAL CHEMICAL PCL COM STK THB 10','B67QFW','PTTGC/F TB'],
    ['EXOTIC FOOD PCL COM STK THB .5','BPSWBP','XO/F TB'],
    ['PHILIPPINES-860800 NET RETURN INDEX USD','_SWAP_EM_PH','.NDEUPHF PM'],
    ['CSIN00300 CSIN00300','_SWAP_CSIN0300','.CSIN300 CH'],
    ['RTS/MMG LIMITED  EXP07/05/2024 OTC RIGHTS','672879_r','2451062D HK'],
    ['WINBOND ELECTRONICS CORP NON RENOUNCEABL','696651_rr','2454135D TT'],
    ['VS INDUSTRY BHD-CW26','611342_r','2455476D MK'],
    ['V.S.  RIGHT 05SEP26','611342_r','2455476D MK'],
    ['VS INDUSTRY BHD WTS 05/09/26 MYR1.13','611342_r','2455476D MK']    
], columns=['name', 'sedol', 'bb_code']).set_index('name')

# manual filter out unrecognized ISIN from brokers holdings
isin_manual_clear = ['AU0000354565', 'KRA0054201D5', 'AU0000306110','AU0000306904','SGXN47488501','AU0000328130','TH00000RBTS0','MYL6963WC695','SGXZ92105022','SGXZ63876288','HK0001076071','AU0000365181']

# get GS files
# these are  all on swap..missing the physicals which are in the "custody and cont" file
def process_gs_holdings(path=workflow_path + '/Holdings/20211214/GS', this_date=datetime.date(2021, 12, 14),
                        ops_param={}):
    india_ssf_map = ou.exec_sql("select * from zimdb_ops..india_ssf_multiplier").set_index('ric_code')   
    index_map = ou.exec_sql("select replace(bb_code, ' Index', '') as bb_code, replace(bb_code_long, ' Equity', '') as bb_code_z from zimdb..universe_backfill where sedol like '_SWAP%'")
    index_map = index_map.append(pd.DataFrame([['CSIN00905', '.CSIN905 CH']],
                             columns = ['bb_code', 'bb_code_z']))
    index_map = index_map.set_index('bb_code')['bb_code_z']
    # path = 'S:/Operations/Workflow/Holdings/'+ YYYYMMDD + '/GS'
    files = [x for x in os.listdir(path) if (x.find('_DATA_CFD_Positio') > -1) and (x[-4:] == '.dat')]
    gs_keep = pd.DataFrame(columns=holdings_headings)
    for this_file in files:
        gs = pd.read_csv(path + "/" + this_file, sep='|', skiprows=1)
        this_pos = 0
        tmp_header = [x.strip() for x in gs.columns]
        while this_pos < len(gs):
            tmp = pd.DataFrame(np.transpose([[np.nan] for x in holdings_headings]), columns=holdings_headings)
            tmp_line = pd.Series(list(gs.iloc[this_pos]), index=tmp_header)
            # Include all trades even it is without bloomberg code
            if ((type(tmp_line['Underlyer Bloomberg Code']) is str) and (tmp_line['Underlyer Bloomberg Code'] != "")) or ((type(tmp_line['Underlyer RIC']) is str) and ((":NS" in tmp_line['Underlyer RIC']) or (".SZ" in tmp_line['Underlyer RIC']))) or((type(tmp_line['Underlyer RIC']) is str) and (type(tmp_line['Underlyer Bloomberg Code']) == float and np.isnan(tmp_line['Underlyer Bloomberg Code']))):
                tmp['prime'] = 'GS'
                tmp['src'] = 'GS'
                if ops_param['swap_ff'] == True:
                    tmp['on_swap'] = 'SWAP_FF'  # since this file is all swaps
                else:
                    tmp['on_swap'] = 'SWAP'  # since this file is all swaps
                tmp['date'] = this_date
                tmp['name'] = tmp_line['Underlyer Name']
                if ((type(tmp_line['Underlyer RIC']) is str) and (":NS" in tmp_line['Underlyer RIC'])):
                    try:
                        tmp['bb_code'] = india_ssf_map.loc[tmp_line['Underlyer RIC']]['bb_code']
                    except:
                        tmp['bb_code'] = tmp_line['Underlyer RIC']
                else:
                    tmp['bb_code'] = tmp_line['Underlyer Bloomberg Code']
                # check if it is an index and convert name
                if tmp['bb_code'][0] in index_map.index : 
                    tmp['bb_code'] = index_map[tmp['bb_code'][0]]
                tmp['ric'] = tmp_line['Underlyer RIC']
                tmp['sedol'] = str(tmp_line['Underlyer Sedol'])[:6]
                tmp['isin'] = tmp_line['Underlyer ISIN']
                tmp['type'] = "L" if tmp_line['Long/Short'] == "Long" else "S"
                tmp['quantity'] = tmp_line['Traded Quantity']
                tmp['src_ref'] = tmp_line['Contract ID']
                tmp['src_file'] = path + "/" + this_file
                tmp['net_price_usd'] = tmp_line['Initial / Reset Net Price']
                tmp['net_notional_usd'] = tmp_line['Initial/Reset Net Notional']
                tmp['curr_underlying'] = tmp_line['Underlyer Currency']
                tmp['curr_swap'] = tmp_line['Contract CCY']
                tmp['fx_rate'] = tmp_line['FX Contract to Underlyer']
                tmp['ref_index'] = tmp_line['Benchmark']
                tmp['ref_rate'] = tmp_line['Benchmark Rate %'] / 100.0
                tmp['ref_spread'] = tmp_line['Spread L/S (bps)'] / 10000.0 * (-1 if tmp['type'][0] == 'S' else 1)
                tmp['accrued_interest_usd'] = tmp_line['Total Interest Accrued']
                tmp['accrued_div'] = tmp_line['Dividend Accrued']
                tmp['div_pct'] = tmp_line['Dividend %']
                tmp['net_price_local'] = tmp['net_price_usd'] * tmp['fx_rate']
                tmp['net_notional_local'] = tmp['net_notional_usd'] * tmp['fx_rate']
                gs_keep = gs_keep.append(tmp)
            this_pos = this_pos + 1
    files = [x for x in os.listdir(path) if (x.find('_DATA_Custody_Pos') > -1) and (x[-4:] == '.dat')]
    for this_file in files:
        gs = pd.read_csv(path + "/" + this_file, sep='|', skiprows=1)
        this_pos = 0
        tmp_header = [x.strip() for x in gs.columns]
        gs = gs.loc[[x in ['EQ', 'FU'] for x in gs['Product Type']]]
        while this_pos < len(gs):
            tmp = pd.DataFrame(np.transpose([[np.nan] for x in holdings_headings]), columns=holdings_headings)
            tmp_line = pd.Series(list(gs.iloc[this_pos]), index=tmp_header)
            if ('Underlyer Bloomberg Code' not in tmp_header):
                tmp['prime'] = 'GS'
                tmp['src'] = 'GS'
                tmp['on_swap'] = 'PHYSICAL'
                tmp['date'] = this_date
                tmp['name'] = tmp_line['Product Description']
                # tmp['bb_code'] = tmp_line['Underlyer Bloomberg Code']
                tmp['isin'] = tmp_line['Underlying ISIN']
                tmp['isin'] = (tmp_line['ISIN'] if tmp_line['Underlying ISIN'] == ""
                else tmp_line['Underlying ISIN'])
                # tmp['ric'] = tmp_line['Underlyer RIC']
                tmp['sedol'] = (str(tmp_line['Sedol'])[:6] if tmp_line['Underlying Sedol'] == ""
                else str(tmp_line['Underlying Sedol'])[:6])
                # tmp['sedol'] = str(tmp_line['Underlying Sedol'])[:6]
                # tmp['isin'] = tmp_line['Underlying CUSIP']
                tmp['type'] = tmp_line['Long Short Flag']
                tmp['quantity'] = tmp_line['Trade Date Quantity']
                tmp['src_file'] = path + "/" + this_file
                tmp['fx_rate'] = 1 / ou.text2no(tmp_line['Ending Local Base FX Rate'])
                tmp['net_price_usd'] = tmp_line['Ending USD Price']
                tmp['net_price_local'] = tmp_line['Ending Local Price']
                tmp['net_notional_local'] = tmp_line['local market value']
                gs_keep = gs_keep.append(tmp)
            elif (type(tmp_line['Underlyer Bloomberg Code']) is str) and (tmp_line['Underlyer Bloomberg Code'] != ""):
                tmp['prime'] = 'GS'
                tmp['src'] = 'GS'
                tmp['on_swap'] = 'PHYSICAL'
                tmp['date'] = this_date
                tmp['name'] = tmp_line['Product Description']
                tmp['bb_code'] = tmp_line['Bloomberg Code'] if tmp_line['Product Type'] == 'FU' else tmp_line[
                    'Underlyer Bloomberg Code']
                tmp['bb_code'] = str(tmp['bb_code'][0]).replace(" Equity", "")
                tmp['isin'] = tmp_line['Underlyer ISIN']
                # tmp['ric'] = tmp_line['Underlyer RIC']
                tmp['sedol'] = str(tmp_line['Sedol'])[:6] if type(tmp_line['Sedol']) == str else str(
                    tmp_line['Underlyer Report Product ID'])[:6]
                tmp['isin'] = tmp_line['Underlying Cusip']
                tmp['type'] = tmp_line['L/S']
                tmp['quantity'] = tmp_line['TD Qty Close']
                tmp['src_file'] = path + "/" + this_file
                tmp['fx_rate'] = tmp_line['Exposure FX Base Rate']
                tmp['net_price_usd'] = tmp_line['USD Price']
                tmp['net_price_local'] = tmp_line['Price']
                tmp['net_notional_local'] = tmp_line['Initial Notional (Local)']
                gs_keep = gs_keep.append(tmp)
            this_pos = this_pos + 1
    # Check for GS margins files and if there then load to db
    files = [x for x in os.listdir(path) if (x.find('ZENTIFIC_NSEMarginSu') > -1) and (x[-4:] == '.xls')]
    if len(files) > 0:
        for this_file in files:
            wb = ou.read_spreadsheet_xml(path + "/" + this_file)
            this_line = pd.Series([this_date, 'GS', 'INR', np.nan, np.nan],
                                index=['date', 'broker', 'currency', 'cash', 'margin'])
            if type(wb) == list :
                try :
                    tmp = wb[2].copy()
                    tmp.index = list(tmp.iloc[:,0])
                    tmp.columns = list(tmp.iloc[0,:])
                    tmp = tmp.iloc[1:,1:]
                    this_line['cash'] = np.float(tmp.loc['TOTAL','BALANCE AVAILABLE AFTER INTRA TRANSFER'])
                    this_line['margin'] = np.float(tmp.loc['TOTAL', 'END OF DAY BALANCE (AFTER DEDUCTIONS OF MARGIN UTILISED)'])
                except : 
                    pass
            else : 
                if (wb.iloc[17, 4] == 'BALANCE AVAILABLE AFTER INTRA TRANSFER') and (wb.iloc[20, 0] == 'TOTAL'):
                    this_line['cash'] = wb.iloc[20, 4]
                if (wb.iloc[17, 7] == 'END OF DAY BALANCE (AFTER DEDUCTIONS OF MARGIN UTILISED)') and (
                        wb.iloc[20, 0] == 'TOTAL'):
                    this_line['margin'] = wb.iloc[20, 7]
            if this_line['cash'] != np.nan:
                sqlcmd = "delete from [" + ops_param['db'] + "]..[" + ops_param[
                    'broker_margin_table'] + "] where [date] = '" + str(this_date) + "'"
                ou.exec_sql(sqlcmd, isselect=0)
                ou.load_to_table(df=pd.DataFrame(this_line).transpose(), table=ops_param['broker_margin_table'],
                                db='zimdb_ops')
    return gs_keep[holdings_headings]


# get cs files
# cs swap
def process_cs_holdings(path=workflow_path + '/Holdings/20170403/CS/', this_date=datetime.date(2017, 4, 3),
                        ops_param={}):
    if ops_param != {}:
        tmp = ops_param['account']
    else:
        tmp = 'ZEN_MST'
    cs_accounts = {'ZEN_MST': ['ZENTIFIC ASIA OPP OFFSHRE MSTR / 0VGL', 'ZENTIFIC AC ASIA OPPORTUNITIES OFFS YZRYB0',
                               'ZENTIFIC ASIA OPP OFFSHRE MSTR / 0DQW'],
                   'ZEN_PCO': ['PCH MANAGER FUND SP 205 / 7PYK80', 'ZENTIFIC AC PCH MANAGER SPC 205 OTC Y0Y4R0'],
                   'ZEN_CNA': ['ZENTIFIC CHINA A INV CO LTD / 0DUO', 'ZENTIFIC AC CHINA A INVESTMENT COMP Y3ND00'],
                   'ZEN_PCN': ['PCH FUND SP 205 - CONNECT / 7PA4R0', 'ZENTIFIC AC SEGREGATED PORTFOLIO 20 Y3R210'],
                   'ZEN_NHL': ['UNKNOWN'],
                   'ZEN_NHT': ['UNKNOWN']
                   }[tmp]
    files = [x for x in os.listdir(path) if (x.find('MTM-ByPosition_') > -1) and (x[-4:] == '.csv')]
    cs_keep = pd.DataFrame(columns=holdings_headings)
    for this_file in files:
        cs = pd.read_csv(path + "/" + this_file)
        this_pos = 0
        tmp_header = cs.columns
        while this_pos < len(cs):
            tmp = pd.DataFrame(np.transpose([[np.nan] for x in holdings_headings]), columns=holdings_headings)
            tmp_line = pd.Series(list(cs.iloc[this_pos]), index=tmp_header)
            if (type(tmp_line['Entity']) is str) and (tmp_line['Entity'] != ""):
                tmp['prime'] = 'CS'
                tmp['src'] = 'CS'
                tmp['on_swap'] = 'SWAP'  # since this file is all swaps
                tmp['date'] = this_date
                tmp['name'] = tmp_line['Description']
                tmp['sedol'] = str(tmp_line['SEDOL'])[:6]
                tmp['isin'] = tmp_line['ISIN']
                tmp['type'] = "L" if tmp_line['Long / Short'].lower() == "long" else "S"
                tmp['quantity'] = tmp_line['Quantity']
                tmp['src_ref'] = tmp_line['Swap Number']
                tmp['src_file'] = path + "/" + this_file
                tmp['net_price_local_usd'] = tmp_line['Cost Price']
                tmp['net_notional_local'] = abs(tmp_line['Cost Notional'])
                tmp['net_notional_usd'] = abs(tmp_line['Implied Cost Notional [USD]'])
                tmp['curr_underlying'] = tmp_line['Local CCY']
                tmp['curr_swap'] = tmp_line['Swap Pay CCY']
                tmp['fx_rate'] = tmp_line['Cost FX']
                tmp['ref_index'] = tmp_line['Reference Rate']
                tmp['ref_rate'] = tmp_line['All In Base Rate'] / 100.0
                tmp['ref_spread'] = tmp_line['Spread BPS'] / 10000.0 * (-1 if tmp['type'][0] == 'S' else 1)
                tmp['accrued_interest_usd'] = tmp_line['MTM Unpaid Interest']
                tmp['accrued_div'] = tmp_line['MTM Unpaid Dividends / Coupons']
                tmp['div_pct'] = tmp_line['Dividend / Coupon %']
                if tmp['fx_rate'][0] > 0:
                    if (tmp['curr_underlying'][0] == 'AUD'):
                        tmp['fx_rate'] = 1 / tmp['fx_rate'][0]
                    tmp['net_price_local'] = tmp_line['Cost Price'] * tmp['fx_rate']
                if tmp_line['Account Name / Account'] in cs_accounts:
                    cs_keep = cs_keep.append(tmp)
            this_pos = this_pos + 1
    files = [x for x in os.listdir(path) if (x.find('TradeDatePositions_') > -1) and (x[-4:] == '.csv')]
    for this_file in files:
        cs = pd.read_csv(path + "/" + this_file)
        this_pos = 0
        tmp_header = cs.columns
        while this_pos < len(cs):
            tmp = pd.DataFrame(np.transpose([[np.nan] for x in holdings_headings]), columns=holdings_headings)
            tmp_line = pd.Series(list(cs.iloc[this_pos]), index=tmp_header)
            if (type(tmp_line['Account Name / Account']) is str) and (tmp_line['Account Name / Account'] != ""):
                tmp['prime'] = 'CS'
                tmp['src'] = 'CS'
                tmp['on_swap'] = 'PHYSICAL'  # since this file is all swaps
                tmp['date'] = this_date
                tmp['name'] = tmp_line['Description']
                try:
                    tmp['sedol'] = str(tmp_line['Sedol'])[:6]
                except:
                    tmp['sedol'] = str(tmp_line['Security ID'])
                tmp['quantity'] = tmp_line['Trade Date Quantity']
                tmp['type'] = "L" if tmp['quantity'][0] > 0 else "S"
                tmp['src_file'] = path + "/" + this_file
                tmp['net_price_local'] = tmp_line['Price']
                tmp['net_price_usd'] = tmp_line['Price (USD)']
                tmp['net_notional_local'] = abs(tmp_line['Trade Date Market Value'])
                tmp['net_notional_usd'] = abs(tmp_line['Trade Date Market Value (USD)'])
                tmp['curr_underlying'] = tmp_line['CCY']
                tmp['fx_rate'] = tmp_line['Exchange Rate']
                if (len(tmp['sedol'][0]) < 6) and (tmp['curr_underlying'][0] == 'JPY'):
                    tmp['bb_code'] = tmp['sedol'][0] + " JP"
                    tmp['sedol'] = np.nan
                if tmp_line['Account Name / Account'] in cs_accounts:
                    cs_keep = cs_keep.append(tmp)
            this_pos = this_pos + 1
    return cs_keep[holdings_headings]


# get baml files
# this gives physicals and swaps but not the reference rates
def process_boaml_holdings(path='//zimnashk/sd.zentific-im.com$/Operations/Workflow/zen_nht/Holdings/20231026/BOAML',
                           this_date=datetime.date(2023,10, 26), ops_param={}):
    india_ssf_map = ou.exec_sql("select distinct bb_code, multiplier from zimdb_ops..india_ssf_multiplier").set_index(
        'bb_code')
        
    if ops_param != {}:
        tmp = ops_param['account']
    else:
        tmp = 'ZEN_MST'
    boaml_accounts = {'ZEN_MST': ['ZEN_ASIAOPS'],
                    'ZEN_PCO': ['Unknown'],
                    'ZEN_CNA': ['Unknown'],
                    'ZEN_PCN': ['Unknown'],
                    'ZEN_CNL': ['ZEN_S31FF'],
                    'ZEN_CNG': ['ZEN_S41'],
                    'ZEN_NHL': ['405MSTV_ZEN'],
                    'ZEN_NHT': ['NTHAFD_ZEN'],
                    'ZEN_SEA':['BHARBZENTIFIC_MLI']
                    }[tmp]
    try:
        files = [x for x in os.listdir(path) if ((x.find('ValuationsandBalancesExtractIntegrated_') > -1) or (
                x.find('ValuationsandBalancesExtractIntegrated_') > -1)) and (x[-4:] == '.csv')]
    except:
        files = []
    boaml_keep = pd.DataFrame(columns=holdings_headings)
    for this_file in files:
        boaml = pd.read_csv(path + "/" + this_file)
        this_pos = 0
        tmp_header = boaml.columns
        while this_pos < len(boaml):
            tmp = pd.DataFrame(np.transpose([[np.nan] for x in holdings_headings]), columns=holdings_headings)
            tmp_line = pd.Series(list(boaml.iloc[this_pos]), index=tmp_header)
            if (tmp_line['Asset Class'] == 'Equities') and (tmp_line['Position Type'] == 'PHYSICAL'):
                tmp['prime'] = 'BOAML'
                tmp['src'] = 'BOAML'
                tmp['name'] = tmp_line['Product Full Description']
                tmp['on_swap'] = "PHYSICAL"
                tmp['date'] = this_date
                tmp['sedol'] = str(tmp_line['SEDOL'])[:6]
                tmp['sedol'] = np.nan if tmp['sedol'][0] == 'nan' else tmp['sedol'][0]
                tmp['isin'] = tmp_line['ISIN']
                tmp['bb_code'] = tmp_line['Bloomberg Ticker']
                tmp['type'] = "L" if tmp_line['Long/Short Indicator'].lower() == "l" else "S"
                tmp['quantity'] = tmp_line['TD Quantity']
                tmp['src_file'] = path + "/" + this_file
                tmp['net_price_local'] = tmp_line['Local CCY Clean Price']
                tmp['net_notional_local'] = tmp_line['TD Net MV Local CCY']
                tmp['net_price_usd'] = tmp_line['Base Price']
                tmp['net_notional_usd'] = tmp_line['TD Net MV Base CCY']
                tmp['curr_underlying'] = tmp_line['Pricing Currency']
                if tmp_line['Base Price'] > 0:
                    tmp['fx_rate'] = tmp_line['Local CCY Clean Price'] / tmp_line['Base Price']
                boaml_keep = boaml_keep.append(tmp)
            elif tmp_line['Asset Class'] == 'Currency':
                pass
            this_pos = this_pos + 1
    # read in borrow costs
    def agg_holdings(holdings):
        result = holdings.iloc[0, :].copy()
        if len(holdings) > 1:
            avg_cols = ['Interest Rate', 'financing']
            scale = list(holdings['Position']) / np.sum(np.abs(list(holdings['Position'])))
            result[avg_cols] = holdings[avg_cols].apply(
                func=lambda x: np.sum([y * z for y, z in zip(list(x), scale)]), axis=0)
            sum_cols = ['Position']
            result[sum_cols] = holdings[sum_cols].apply(func=lambda x: np.sum(list(x)), axis=0)
        return result
    try:
        files = [x for x in os.listdir(path) if (('SwapValDetailPosInterest' in x) or ('SwapValuationExtract_RGxA' in x)) and (x[-4:] == '.csv')]
    except:
        files = []
    boaml_rates = pd.DataFrame()
    if len([x for x in files if ('SwapValDetailPosInterest' in x)]) > 0 :
        this_file = [x for x in files if ('SwapValDetailPosInterest' in x)][0]
        boaml = pd.read_csv(path + "/" + this_file)
        # on reset days BOAML don't provide the spread costs so need to use file from yesterday        
        if len(boaml) < 1:
            tmp = os.listdir(path + "/../..")
            tmp = [x for x in tmp if x.isdigit() and (x < this_date.strftime("%Y%m%d"))]
            tmp.sort()
            last_date = tmp[-1]
            files = [x for x in os.listdir(path + "/../../" + last_date + "/BOAML/") if
                     (x.find('SwapValDetailPosInterest') > -1) and (x[-4:] == '.csv')]
            if len(files) > 0:
                this_file = files[0]
                boaml = pd.read_csv(path + "/../../" + last_date + "/BOAML/" + this_file)
        boaml.columns = [x.strip() for x in boaml.columns]
        boaml['financing'] = boaml['Long Financing Spread'] - boaml['Short Financing Spread']
        boaml['type'] = ['L' if x >= 0 else 'S' for x in boaml['Position']]
        boaml['Position'] = boaml['Position'].abs()
        boaml = boaml[['Underlying Sedol', 'Primary RIC', 'Underlying Internal ID', 'Position', 'type', 'Interest Rate',
                       'financing']]
        # if missing primary ric use underlying internal id
        boaml['Primary RIC'] = [x if type(x) == str else y for x, y in
                                zip(boaml['Primary RIC'], boaml['Underlying Internal ID'])]
        # replace missing sedols with rics
        filt = [type(x) != str and np.isnan(x) for x in boaml['Underlying Sedol']]
        boaml.loc[filt, 'Underlying Sedol'] = boaml.loc[filt, 'Primary RIC']

        if len(boaml) > 0:
            boaml = boaml.groupby(['Underlying Sedol', 'type']).apply(agg_holdings).reset_index(drop=True)
            boaml['sedol'] = [str(x)[:6] if len(str(x)) == 7 else x for x in boaml['Underlying Sedol']]
            boaml = boaml.set_index(['sedol', 'type'])
            boaml_rates = boaml_rates.append(boaml)
    elif len([x for x in files if ('SwapValuationExtract_RGxA' in x)]) > 0 : 
        this_file = [x for x in files if ('SwapValuationExtract_RGxA' in x)][0]
        boaml = pd.read_csv(path + "/" + this_file)
        # on reset days BOAML don't provide the spread costs so need to use file from yesterday        
        if len(boaml) < 1:
            tmp = os.listdir(path + "/../..")
            tmp = [x for x in tmp if x.isdigit() and (x < this_date.strftime("%Y%m%d"))]
            tmp.sort()
            last_date = tmp[-1]
            files = [x for x in os.listdir(path + "/../../" + last_date + "/BOAML/") if
                     (x.find('SwapValuationExtract_RGxA') > -1) and (x[-4:] == '.csv')]
            if len(files) > 0:
                this_file = files[0]
                boaml = pd.read_csv(path + "/../../" + last_date + "/BOAML/" + this_file)
        boaml.columns = [x.strip() for x in boaml.columns]
        # filter for account
        boaml = boaml.loc[boaml['ACCOUNT ID'] == boaml_accounts[0]]
        boaml['financing'] = boaml['LONG FINANCING SPREAD'] - boaml['SHORT FINANCING SPREAD']
        # filter for holding
        filt = [np.isnan(x) == False and np.isnan(y) == False for x,y in zip(boaml['QUANTITY'], boaml['financing'])]
        boaml = boaml.loc[filt]
        boaml['type'] = ['L' if x >= 0 else 'S' for x in boaml['QUANTITY']]
        boaml['Position'] = boaml['QUANTITY'].abs()
        boaml = boaml[['UNDERLYING SEDOL', 'PRIMARY RIC CODE', 'UNDERLYING INTERNAL ID', 'Position', 'type', 'BASE RATE',
                       'financing']]
        # if missing primary ric use underlying internal id
        boaml['Interest Rate'] = boaml['BASE RATE']
        boaml['Primary RIC'] = [x if type(x) == str else y for x, y in
                                zip(boaml['PRIMARY RIC CODE'], boaml['UNDERLYING INTERNAL ID'])]
        # replace missing sedols with rics
        boaml['sedol'] = boaml['UNDERLYING SEDOL']
        filt = [type(x) != str and np.isnan(x) for x in boaml['sedol']]
        boaml.loc[filt, 'sedol'] = boaml.loc[filt, 'Primary RIC']

        if len(boaml) > 0:
            boaml = boaml.groupby(['sedol', 'type']).apply(agg_holdings).reset_index(drop=True)
            boaml['sedol'] = [str(x)[:6] if len(str(x)) == 7 else x for x in boaml['sedol']]
            boaml = boaml.set_index(['sedol', 'type'])
            boaml_rates = boaml_rates.append(boaml)
        
    # read in swap positions
    try:
        files = [x for x in os.listdir(path) if (x.find('SwapValDetailPositionMTM') > -1) and (x[-4:] == '.csv')]
    except:
        files = []
    for this_file in files:
        boaml = pd.read_csv(path + "/" + this_file)
        this_pos = 0
        tmp_header = boaml.columns
        acct_name_last = ''
        while this_pos < len(boaml):
            tmp = pd.DataFrame(np.transpose([[np.nan] for x in holdings_headings]), columns=holdings_headings)
            tmp_line = pd.Series(list(boaml.iloc[this_pos]), index=tmp_header)
            if (type(tmp_line['Client']) is str) and (tmp_line['Client'] != ""):
                acct_name_last = tmp_line['Client'].strip()
                tmp['prime'] = 'BOAML'
                tmp['src'] = 'BOAML'
                tmp['name'] = tmp_line['Underlying - Description']
                if (ops_param['swap_ff']) & (tmp_line['Product Type'] != 'Equity Index'):
                    tmp['on_swap'] = "SWAP_FF"
                else:
                    tmp['on_swap'] = "SWAP"
                tmp['date'] = this_date
                tmp['sedol'] = str(tmp_line['Underlying Sedol'])[:6]
                tmp['sedol'] = np.nan if tmp['sedol'][0] == 'nan' else tmp['sedol'][0]
                tmp['isin'] = tmp_line['Underlying ISIN']
                tmp['bb_code'] = str(tmp_line['BB Ticker']).replace(' Index', '')
                tmp['ric'] = tmp_line['Primary Ric Code']
                tmp['quantity'] = tmp_line['Position -  Quantity']
                tmp['type'] = "L" if tmp['quantity'][0] >= 0 else "S"
                tmp['src_file'] = path + "/" + this_file
                tmp['net_price_local'] = tmp_line['Position ? Loc Price']
                tmp['net_price_usd'] = tmp_line['Position - Cost Basis']
                tmp['net_notional_local'] = tmp['net_price_local'] * tmp['quantity']
                tmp['net_notional_usd'] = tmp['net_price_usd'] * tmp['quantity']
                tmp['curr_underlying'] = tmp_line['Underlying - CCY']
                tmp['curr_swap'] = tmp_line['Swap Currency']
                tmp['src_ref'] = tmp_line['Swap Admin No']
                if tmp['net_price_usd'][0] > 0:
                    tmp['fx_rate'] = tmp['net_price_local'] / tmp['net_price_usd']
                    # load up the updated spread if it exists
                if (tmp['sedol'][0], tmp['type'][0]) in list(boaml_rates.index):
                    tmp['ref_spread'] = boaml_rates.loc[(tmp['sedol'][0], tmp['type'][0]), 'financing'] / 100.0
                elif tmp['ric'][0] in boaml_rates.index:
                    tmp['ref_spread'] = boaml_rates.loc[(tmp['ric'][0], tmp['type'][0]), 'financing'] / 100.0
                elif boaml_rates.index.isin([(tmp_line['Underlying Internal ID'], tmp['type'][0])]).any():
                    tmp['ref_spread'] = boaml_rates.loc[
                                            (tmp_line['Underlying Internal ID'], tmp['type'][0]), 'financing'] / 100.0
                else:
                    tmp['ref_spread'] = (0 if type(tmp_line['Spread']) == str else tmp_line['Spread']) / 100.0
                tmp['ref_spread'] = 0 if np.isnan(tmp['ref_spread'][0]) else tmp['ref_spread'][0]
                try:
                    if datetime.datetime.strptime(str(tmp_line['Maturity Date']), '%m/%d/%Y').date() - this_date <= datetime.timedelta(days=7):
                        tmp['ric'] = 'Expiring'
                except:
                    pass
                if (tmp['quantity'][0] != 0) and (acct_name_last in boaml_accounts):
                    boaml_keep = boaml_keep.append(tmp)
            this_pos = this_pos + 1
    # boaml indian future
    try:
        files = [x for x in os.listdir(path) if (x.find('Position_110125') > -1) and (x[-4:] == '.csv')]
    except:
        files = []
    for this_file in files:
        boaml = pd.read_csv(path + "/" + this_file)
        this_pos = 0
        tmp_header = boaml.columns
        acct_name_last = ''
        while this_pos < len(boaml):
            tmp = pd.DataFrame(np.transpose([[np.nan] for x in holdings_headings]), columns=holdings_headings)
            tmp_line = pd.Series(list(boaml.iloc[this_pos]), index=tmp_header)
            if (type(tmp_line['Client']) == str) and (tmp_line['Client'] != ''):
                acct_name_last = tmp_line['Client'].strip()
            if (tmp_line['Bloomberg Code'] != "") and (type(tmp_line['Bloomberg Code']) is str):
                tmp['prime'] = 'BOAML'
                tmp['src'] = 'BOAML'
                tmp['name'] = tmp_line['Scrip Name']
                tmp['on_swap'] = "PHYSICAL"
                tmp['date'] = this_date
                tmp['bb_code'] = str(tmp_line['Bloomberg Code']).replace(' EQUITY', '')
                tmp['quantity'] = tmp_line['Closing Bal. Long/Short (-) (Qty)'] / \
                                  india_ssf_map.loc[tmp['bb_code']]['multiplier'][0]  # divided by multiplier
                tmp['type'] = "L" if tmp['quantity'][0] >= 0 else "S"
                tmp['src_file'] = path + "/" + this_file
                tmp['net_price_local'] = tmp_line['Sett. Price (INR)']
                tmp['net_notional_local'] = tmp_line['MTM Value (INR)']
                if (tmp['quantity'][0] != 0) and (acct_name_last in boaml_accounts):
                    boaml_keep = boaml_keep.append(tmp)
            this_pos = this_pos + 1
    return boaml_keep[holdings_headings]


# get baml files
# this gives physicals and swaps but not the reference rates
def process_ubs_holdings(path='//zimnashk/sd.zentific-im.com$/Operations/Workflow/Holdings/20211004/UBS',
                         this_date=datetime.date(2021, 10, 4), ops_param={}):
    if ops_param != {}:
        tmp = ops_param['account']
    else:
        tmp = 'ZEN_MST'
    ubs_accounts = {'ZEN_MST': ['Zentific Asia Opp Offshore Mst'],
                    'ZEN_PCO': ['PCH Manager Fd SPC-Seg Port205'],
                    'ZEN_CNA': ['ZENTIFIC CHINA INVEST CO LTD', 'ZENTIFIC CHINA A INVEST CO LTD'],
                    'ZEN_PCN': ['PCH China A Strategy'],
                    'ZEN_CNG': ['ZENTIFIC CHINA OPP OFFSHORE FD'],
                    'ZEN_NHL': ['UNKNOWN'],
                    'ZEN_NHT': ['New Holland TAF - Zentific PRT'],
                    'ZEN_SEA':['BLUEHARBOUR MAP I LP-ZENTIFIC']
                    }[tmp]
    ubs_physical_acct_id = '75208584'
    # read in borrow files
    try:
        files = [x for x in os.listdir(path) if
                 (x.find('PRTOpenTranReport-BloombergSBL') > -1) and (x[-4:].lower() == '.csv')]
    except:
        files = []
    ubs_rates = pd.DataFrame(columns=holdings_headings)
    for this_file in files:
        data_in = pd.read_csv(path + "/" + this_file)
        this_pos = 0
        tmp_header = data_in.columns
        acct_name_last = ''
        swap_id_last = ''
        while this_pos < len(data_in):
            tmp = pd.DataFrame(np.transpose([[np.nan] for x in holdings_headings]), columns=holdings_headings)
            tmp_line = pd.Series(list(data_in.iloc[this_pos]), index=tmp_header)
            if (type(tmp_line['Account Name']) == str) and (tmp_line['Account Name'] != ''):
                acct_name_last = tmp_line['Account Name'].strip()
            if (type(tmp_line['Swap ID']) == str) and (tmp_line['Swap ID'] != ''):
                swap_id_last = tmp_line['Swap ID']
            if ((type(tmp_line['BB Ticker']) == str) and (tmp_line['BB Ticker'] != '')) or (
                    (type(tmp_line['Bloomberg Ticker']) == str) and (tmp_line['Bloomberg Ticker'] != '')) or \
                    ((type(tmp_line['Security Description']) == str) and (tmp_line[
                                                                              'Security Description'] != '')):  # Adrian 20211005: should show blank security description missing tab
                tmp['prime'] = 'UBS'
                tmp['src'] = 'UBS'
                tmp['name'] = tmp_line['Security Description']
                tmp['on_swap'] = "SWAP"
                tmp['date'] = this_date
                tmp['sedol'] = str(tmp_line['SEDOL'])[:6]
                tmp['bb_code'] = tmp_line['BB Ticker'] if type(tmp_line['BB Ticker']) == str else (
                    tmp_line['Bloomberg Ticker'] + ' IS' if type(tmp_line['Bloomberg Ticker']) == str else np.nan)
                tmp['quantity'] = np.abs(ou.text2no(tmp_line['Open Quantity']))
                tmp['type'] = "S" if tmp_line['Open Quantity'][0] in ["(", '-'] else "L"
                tmp['quantity'] = (-1 if tmp['type'][0] == 'S' else 1) * tmp['quantity']
                tmp['src_file'] = path + "/" + this_file
                tmp['net_price_usd'] = ou.text2no(tmp_line['Cost Price'])
                tmp['net_notional_usd'] = np.abs(ou.text2no(tmp_line['Market Value']))
                tmp['ref_spread'] = (ou.text2no(tmp_line['Interest Spread']) / 100.0) - ou.text2no(
                    tmp_line['Borrow Rate']) / 100.0
                if acct_name_last in ubs_accounts:
                    ubs_rates = ubs_rates.append(tmp)
            this_pos = this_pos + 1

    # now aggregate separate trades
    def agg_holdings(holdings):
        result = holdings.iloc[0, :].copy()
        if len(holdings) > 1:
            avg_cols = ['ref_spread']
            scale = list(np.abs(holdings['quantity'])) / np.sum(np.abs(list(holdings['quantity'])))
            result[avg_cols] = holdings[avg_cols].apply(func=lambda x: np.sum([y * z for y, z in zip(list(x), scale)]),
                                                        axis=0)
            sum_cols = ['quantity']
            result[sum_cols] = holdings[sum_cols].apply(func=lambda x: np.sum(list(x)), axis=0)
        return result

    if len(ubs_rates) > 0:
        ubs_rates = ubs_rates.groupby(['sedol', 'type']).apply(agg_holdings).reset_index(drop=True)
        ubs_rates = ubs_rates.set_index(['sedol', 'type'])
        ubs_rates = ubs_rates['ref_spread']
    else:
        ubs_rates = pd.Series()
        ubs_rates.name = 'ref_spread'
    # read in position files
    try:
        files = [x for x in os.listdir(path) if
                 (x.find('PRTNetOpenPositions_BBG.GRPZENTI') > -1) and (x[-4:].lower() == '.csv')]
    except:
        files = []
    ubs_keep = pd.DataFrame(columns=holdings_headings)
    for this_file in files:
        data_in = pd.read_csv(path + "/" + this_file)
        this_pos = 0
        tmp_header = data_in.columns
        acct_name_last = ''
        swap_id_last = ''
        while this_pos < len(data_in):
            tmp = pd.DataFrame(np.transpose([[np.nan] for x in holdings_headings]), columns=holdings_headings)
            tmp_line = pd.Series(list(data_in.iloc[this_pos]), index=tmp_header)
            if (type(tmp_line['Account Name']) == str) and (tmp_line['Account Name'] != ''):
                acct_name_last = tmp_line['Account Name'].strip()
            if (type(tmp_line['Swap ID']) == str) and (tmp_line['Swap ID'] != ''):
                swap_id_last = tmp_line['Swap ID']
            if tmp_line['Security Description'] != "Security Description" and not (type(tmp_line['Security Description']) == float and np.isnan(tmp_line['Security Description'])):
                tmp['prime'] = 'UBS'
                tmp['src'] = 'UBS'
                tmp['name'] = tmp_line['Security Description']
                tmp['on_swap'] = "SWAP"
                tmp['date'] = this_date
                tmp['sedol'] = str(tmp_line['SEDOL'])[:6]
                tmp['bb_code'] = tmp_line['BB Ticker'] if type(tmp_line['BB Ticker']) == str else (
                    tmp_line['Bloomberg Ticker'] + ' IS' if type(tmp_line['Bloomberg Ticker']) == str else np.nan)
                tmp['quantity'] = np.abs(ou.text2no(str(tmp_line['TD Quantity'])))
                tmp['type'] = "S" if str(tmp_line['TD Quantity'])[0] in ["(", '-'] else "L"
                tmp['quantity'] = (-1 if tmp['type'][0] == 'S' else 1) * tmp['quantity']
                tmp['src_file'] = path + "/" + this_file
                tmp['net_price_usd'] = ou.text2no(tmp_line['Cost Price (Swap)'])
                tmp['net_notional_usd'] = np.abs(ou.text2no(str(tmp_line['Market Value (Swap)'])))
                if (tmp['sedol'][0], tmp['type'][0]) in ubs_rates.index:
                    tmp['ref_spread'] = ubs_rates[(tmp['sedol'][0], tmp['type'][0])]
                if (tmp['quantity'][0] != 0) and (acct_name_last in ubs_accounts):
                    ubs_keep = ubs_keep.append(tmp)
            this_pos = this_pos + 1

    # read in manually downloaded position files
    # try:
    #     files = [x for x in os.listdir(path) if
    #              (x.find('PRTNetOpenPositions_BBG') > -1) and (x[-4:].lower() == '.csv')]
    # except:
    #     files = []
    # ubs_keep = pd.DataFrame(columns=holdings_headings)
    # for this_file in files:
    #     data_in = pd.read_csv(path + "/" + this_file)
    #     this_pos = 0
    #     tmp_header = data_in.columns
    #     acct_name_last = ''
    #     swap_id_last = ''
    #     while this_pos < len(data_in):
    #         tmp = pd.DataFrame(np.transpose([[np.nan] for x in holdings_headings]), columns=holdings_headings)
    #         tmp_line = pd.Series(list(data_in.iloc[this_pos]), index=tmp_header)
    #         if (type(tmp_line['Account Name']) == str) and (tmp_line['Account Name'] != ''):
    #             acct_name_last = tmp_line['Account Name'].strip()
    #         if (type(tmp_line['Swap ID']) == str) and (tmp_line['Swap ID'] != ''):
    #             swap_id_last = tmp_line['Swap ID']
    #         if tmp_line['Security Description'] != "Security Description" and not (type(tmp_line['Security Description']) == float and np.isnan(tmp_line['Security Description'])):
    #             tmp['prime'] = 'UBS'
    #             tmp['src'] = 'UBS'
    #             tmp['name'] = tmp_line['Security Description']
    #             tmp['on_swap'] = "SWAP"
    #             tmp['date'] = this_date
    #             tmp['sedol'] = str(tmp_line['SEDOL'])[:6]
    #             tmp['bb_code'] = tmp_line['BB Ticker'] if type(tmp_line['BB Ticker']) == str else np.nan
    #             tmp['quantity'] = np.abs(ou.text2no(str(tmp_line['TD Quantity'])))
    #             tmp['type'] = "S" if str(tmp_line['TD Quantity'])[0] in ["(", '-'] else "L"
    #             tmp['quantity'] = (-1 if tmp['type'][0] == 'S' else 1) * tmp['quantity']
    #             tmp['src_file'] = path + "/" + this_file
    #             tmp['net_price_usd'] = ou.text2no(tmp_line['Cost Price (Swap)'])
    #             tmp['net_notional_usd'] = np.abs(ou.text2no(str(tmp_line['Market Value (Swap)'])))
    #             if (tmp['sedol'][0], tmp['type'][0]) in ubs_rates.index:
    #                 tmp['ref_spread'] = ubs_rates[(tmp['sedol'][0], tmp['type'][0])]
    #             if (tmp['quantity'][0] != 0) and (acct_name_last in ubs_accounts):
    #                 ubs_keep = ubs_keep.append(tmp)
    #         this_pos = this_pos + 1

    # read in BlueHarbour Physcial Positions file
    try:
        files = [x for x in os.listdir(path) if
                 (x.find('CombinedPositionReportFlat.GRPZENTI') > -1) and (x[-4:].lower() == '.csv')]
    except:
        files = []
    # ubs_keep = pd.DataFrame(columns=holdings_headings)
    for this_file in files:
        data_in = pd.read_csv(path + "/" + this_file)
        this_pos = 0
        tmp_header = data_in.columns
        acct_name_last = ''
        acct_id_last = ''
        while this_pos < len(data_in):
            tmp = pd.DataFrame(np.transpose([[np.nan] for x in holdings_headings]), columns=holdings_headings)
            tmp_line = pd.Series(list(data_in.iloc[this_pos]), index=tmp_header)
            if (type(tmp_line['Account Name']) == str) and (tmp_line['Account Name'] != ''):
                acct_name_last = tmp_line['Account Name'].strip()
            if (type(tmp_line['Reference Account ID']) == np.int64) and (tmp_line['Reference Account ID'] != ''):
                acct_id_last = str(tmp_line['Reference Account ID']).strip()
            if tmp_line['Security Description'] != "Security Description" and not (type(tmp_line['Security Description']) == float and np.isnan(tmp_line['Security Description'])):
                tmp['prime'] = 'UBS'
                tmp['src'] = 'UBS'
                tmp['name'] = tmp_line['Security Description']
                tmp['on_swap'] = "PHYSICAL"
                tmp['date'] = this_date
                tmp['sedol'] = str(tmp_line['SEDOL'])
                tmp['isin'] = str(tmp_line['ISIN'])
                tmp['quantity'] = np.abs(ou.text2no(str(tmp_line['Trade Date Quantity'])))
                tmp['type'] = "S" if str(tmp_line['Trade Date Quantity'])[0] in ["(", '-'] else "L"
                tmp['quantity'] = (-1 if tmp['type'][0] == 'S' else 1) * tmp['quantity']
                tmp['src_file'] = path + "/" + this_file
                # tmp['net_price_usd'] = ou.text2no(tmp_line['Cost Price (Swap)'])
                # tmp['net_notional_usd'] = np.abs(ou.text2no(str(tmp_line['Market Value (Swap)'])))
                if (tmp['sedol'][0], tmp['type'][0]) in ubs_rates.index:
                    tmp['ref_spread'] = ubs_rates[(tmp['sedol'][0], tmp['type'][0])]
                if (tmp['quantity'][0] != 0) and (acct_name_last in ubs_accounts) and (acct_id_last == ubs_physical_acct_id):
                    ubs_keep = ubs_keep.append(tmp)
            this_pos = this_pos + 1
    return ubs_keep[holdings_headings]


def process_jpm_holdings(path='//zimnashk/sd.zentific-im.com$/Operations/Workflow/Holdings/20190403/JPM',
                         this_date=datetime.date(2019, 4, 3), ops_param={}):
    if ops_param != {}:
        tmp = ops_param['account']
    else:
        tmp = 'ZEN_MST'
    jpm_accounts = {'ZEN_MST': ['ZENTIFIC ASIA OPPORTUNITIES OFFSHORE MASTER FUND LIMITED', 'PL4', 'ZENTIFIC INVESTMENT MANAGEMENT LTD  - ZENTIFIC ASIA OPPS OFSHR MASTER FD LTD'],
                    'ZEN_PCO': ['PCH MANAGER FUND SPC-SEGREGATED PORTFOLIO 205-ZENTIFIC INVESTMENT MANAGEMENT LTD',
                                'YSK', 'PCH MANAGER SPC-SEGREGATED (102-53720)', 'PCH MANAGER SPC-SEGREGATED',
                                'PCH MANAGER SPC SEGREGATED'],
                    'ZEN_CNA': ['ZENTIFIC CHINA A INVESTMENT COMPANY LIMITED'],
                    'ZEN_PCN': ['PCH MGR FUND SPC - SEG PORTFOLIO 205 - CHINA', 'PCH MGR SPC-205-CHINA A SEG',
                                'PCH MGR SPC-205-CHINA A SEG (102-53725)'],
                    'ZEN_CNG': ['ZENTIFIC CHINA OPPORTUNITIES OFFSHORE FUND LIMITED', 'Y8S'],
                    'ZEN_NHL': ['405 MSTV I LP- ZENTIFIC INVESTMENT MANAGEMENT LIMITED'],
                    'ZEN_NHT': ['NEW HOLLAND TACTICAL ALPHA FUND LP- ZENTIFIC INVESTMENT MANAGEMENT LIMITED'],
                    'ZEN_SEA': ['BLUEHARBOUR MAP I LP - ZENTIFIC','BLUEHARBOUR MAP I LP']
                    }[tmp]
    jpm_keep = pd.DataFrame(columns=holdings_headings)
    # read in borrow costs
    try:
        files = [x for x in os.listdir(path) if
                 (x.find('Zentific_Portfolio_Swap_Trade_Detail_Report') > -1) and (x[-4:] == '.csv')]
    except:
        files = []
    jpm_rates = pd.Series()
    for this_file in files:
        data_in = pd.read_csv(path + "/" + this_file)
        data_in.columns = [x.strip() for x in data_in.columns]
        data_in['Average Financing Spread'] = [-y if x.strip() == 'S' else y for x, y in
                                               zip(data_in['Synthetic Buy / Sell'],
                                                   data_in['Average Financing Spread'])]
        filt = [this_type == 'S' for this_type in data_in['Synthetic Buy / Sell']]
        if sum(filt) > 0:
            tmp = data_in.loc[filt].groupby('Bloomberg Code')['Average Financing Spread'].mean()
            tmp.index.name = 'bb_code'
            tmp.name = 'ref_spread'
            tmp = tmp / 100
            jpm_rates = jpm_rates.append(tmp)
    # read in SWAP position files
    try:
        files = [x for x in os.listdir(path) if
                 (x.find('Zentific_Swap_Detail_Position_Report') > -1 or
                  x.find('China_Offshore_Portfolio_Swap_Detail_Position') > -1 or
                  x.find('NH_Portfolio_Swap_Detail_Position') > -1 or
                  x.find('Blue_Portfolio_Swap_Detail_Position') > -1)
                 and (x[-4:].lower() == '.csv')]
    except:
        files = []
    for this_file in files:
        data_in = pd.read_csv(path + "/" + this_file)
        this_pos = 0
        tmp_header = data_in.columns
        acct_name_last = ''
        swap_id_last = ''
        while this_pos < len(data_in):
            tmp = pd.DataFrame(np.transpose([[np.nan] for x in holdings_headings]), columns=holdings_headings)
            tmp_line = pd.Series(list(data_in.iloc[this_pos]), index=tmp_header)
            if (type(tmp_line['Fund']) == str) and (tmp_line['Fund'] != ''):
                if acct_name_last != tmp_line['Fund'].strip():
                    acct_name_last = tmp_line['Fund'].strip()
            if (type(tmp_line['Swap Deal Reference']) == str) and (tmp_line['Swap Deal Reference'] != ''):
                swap_id_last = tmp_line['Swap Deal Reference']
            if ((type(tmp_line['Bloomberg Code']) == str) and (tmp_line['Bloomberg Code'] != '')) or (
                    (type(tmp_line['Sedol Code']) == str) and (tmp_line['Sedol Code'] != '')):
                tmp['prime'] = 'JPM'
                tmp['src'] = 'JPM'
                tmp['name'] = tmp_line['Underlier Name']
                tmp['on_swap'] = "SWAP"
                tmp['date'] = this_date
                tmp['sedol'] = str(tmp_line['Sedol Code'])[:6]
                tmp['bb_code'] = tmp_line['Bloomberg Code'] if type(tmp_line['Bloomberg Code']) == str else np.nan
                tmp['quantity'] = np.abs(ou.text2no(tmp_line['Traded Position']))
                tmp['type'] = "S" if tmp_line['Traded Position'] < 0 else "L"
                tmp['quantity'] = (-1 if tmp['type'][0] == 'S' else 1) * tmp[
                    'quantity']  # Adrian 20200616 add quantity +/- adj
                tmp['src_file'] = path + "/" + this_file
                tmp['net_price_usd'] = ou.text2no(tmp_line['Weighted Average Price'])
                tmp['net_notional_usd'] = np.abs(ou.text2no(tmp_line['Underlying Market Value']))
                tmp['ref_spread'] = ou.text2no(tmp_line['Average Financing Spread']) / 100.0 * (
                    -1 if tmp['type'][0] == 'S' else 1)
                # if reference spread missing set it to np.nan
                if tmp['ref_spread'][0] == 0 and tmp['type'][0] == "S":
                    if tmp['bb_code'][0] in jpm_rates.index:
                        try:
                            tmp['ref_spread'] = jpm_rates[tmp['bb_code'][0]] #richard: amended due to duplicated jpm_rates 10/18/2024
                        except:
                            tmp['ref_spread'] = jpm_rates[tmp['bb_code'][0]].iloc[0] #richard: amended due to duplicated jpm_rates 10/18/2024
                    else:
                        tmp['ref_spread'] = np.nan

                tmp['ref_rate'] = ou.text2no(tmp_line['Current Average Benchmark Rate']) / 100.0
                tmp['curr_swap'] = tmp_line['Reporting Currency']
                # tmp['fx_rate'] = tmp_line['']
                # tmp['curr_underlying'] =
                tmp['src_ref'] = swap_id_last
                if datetime.datetime.strptime(tmp_line['Termination Date'], '%Y-%m-%d').date() - this_date <= datetime.timedelta(days=7):
                    tmp['ric'] = 'Expiring'
                if (tmp['quantity'][0] != 0) and (
                        acct_name_last in jpm_accounts):  # Adrian20200616 >0 to !=0 to avoid remove negative qty (sell)
                    jpm_keep = jpm_keep.append(tmp)
            this_pos = this_pos + 1
    # read in Cash position files
    # to add 27738_198588429_358990_10001973^Blue_Position_and_PL_csv20241014.csv
    try:
        files = [x for x in os.listdir(path) if
                 (x.find('Blue_Position_and_PL') > -1) and (x[-4:].lower() == '.csv')]
    except:
        files = []
    for this_file in files:
        data_in = pd.read_csv(path + "/" + this_file)
        this_pos = 0
        tmp_header = data_in.columns
        acct_name_last = ''
        while this_pos < len(data_in):
            tmp = pd.DataFrame(np.transpose([[np.nan] for x in holdings_headings]), columns=holdings_headings)
            tmp_line = pd.Series(list(data_in.iloc[this_pos]), index=tmp_header)
            if (type(tmp_line['Account Name']) == str) and (tmp_line['Account Name'] != ''):
                acct_name_last = tmp_line['Account Name'].strip()
            if ((type(tmp_line['Security ID']) == str) and (tmp_line['Security ID'] != '')):    #Temporary fix for missing sedol
                tmp['prime'] = 'JPM'
                tmp['src'] = 'JPM'
                tmp['name'] = tmp_line['Security Description']
                tmp['on_swap'] = "PHYSICAL"
                tmp['date'] = this_date
                tmp['sedol'] = str(tmp_line['Security ID'])[:6]
                tmp['type'] = tmp_line['Long Short Indicator']
                # tmp['bb_code'] = np.nan
                tmp['quantity'] = np.abs(ou.text2no(tmp_line['Trade Date Quantity']))
                # Make quantity negative only if 'type' is explicitly 'S'
                # if tmp['type'] == 'S':
                #     tmp['quantity'] = -tmp['quantity']

                tmp['quantity'] = (-1 if tmp['type'][0] == 'S' else 1) * tmp[
                    'quantity']  # Adrian 20200616 add quantity +/- adj
                tmp['src_file'] = path + "/" + this_file
                tmp['net_price_local'] = ou.text2no(tmp_line['Market Price (Local)'])
                tmp['net_notional_local'] = np.abs(ou.text2no(tmp_line['Trade Date Market Value (Local)']))
                tmp['net_price_usd'] = ou.text2no(tmp_line['Market Price (Base)'])
                tmp['net_notional_usd'] = np.abs(ou.text2no(tmp_line['Trade Date Market Value (Base)']))
                # tmp['ref_spread'] = tmp_line['Average Financing Spread']
                # tmp['ref_rate'] = tmp_line['Current Average Benchmark Rate']
                # tmp['curr_swap'] = tmp_line['Reporting Currency']
                # tmp['fx_rate'] = tmp_line['']
                tmp['curr_underlying'] = tmp_line['Issue Currency']
                # tmp['src_ref'] = swap_id_last
                # if (tmp['quantity'][0] != 0) and (
                #         acct_name_last in jpm_accounts):  # Adrian20200616 >0 to !=0 to avoid remove negative qty (sell)
                #     jpm_keep = jpm_keep.append(tmp)
                if (tmp['quantity'][0] != 0) and (acct_name_last in jpm_accounts):
                    jpm_keep = pd.concat([jpm_keep, tmp], ignore_index=True)
            this_pos = this_pos + 1
    return jpm_keep[holdings_headings]


def process_ms_holdings(path='//zimnashk/sd.zentific-im.com$/Operations/Workflow/Holdings/20231026/MS',
                        this_date=datetime.date(2023, 10, 26), ops_param={}):
    if ops_param != {}:
        this_account = ops_param['account']
    else:
        this_account = 'ZEN_MST'
    ms_accounts = {'ZEN_MST': ['ZENTIFIC ASIA OPPORTUNITIES OFFSHORE MASTER FUND LIMITED'],
                   'ZEN_CNA': ['ZENTIFIC CHINA A INVESTMENT COMPANY LIMITED'],
                   'ZEN_CNG': ['ZENTIFIC CHINA OPPORTUNITIES OFFSHORE FUND LIMITED'],
                   'ZEN_NHL': ['405 MSTV I LP -ZENTIFIC'],
                   'ZEN_NHT': ['NEW HOLLAND TACTICAL ALPHA FUND LP - ZENTIFIC'],
                   'ZEN_SEA': ['BLUEHARBOUR MAP I LP - ZENTIFIC']
                   }[this_account]
    ms_keep = pd.DataFrame(columns=holdings_headings)

    # read in borrow costs
    prefix = ['STKLOAN']
    files = [x for x in os.listdir(path) if (sum([y in x for y in prefix]) > 0) and (x[-4:] == '.zip')]
    if len(files) > 0:
        try:
            # extract any files
            for this_file in files:
                myzip = ZipFile(path + "/" + this_file, 'r')
                myzip.extractall(path)
            prefix = ['STKLOAN001X']
            files = [x for x in os.listdir(path) if (sum([y in x for y in prefix]) > 0) and (x[-4:] == '.txt')]
        except:
            files = []
        data_in = pd.DataFrame()
        for this_file in files:
            data_in = data_in.append(pd.read_csv(path + "/" + this_file))
        if len(data_in) > 0:
            ms_rates = data_in.loc[data_in['ACCOUNT NAME'] == ms_accounts[0]]
            ms_rates = ms_rates[['CUSIP', 'AV DEAL RATE(%)']]
            ms_rates.columns = ['CUSIP', 'spread']
            ms_rates = ms_rates.drop_duplicates(subset=['CUSIP'])
            ms_rates = ms_rates.set_index('CUSIP')['spread']
            ms_rates = ms_rates / 100.0
        else:
            ms_rates = pd.Series()
    else:
        # read in borrow costs
        try:
            if ops_param['account'] in ['ZEN_NHT', 'ZEN_NHL'] :
                prefix = ['ZIM-EQSWAP40X']
            else :
                prefix = ['EQSWAP40X.']
            files = [x for x in os.listdir(path) if (sum([y in x for y in prefix]) > 0) and (x[-4:] == '.csv')]
        except:
            files = []

        ms_rates = pd.DataFrame()
        spread_cols = ['Spread', 'Spread BPS']
        account_cols = ['Sub Account Name', 'Account Name']
        id_cols = ['Ric', 'RIC Code']
        if len(files) > 0 :
            this_file = files[0]
            data_in = pd.read_csv(path + "/" + this_file)
            data_in.columns = [x.strip() for x in data_in.columns]
            if len(set(data_in.columns) & set(spread_cols)) > 0 and len(
                    set(data_in.columns) & set(account_cols)) > 0 and len(set(data_in.columns) & set(id_cols)) > 0:
                spread_col = list(set(data_in.columns) & set(spread_cols))[0]
                account_col = list(set(data_in.columns) & set(account_cols))[0]
                id_col = list(set(data_in.columns) & set(id_cols))[0]
                # filter the accounts, the date and only open and existing positions
                data_in = data_in.loc[[x in ms_accounts for x in data_in[account_col]]]
                data_in = data_in.loc[[x == str(this_date) for x in data_in['Trade Date']]]
                data_in = data_in.loc[[x in ['O', 'FIX'] for x in data_in['Event']]]
                filt = data_in[['Spread', 'Quantity']].notnull().all(axis=1)
                data_in = data_in.loc[filt]
                data_in = data_in.loc[data_in['Spread']!=0]
                data_in['Quantity'] =  data_in['Quantity'].astype(int)
                data_in[spread_col] = [ou.text2no(y) / (10000) if 'Long' in x else -1 * ou.text2no(y) / (10000) for x,y in zip(data_in['Basket Description'],data_in[spread_col])]
                data_in = data_in[data_in[spread_col] != 0]
                data_in['type'] = ['L' if 'Long' in x else 'S' for x in data_in['Basket Description']]
                tmp = data_in[[id_col, spread_col, 'Quantity', 'type']]
                tmp = tmp.drop_duplicates()
                map_total = tmp.groupby([id_col, 'type'])['Quantity'].sum()
                keys = tmp.set_index([ id_col, 'type']).index
                tmp['total'] = list(map_total[keys])
                tmp['spread_wgt'] = tmp['Spread'] * tmp['Quantity'] / tmp['total']
                tmp = tmp.groupby([id_col, 'type'])['spread_wgt'].sum().reset_index()
                filt = [np.isnan(x) == False and np.isinf(abs(x)) == False for x in tmp.spread_wgt]
                tmp = tmp.loc[filt]
                ms_rates = ms_rates.append(tmp).set_index(['RIC Code', 'type'])
    # read in  position files (swap for all + physical for SEA)
    try:
        if this_account == 'ZEN_MST':
            files = [x for x in os.listdir(path) if (x.startswith('MAC001X_Earlyrun') == True) and (x[-4:].lower() == '.csv')]
            if len(files) == 0:
                files = [x for x in os.listdir(path) if (x.find('MAC001X') > -1) and (x[-4:].lower() == '.csv')]
        elif this_account == 'ZEN_CNG':
            files = [x for x in os.listdir(path) if (x.startswith('MAC001X_Earlyrun') == True) and (x[-4:].lower() == '.csv')]
            if len(files) == 0:
                files = [x for x in os.listdir(path) if (x.find('BBB04337279-MAC001X') > -1) and (x[-4:].lower() == '.csv')]
        elif this_account in ['ZEN_NHT', 'ZEN_NHL']:
            files = [x for x in os.listdir(path) if (x.startswith('ZIM-MAC001X') == True) and (x[-4:].lower() == '.csv')]
        elif this_account == 'ZEN_SEA':
            files = [x for x in os.listdir(path) if (x.startswith('ZIM-MAC001X-GlobalPositionsExtract-CEOD') == True) and (x[-4:].lower() == '.csv')]
        else :
            files = []
    except:
        files = []
    if len(files) > 0:
        files = sorted(files)
        files = [files[-1]] # get the largest timestamp

    for this_file in files:
        data_in = pd.read_csv(path + "/" + this_file)
        this_pos = 0
        tmp_header = data_in.columns
        acct_name_last = ''
        position_type_last = ''
        position_type_map = {'PB':'PHYSICAL','EQS':'SWAP'}
        while this_pos < len(data_in):

            tmp = pd.DataFrame(np.transpose([[np.nan] for x in holdings_headings]), columns=holdings_headings)
            tmp_line = pd.Series(list(data_in.iloc[this_pos]), index=tmp_header)
            if (type(tmp_line['Sub Account Name']) == str) and (tmp_line['Sub Account Name'] != ''):
                acct_name_last = tmp_line['Sub Account Name'].strip()
            if (type(tmp_line['Position Type']) == str) and (tmp_line['Position Type'] != ''):
                position_type_last = position_type_map.get(tmp_line['Position Type'].strip(),'')
            if ((type(tmp_line['Quantity']) == str and tmp_line['Quantity'] != 'Quantity') and ou.text2no(tmp_line['Quantity']) != 0) or (isinstance(tmp_line['Quantity'], float) and tmp_line['Quantity'] != 0 and np.isnan(tmp_line['Quantity'])== False):
                tmp['prime'] = 'MS'
                tmp['src'] = 'MS'
                tmp['name'] = tmp_line['Security Description']
                tmp['on_swap'] = position_type_last
                tmp['date'] = this_date
                tmp['sedol'] = str(tmp_line['SEDOL'])[:6]
                tmp['ric'] = str(tmp_line['RIC'])
                tmp['isin'] = str(tmp_line['ISIN'])
                tmp['bb_code'] = tmp_line['Bloomberg Ticker']
                tmp['quantity'] = np.abs(ou.text2no(tmp_line['Quantity']))
                tmp['type'] = tmp_line['Long/Short Code']
                tmp['quantity'] = (-1 if tmp['type'][0] == 'S' else 1) * tmp[
                    'quantity']  # Adrian 20200616 add quantity +/- adj
                tmp['fx_rate'] =  tmp_line['FX Rate Issue To USD']  
                tmp['src_file'] = path + "/" + this_file

                tmp['net_notional_local'] = np.abs(ou.text2no(tmp_line['Notional Cost (Issue)']))
                tmp['net_price_local'] = tmp['net_notional_local'] / tmp['quantity']
                tmp['net_notional_usd'] = np.abs(ou.text2no(tmp_line['Notional Cost (USD)']))
                tmp['net_price_usd'] = tmp['net_notional_usd'] / tmp['quantity']
                #                tmp['ref_spread'] = ou.text2no(tmp_line['Average Financing Spread']) / 100.0 * (-1 if tmp['type'][0] == 'S' else 1)
                # if reference spread missing set it to np.nan
                if type(ms_rates.index.name) == str :
                    if tmp['type'][0] == "S" and len(ms_rates) > 0:
                        if tmp_line['CUSIP'] in ms_rates.index:
                            tmp['ref_spread'] = ms_rates[tmp_line['CUSIP']].astype(float)
                        else:
                            tmp['ref_spread'] = np.nan
                else : 
                    if (tmp['ric'][0], tmp['type'][0]) in ms_rates.index : 
                        tmp['ref_spread'] = ms_rates.loc[(tmp['ric'][0], tmp['type'][0])][0].astype(float)

                #                tmp['ref_rate'] = ou.text2no(tmp_line['Current Average Benchmark Rate']) / 100.0
                tmp['curr_underlying'] = tmp_line['Issue Currency']
                tmp['curr_swap'] = tmp_line['Swap Currency']

                if type(tmp_line['Maturity Date'])==str and datetime.datetime.strptime(tmp_line['Maturity Date'], '%Y-%m-%d').date() - this_date <= datetime.timedelta(days=7):
                    tmp['ric'] = 'Expiring'
                if (acct_name_last in ms_accounts) and (
                        tmp['quantity'][0] != 0):  # Adrian20200616 >0 to !=0 to avoid remove negative qty (sell)
                    ms_keep = ms_keep.append(tmp)
            this_pos = this_pos + 1

    return ms_keep[holdings_headings]

def process_cicc_holdings(path='//zimnashk/sd.zentific-im.com$/Operations/Workflow/Holdings/20221006/CICC',
                         this_date=datetime.date(2022, 11, 2), ops_param={}):
    if ops_param != {}:
        tmp = ops_param['account']
    else:
        tmp = 'ZEN_MST'
    cicc_accounts = {'ZEN_MST': ['FT_Z126'],
                    'ZEN_CNG': ['FT_Z127'],
                    'ZEN_NHL': ['UNKNOWN'],
                    'ZEN_NHT': ['UNKNOWN']
                    }[tmp]
    cicc_keep = pd.DataFrame(columns=holdings_headings)
    # read in borrow costs
    try:
        files = [x for x in os.listdir(path) if
                 (x.find('Zentific Asia_ValuationReport') > -1 or
                  x.find('Zentific China_ValuationReport') > -1) and (x[-5:].lower() == '.xlsx')]
    except:
        files = []
    cicc_rates = pd.Series()
    for this_file in files:
        data_in = pd.read_excel(path + "/" + this_file, sheetname = 'Valuation', header = 11)
        data_in.columns = [x.strip() for x in data_in.columns]
        data_in['Bloomberg Code'] = data_in['Bloomberg Code'].str.replace(' Equity', '')
        tmp = data_in.groupby('Bloomberg Code')['Financing Spread (bps)'].mean()
        tmp.index.name = 'bb_code'
        tmp.name = 'ref_spread'
        tmp = tmp / 10000
        cicc_rates = cicc_rates.append(tmp)
    # read in SWAP position files
    for this_file in files:
        data_in = pd.read_excel(path + "/" + this_file, sheetname = 'Valuation', header = 11)
        this_pos = 0
        tmp_header = data_in.columns
        acct_name_last = ''
        swap_id_last = ''
        while this_pos < len(data_in):
            tmp = pd.DataFrame(np.transpose([[np.nan] for x in holdings_headings]), columns=holdings_headings)
            tmp_line = pd.Series(list(data_in.iloc[this_pos]), index=tmp_header)
            if (type(tmp_line['Account Code']) == str) and (tmp_line['Account Code'] != ''):
                acct_name_last = tmp_line['Account Code'].strip()
            if (type(tmp_line['ID']) == str) and (tmp_line['ID'] != ''):
                swap_id_last = tmp_line['ID']
            if ((type(tmp_line['Bloomberg Code']) == str) and (tmp_line['Bloomberg Code'] != '')) or (
                    (type(tmp_line['Sedol Code']) == str) and (tmp_line['Sedol Code'] != '')):
                tmp['prime'] = 'CICC'
                tmp['src'] = 'CICC'
                tmp['on_swap'] = "SWAP"
                tmp['date'] = this_date
                tmp['sedol'] = str(tmp_line['Sedol Code'])[:6]
                tmp['ric'] = str(tmp_line['Security Code'])
                tmp['isin'] = str(tmp_line['ISIN'])
                tmp['bb_code'] = tmp_line['Bloomberg Code'].replace(' Equity', '')
                tmp['type'] = "S" if tmp_line['Remaining Qty(Trade Date)'] < 0 else "L"
                tmp['quantity'] = ou.text2no(tmp_line['Remaining Qty(Trade Date)'])
                tmp['src_file'] = path + "/" + this_file
                tmp['fx_rate'] =  tmp_line['Contract FX']
                tmp['accrued_interest_usd'] = tmp_line['Interest Accrued (Settle CCY)']
                tmp['net_notional_local'] = np.abs(ou.text2no(tmp_line['Notional (Local CCY)']))
                tmp['net_price_local'] = np.abs(ou.text2no(tmp_line['Initial/Reset Net Price (Local CCY)']))
                tmp['net_price_usd'] = ou.text2no(tmp_line['Initial/Reset Net Price (Local CCY)'])/ou.text2no(tmp_line['Contract FX'])
                tmp['net_notional_usd'] = np.abs(ou.text2no(tmp_line['Notional (Settle CCY)']))
                tmp['ref_spread'] = ou.text2no(tmp_line['Financing Spread (bps)']) / 10000.0
                # if reference spread missing set it to np.nan
                if tmp['ref_spread'][0] == 0 and tmp['type'][0] == "S":
                    tmp['ref_spread'] = np.nan
                tmp['ref_rate'] = ou.text2no(tmp_line['Benchmark Rate(%)']) / 100.0 * (
                    -1 if tmp['type'][0] == 'S' else 1)
                tmp['curr_swap'] = tmp_line['Settlement CCY']
                # tmp['fx_rate'] = tmp_line['']
                tmp['curr_underlying'] = tmp_line['Underlier (Local CCY)']
                tmp['src_ref'] = swap_id_last
                if datetime.datetime.strptime(tmp_line['Expiry Date'], '%Y/%m/%d').date() - this_date <= datetime.timedelta(days=7):
                    tmp['ric'] = 'Expiring'
                if (tmp['quantity'][0] != 0) and (
                        acct_name_last in cicc_accounts):  
                    cicc_keep = cicc_keep.append(tmp)
            this_pos = this_pos + 1
    return cicc_keep[holdings_headings]

# bloomberge fields
def process_bbg_holdings(path=workflow_path + '/Holdings/20150618/Bloomberg', this_date=datetime.date(2019, 6, 18),
                         ops_param={}):
    try:
        files = [x for x in os.listdir(path) if (x[0] != '~') and (x[-5:] == '.xlsx')]
    except:
        files = []
    bbg_keep = pd.DataFrame(columns=holdings_headings)
    for this_file in files:
        bbg = pd.read_excel(path + "/" + this_file, skiprows=6)
        broker_map = pd.Series(['GS', 'GS', 'BBG', 'BOAML', 'BOAML', 'CS', 'CICC'],
                               index=['GS', 'GSI', 'BBG', 'BOAML', 'BOAML-PT', 'CS', 'CICC'])
        bbg['prime'] = [broker_map[x] if x in broker_map.index else np.nan for x in bbg.Name]
        bbg['prime'] = bbg['prime'].ffill()
        bbg = bbg[pd.notnull(bbg['CFD Y/N?'])]
        bbg = bbg.iloc[[x.startswith('{cash}') == False for x in bbg.Name]]
        bbg = bbg.iloc[[x.startswith('{accrual}') == False for x in bbg.Name]]
        this_pos = 0
        tmp_header = bbg.columns
        while this_pos < len(bbg):
            tmp = pd.DataFrame(np.transpose([[np.nan] for x in holdings_headings]), columns=holdings_headings)
            tmp_line = pd.Series(list(bbg.iloc[this_pos]), index=tmp_header)
            tmp['prime'] = tmp_line['prime']
            tmp['src'] = 'BBG'
            tmp['on_swap'] = "SWAP" if tmp_line['CFD Y/N?'] == "SWAP" else "PHYSICAL"
            tmp['date'] = this_date
            tmp['bb_code'] = str(tmp_line['Name']).replace(' [NOSH]', '').replace('[', '').replace(']', '')
            tmp['type'] = "L" if tmp_line['Position'] >= 0 else "S"
            tmp['quantity'] = tmp_line['Position']
            try:
                tmp['src_file'] = path + "/" + this_file
                tmp['net_price_usd'] = tmp_line['Avg Cost'] * tmp_line['SEC WS Fx Rate']
                tmp['net_price_local'] = tmp_line['Avg Cost']
                tmp['net_notional_usd'] = np.abs(
                    tmp_line['Position'] * tmp_line['Avg Cost'] * tmp_line['SEC WS Fx Rate'])
                tmp['net_notional_local'] = np.abs(tmp_line['Position'] * tmp_line['Avg Cost'])
                tmp['fx_rate'] = tmp_line['FX_Rate_Inverse']
            except:
                pass
            bbg_keep = bbg_keep.append(tmp)
            this_pos = this_pos + 1
    return bbg_keep[holdings_headings]


def parse_holding_files(path=workflow_path + "/Holdings/", start_date=None, end_date=None, overwrite_exist_dates=True,
                        ops_param={}):
    db = ops_param['db']
    broker_holdings_table = ops_param['broker_holdings_table']
    bulk_load_path = ops_param['bulk_load_path']
    result = pd.DataFrame()
    filenames = os.listdir(path)
    # convert to dates
    all_dates = [datetime.datetime.strptime(x, "%Y%m%d").date() for x in filenames if x.isdigit() and (len(x) == 8)]
    if start_date is not None:
        all_dates = [x for x in all_dates if x >= start_date]
    if end_date is not None:
        all_dates = [x for x in all_dates if x <= end_date]
    # this_date = all_dates[0]
    for this_date in all_dates:
        this_file = str(this_date).replace("-", "")
        if os.path.isdir(path + this_file + "/GS"):
            result = result.append(
                process_gs_holdings(path=path + this_file + "/GS", this_date=this_date, ops_param=ops_param))
        if os.path.isdir(path + this_file + "/CS"):
            result = result.append(
                process_cs_holdings(path=path + this_file + "/CS", this_date=this_date, ops_param=ops_param))
        if os.path.isdir(path + this_file + "/BOAML"):
            result = result.append(
                process_boaml_holdings(path=path + this_file + "/BOAML", this_date=this_date, ops_param=ops_param))
        if os.path.isdir(path + this_file + "/UBS"):
            result = result.append(
                process_ubs_holdings(path=path + this_file + "/UBS", this_date=this_date, ops_param=ops_param))
        if os.path.isdir(path + this_file + "/JPM"):
            result = result.append(
                process_jpm_holdings(path=path + this_file + "/JPM", this_date=this_date, ops_param=ops_param))
        if os.path.isdir(path + this_file + "/MS"):
            result = result.append(
                process_ms_holdings(path=path + this_file + "/MS", this_date=this_date, ops_param=ops_param))
        if os.path.isdir(path + this_file + "/CICC"):
            result = result.append(
                process_cicc_holdings(path=path + this_file + "/CICC", this_date=this_date, ops_param=ops_param))
        if os.path.isdir(path + this_file + "/Bloomberg"):
            result = result.append(
                process_bbg_holdings(path=path + this_file + "/Bloomberg", this_date=this_date, ops_param=ops_param))
        result['sedol'] = [str(x) for x in result['sedol']]
    # match custom swaps to our naming
    keys = list(result.iloc[[x in ticker_map_manual.index for x in result.name]]['name'])
    result.loc[[x in ticker_map_manual.index for x in result.name], 'bb_code'] = list(
        ticker_map_manual.loc[keys]['bb_code'])
    result.loc[[x in ticker_map_manual.index for x in result.name], 'sedol'] = list(
        ticker_map_manual.loc[keys]['sedol'])
    trade_type_for_mapping = ['SSF' if (type(bb) == str and '=' in bb) else t_init for t_init, bb in
                              zip(result['type'], result['bb_code'])]

    # Replace swap bb_codes with ours
    swaps = ou.get_hist_ticker_map(start_date=None)[['sedol', 'bb_code']]
    swaps = swaps.loc[[type(s) == str and '_SWAP_' in s for s in swaps['sedol']]]
    swaps['bb_code'] = [b.replace(' Index', '') for b in swaps['bb_code']]
    swaps = swaps.set_index('bb_code')
    result['sedol'] = [swaps.loc[b]['sedol'] if b in swaps.index.get_level_values('bb_code') else s for s, b in
                       zip(result['sedol'], result['bb_code'])]
    # Do the usual mapping
    result['isin'] = [np.nan if isin in isin_manual_clear else isin for isin in result['isin']] # Steven: Filter out all the unrecognized isin manually
    tmp = ou.map_z_sedol(sedol=result['sedol'], bb_code=result['bb_code'], isin=result['isin'],
                         trade_type=trade_type_for_mapping)
    result['sedol'] = ['' if x == 'nan' else x for x in result.sedol]
    result['z_sedol'] = list(tmp['z_sedol'])
    result['z_bb_code'] = list(tmp['z_bb_code'])
    result['z_bb_code_short'] = list(tmp['z_bb_code_short'])

    # cater for trades missing identifies
    result['bb_code'] = result['bb_code'].fillna("")
    result['sedol'] = result['sedol'].fillna("")

    # strip comma's from name
    result['name'] = [x.replace(",", "") if type(x) == str else x for x in result['name']]
    # calculate and update the z_ref
    tmp = \
        ou.exec_sql("select coalesce(max(h_ref),0) from [" + db + "]..[" + broker_holdings_table + "]", isselect=1).ix[
            0, 0]
    result['h_ref'] = [x for x in range(tmp + 1, tmp + len(result) + 1)]
    all_dates = list(np.unique(result.date))
    # get max trade ref id and increment
    if overwrite_exist_dates and len(all_dates) > 0:
        #        ou.archive_table(table = broker_holdings_table, num_to_archive=5, db=db)
        sqlcmd = "delete from [" + db + "]..[" + broker_holdings_table + "] where [date] in ('" + "', '".join(
            [str(x) for x in all_dates]) + "')"
        ou.exec_sql(sqlcmd, isselect=0)
    if len(result.index) > 0:
        sqlcmd = "select column_name from [" + db + "].information_schema.columns where TABLE_NAME= '" + broker_holdings_table + "' order by ORDINAL_POSITION"
        tmp = ou.exec_sql(sqlcmd, isselect=1)
        tmp = result.reset_index()[list(tmp['column_name'])]
        ou.load_to_table(df=tmp, table=broker_holdings_table, db=db, index=False, pathloc=bulk_load_path)


def backfill():
    start_date = datetime.date(2024, 1, 23)
    end_date = datetime.date(2024, 2, 22)
    ops_param = ou.get_ops_param('ZEN_CNG')
    parse_holding_files(path=ops_param['workflow_path'] + "Holdings/", start_date=start_date, end_date=end_date,
                        overwrite_exist_dates=True, ops_param=ops_param)


def main(argv):
    if mode != 'DEBUG':
        start_date = (pd.datetime.today() - pd.tseries.offsets.BDay(1)).to_datetime().date()
        end_date = (pd.datetime.today()).date()
        ops_param = ou.get_ops_param('ZEN_CNG')
        if len(argv) > 0:
            print("called with " + str(len(argv)) + " paramenters " + argv[0])
            ops_param = ou.get_ops_param(argv[0])
    else:
        ops_param = ou.get_ops_param('ZEN_CNG')
        start_date = datetime.date(2024, 3, 26)
        end_date = start_date
        #this_file = str(start_date).replace("-", "")
        #process_cs_holdings(path=workflow_path + "/Holdings/" + this_file + "/CS", this_date=start_date, ops_param=ops_param)
    print("AT " + str(datetime.datetime.now()) + " Doing date " + str(start_date), flush=True)

    parse_holding_files(path=ops_param['workflow_path'] + "Holdings/", start_date=start_date, end_date=end_date,
                        overwrite_exist_dates=True, ops_param=ops_param)


# debug
def debug_stuff():
    ops_param = ou.get_ops_param('ZEN_NHL')
    start_date = datetime.date(2023,11, 13)
    end_date = datetime.date(2023,11, 15)
    path = ops_param['workflow_path'] + "Holdings/"

    db = ops_param['db']
    broker_holdings_table = ops_param['broker_holdings_table']
    bulk_load_path = ops_param['bulk_load_path']
    result = pd.DataFrame()
    filenames = os.listdir(path)
    # convert to dates
    all_dates = [datetime.datetime.strptime(x, "%Y%m%d").date() for x in filenames if x.isdigit() and (len(x) == 8)]
    if start_date is not None:
        all_dates = [x for x in all_dates if x >= start_date]
    if end_date is not None:
        all_dates = [x for x in all_dates if x <= end_date]
    for this_date in all_dates:
        this_file = str(this_date).replace("-", "")
        result = result.append(
            process_ubs_holdings(path=path + this_file + "/UBS", this_date=this_date, ops_param=ops_param))

    result['sedol'] = [str(x) for x in result['sedol']]
    # match custom swaps to our naming
    keys = list(result.iloc[[x in ticker_map_manual.index for x in result.name]]['name'])
    result.loc[[x in ticker_map_manual.index for x in result.name], 'bb_code'] = list(
        ticker_map_manual.loc[keys]['bb_code'])
    result.loc[[x in ticker_map_manual.index for x in result.name], 'sedol'] = list(
        ticker_map_manual.loc[keys]['sedol'])
    trade_type_for_mapping = ['SSF' if (type(bb) == str and '=' in bb) else t_init for t_init, bb in
                              zip(result['type'], result['bb_code'])]
    # Replace swap bb_codes with ours
    swaps = ou.get_hist_ticker_map(start_date=None)[['sedol', 'bb_code']]
    swaps = swaps.loc[[type(s) == str and '_SWAP_' in s for s in swaps['sedol']]]
    swaps['bb_code'] = [b.replace(' Index', '') for b in swaps['bb_code']]
    swaps = swaps.set_index('bb_code')
    result['sedol'] = [swaps.loc[b]['sedol'] if b in swaps.index.get_level_values('bb_code') else s for s, b in
                       zip(result['sedol'], result['bb_code'])]
    # Do the usual mapping
    tmp = ou.map_z_sedol(sedol=result['sedol'], bb_code=result['bb_code'], isin=result['isin'],
                         trade_type=trade_type_for_mapping)
    result['sedol'] = ['' if x == 'nan' else x for x in result.sedol]
    result['z_sedol'] = list(tmp['z_sedol'])
    result['z_bb_code'] = list(tmp['z_bb_code'])
    result['z_bb_code_short'] = list(tmp['z_bb_code_short'])
    # strip comma's from name
    result['name'] = [x.replace(",", "") if type(x) == str else x for x in result['name']]
    # calculate and update the z_ref
    tmp = \
        ou.exec_sql("select coalesce(max(h_ref),0) from [" + db + "]..[" + broker_holdings_table + "]", isselect=1).ix[
            0, 0]
    result['h_ref'] = [x for x in range(tmp + 1, tmp + len(result) + 1)]
    all_dates = list(np.unique(result.date))

    ou.load_to_table(result, 'zzz_tmp', db='zimdb_ops')


def debug_ms():
    process_ms_holdings(path='//zimnashk/sd.zentific-im.com$/Operations/Workflow/zen_nht/Holdings/20231027/MS',
                        this_date=datetime.date(2023, 10, 27), ops_param = ou.get_ops_param('ZEN_NHT'))

def backfill_ms_boaml_rates() :
    this_account = 'ZEN_NHT'
    ops_param = ou.get_ops_param(this_account)
    start_date = datetime.date(2023,1, 11)
    end_date = datetime.date(2023,10, 26)
    path=ops_param['workflow_path'] + 'Holdings/' + str(end_date).replace("-","") +'/MS'

    ms_accounts = {'ZEN_MST': ['ZENTIFIC ASIA OPPORTUNITIES OFFSHORE MASTER FUND LIMITED'],
                   'ZEN_CNA': ['ZENTIFIC CHINA A INVESTMENT COMPANY LIMITED'],
                   'ZEN_CNG': ['ZENTIFIC CHINA OPPORTUNITIES OFFSHORE FUND LIMITED'],
                   'ZEN_NHL': ['405 MSTV I LP -ZENTIFIC'],
                   'ZEN_NHT': ['NEW HOLLAND TACTICAL ALPHA FUND LP - ZENTIFIC']
                   }[this_account]

    # read in MS borrow costs
    try:
        if ops_param['account'] in ['ZEN_NHT', 'ZEN_NHL'] :
            prefix = ['ZIM-EQSWAP40X']
        else :
            prefix = ['EQSWAP40X.']
        files = [x for x in os.listdir(path) if (sum([y in x for y in prefix]) > 0) and (x[-4:] == '.csv')]
    except:
        files = []

    ms_rates = pd.DataFrame()
    spread_cols = ['Spread', 'Spread BPS']
    account_cols = ['Sub Account Name', 'Account Name']
    id_cols = ['Ric', 'RIC Code']
    if len(files) > 0 :
        this_file = files[0]
        data_in = pd.read_csv(path + "/" + this_file)
        data_in.columns = [x.strip() for x in data_in.columns]
        spread_col = list(set(data_in.columns) & set(spread_cols))[0]
        account_col = list(set(data_in.columns) & set(account_cols))[0]
        id_col = list(set(data_in.columns) & set(id_cols))[0]
        data_in = data_in.loc[[x in ms_accounts for x in data_in[account_col]]]
        data_in = data_in.loc[[x in ['O', 'FIX'] for x in data_in['Event']]]
        data_in[spread_col] = [np.abs(ou.text2no(y)) / (10000) for y in data_in[spread_col]]
        data_in = data_in[data_in[spread_col] != 0]
        data_in['type'] = ['L' if 'Long' in x else 'S' for x in data_in['Basket Description']]
        filt = [x !=0 and np.isnan(x) == False and np.isnan(y) == False for x,y in zip(data_in['Spread'], data_in['Quantity'])]
        data_in = data_in.loc[filt]
        tmp = data_in[['Trade Date', id_col, spread_col, 'Quantity', 'type']]
        tmp = tmp.drop_duplicates()
        map_total = tmp.groupby(['Trade Date', id_col, 'type'])['Quantity'].sum()
        keys = tmp.set_index(['Trade Date', id_col, 'type']).index
        tmp['total'] = list(map_total[keys])
        tmp['spread_wgt'] = tmp['Spread'] * tmp['Quantity'] / tmp['total']
        tmp = tmp.groupby(['Trade Date', id_col, 'type'])['spread_wgt'].sum().reset_index()
        tmp.columns = ['date', 'ric', 'type', 'spread']
        ms_rates = tmp.set_index(['date', 'ric', 'type'])

    # now do boaml
    boaml_accounts = {'ZEN_MST': ['ZEN_ASIAOPS'],
                'ZEN_PCO': ['Unknown'],
                'ZEN_CNA': ['Unknown'],
                'ZEN_PCN': ['Unknown'],
                'ZEN_CNL': ['ZEN_S31FF'],
                'ZEN_CNG': ['ZEN_S41'],
                'ZEN_NHL': ['405MSTV_ZEN'],
                'ZEN_NHT': ['NTHAFD_ZEN']
                }[this_account]

    def agg_holdings(holdings):
        result = holdings.iloc[0, :].copy()
        if len(holdings) > 1:
            avg_cols = ['Interest Rate', 'financing']
            scale = list(holdings['Position']) / np.sum(np.abs(list(holdings['Position'])))
            result[avg_cols] = holdings[avg_cols].apply(
                func=lambda x: np.sum([y * z for y, z in zip(list(x), scale)]), axis=0)
            sum_cols = ['Position']
            result[sum_cols] = holdings[sum_cols].apply(func=lambda x: np.sum(list(x)), axis=0)
        return result

    boaml_rates = pd.DataFrame()
    this_date = [x.date() for x in pd.date_range(start_date, end_date,freq='B')][0]
    for this_date in [x.date() for x in pd.date_range(start_date, end_date,freq='B')] :
        path=ops_param['workflow_path'] + 'Holdings/' + str(this_date).replace("-","") +'/BOAML'

        try:
            files = [x for x in os.listdir(path) if (('SwapValDetailPosInterest' in x) or ('SwapValuationExtract_RGxA' in x)) and (x[-4:] == '.csv')]
        except:
            files = []

        if len([x for x in files if ('SwapValuationExtract_RGxA' in x)]) > 0 : 
            this_file = [x for x in files if ('SwapValuationExtract_RGxA' in x)][0]
            boaml = pd.read_csv(path + "/" + this_file)
            # on reset days BOAML don't provide the spread costs so need to use file from yesterday        
            if len(boaml) < 1:
                tmp = os.listdir(path + "/../..")
                tmp = [x for x in tmp if x.isdigit() and (x < this_date.strftime("%Y%m%d"))]
                tmp.sort()
                last_date = tmp[-1]
                files = [x for x in os.listdir(path + "/../../" + last_date + "/BOAML/") if
                        (x.find('SwapValuationExtract_RGxA') > -1) and (x[-4:] == '.csv')]
                if len(files) > 0:
                    this_file = files[0]
                    boaml = pd.read_csv(path + "/../../" + last_date + "/BOAML/" + this_file)
            boaml.columns = [x.strip() for x in boaml.columns]
            # filter for account
            boaml = boaml.loc[boaml['ACCOUNT ID'] == boaml_accounts[0]]
            boaml['financing'] = boaml['LONG FINANCING SPREAD'] - boaml['SHORT FINANCING SPREAD']
            # filter for holding
            filt = [np.isnan(x) == False and np.isnan(y) == False for x,y in zip(boaml['QUANTITY'], boaml['financing'])]
            boaml = boaml.loc[filt]
            boaml['type'] = ['L' if x >= 0 else 'S' for x in boaml['QUANTITY']]
            boaml['Position'] = boaml['QUANTITY'].abs()
            boaml = boaml[['UNDERLYING SEDOL', 'PRIMARY RIC CODE', 'UNDERLYING INTERNAL ID', 'Position', 'type', 'BASE RATE',
                        'financing']]
            # if missing primary ric use underlying internal id
            boaml['Interest Rate'] = boaml['BASE RATE']
            boaml['Primary RIC'] = [x if type(x) == str else y for x, y in
                                    zip(boaml['PRIMARY RIC CODE'], boaml['UNDERLYING INTERNAL ID'])]
            # replace missing sedols with rics
            boaml['sedol'] = boaml['UNDERLYING SEDOL']
            filt = [type(x) != str and np.isnan(x) for x in boaml['sedol']]
            boaml.loc[filt, 'sedol'] = boaml.loc[filt, 'Primary RIC']

            if len(boaml) > 0:
                boaml = boaml.groupby(['sedol', 'type']).apply(agg_holdings).reset_index(drop=True)
                boaml['sedol'] = [str(x)[:6] if len(str(x)) == 7 else x for x in boaml['sedol']]
                boaml = boaml.set_index(['sedol', 'type'])
                boaml['date'] = this_date
                boaml['ric'] = boaml['PRIMARY RIC CODE']
                boaml['spread'] = boaml['financing']
                boaml_rates = boaml_rates.append(boaml.reset_index()[['date','ric','type','spread']])
        
    ms_rates['src'] = 'MS'
    boaml_rates['src'] = 'BOAML'
    boaml_rates['spread'] = boaml_rates['spread'] / 100
    all_rates =    ms_rates.reset_index().append(boaml_rates)
    # clean up infs
    filt = [np.isnan(x) == False and abs(x) != np.inf for x in all_rates['spread']]
    all_rates = all_rates.loc[filt]
    filt = [np.isnan(x) == False and np.isinf(abs(x)) == False for x in all_rates.spread]
    ou.create_table(all_rates, 'xxx_tmp', db='zimdb_ops')

if (__name__ == '__main__'):
    main(sys.argv[1:])
#     debug_ms()
#    backfill()

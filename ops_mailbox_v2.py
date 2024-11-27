import activate_env
import os
import sys
import datetime
import imaplib
import pandas as pd
import numpy as np
import email as e
import re
from email.headerregistry import DateHeader
import win32com
import time
# import bs4
from io import StringIO
from io import BytesIO
from openpyxl import load_workbook

global basepath
basepath = "S:/Operations/Workflow"
code_path = r'S:\\Operations\Workflow\zim_ops'
sub_code_path = r'S:\\Operations\Workflow\tradefillsmacro'
sys.path.append(code_path)
sys.path.append(sub_code_path)
import ops_utils as ou
import win32com.client as win32

# Enter the brokers you want to seek for recap here
brokers_list = [
                'GS',
                'MS', 
                'BOAML', 
                'JPM', 
                'UBS',
                'HAIT', 
                'CICC'
                ]

debug = False

class email_bulk:
    
    def __init__(self, broker, mode):
        self.broker = broker 
        if debug:
            self.date = datetime.date(2024,11,14)
        elif mode == 'report':
            self.date = (datetime.datetime.now().date() - pd.tseries.offsets.BDay(1)).date() # Set to last business day
            # self.date = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
        else:
            self.date = datetime.datetime.now().date()
            # self.date = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
    
    def update_body(self, title, dest, broker, keywords=None, market=None):
        self.title = title
        self.dest = dest
        self.keywords = keywords
        self.market = market
        
    def get_destpath(self, basepath, debug=False):
        if debug:
            print('Debugging...')
            self.destpath = basepath + self.dest + 'test' 
        else:
            self.destpath = basepath + self.dest + str(self.date).replace('-','') 
            
class attachment:
    def __init__(self):
        self.placing = True
        self.skipping = False
        
    def ms_recall_file(self):
        """
        Description: Get recall file for Morgan Stanley 
        

        """
        # if self.xlsfilename.endswith('- recall.xls') or self.xlsfilename.endswith('- market trade.xls'):
        #     print(f"[DEBUG] File already amended: {self.xlsfilename}. Skipping amendment.")
        #     return
        
        df = pd.read_excel(self.xlsfilename)
        try:
            if df['Commission Charge'].astype(float)[0] < 0 : 
                ms_filename = self.xlsfilename[:-4] + ' - recall.xls'
                print('MS CN Recall recap allocating...')
            else:
                ms_filename = self.xlsfilename[:-4] + ' - market trade.xls'
                print('MS CN QFII recap allocating...')
            if self.placing and os.path.exists(self.xlsfilename):
                os.remove(self.xlsfilename)
                with open(ms_filename, "wb+") as f : 
                    f.write(self.xlsbytes)
                    f.close()
        except:
            print('MS file amendment fail!!!!!!!!!!!! \n pls manually modify file:', self.xlsfilename)
            
    def jpm_account_fill(self):
        df = pd.read_excel(self.xlsfilename)
        df["SLE"] = df["SLE"].fillna("ZEN_NHL")
        df.to_excel(self.xlsfilename[:-4] + '_amended.xls', index = False)
             
    def Remove_password_xlsx(self):
        print(self.xlsfilename)
        call_vba_func('Module 1.SaveUnencryptedFile', '1_distri - 20201112', vba_param=None)



    def delisting(self, mode):
        if (
        (os.path.exists(self.xlsfilename[:-4] + ' - recall.xls') and mode == 'auto')
        or (os.path.exists(self.xlsfilename[:-4] + ' - market trade.xls') and mode == 'auto')
        or (os.path.exists(self.xlsfilename[:-4] + '_amended.xls') and mode == 'auto')
        or (os.path.exists(self.xlsfilename) and mode == 'auto')  
        ):
            self.placing = False #place = override
        
        elif (
            (self.title.find('(ZENTIFIC) BofA Trade Recaps') > -1 and self.title.find('All') > -1)
        or (self.title.find('NTFCINVT (DTM) CONFIRM') > -1 and self.filename[-4:].lower() == '.xls')
        or (self.title.find('TRADE CONFIRMATION FILE') > -1 and self.filename.find('GIVE_UP_FILE') > -1)
        or (self.title.find('C31283') > -1 and self.filename.find('CN') > -1) 
        or (self.filename[-4:].lower() == '.pdf') 
        ):
            self.skipping = True #skipping = skip the emails
            
        if (os.path.exists(self.xlsfilename)) and (mode == 'manual') :
            os.remove(self.xlsfilename)
    

    def date_filter(self, date = datetime.datetime.now().date()):
        date_str1 = date.strftime("%Y%m%d")
        date_str2 = date.strftime("%d%b%Y")
        date_str3 = date.strftime("%m%d%y")
        tmp_name = re.sub(r'[-/:\r\n]','', self.title + self.filename)
        if  tmp_name.find(date_str1) == -1 and tmp_name.find(date_str2) == -1 and tmp_name.find(date_str3) == -1 and tmp_name.find('C31283') == -1:
            self.skipping = True       
                
def ops_zim_mailbox(mail_line, mail, imapsession, mode='auto') : 
    # mode : {auto: auto T0 using, drag the latest recaps in each side}; {manual: click buttom to trigger, drap all the recaps and over write the same files }
    # loop through all expected emails for today
    # this_row = emails.iloc[0]
    # if mail_line['dest'] == '/zen_sea/Trades/':
    #     print(mail_line)
    mail.update_body(mail_line['title'], mail_line['dest'], mail_line['broker'], market=mail_line['market'])
    mail.get_destpath(basepath, debug)
    today = mail.date
    # today = datetime.date(2024, 11, 10)
    
    # find emails with title and arrived today 
    if not isinstance(mail.market, str):
        search_str = '(SINCE "'+ today.strftime("%d-%b-%Y") +'" SUBJECT "' + mail.title + '")'
    elif mail.market == 'IN':
        search_str = '(SINCE "'+ today.strftime("%d-%b-%Y") +'" SUBJECT "' + mail.title + '")'
    else:
        search_str = '(SINCE "'+ today.strftime("%d-%b-%Y") +'" SUBJECT "' + mail.title + '" SUBJECT "' + mail.market + '")'
    result, maillist = imapsession.search(None, search_str)
    # get latest one
    ids = maillist[0].split()
    if len(ids) == 0:
        return
        
    part = attachment()    
    if mode == 'manual':               
        for this_id in ids:   
            mailbox_processing(this_id, imapsession, mail, part, mode)  
    else:
        for this_id in reversed(ids): 
            if part.placing :
                mailbox_processing(this_id, imapsession, mail, part, mode)

def ops_zim_mailbox_report(mail_line, mail, imapsession) : 
    # mode : {auto: auto T0 using, drag the latest recaps in each side}; {manual: click buttom to trigger, drap all the recaps and over write the same files }
    # loop through all expected emails for today
    # this_row = emails.iloc[0]
    mail.update_body(mail_line['title'], mail_line['dest'], mail_line['broker'], keywords=mail_line['keywords'])
    mail.get_destpath(basepath, debug)
    today = mail.date
    
    # find emails with title and arrived today 
    if isinstance(mail.keywords, str):
        search_str = '(SINCE "'+ today.strftime("%d-%b-%Y") +'" SUBJECT "' + mail.title + '" BODY "' + mail.keywords + '")'
    else:
        search_str = '(SINCE "'+ today.strftime("%d-%b-%Y") +'" SUBJECT "' + mail.title + '")'
    result, maillist = imapsession.search(None, search_str)
    # get latest one
    ids = maillist[0].split()
    if len(ids) == 0:
        print('non recap')
        return
    
    part = attachment()        
    for this_id in reversed(ids): 
        mailbox_processing(this_id, imapsession, mail, part, mode='report')

def mailbox_processing(this_id, imapsession, mail, part, mode):


    def clean_up_older_versions(new_file_path,suffix_filter=None):
        """
        Compare the new file with existing files in the folder and delete older versions.
        Args:
            new_file_path: Full path of the new file just saved into the folder.
        """
        folder_path = os.path.dirname(new_file_path)
        new_file_name = os.path.basename(new_file_path)
        core_name_match = re.match(r"(.*)_\d+\.\w+$", new_file_name)  
        if not core_name_match:
            # print("[DEBUG] Unable to extract core name for file: {}".format(new_file_name))
            return

        core_name = core_name_match.group(1)
        # print("[DEBUG] Core filename for comparison: {}".format(core_name))

        # List all files in the folder that match the core filename
        existing_files = [
            f for f in os.listdir(folder_path)
            if f.startswith(core_name) and re.search(r"_(\d+)\.\w+$", f) and
            (suffix_filter is None or suffix_filter in f) 
        ]

        # Extract version numbers from the filenames
        file_versions = []
        # import pdb; pdb.set_trace()
        for file in existing_files:
            if suffix_filter:
                # Find the version number by slicing from the back until a space
                parts = file.rsplit(" ", 1)  # Split at the last space
                if len(parts) > 1 and re.match(r"_(\d+)", parts[1]):
                    version = int(parts[1].lstrip("_").split(".")[0])  # Extract version before file extension
                    file_versions.append((file, version))
                    print("[DEBUG] Found version {} in file: {}".format(version, file))
            else:
                # Use regex to extract version number
                version_match = re.search(r"_(\d+)\.\w+$", file)
                if version_match:
                    version = int(version_match.group(1))
                    file_versions.append((file, version))
                    # print("Found version {} in file: \n{}".format(version, file))

        # Determine the highest version
        if not file_versions:
            print("[DEBUG] No matching versions found for {}.".format(core_name))
            return

        file_versions.sort(key=lambda x: x[1], reverse=True)  # Sort by version descending
        highest_version = file_versions[0][1]
        # print("Highest version for \n{}: {}".format(core_name, highest_version))

        # Delete files with lower versions
        for file, version in file_versions:
            if version < highest_version:
                file_to_delete = os.path.join(folder_path, file)
                print("Deleting older version: \n{}".format(file_to_delete))
                os.remove(file_to_delete)
    '''
    Description: Main function of this script: to get recap files from emails in broker
    '''
    result,messageparts = imapsession.fetch(this_id.decode(),'(RFC822)')
    msg = e.message_from_bytes(messageparts[0][1])
    datestr = msg["Date"]
    part.title = msg.get('subject')
    kwds = {}   # This dict is modified in-place
    DateHeader.parse(datestr, kwds)
    mail_date = kwds['datetime']
    mail_date= mail_date.astimezone(tz=datetime.timezone.utc)
    mail_time = kwds['datetime']
    mail_time = mail_time.astimezone(tz=datetime.timezone.utc)

    for this_part in msg.get_payload(decode=False) : 
        try:
            
            if 'xls' in this_part['Content-Type'] or 'application' in this_part['Content-Type'] or 'csv' in this_part['Content-Type']:
                part.skipping = False
                part.xlsbytes = this_part.get_payload(decode=True)
                part.filename = this_part.get_filename()
                part.xlsfilename = mail.destpath + '/' + mail.broker + "/" + re.sub(r'[-/:\r\n]','', part.title) + " - " + re.sub(r'[-/:\r\n]','', part.filename)
                # print("Processing part with Content-Type: {}".format(this_part['Content-Type']))
                # print("Attachment filename: {}".format(this_part.get_filename()))

                if part.title.find('UBS EOD Report') > -1:
                    part.replace_header('Content-Type', 'application/octet-stream')

                if mode!='report' :
                    part.date_filter(mail.date)
            try:

                if part.title.find('Swaps Execution Report - ZENTIFIC') > -1: # JPM filename contain timestamp, need to clear out
                    part.filename = re.sub(r'[0-9]+', '', part.filename)
                    part.xlsfilename = mail.destpath + '/' + mail.broker + "/" + re.sub(r'[-/:\r\n]','', part.title) + " - " + re.sub(r'[-/:\r\n]','', part.filename)
                if part.title.find('(ZENTIFIC) BofA Trade Recaps') > -1: # BOAML filename contain date when revised, need to clear out
                    datestr = mail.date.strftime("%d%b%Y")
                    part.xlsfilename = part.xlsfilename.replace(' ' + datestr, '')
                # files delisting
                part.delisting(mode)
                # print("[DEBUG] part.placing: {}, part.skipping: {}".format(part.placing, part.skipping))


                if part.placing and (not part.skipping):

                    with open(part.xlsfilename, "wb+") as f : 
                        f.write(part.xlsbytes)
                        f.close()
                        print('...')
                        if mode == 'auto':
                            part.placing = False
                        
                            
                        if part.filename.find('Pre allocation China Stocks') > -1 or part.filename.find('Pre_allocation_China_Stocks') > -1: # MS fill CN and recall in the files with same format
                            part.placing=True
                            part.ms_recall_file()
                            clean_up_older_versions(part.xlsfilename,suffix_filter="- market trade")
                            
                            # if part.title.find('Swaps Execution Report - ZENTIFIC') > -1: # JPM filename to fill ZEN_NHL
#                            part.filename = part.jpm_account_fill()
                        
                        if (os.path.exists(part.xlsfilename[:-4] + '_amended.xls') == True): # special file amended rule
                            os.remove(part.xlsfilename)                             
                        
                        if part.title.find('TRADE CONFIRMATION FILE') > -1 and part.filename.find('TRADE_CONFIRMS') > -1 : # India file unprotect process
                            try:
                                part.Remove_password_xlsx()
                                print('GS India trade recap in, auto unencrypted!')
                            except:
                                print('Fail to unencrypt GS India trade recap! Plsease manually open and unencrypt')
                    clean_up_older_versions(part.xlsfilename)
                        
                    
                    
            except:
                pass
        except:
            pass

def call_vba_func(func, excel_name, vba_param=None): 
    # This function is not ready
    try:
        # Deal with AttributeError("%s.%s" % (self._username_, attr))
        # caused by the Excel application is open (possibly hidden) and it is in a modal loop such as
        # editing a cell or an open/save file dialog
        retry_count = 0
        while retry_count < 6:
            try:
                xl = win32com.client.Dispatch("Excel.Application")
                if vba_param is None:
                    #xl.Application.Run("'S:\\Operations\\Workflow\\Bloomberg files\\{}.xlsm'!{}".format(params['excel_name'],func))
                    xl.Application.Run(
                        "'{}.xlsm'!{}".format(excel_name, func))
                else:
                    # xl.Application.Run("'S:\\Operations\\Workflow\\Bloomberg files\\{}.xlsm'!{}".format(params['excel_name'], func), vba_param)
                    xl.Application.Run(
                        "'{}.xlsm'!{}".format(excel_name, func),
                        vba_param)
                del xl
                return
            except AttributeError:
                # Retry five times. Raise error and stop program.
                if retry_count < 6:
                    retry_count = retry_count + 1
                    time.sleep(5)
                    pass
                else:
                    os.system("pause")
                    sys.exit()
    except KeyError:
        # KeyError is raised when the target file is not opening
        os.system("pause")
        sys.exit()
    except:
        os.system("pause")
        sys.exit()
        
def folder_initialize(account):
    #Setting up paths for different funds, and set up borker files inside
    account_src_map = {'zen_mst': ['HAIT', 'BOAML', 'GS', 'CICC', 'Bloomberg'],
                       'zen_cng': ['CICC', 'Bloomberg'],
                       'zen_cnl': ['Bloomberg'],
                       'zen_nht': ['Bloomberg'],
                       'zen_sea':['Bloomberg', 'MS', 'BOAML', 'GS', 'UBS', 'JPM'],
                       'zen_agg': ['MS', 'BOAML', 'GS', 'JPM', 'UBS']}
    if account == 'zen_mst':
        destpath = basepath + '/Trades/' + str(datetime.datetime.now().date()).replace('-','')
    else:
        destpath = basepath + '/' + account + '/Trades/' + str(datetime.datetime.now().date()).replace('-','')
    brokers = account_src_map[account]
    if not os.path.exists(destpath ): 
        os.mkdir(destpath)
        print("NEW folders for {0} created!".format(account))
    for broker in brokers:
        if not os.path.exists(destpath + '/' + broker): 
            os.mkdir(destpath + '/' + broker)
            print("NEW folders for {0} created!".format(broker))
            
def imap_initialize():
    # get mail server account
    imaplib._MAXLINE = 60000
    # imapsession = imaplib.IMAP4_SSL(account['web_site'])
    # imapsession.login(account['username'], account['password'])
    try:
        account =ou.get_web_account('zimimap|ops_zim') # Need to assess ZIM05
        imapsession = ou.imap_auth_o365(username=account['username'], password=account['password'])
    except:
        account =ou.get_web_account('zimimap|investment_team1_zim') # Backup for link to ZIM14
        imapsession = ou.imap_auth_o365(username=account['username'], password=account['password'])
        print("Using backup IMAP account")
    imapsession.select('"INBOX"', readonly=True)
    return imapsession
    
def main(argv):
    mode = 'auto'
    if len(argv) > 0 :
        print ("called with " + str(len(argv)) + " paramenters " + argv[0])
        mode = argv[0]
        
    if mode != 'report' and not debug:
        for account in ['zen_mst', 'zen_cng', 'zen_nht', 'zen_sea', 'zen_agg']:
            folder_initialize(account) # create the folder paths
            
    imapsession = imap_initialize() 
    
    for broker in brokers_list:
        if mode == 'report':
            emails = pd.read_excel(code_path + '/ops_mail_details.xlsx', sheet_name = 'report')
        else:
            emails = pd.read_excel(code_path + '/ops_mail_details.xlsx', sheet_name = 'recap')
        emails = emails.loc[emails['broker'] == broker]
        print('Dragging recap from: ', broker)
        mail = email_bulk(broker, mode)
        for i, this_row in emails.iterrows() :
            if mode == 'report':
                ops_zim_mailbox_report(this_row, mail, imapsession)
            else:
                ops_zim_mailbox(this_row, mail, imapsession, mode)
        
if __name__ == "__main__":
    main(sys.argv[1:])
    

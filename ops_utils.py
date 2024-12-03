try:
    import pyodbc as p
except:
    raise Exception(
        'Install pyodbc from: s:\installers\pyodbc-3.0.7.win-amd64-py3.4.exe')
import pandas as pd
import numpy as np
import csv
import random
import os
import errno
import shutil
import time
import datetime
from pandas.tseries.offsets import BDay
from scipy.stats.stats import pearsonr
import urllib.request
import warnings
warnings.filterwarnings('ignore', message='.*pandas only support SQLAlchemy.*')
warnings.filterwarnings(
    'ignore', message='.*pandas only supports SQLAlchemy.*')

# Get the ODBC connection (cursor) for a named database
# Currently DB connection stringsare here - could move these to a config file?
__db_connection__ = None
__db_connect_time__ = datetime.datetime.now()-pd.offsets.Day(1)
__db_connect_timeout__ = 3 * 60 * 60 # timeout in seconds
__db_reuse__ = True


def get_db_conn(alias='zim_ops', reuse=None):
    global __db_connection__, __db_reuse__, __db_connect_time__, __db_connect_timeout__
    if reuse is None:
        reuse = __db_reuse__
    # check if the connection has been alive for more than time out  - if so close and create a new one
    if (__db_connection__  is not None and __db_connect_time__ < (datetime.datetime.now() - pd.offsets.Second(__db_connect_timeout__))):
        reset_db_conn()
    if __db_connection__  is not None and reuse==True :
        this_connection = __db_connection__
    else :
        conn_str = ""
        try:
            # override string to use a specific DB cnnection
            path = os.path.expanduser("~") + '/Documents/'
            if (os.path.isfile(path + "/connect_string.txt") == True):
                with open(path + "/connect_string.txt", 'r') as file:
                    conn_str = file.read().replace('\n', '')
        except Exception:
            pass
        if conn_str == "":
            # Production db
            if alias == 'zim_prod':
                # Create the connection using the connstring
                conn_str = 'Driver={ODBC Driver 17 for SQL Server};SERVER=zimsqlhk.options-it.com;DATABASE=zimdb;Trusted_Connection=yes'
                # Development db - not set up yet
            elif alias == 'zim_dev':
                # Create the connection using the connstring
                conn_str = 'Driver={ODBC Driver 17 for SQL Server};SERVER=zimsqlhk.options-it.com;DATABASE=zimdb_dev;Trusted_Connection=yes'
            elif alias == 'zim_ops':
                # Create the connection using the connstring
                conn_str = 'Driver={ODBC Driver 17 for SQL Server};SERVER=zimsqlhk.options-it.com;DATABASE=zimdb_ops;Trusted_Connection=yes'
        # over-ride connection string
        this_connection = p.connect(conn_str)
        exec_sql(sql='SET ARITHABORT ON;', conn=this_connection, isselect=False)
    if (__db_connection__  is None and reuse==True) :
        __db_connection__ = this_connection
        __db_connect_time__ = datetime.datetime.now()
    return this_connection


def reset_db_conn():
    global __db_connection__, __db_connect_time__
    if __db_connection__ is not None:
        __db_connection__.close()
        __db_connection__ = None
        __db_connect_time__ = datetime.datetime.now()-pd.offsets.Day(1)

# Executes sql on a connection (cursor) - creates the cursor itself so no need
# for separately creating a connection, just give an alias


def exec_sql(sql, isselect=1, alias='zim_ops', check_sql=False, conn=None):
    # Create a cursor for the db in use
    if conn is None:
        conn = get_db_conn(alias)
    # Check whether it is necessary to commit changes (insert/update/delete) or
    # return the records (select)
    df = None
    if isselect == 1:
        # Use pandas to return straight to a dataframe
        success = False
        max_attempts = 5
        while not success:
            try:
                chunksize = None  # 5_000_000
                if chunksize is not None:
                    df = pd.concat(pd.io.sql.read_sql(
                        sql, conn, chunksize=chunksize))
                else:
                    df = pd.io.sql.read_sql(sql, conn)
                success = True
            except p.Error as e:
                message = str(e)
                if ('has been chosen as the deadlock victim. Rerun the transaction.' in message) and (max_attempts > 0):
                    max_attempts = max_attempts - 1
                    time.sleep(3)
                elif (conn == __db_connection__) and ('Communication link failure (' in message) and (max_attempts > 0):
                    reset_db_conn()
                    conn = None
                    conn = get_db_conn(alias)
                    max_attempts = max_attempts - 1
                else:
                    raise
        # For select fetch all the row data and the column names
        # data = curs.fetchall()
        # cols = [d[0] for d in curs.description]
        # Return the data in a pandas data frame
        # df = pd.DataFrame(data=data, columns=cols)
    else:
        # Execute sql
        success = False
        max_attempts = 5
        while not success:
            try:
                with conn.cursor() as curs:
                    curs.execute(sql)
                    # Commit the sql - required for insert/update/delete
                    conn.commit()
                    success = True
                    # Return the number of rows changed
                    df = curs.rowcount
            except p.Error as e:
                message = str(e)
                if ('has been chosen as the deadlock victim. Rerun the transaction.' in message) and (max_attempts > 0):
                    max_attempts = max_attempts - 1
                    time.sleep(3)
                else:
                    raise
    return df


def sql_table_exists(table, db='', alias='zim_ops'):
    if db == '':
        tmp = exec_sql("SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" +
                       table+"'", alias=alias).iloc[0, 0] == 1
    else:
        tmp = exec_sql("SELECT count(*) FROM " + db +
                       ".INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '"+table+"'", alias=alias).iloc[0, 0] == 1
    return tmp


def drop_table_if_exists(table, alias='zim_ops'):
    # Check if table exists, and if so drop it
    # dropsql = "IF (EXISTS (SELECT * " + \
    #       "            FROM INFORMATION_SCHEMA.TABLES " + \
    #       "            WHERE TABLE_NAME = '" + table + "') " + \
    #       "BEGIN " + \
    #       "    DROP TABLE " + table + " " + \
    #       "END"
    try:
        dropsql = "DROP TABLE " + table
        # Execute the drop
        result = exec_sql(dropsql, isselect=0, alias=alias)
        return result
    except:
        print("can't find " + table, flush=True)


def merge_df_list(dflist, merge_type):
    """
    Merges a list of DataFrames based on specified merge type

    :param dflist: List of DataFrames to merge
    :param merge_type: 'v'/'vertical' for stacking, 'h'/'horizontal' for joining on index
    :return: pandas.DataFrame
    """
    if not dflist:
        return pd.DataFrame()

    merge_type = merge_type.lower()[0]  # Take first letter of input
    if merge_type not in ['v', 'h']:
        raise ValueError("merge_type must be 'vertical'/'v' or 'horizontal'/'h'")

    if merge_type == 'v':
        return pd.concat(dflist, axis=0, ignore_index=True)

    df = dflist[0]
    for input_df in dflist[1:]:
        df = pd.merge(df, input_df, how='outer',
                      left_index=True, right_index=True)
    return df


def sql_table_cols(table, db='', alias='', details=False, conn=None):
    tmp = exec_sql("select column_name " + (",data_type,character_maximum_length" if details else "") + " from " + ('' if db ==
                   '' or db is None else db + ".") + "information_schema.columns where TABLE_NAME= '"+table+"' order by ORDINAL_POSITION", alias=alias, conn=conn)
    if len(tmp.columns) == 1:
        tmp = list(tmp.iloc[:, 0])
    else:
        tmp.columns = ['name', 'type', 'len']
    return tmp

# Wrapper for bulk insert that simplifies the load and checks load was successful
# Index is not loaded to table. Use .reset_index() prior to sending if index columns are to be kept


def load_to_table(df, table, db=None, index=False, keepnulls=True, pathloc=None, verbose=False, encoding=None, step_size=1e100, pre_sql_cmd='', printrows=False):
    pathloc = activate_env.sql_tmp
    # match the columns of the df to the table
    tmp = sql_table_cols(table, db=db)
    for this_col in set(tmp) - set(df.reset_index().columns):
        df[this_col] = np.nan
    tmp_df = df.reset_index()[tmp]
    # Create a random file name
    randname = 'xxx_' + datetime.datetime.now().strftime('%Y%m%d%H%M') + '_' + \
        str(round(random.random()*1e15))
    # Use this location for a temp file
    fileloc = pathloc + randname + '.csv'
    # do it in row steps specified
    try:
        pos = 0
        rowcount = 0
        while pos < len(tmp_df):
            pos_next = min(pos + step_size, len(tmp_df))
            # Save to csv with no index (due to index reset, index is just ints)
            if encoding:
                tmp_df.iloc[pos:pos_next].to_csv(
                    fileloc, index=False, encoding=encoding)
            else:
                tmp_df.iloc[pos:pos_next].to_csv(fileloc, index=False)
            # Run the bulk insert
            if db is not None and db != '':
                tmp_db_name = db + ".." + table
            else:
                tmp_db_name = table
            sqlcmd = bulk_insert(fileloc, tmp_db_name, rterm='\\n',
                                 keepnulls=keepnulls, returnsql=True, encoding=encoding)
            randname = str(round(random.random()*1e15))
            # Prepare and execute the transaction as a whole
            sqltransaction = "begin transaction t_" + randname + "; " + \
                (('SET NOCOUNT ON; ' + pre_sql_cmd + "; SET NOCOUNT OFF; ") if pre_sql_cmd != "" else "") + \
                sqlcmd + "; " + \
                "commit transaction t_" + randname + "; "
            if verbose:
                print("AT " + str(datetime.datetime.now()) + " doing rows " +
                      str(pos) + " out of " + str(len(tmp_df)) + " SQL : " + sqltransaction)
            rowcount = rowcount + exec_sql(sqltransaction, isselect=0)
            pos = pos_next
    except:
        print('Problem file: ' + fileloc, flush=True)
        raise
    # rowcount = bulk_insert(fileloc, tmp, rterm='\\n', keepnulls=keepnulls)
    # Check that all rows were inserted
    # don't check if pre_sql_cmd used - usually a delete first, but that means the number of rows changed may not equal
    # if some rows already exist
    if rowcount != len(df) and pre_sql_cmd == '':
        print("table " + table + " expected " +
              str(len(df)) + " got " + str(rowcount))
        raise Exception('Bulk upload in load_to_table failed')
    else:
        # Remove the temporary file
        silentremove(fileloc)
    # Return count of inserted rows
    if printrows:
        print('Loaded ' + str(rowcount) + ' rows to the ' +
              table + ' table.', flush=True)
    return rowcount



def bulk_insert(fileloc, table, fterm=',', rterm='\\n', keepnulls=True, returnsql=False, encoding=None):
    # Simple function to use SQL bulk insert to fill a table
    # Replace Z:/ notation with //
    fileloc = fileloc.replace('Z:', '//zimsqlhk/sd.zentific-im.com$')
    fileloc = fileloc.replace('z:', '//zimsqlhk/sd.zentific-im.com$')
    fileloc = fileloc.replace(
        '//zimsqlhk/sd.zentific-im.com$', 'E:/Hosting/zentific-im.com/sd.zentific-im.com')
    # Prepare the sql statement with custom table, file loc and params
    sql = "BULK INSERT " + table + " "\
          "FROM '" + fileloc + "' " + \
          "WITH " + \
          "(FIRSTROW = 2, " + \
          "FIELDTERMINATOR = '" + fterm + "', " + \
          "ROWTERMINATOR = '" + rterm + "'"
    if encoding is not None and encoding in ['utf-16']:
        sql = sql + ", DATAFILETYPE = 'widechar' "
    if keepnulls:
        sql = sql + ", KEEPNULLS"
    sql = sql + ")"
    # Check what the needs to be sent back
    if returnsql:
        return sql
    else:
        # Send the sql to the DB
        result = exec_sql(sql + ";", isselect=0)
        # Return the result for checking
        return result


def create_index_on_table(table):
    # Create index on the table given
    # If a weekly or monthly table, include the forward returns
    if table in ['insight_weekly']:
        sqlinclude = "INCLUDE ([fwdret_total_1w],[fwdret_total_1w_1dplus])"
    elif table in ['insight_monthly']:
        sqlinclude = "INCLUDE ([fwdret_total_1m],[fwdret_total_1m_1dplus])"
    else:
        sqlinclude = ''
    # Create the main stats and index
    sql = "CREATE STATISTICS [_stats_" + table + "_date] ON [dbo].[" + table + "]([date]) " + \
          "CREATE NONCLUSTERED INDEX [_index_" + table + "_nonclustered] ON [dbo].[" + table + "] " + \
        "([date] ASC, [sedol] ASC) " + \
        sqlinclude + " " + \
        "WITH (SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY] "
    # Submit for creation
    print('Starting create index on ' + table + '...', flush=True)
    result = exec_sql(sql, isselect=0)
    return result


def create_clustered_index_on_table(table):
    # Create index on the table given
    # If a weekly or monthly table, include the forward returns
    if table in ['universe_alpha']:
        sqlextracols = ", [univ_tradeable] DESC"
    else:
        sqlextracols = ''
    # Create the main stats and index
    sql = "CREATE STATISTICS [_stats_" + table + "_date] ON [dbo].[" + table + "]([date]) " + \
          "CREATE CLUSTERED INDEX [_index_" + table + "_clustered] ON [dbo].[" + table + "] " + \
        "([date] ASC, [sedol] ASC " + \
        sqlextracols + ") " + \
        "WITH (SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY] "
    # Submit for creation
    print('Starting create clustered index on ' + table + '...', flush=True)
    result = exec_sql(sql, isselect=0)
    return result


def drop_index_on_table(table):
    # Drop index and stats on the table given
    sql = "DROP STATISTICS [" + table + "].[_stats_" + table + "_date] " + \
          "DROP INDEX [_index_" + table + \
        "_nonclustered] ON [dbo].[" + table + "] "
    # Submit for creation
    print('Starting drop index on ' + table + '...', flush=True)
    result = exec_sql(sql, isselect=0)
    return result


# Returns the indices of the iterable that are true
# Equivalent to the matlab find() function
def findi(inputtf):
    # Check input
    if type(inputtf) in [pd.DataFrame, pd.Series]:
        inputtf = inputtf.values.tolist()
    elif type(inputtf) in [np.array]:
        inputtf = inputtf.tolist()
    # Find indices
    return [i for (i, val) in enumerate(inputtf) if val is True]


# Remove a file without complaining if the file doesn't exist
def silentremove(filename):
    try:
        os.remove(filename)
    except OSError as e:  # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT:  # errno.ENOENT = no such file or directory
            raise  # re-raise exception if a different error occured

    # Remove a file without complaining if the file doesn't exist


def silentremovedir(dirname):
    try:
        shutil.rmtree(dirname)
    except OSError as e:  # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT:
            raise  # re-raise exception if a different error occured


# Calculate checksum and convert 6 digit sedol to 7 digit
def create7digitsedol(sedols6input):
    # Check input is a list
    usedf = False
    if type(sedols6input) == str:
        sedols6 = [sedols6input]
    elif type(sedols6input) == pd.DataFrame:
        usedf = True
        sedols6 = sedols6input.iloc[:, 0].values.tolist()
    elif type(sedols6input) == pd.Series:
        sedols6 = sedols6input.values.tolist()
    else:
        sedols6 = sedols6input
    # Helper to convert characters to numbers

    def char2value(c):
        assert c not in 'AEIOU', "No vowels"
        return int(c, 36)
    # Weights for each digit in the sedol
    sedolweight = [1, 3, 1, 7, 3, 9]
    # Helper function to calculate the checksum digit

    def checksum(sedol):
        tmp = sum(map(lambda ch, weight: char2value(
            ch) * weight, sedol, sedolweight))
        return str((10 - (tmp % 10)) % 10)
    # Calculate checksums and return 7digit sedols
    try:
        sedols7vals = [x if x.startswith(
            '_') else x + checksum(x) for x in sedols6]
    except:
        sedols7vals = sedols6
    # Format and return
    if usedf:
        sedols7 = pd.DataFrame(
            sedols7vals, index=sedols6input.index, columns=sedols6input.columns)
    else:
        sedols7 = sedols7vals
    return sedols7


"""
return 0 means it fill up all the column on toTable.
       1 means it fill up part of toTable and the one missing is filled with NULL
"""


def inserttable2table(fromtable, totable):

    toheadersql = "select column_name from information_schema.columns where table_name = '" + totable + "' "
    toheaders = exec_sql(toheadersql).values.flatten().tolist()

    print("to Table cols: " + str(toheaders))

    fromheadersql = "select column_name from information_schema.columns where table_name = '" + fromtable + "' "
    fromheaders = exec_sql(fromheadersql).values.flatten().tolist()

    print("from Table cols: " + str(fromtable))

    # the to headers is always less the from headers.
    if set(toheaders).issubset(fromheaders):
        print(totable + " has less or same columns as " + fromtable)

        tocols = ','.join(toheaders)
        fromcols = ','.join(["t1." + col for col in toheaders])

        insertsql = "INSERT INTO " + totable + '(' + tocols + ')' + \
                    " SELECT " + fromcols + " FROM " + fromtable + " t1" +\
                    " WHERE NOT EXISTS (SELECT date from " + \
            totable + " t2 where t2.date = t1.date)"

        print("sql: " + insertsql)
        exec_sql(insertsql + ";", isselect=0)
    else:
        print(fromtable + "'s header has too few column to insert into " +
              totable + " filling it with null")

        tocols = ','.join(toheaders)
        fromcols = ','.join(
            [['null as '+col, 't1.'+col][col in fromheaders] for col in toheaders])

#        nullheader = set(toheaders).difference(fromheaders)
#        nullcols = ["null as " + col for col in nullheader]
#        nullcols = ",".join(nullcols)
#        cols = cols + "," + nullcols

        insertsql = "INSERT INTO " + totable + '(' + tocols + ')' + \
                    " SELECT " + fromcols + " FROM " + fromtable + " t1" +\
                    " WHERE NOT EXISTS (SELECT date from " + \
            totable + " t2 where t2.date = t1.date)"

        print("sql: " + insertsql)
        exec_sql(insertsql + ";", isselect=0)

# Function to clean up archived tables


def archive_table_cleanup(table, num_to_archive=5, db='zimdb_ops'):
    # Get the list of tables too old and drop
    tmp = exec_sql("select table_name from " + db +
                   ".information_schema.tables where substring(table_name,1,len('xxx_' + '"+table+"_')) = 'xxx_' + '"+table+"_'")
    tmp['pos'] = [x.replace('xxx_'+table+'_', '') for x in tmp.table_name]
    tmp = tmp.loc[[x.isdigit() for x in tmp.pos]]
    tmp['pos'] = [int(x) for x in tmp.pos]
    tmp = list(sort_dataframe(tmp,columns=['pos'])['table_name'])
    result = []
    if len(tmp) > num_to_archive:
        droptables = tmp[:(len(tmp)-num_to_archive)]
        result = [drop_table_if_exists("["+db+"]..["+x+"]")
                  for x in droptables]
    return result

# Function to archive a daily table


def archive_table(table, num_to_archive=5, db='zimdb_ops', archivedate=None, datefield='date'):
    # Get the date in the table for archiving
    archive_table_cleanup(table, num_to_archive, db=db)
    if archivedate is None:
        archivedate = exec_sql(
            "select max("+datefield+") from " + db + ".." +table).values.flatten()[0]
    result = None
    if archivedate is not None:
        archivetable = 'xxx_' + table + '_' + \
            str(datetime.datetime.strftime(archivedate, '%Y%m%d'))
        time.sleep(1)
        tmp = list(exec_sql("select table_name from " + db +
                   ".information_schema.tables where substring(table_name,1,len('xxx_' + '"+table+"_')) = 'xxx_' + '"+table+"_'")['table_name'])
        if archivetable in tmp:
            drop_table_if_exists("["+db+"]..["+archivetable+"]")
        # Select into the archive
        result = exec_sql('select * into ' + db + ".." +
                          archivetable + ' from ' + db + ".." + table, isselect=0)
    return result


# Helper function
def cum_pct(x):
    y = x.copy()
    y.sort()
    y = y[::-1].cumsum()
    y = y/y.max()
    return y[x.index]


# A quick timer object for simple timing jobs
# Usage:
# with timer('z'):
#    np.rand(500000)
class timer(object):
    def __init__(self, name=None):
        self.name = name

    def __enter__(self):
        self.tstart = time.time()

    def __exit__(self, type, value, traceback):
        if self.name:
            txt = ' (' + self.name + ')'
        else:
            txt = ''
        print('Elapsed' + txt + ': ' +
              '{:.2f}'.format(time.time() - self.tstart) + ' seconds', flush=True)


# code to map the various tickers into one common 6 digit sedol

def get_hist_ticker_map(start_date):
    ticker_map = exec_sql("select sedol, bb_code, AxiomaTicker, [local], factset_ticker, isin, AxiomaID, name, country_exchange, AxiomaCountry, gics_code, bb_code_short, sedol_short, bb_code_long, sedol_long, bb_code_long_alt, bb_code_short_alt, sedol_long_alt, sedol_short_alt, sedol_hist_0, sedol_hist_1, sedol_hist_2, sedol_hist_3, currency, multiplier_long, multiplier_short, fs_id_r  from zimdb_ops..ticker_map")
    if start_date is not None:
        ticker_changes = exec_sql(
"SELECT date, sedol_old AS sedol, bb_code_old AS bb_code, AxiomaTicker_old AS AxiomaTicker, local_old AS local, factset_ticker_old AS factset_ticker, isin_old AS isin, AxiomaID_old AS AxiomaID, name_old AS name, country_exchange_old AS country_exchange,              AxiomaCountry_old AS AxiomaCountry, gics_code_old AS gics_code, bb_code_short_old AS bb_code_short, sedol_short_old AS sedol_short, bb_code_long_old AS bb_code_long, sedol_long_old AS sedol_long, bb_code_short_alt_old AS bb_code_short_alt, bb_code_long_alt_old AS bb_code_long_alt, sedol_short_alt_old AS sedol_short_alt, sedol_long_alt_old AS sedol_long_alt, sedol_hist_0_old AS sedol_hist_0, sedol_hist_1_old AS sedol_hist_1, sedol_hist_2_old AS sedol_hist_2, sedol_hist_3_old AS sedol_hist_3, currency_old AS currency, multiplier_long_old as multiplier_long, multiplier_short_old as multiplier_short, fs_id_r_old as fs_id_r from zimdb_ops..ticker_changes where [date] > '" + str(start_date) + "'")
        ticker_changes = sort_dataframe(
            data=ticker_changes, columns=['date'], ascending=True)
        ticker_changes.drop_duplicates('bb_code', inplace=True)
        ticker_changes.drop_duplicates('sedol', inplace=True)
        ticker_map = ticker_changes[ticker_map.columns].append(ticker_map)
    ticker_map.drop_duplicates('bb_code', inplace=True)
    ticker_map.drop_duplicates('sedol', inplace=True)
    return ticker_map

# id_info = dataframe with columns [sedol, bb_code, isin, type]
# returns mapping of 'z_sedol', 'z_bb_code', 'sedol', 'bb_code', 'fs_id_r', 'ric', 'isin'


def map_tickers(id_info, at_date=None):
    ticker_map = get_hist_ticker_map(at_date)

    def bb_country_map(x):
        this_map = {'JT': 'JP', 'JM': 'JP', 'JS': 'JP', 'JF': 'JP', 'JI': 'JP', 'JJ': 'JP', 'JE': 'JP', 'JW': 'JP',
                    'JU': 'JP', 'IS': 'IN', 'IB': 'IN', 'AT': 'AU', 'AH': 'AU', 'KP': 'KS', 'KQ': 'KS', 'CG': 'CH',  'CS': 'CH'}
        return this_map[x] if x in this_map.keys() else x
    # for future change to fs_id_r
    ticker_map['sedol'] = ticker_map['sedol_hist_0']
    all_map = pd.DataFrame()
    for i in ['', '_long', '_short', '_long_alt', '_short_alt']:
        tmp = ticker_map[['bb_code'+i, 'sedol'+i,
                          'fs_id_r', 'isin', 'sedol', 'bb_code']].copy()
        tmp.columns = ['bb_code', 'sedol', 'fs_id_r',
                       'isin', 'z_sedol', 'z_bb_code']
        tmp['src'] = i
        filt = [(type(x) == str and x != 'NULL' and x != 'NO_ID') or (type(
y) == str and y != 'NULL' and y != 'NO_ID') for x, y in zip(tmp['bb_code'], tmp['sedol'])]
        all_map = all_map.append(tmp.loc[filt])
    # add in the India futures
    filt = [x == 'IN' and type(y) == str for x, y in zip(
        ticker_map.country_exchange, ticker_map.bb_code_short)]
    tmp = ticker_map.loc[filt, ['bb_code_short', 'sedol',
                                'fs_id_r', 'isin', 'sedol', 'bb_code_short']]
    tmp.columns = ['bb_code', 'sedol', 'fs_id_r',
                   'isin', 'z_sedol', 'z_bb_code']
    tmp['src'] = 'india'
    filt = [(type(x) == str and x != 'NULL' and x != 'NO_ID') or (type(
        y) == str and y != 'NULL' and y != 'NO_ID') for x, y in zip(tmp['bb_code'], tmp['sedol'])]
    all_map = all_map.append(tmp.loc[filt])
    all_map['bb_code'] = [x.replace(" Equity", "") if type(
        x) == str else np.nan for x in all_map['bb_code']]
    all_map['z_bb_code'] = [x.replace(" Equity", "") if type(
        x) == str else np.nan for x in all_map['z_bb_code']]
    all_map = all_map.replace("NULL", np.nan)
    # for futures convert bb_code to strip the timing of the future part of the ticker
    all_map['z_bb_code'] = [x.split("=")[
        0] + "= IS" if type(x) == str and "=" in x else x for x in all_map['z_bb_code']]
    all_map = all_map.loc[[type(x) == str for x in all_map.sedol]]

    result = id_info.copy()
    result['sedol'] = [x[:6] if type(x) == str and len(
        x) == 7 and "_" not in x else x for x in result['sedol']]
    result['sedol'] = [np.nan if type(x) != str or (type(x) == str and x in [
        'nan', 'NULL', 'NO_ID']) else x for x in result['sedol']]
    # clean up the country codes for bb_codes
    tmp = [[y.strip() for y in x.split(" ") if y != ''] if type(x)
           == str else [] for x in result['bb_code']]
    tmp = [x[:-1] if len(x) > 1 and x[-1].upper() ==
           'EQUITY' else x for x in tmp]
    tmp = [(x[0] + " " + (bb_country_map(x[1].upper()) if "=" not in x[0]
            and x[1] != 'Corp' else x[1])) if len(x) > 1 else " ".join(x) for x in tmp]
    result['bb_code'] = tmp
    result['bb_code'] = [
        x if type(x) == str and x != '' else np.nan for x in result['bb_code']]
    # to handle india names
    result['z_bb_code'] = [x.split(
        "=")[0]+"= IS" if type(x) == str and "=" in x else x for x in result['bb_code']]
    result['fs_id_r'] = np.nan
    for this_col in ['bb_code', 'sedol', 'isin', 'fs_id_r', 'z_bb_code']:
        other_cols = [x for x in ['bb_code', 'sedol',
                                  'isin', 'z_bb_code'] if x != this_col]
        for check_col in other_cols:
            filt = [type(x) == str and type(y) == str for x,
                    y in zip(all_map[this_col], all_map[check_col])]
            lookup = all_map.loc[filt].drop_duplicates(
                check_col).set_index(check_col)
            result[this_col] = [x[this_col] if type(x[this_col]) == str or type(
                x[check_col]) != str else lookup[this_col].reindex([x[check_col]])[0] for i, x in result.iterrows()]

    result['fs_id_r'] = [x if type(x) == str else y for x, y in zip(
        result['fs_id_r'], result['sedol'])]
    filt = [type(x) == str and type(y) == str for x, y in zip(
        ticker_map['sedol'], ticker_map['fs_id_r'])]
    lookup = ticker_map.loc[filt].drop_duplicates(
        'fs_id_r').set_index('fs_id_r')
    result['z_sedol'] = list(lookup['sedol'].reindex(list(result['fs_id_r'])))
    result['z_sedol'] = [x if type(x) == str else y for x, y in zip(
        result['z_sedol'], result['sedol'])]

    return result


def map_z_sedol(sedol, bb_code, isin, trade_type, at_date=None):
    ticker_map = get_hist_ticker_map(at_date)
    bb_country_map = {'JT': 'JP',
                      'JM': 'JP',
                      'JS': 'JP',
                      'JF': 'JP',
                      'JI': 'JP',
                      'JJ': 'JP',
                      'JE': 'JP',
                      'JW': 'JP',
                      'JU': 'JP',
                      'IS': 'IN',
                      'IB': 'IN',
                      'AT': 'AU',
                      'AH': 'AU',
                      'KP': 'KS',
                      'KQ': 'KS',
                      'CG': 'CH',  # Adrian 20201120: add CG/CS to map CH for JPM QFII
                      'CS': 'CH'}  # Adrian 20201120: add CG/CS to map CH for JPM QFII
    # for futures convert bb_code to strip the timing of the future part of the ticker
    ticker_map['bb_code_short'] = [x.split(
        "=")[0]+"= IS" if "=" in str(x) else str(x) for x in ticker_map['bb_code_short']]
    ticker_map['bb_code_long'] = [x.split(
        "=")[0]+"= IS" if "=" in str(x) else str(x) for x in ticker_map['bb_code_long']]
    ticker_map['bb_code_short_alt'] = [x.split(
        "=")[0]+"= IS" if "=" in str(x) else str(x) for x in ticker_map['bb_code_short_alt']]
    ticker_map['bb_code_long_alt'] = [x.split(
        "=")[0]+"= IS" if "=" in str(x) else str(x) for x in ticker_map['bb_code_long_alt']]
    # for future change to fs_id_r
    ticker_map['sedol'] = ticker_map['sedol_hist_0']
    # create a sedol to bb_code map
    all_map = pd.DataFrame()
    for i in ['', '_long', '_short', '_long_alt', '_short_alt']:
        tmp = ticker_map[['bb_code'+i, 'sedol'+i]]
        tmp.columns = ['bb_code', 'sedol']
        all_map = all_map.append(tmp)
    all_map['bb_code'] = [x.replace(" Equity", "") if type(
        x) == str else np.nan for x in all_map['bb_code']]
    all_map = all_map.drop_duplicates('sedol')
    all_map = all_map.loc[[type(x) == str for x in all_map.sedol]]
    all_map = all_map.set_index('sedol').sort_index()['bb_code']
    # create a secondary to primary z_sedols mapping
    parent_map = pd.DataFrame()
    for i in ['', '_long', '_short', '_long_alt', '_short_alt']:
        tmp = ticker_map[['bb_code'+i, 'sedol'+i, 'sedol', 'fs_id_r']]
        tmp.columns = ['bb_code', 'sedol', 'z_sedol', 'fs_id_r']
        parent_map = parent_map.append(tmp)
    parent_map['bb_code'] = [x.replace(" Equity", "") if type(
        x) == str else np.nan for x in parent_map['bb_code']]
    par_bb_map = parent_map.loc[[type(x) == str for x in parent_map['bb_code']]].drop_duplicates(
        'bb_code').set_index('bb_code').sort_index()[['z_sedol', 'fs_id_r']]
    par_sedol_map = parent_map.loc[[type(x) == str for x in parent_map['sedol']]].drop_duplicates(
        'sedol').set_index('sedol').sort_index()[['z_sedol', 'fs_id_r']]

    bb_code = [x.split("=")[0]+"= IS" if "=" in str(x) else str(x)
               for x in bb_code]
    for this_key in bb_country_map.keys():
        this_val = bb_country_map[this_key]
        bb_code = [x[:-2] + this_val if (x[-2:] == this_key)
                   and ("=" not in x) else x for x in bb_code]
    # find the base z_sedol of primary listing
    z_sedol = pd.DataFrame(columns=['z_sedol'])
    z_sedol['sedol'] = [x[:6] if (x.startswith("_CASH_") == False) and (
        x.startswith("_FRWD_") == False) and ('_SWAP_' not in x) else x for x in sedol]
    z_sedol['sedol'] = [
        x if type(x) == str and x != 'nan' else np.nan for x in z_sedol['sedol']]
    # store the known sedols and bb_code of the traded instrument
    z_sedol['bb_code'] = [
        x if type(x) == str and x != 'nan' else np.nan for x in bb_code]
    z_sedol['z_sedol'] = [par_sedol_map['z_sedol'][x] if type(
        x) == str and x in par_sedol_map.index else x for x in z_sedol['sedol']]
    z_sedol['z_sedol'] = [par_bb_map['z_sedol'][x] if type(x) == str and type(
        y) != str and x in par_bb_map.index else y for x, y in zip(z_sedol['bb_code'], z_sedol['z_sedol'])]
    z_sedol['z_bb_code'] = [all_map[x] if type(
        x) == str and x in all_map.index else y for x, y in zip(z_sedol['z_sedol'], z_sedol['bb_code'])]
    z_sedol['trade_type'] = list(trade_type)
    z_sedol['z_sedol_long'] = [x if type(x) == str else par_bb_map['z_sedol'][y] if type(
        y) == str and y in par_bb_map.index else np.nan for x, y in zip(z_sedol['sedol'], z_sedol['bb_code'])]
    z_sedol['z_sedol_short'] = [x if type(x) == str else par_bb_map['z_sedol'][y] if type(
        y) == str and y in par_bb_map.index else np.nan for x, y in zip(z_sedol['sedol'], z_sedol['bb_code'])]
    z_sedol['z_bb_code_short'] = [x if type(x) == str else all_map[y] if type(
        y) == str and y in all_map.index else np.nan for x, y in zip(z_sedol['bb_code'], z_sedol['z_sedol_short'])]
    z_sedol['z_bb_code_long'] = [x if type(x) == str else all_map[y] if type(
        y) == str and y in all_map.index else np.nan for x, y in zip(z_sedol['bb_code'], z_sedol['z_sedol_long'])]
    # fill in any missing _long and _short with ticker_map
    # check for isin
    if isin is not None:
        filt = [type(x) == str and x != 'nan' for x in ticker_map['isin']]
        isin_map = ticker_map.loc[filt, ['isin', 'sedol', 'bb_code',
                                         'sedol_short', 'sedol_long', 'bb_code_short', 'bb_code_long']]
        isin_map['isin'] = [x.strip() for x in isin_map['isin']]
        isin_map = isin_map.drop_duplicates('isin').set_index('isin')
        z_sedol['isin'] = [
            x if type(x) == str and x != 'nan' else np.nan for x in isin]
        for i in ['sedol', 'bb_code', 'sedol_long', 'sedol_short', 'bb_code_long', 'bb_code_short']:
            z_sedol['z_' + i] = [isin_map[i][y] if type(x) != str and type(
                y) == str else x for x, y in zip(z_sedol['z_'+i], z_sedol['isin'])]
    return z_sedol[['z_sedol', 'z_bb_code', 'z_bb_code_short', 'z_bb_code_long', 'z_sedol_long', 'z_sedol_short']]


# special xml spreadsheet file reader to handle the malformed XML Spreadsheet files
# that GS produce
# this_file = '//zimnashk/sd.zentific-im.com$/Operations/Workflow/Holdings/20220603/GS/20220603.ZENTIFIC_NSEMarginSu57ary.xls'
# this_file = '//zimnashk/sd.zentific-im.com$/Operations/Workflow/Holdings/20220602/GS/20220602.ZENTIFIC_NSEMarginSu15ary.xls'
def read_spreadsheet_xml(this_file):
    # if csv then just read it in
    if this_file[-4:].lower() == '.csv':
        result = pd.read_csv(this_file)
        result = pd.DataFrame(result.columns).transpose().append(
            pd.DataFrame(np.array(result)))
        return result
    # other wise check for other formats
    import xlrd
    from bs4 import BeautifulSoup
    from xml.sax import parseString, handler

    class ExcelHandler(handler.ContentHandler):
        def __init__(self):
            self.chars = []
            self.cells = []
            self.rows = []
            self.tables = []

        def characters(self, content):
            self.chars.append(content)

        def startElement(self, name, atts):
            if name == "ss:Cell" or name == "Cell":
                self.chars = []
            elif name == "ss:Row" or name == "Row":
                self.cells = []
            elif name == "ss:Table" or name == "Table":
                self.rows = []

        def endElement(self, name):
            if name == "ss:Cell" or name == "Cell":
                self.cells.append(''.join(self.chars))
            elif name == "ss:Row" or name == "Row":
                self.rows.append(self.cells)
            elif name == "ss:Table" or name == "Table":
                self.tables.append(self.rows)
    excelHandler = ExcelHandler()
    with open(this_file, "rb") as myfile:
        buffer = myfile.read()
    buffer = buffer.decode("latin1").replace(
        ' & ', ' &amp; ').replace("\n", "")
    if buffer[:15] == '<?xml version="':
        buffer1 = BeautifulSoup(str(buffer), 'xml')
        excelHandler = ExcelHandler()
        parseString(buffer1.Table.encode('utf-8'), excelHandler)
        result = pd.DataFrame(excelHandler.tables[0])
        # sub function to convert xls values

        def cleanup_value(x):
            ret_val = x
            if type(ret_val) == str:
                ret_val = x.replace(",", "")
                if (ret_val.replace(".", "").isdigit()) and (ret_val.find(".") > 0):
                    try:
                        ret_val = float(ret_val)
                    except:
                        pass
                elif (len(ret_val) == 10) and (ret_val.replace("-", "").isdigit()) and (ret_val[4] == "-") and (ret_val[7] == "-"):
                    try:
                        ret_val = datetime.datetime.strptime(
                            ret_val, "%Y-%m-%d").date()
                    except:
                        pass
            return ret_val
        result = result.applymap(cleanup_value)
        pd.DataFrame([cleanup_value(x) for x in result.values])
    elif buffer[:12].upper() == '<HTML><HEAD>':
        result = pd.read_html(buffer)
    else:
        # sub function to convert xls values
        def xls_to_value(cell):
            badcells = ['@NA', 'Inf', '-Inf']
            if cell.ctype == xlrd.XL_CELL_ERROR or cell.value in (badcells):
                return float('NaN')
            else:
                return cell.value
        buffer = xlrd.open_workbook(this_file)
        result = pd.DataFrame()
        this_slice = 0
        while this_slice < buffer.sheet_by_index(0).nrows:
            this_line = pd.DataFrame([xls_to_value(x) for x in buffer.sheet_by_index(
                0).row_slice(this_slice, 0)]).transpose()
            result = result.append(this_line)
            this_slice = this_slice + 1
    return result

# function to handle the various text date formats!


def text2date(in_text, us_date=False):
    import xlrd
    this_date = in_text
    if type(in_text) == datetime.date:
        this_date = in_text
    elif (type(in_text) == float) or (type(in_text) == int):
        if (float(str(int(in_text))[:4]) > 1975) and (float(str(int(in_text))[:4]) < 2100) and (len(str(int(in_text))) == 8):
            in_text = str(int(in_text))
        else:
            in_text = str(xlrd.xldate.xldate_as_datetime(
                in_text, datemode=0).date())
    if type(in_text) == str:
        date_text = in_text.split(" ")
        date_text = date_text[0].split("/")
        if len(date_text) == 1:
            date_text = date_text[0].split("-")
        if len(date_text) == 1:
            date_text = date_text[0].split("\\")
        if len(date_text) == 1:
            try:
                this_date = datetime.datetime.strptime(
                    date_text[0], "%Y%m%d").date()
            except:
                pass
        elif (date_text[1].isdigit() == False) and (len(date_text[2]) == 4):
            if us_date:
                this_date = datetime.datetime.strptime(
                    in_text, "%b-%d-%Y").date()
            else:
                this_date = datetime.datetime.strptime(
                    in_text, "%d-%b-%Y").date()
        elif (date_text[1].isdigit() == False) and (len(date_text[2]) == 2):
            if us_date:
                this_date = datetime.datetime.strptime(
                    in_text, "%b-%d-%y").date()
            else:
                this_date = datetime.datetime.strptime(
                    in_text, "%d-%b-%y").date()
        elif int(date_text[0]) > 1900:
            if us_date:
                this_date = datetime.date(int(date_text[0]), int(
                    date_text[2]), int(date_text[1]))
            else:
                this_date = datetime.date(int(date_text[0]), int(
                    date_text[1]), int(date_text[2]))
        elif int(date_text[2]) > 1900:
            if us_date:
                this_date = datetime.date(int(date_text[2]), int(
                    date_text[0]), int(date_text[1]))
            else:
                this_date = datetime.date(int(date_text[2]), int(
                    date_text[1]), int(date_text[0]))
        elif int(date_text[0]) <= 31:
            if us_date:
                this_date = datetime.datetime.strptime(
                    date_text[1]+date_text[0]+date_text[2], "%d%m%y").date()
            else:
                this_date = datetime.datetime.strptime(
                    date_text[0]+date_text[1]+date_text[2], "%d%m%y").date()
    return this_date


def bdaterange(start_date, end_date):
    import dateutil
    return [x.date() for x in (dateutil.rrule.rrule(dateutil.rrule.DAILY, dtstart=start_date, until=end_date, byweekday=(dateutil.rrule.MO, dateutil.rrule.TU, dateutil.rrule.WE, dateutil.rrule.TH, dateutil.rrule.FR)))]


def text2no(in_text):
    if type(in_text) == str:
        in_text = in_text.strip()
        if in_text == '-':
            in_text = '0.0'
        if in_text.startswith('('):
            in_text = in_text.replace('(', '-').replace(')', '')
        return np.float64(in_text.replace(",", ""))
    else:
        return in_text


def get_ops_param(account, debug=True):

    if account == 'ZEN_PCO':
        ops_param = {
            'db': 'zimdb_ops',
            'holdings_table': 'pco_holdings',
            'cash_tickets_table': 'pco_cash_tickets',
            'reconciled_trades_table': 'pco_reconciled_trades',
            'trade_data_table': 'pco_tradedata',
            'broker_holdings_table': 'pco_broker_holdings',
            'bulk_load_path': '//zimsqlhk/sd.zentific-im.com$/zim/tmp/',
            'workflow_path': "//zimnashk/sd.zentific-im.com$/Operations/Workflow/Paamco/",
            'account': 'ZEN_PCO',
            'swap_ff': False,
            'id': 'sedol',
            'debug': debug
        }
    elif account == 'ZEN_CNA':
        ops_param = {
            'db': 'zimdb_ops',
            'holdings_table': 'cna_holdings',
            'cash_tickets_table': 'cna_cash_tickets',
            'reconciled_trades_table': 'cna_reconciled_trades',
            'trade_data_table': 'cna_tradedata',
            'broker_holdings_table': 'cna_broker_holdings',
            'broker_margin_table': 'cna_broker_margin',
            'bulk_load_path': '//zimsqlhk/sd.zentific-im.com$/zim/tmp/',
            'workflow_path': "//zimnashk/sd.zentific-im.com$/Operations/Workflow/zen_cna/",
            'account': 'ZEN_CNA',
            'swap_ff': False,
            'id': 'sedol',
            'debug': debug
        }
    elif account == 'ZEN_CNG':
        ops_param = {
            'db': 'zimdb_ops',
            'holdings_table': 'cng_holdings',
            'cash_tickets_table': 'cng_cash_tickets',
            'reconciled_trades_table': 'cng_reconciled_trades',
            'trade_data_table': 'cng_tradedata',
            'broker_holdings_table': 'cng_broker_holdings',
            'broker_margin_table': 'cng_broker_margin',
            'bulk_load_path': '//zimsqlhk/sd.zentific-im.com$/zim/tmp/',
            'workflow_path': "//zimnashk/sd.zentific-im.com$/Operations/Workflow/zen_cng/",
            'account': 'ZEN_CNG',
            'swap_ff': False,
            'id': 'sedol',
            'debug': debug
        }
    elif account == 'ZEN_MLP':
        ops_param = {
            'db': 'zimdb_ops',
            'holdings_table': 'mlp_holdings',
            'cash_tickets_table': 'mlp_cash_tickets',
            'reconciled_trades_table': 'mlp_reconciled_trades',
            'trade_data_table': 'mlp_tradedata',
            'broker_holdings_table': 'mlp_broker_holdings',
            'broker_margin_table': 'mlp_broker_margin',
            'bulk_load_path': '//zimsqlhk/sd.zentific-im.com$/zim/tmp/',
            'workflow_path': "//zimnashk/sd.zentific-im.com$/Operations/Workflow/zen_mlp/",
            'account': 'ZEN_MLP',
            'swap_ff': False,
            'id': 'sedol',
            'debug': debug
        }
    elif account == 'ZEN_ML2':
        ops_param = {
            'db': 'zimdb_ops',
            'holdings_table': 'ml2_holdings',
            'cash_tickets_table': 'ml2_cash_tickets',
            'reconciled_trades_table': 'ml2_reconciled_trades',
            'trade_data_table': 'ml2_tradedata',
            'broker_holdings_table': 'ml2_broker_holdings',
            'broker_margin_table': 'ml2_broker_margin',
            'bulk_load_path': '//zimsqlhk/sd.zentific-im.com$/zim/tmp/',
            'workflow_path': "//zimnashk/sd.zentific-im.com$/Operations/Workflow/zen_ml2/",
            'account': 'ZEN_ML2',
            'swap_ff': False,
            'id': 'sedol',
            'debug': debug
        }
    elif account == 'ZEN_NHT':
        ops_param = {
            'db': 'zimdb_ops',
            'holdings_table': 'nht_holdings',
            'cash_tickets_table': 'nht_cash_tickets',
            'reconciled_trades_table': 'nht_reconciled_trades',
            'trade_data_table': 'nht_tradedata',
            'broker_holdings_table': 'nht_broker_holdings',
            'broker_margin_table': 'nht_broker_margin',
            'bulk_load_path': '//zimsqlhk/sd.zentific-im.com$/zim/tmp/',
            'workflow_path': "//zimnashk/sd.zentific-im.com$/Operations/Workflow/zen_nht/",
            'account': 'ZEN_NHT',
            'swap_ff': False,
            'id': 'sedol',
            'debug': debug
        }
    elif account == 'ZEN_NHL':
        ops_param = {
            'db': 'zimdb_ops',
            'holdings_table': 'nhl_holdings',
            'cash_tickets_table': 'nhl_cash_tickets',
            'reconciled_trades_table': 'nhl_reconciled_trades',
            'trade_data_table': 'nhl_tradedata',
            'broker_holdings_table': 'nhl_broker_holdings',
            'broker_margin_table': 'nhl_broker_margin',
            'bulk_load_path': '//zimsqlhk/sd.zentific-im.com$/zim/tmp/',
            'workflow_path': "//zimnashk/sd.zentific-im.com$/Operations/Workflow/zen_nhl/",
            'account': 'ZEN_NHL',
            'swap_ff': False,
            'id': 'sedol',
            'debug': debug
        }
    elif account == 'ZEN_SEA':
        ops_param = {
            'db': 'zimdb_ops',
            'holdings_table': 'sea_holdings',
            'cash_tickets_table': 'sea_cash_tickets',
            'reconciled_trades_table': 'sea_reconciled_trades',
            'trade_data_table': 'sea_tradedata',
            'broker_holdings_table': 'sea_broker_holdings',
            'broker_margin_table': 'sea_broker_margin',
            'bulk_load_path': '//zimsqlhk/sd.zentific-im.com$/zim/tmp/',
            'workflow_path': "//zimnashk/sd.zentific-im.com$/Operations/Workflow/zen_sea/",
            'account': 'ZEN_SEA',
            'swap_ff': False,
            'id': 'sedol',
            'debug': debug
        }   
    elif account == 'ZEN_MS2':
        ops_param = {
            'db': 'zimdb_ops',
            'holdings_table': 'ms2_holdings',
            'cash_tickets_table': 'ms2_cash_tickets',
            'reconciled_trades_table': 'ms2_reconciled_trades',
            'trade_data_table': 'ms2_tradedata',
            'broker_holdings_table': 'ms2_broker_holdings',
            'broker_margin_table': 'ms2_broker_margin',
            'bulk_load_path': '//zimsqlhk/sd.zentific-im.com$/zim/tmp/',
            'workflow_path': "//zimnashk/sd.zentific-im.com$/Operations/Workflow/zen_ms2/",
            'account': 'ZEN_MS2',
            'swap_ff': False,
            'id': 'sedol',
            'debug': debug
        }
    elif account == 'ZEN_PCN':
        ops_param = {
            'db': 'zimdb_ops',
            'holdings_table': 'pcn_holdings',
            'cash_tickets_table': 'pcn_cash_tickets',
            'reconciled_trades_table': 'pcn_reconciled_trades',
            'trade_data_table': 'pcn_tradedata',
            'broker_holdings_table': 'pcn_broker_holdings',
            'broker_margin_table': 'pcn_broker_margin',
            'bulk_load_path': '//zimsqlhk/sd.zentific-im.com$/zim/tmp/',
            'workflow_path': "//zimnashk/sd.zentific-im.com$/Operations/Workflow/zen_pcn/",
            'account': 'ZEN_PCN',
            'swap_ff': False,
            'id': 'sedol',
            'debug': debug
        }
    elif account == 'ZEN_MST':
        ops_param = {
            'db': 'zimdb_ops',
            'holdings_table': 'holdings',
            'cash_tickets_table': 'cash_tickets',
            'reconciled_trades_table': 'reconciled_trades',
            'trade_data_table': 'tradedata',
            'broker_holdings_table': 'broker_holdings',
            'broker_margin_table': 'broker_margin',
            'bulk_load_path': '//zimsqlhk/sd.zentific-im.com$/zim/tmp/',
            'workflow_path': "//zimnashk/sd.zentific-im.com$/Operations/Workflow/",
            'account': 'ZEN_MST',
            'swap_ff': False,
            'id': 'sedol',
            'debug': debug
        }
    elif account == 'ZEN_AGG':
        ops_param = {
            'db': 'zimdb_ops',
            'holdings_table': None,
            'cash_tickets_table': None,
            'reconciled_trades_table': None,
            'trade_data_table': 'agg_tradedata',
            'broker_holdings_table': None,
            'broker_margin_table': None,
            'bulk_load_path': '//zimsqlhk/sd.zentific-im.com$/zim/tmp/',
            'workflow_path': "//zimnashk/sd.zentific-im.com$/Operations/Workflow/zen_agg/",
            'account': 'ZEN_AGG',
            'swap_ff': False,
            'id': 'sedol',
            'debug': debug
        }
    elif account == 'ZEN_CNL':
        ops_param = {
            'db': 'zimdb_ops',
            'holdings_table': 'cnl_holdings',
            'cash_tickets_table': 'cnl_cash_tickets',
            'reconciled_trades_table': 'cnl_reconciled_trades',
            'trade_data_table': 'cnl_tradedata',
            'broker_holdings_table': 'cnl_broker_holdings',
            'broker_margin_table': 'cnl_broker_margin',
            'bulk_load_path': '//zimsqlhk/sd.zentific-im.com$/zim/tmp/',
            'workflow_path': "//zimnashk/sd.zentific-im.com$/Operations/Workflow/zen_cnl/",
            'account': 'ZEN_CNL',
            'swap_ff': True,
            'id': 'sedol',
            'debug': debug
        }
    elif account == 'ZEN_TEST':
        ops_param = {
            'db': 'zimdb_ops',
            'holdings_table': 'test_holdings',
            'cash_tickets_table': 'test_cash_tickets',
            'reconciled_trades_table': 'test_reconciled_trades',
            'trade_data_table': 'test_tradedata',
            'broker_holdings_table': 'test_broker_holdings',
            'broker_margin_table': 'test_broker_margin',
            'bulk_load_path': '//zimsqlhk/sd.zentific-im.com$/zim/tmp/',
            'workflow_path': "//zimnashk/sd.zentific-im.com$/Operations/Workflow/",
            'account': 'ZEN_MST',
            'swap_ff': False,
            'id': 'fs_id_r',
            'debug': debug
        }
    else:
        ops_param = None
    return ops_param


def get_india_futures_as_it_was(start_date=datetime.datetime.now().date()):
    query = """
        SELECT bb_code, multiplier, ric_code, z_bb_code, Max(date) AS date 
        FROM   (SELECT bb_fut_prefix + '=' 
                       + ism.future_suffix + ' IS' AS bb_code, 
                       ism.multiplier              AS multiplier, 
                       Replace(rbm.ric_code, '.NS', '') 
                       + ism.future_suffix + ':NS' AS ric_code, 
                       ism.ticker + ' IN'          AS z_bb_code, 
                       ism.date                    AS date 
                FROM   zimdb..india_ssf_map_hist AS ism 
                       LEFT OUTER JOIN (SELECT * 
                                        FROM   zimdb..ric_bb_map 
                                        WHERE  RIGHT(ric_code, 3) = '.NS') rbm 
                                    ON rbm.bb_code_exchange = ism.ticker + ' IS Equity' 
                where bb_fut_prefix != ''
                UNION 
                SELECT bb_fut_prefix + '=' 
                       + ism.future_suffix_next + ' IS' AS bb_code, 
                       ism.multiplier_next              AS multiplier, 
                       Replace(rbm.ric_code, '.NS', '') 
                       + ism.future_suffix_next + ':NS' AS ric_code, 
                       ism.ticker + ' IN'               AS z_bb_code, 
                       ism.date                         AS date 
                FROM   zimdb..india_ssf_map_hist AS ism 
                       LEFT OUTER JOIN (SELECT * 
                                        FROM   zimdb..ric_bb_map 
                                        WHERE  RIGHT(ric_code, 3) = '.NS') rbm 
                                    ON rbm.bb_code_exchange = ism.ticker + ' IS Equity' 
                WHERE  future_suffix_next IS NOT NULL and bb_fut_prefix != '') comb 
        GROUP  BY bb_code, multiplier, ric_code, z_bb_code 
        ORDER  BY bb_code 
        """
    ssf_hist = exec_sql(query)
#    ssf_hist['date'] = [(datetime.datetime.strptime(x, "%Y-%m-%d")-pd.offsets.BDay(1)).date() for x in ssf_hist['date']]
    ssf = exec_sql("select * from zimdb_ops..india_ssf_multiplier")
    ssf['date'] = datetime.datetime.now().date()
    tmp = ssf_hist.append(ssf)[['bb_code', 'multiplier', 'date']]
    tmp['date'] = date_index(tmp, 'date')
    tmp = tmp.drop_duplicates(['date', 'bb_code'])
    tmp = tmp[[type(x) == str for x in tmp.bb_code]]
    tmp = tmp.set_index(['date', 'bb_code'])
    tmp = tmp.unstack('bb_code')
    tmp = df_restore_date_index(tmp)
    if start_date not in date_index(tmp):
        new_elem = tmp.iloc[[0]].copy()
        new_elem[:] = np.nan
        new_elem.index = [start_date]
        tmp = tmp.append(new_elem)
    tmp = tmp.sort_index()
    tmp = tmp.fillna(method='bfill')
    tmp = tmp.fillna(method='ffill')
    ssf = tmp.loc[start_date]
    ssf.index = ssf.index.get_level_values('bb_code')
    return ssf


def get_web_account(name, account_type=None):
    datetoday = datetime.date.today()
    sqlmain = "select * from zimdb..web_scraping_accounts where web_name = '" + name + "'"
    sqldate = "select max(date) from zimdb..web_scraping_accounts where  date <= '" + \
        str(datetoday) + "' and web_name = '" + name + "'"
    if account_type:
        sqlmain = sqlmain + " and account_type = '" + account_type + "'"
        sqldate = sqldate + " and account_type = '" + account_type + "'"
    sql = sqlmain + ' and date in (' + sqldate + ')'
    acc = exec_sql(sql)
    if len(acc) > 1:
        raise Exception(
            'Non unique accounts returned from web_scraping_accounts')
    elif len(acc) == 0:
        raise Exception('No account returned from web_scraping_accounts')
    acc = acc.iloc[0].to_dict()
    return acc


def create_table(df, table_name, db='', keys=[], drop_if_exists=True, type_map=None, strlen=50, step_size=1e100, verbose=False):
    df = df.copy()
    if type(df.index.name) != type(None):
        df = df.reset_index()
    if drop_if_exists and sql_table_exists(table_name, db=db):
        if db == '':
            dropsql = "DROP TABLE " + table_name
        else:
            dropsql = "DROP TABLE " + db + ".." + table_name
        # Execute the drop
        exec_sql(dropsql, isselect=0)
    # now clear to create the table
    if type_map is None:
        cols = pd.Series([type(x) for x in df.iloc[0]])
        cols.index = df.columns
    else:
        cols = pd.Series(type_map, index=df.columns)
    sqlcmd = "CREATE TABLE " + ('' if db ==
                                '' else (db + "..")) + table_name + " ("
    first_col = True
    for this_col, this_type in pd.DataFrame(cols).iterrows():
        if (this_col == 'sedol'):
            sqlcmd = sqlcmd + (", " if not first_col else '') + \
                "[" + this_col + "] varchar(15) " + \
                ("NOT " if this_col in keys else '') + "NULL"
        elif this_type[0] in [datetime.date, np.datetime64, datetime.datetime, pd.tslib.Timestamp]:
            sqlcmd = sqlcmd + (", " if not first_col else '') + \
                "[" + this_col + "] date " + \
                ("NOT " if this_col in keys else '') + "NULL"
        elif this_type[0] in [str]:
            sqlcmd = sqlcmd + (", " if not first_col else '') + "[" + this_col + "] varchar(" + str(
                int(strlen)) + ") " + ("NOT " if this_col in keys else '') + "NULL"
        elif this_type[0] in [float, np.float64]:
            sqlcmd = sqlcmd + (", " if not first_col else '') + \
                "[" + this_col + "] float " + \
                ("NOT " if this_col in keys else '') + "NULL"
        elif this_type[0] in [bool]:
            sqlcmd = sqlcmd + (", " if not first_col else '') + \
                "[" + this_col + "] bit " + \
                ("NOT " if this_col in keys else '') + "NULL"
        else:
            sqlcmd = sqlcmd + (", " if not first_col else '') + \
                "[" + this_col + "] varchar(50) " + \
                ("NOT " if this_col in keys else '') + "NULL"
        first_col = False
    sqlcmd = sqlcmd + ")  ON [PRIMARY]"
    exec_sql(sqlcmd, isselect=0)
    if len(keys) > 0:
        sqlcmd = "ALTER TABLE "+('' if db == '' else (db + "..")) + table_name + " ADD CONSTRAINT PK_" + \
            table_name+"_"+str(round(random.random()*1e8)
                               )+" PRIMARY KEY CLUSTERED ("
        sqlcmd = sqlcmd + ", ".join(keys)
        sqlcmd = sqlcmd + \
            ") WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]"
        exec_sql(sqlcmd, isselect=0)
    if len(df) > 0:
        load_to_table(df, table_name, db=db,
                      step_size=step_size, verbose=verbose)


def df_restore_date_index(df, datecol='date', inplace=False):
    if not inplace:
        df = df.copy()
    if len(df.index.names) == 1:
        df.index = date_index(df)
        df.index.name = datecol
    else:
        datecolindex = df.index.names.index(datecol)
        if type(df.index.levels[datecolindex][0]) != datetime.date:
            df.index = df.index.set_levels(
                df.index.levels[datecolindex].date, level=datecolindex)
    if not inplace:
        return df


def date_index(df, label='date'):
    if type(df) == pd.DatetimeIndex:
        result = df
    elif type(df) == list:
        result = pd.Series(df)
    elif label in df.index.names:
        result = df.index.get_level_values(label).copy()
    elif label in df.columns:
        result = df[label].copy()
    else:
        result = None
    do_conv = False
    if result is not None:
        if result.dtype == np.dtype('<M8[ns]'):
            do_conv = True
        else:
            all_types = list(set([type(x) for x in result]))
            if pd.Timestamp in all_types or np.datetime64 in all_types:
                do_conv = True
        if do_conv:
            unique_ts = result.unique()
            unique_date = pd.Series(
                [pd.Timestamp(x).date() if (
                    type(x) in [pd.Timestamp, np.datetime64]) else x for x in unique_ts],
                index=unique_ts)
            result = list(unique_date[result])
        else:
            result = list(result)
    return result


def sort_dataframe(data, *args, **kwargs):
    if hasattr(data, 'sort_values'):
        if 'columns' in kwargs:
            kwargs['by'] = kwargs.pop('columns')
        if 'by' in kwargs or len(args) > 0:
            return data.sort_values(*args, **kwargs)
        else:
            return data.sort_index(*args, **kwargs)
    elif hasattr(data, 'sort'):
        return data.sort(*args, **kwargs)
    else:
        raise Exception('Unable to sort')


def imap_auth_o365(username, password):
    import json
    import msal
    import imaplib
    param_file = 'S:\\Operations\Workflow\zim_ops\keys\o365.json'
    config = json.load(open(param_file))
    app = msal.ConfidentialClientApplication(
        config["client_id"], authority=config["authority"],
        client_credential=config["secret"])
    result = None
    result = app.acquire_token_by_username_password(username=username, password=password, scopes=[
                                                    'https://outlook.office.com/IMAP.AccessAsUser.All'], claims_challenge=None)
    imapsession = None
    if "access_token" in result:
        # Calling graph using the access token
        imapsession = imaplib.IMAP4_SSL('outlook.office365.com')
        auth_string = "user=%s\x01auth=Bearer %s\x01\x01" % (
            result["id_token_claims"]["preferred_username"], result["access_token"])
        imap_result = imapsession.authenticate(
            mechanism='XOAUTH2', authobject=lambda x: auth_string)
        if imap_result[0] != 'OK':
            print("=== ERRROR - 'imaplib' " + str(imap_result))
            raise Exception("=== ERRROR - 'imaplib' " + str(imap_result))
    else:
        print(
            "=== ERRROR - 'imap_app' config['secret'] expires every two years - log into https://portal.azure.com to update the 'certificates & secrets'")
        print("=== Error details : " + result.get("error"))
        print(result.get("error_description"))
        msg = "=== ERRROR - 'imap_app' config['secret'] expires every two years - log into https://portal.azure.com to update the 'certificates & secrets'"
        msg = msg + "\n=== Error details : " + result.get("error")
        msg = msg + "\n" + result.get("error_description")
        raise Exception(msg)

    return imapsession



class Logger:
    LOG_LEVELS = {
        'DEBUG': 1,
        'INFO': 2,
        'WARNING': 3,
        'ERROR': 4,
        'CRITICAL': 5
    }

    def __init__(self, log_level, log_file=None):
        log_level = log_level.upper()
        if log_level not in self.LOG_LEVELS:
            raise ValueError("Invalid log level. Must be one of {}".format(list(self.LOG_LEVELS.keys())))

        self.log_level = self.LOG_LEVELS[log_level]
        self.log_file = log_file
        if log_file is not None:
            open(log_file, 'w').close()

    def clear_log(self):
        if self.log_file is not None:
            open(self.log_file, 'w').close()

    def _log(self, level_name, message):
        level_value = self.LOG_LEVELS[level_name]
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg = "{} - {} - {}".format(timestamp,level_name,message)
        print(msg, flush=True)

        if self.log_file is not None and level_value > self.log_level:
            with open(self.log_file, 'a') as f:
                f.write(msg + '\n')

    def debug(self, message):
        self._log('DEBUG', message)

    def info(self, message):
        self._log('INFO', message)

    def warning(self, message):
        self._log('WARNING', message)

    def error(self, message):
        self._log('ERROR', message)

    def critical(self, message):
        self._log('CRITICAL', message)


def filter_files(filepath, includes=[], excludes=[]):
    '''
    Sort by modification time(descending), then by size(descending)
    Return the latest and largest file or None if file not found
    '''
    files = [x for x in os.listdir(filepath)
            if all(i in x for i in includes)
            and not any(e in x for e in excludes)]
    files.sort(key=lambda x: os.path.getmtime(os.path.join(filepath, x)), reverse=True)
    newest_time = os.path.getmtime(os.path.join(filepath, files[0]))
    newest_files = [f for f in files if os.path.getmtime(os.path.join(filepath, f)) == newest_time]
    if len(newest_files) > 1:
        newest_files.sort(key=lambda x: os.path.getsize(os.path.join(filepath, x)), reverse=True)
        files[:len(newest_files)] = newest_files
    if len(files) > 0:
        return files[0]
    return
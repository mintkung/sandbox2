#from ftplib import FTP
import cx_Oracle
import csv
import os,sys
from datetime import datetime, timedelta
import time
from contextlib import contextmanager
import ftplib
import logging
# import pysftp
import pyodbc
import pandas as pd
import numpy as np
import oracledb
oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb
from sqlalchemy import create_engine, exc, text
from sqlalchemy.engine import URL

logging.basicConfig(filename="15min.log", filemode="a", format="%(process)d - %(levelname)s - %(asctime)s - %(message)s",datefmt='%d-%b-%Y %H:%M:%S', level=logging.INFO)
#today = datetime.today()
now = datetime.now()
startdate = now - timedelta(minutes=15)
dt_end = now.strftime("%Y-%m-%d")
# dt_start = startdate.strftime("%Y-%m-%d")

#print("Today's date:", today)
# print("now =", now)
log_msg = "Start execute 15min query task on " + now.strftime("%Y-%m-%d %H:%M:%S")
logging.info(log_msg)
dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
print("date and time =" + dt_string)
querymin = int(now.strftime("%M"))//15*15
csv_name = now.strftime("%Y%m%d%H")+str(querymin).zfill(2)
print("query time = " + now.strftime("%H") + ":" + str(querymin).zfill(2))
log_msg = "query time = " + now.strftime("%H") + ":" + str(querymin).zfill(2)
logging.info(log_msg)
# print("query start = " + dt_start)
# logging.info("query start = " + dt_start)
# print("query end = " + dt_end)
# logging.info("query end = " + dt_end)

querytime = datetime(int(now.strftime("%Y")), int(now.strftime("%m")), int(now.strftime("%d")), int(now.strftime("%H")), querymin)
#for testing
querytime = querytime - timedelta(minutes=15) #delay total 30 mins
default_query_time = datetime(2023,2,1,0,15)
min_sub = 15
dt_start = querytime.strftime("%Y-%m-%d %H:%M")
# print("query start = " + dt_start)
# logging.info("query start = " + dt_start)
# print("query end = " + dt_end)
# logging.info("query end = " + dt_end)
conn = cx_Oracle.connect(
    user=r'pea_kritsakorn', password='pea_kritsakorn', dsn='mdamr_srv', encoding="UTF-8")
cursor = conn.cursor()
folder_name = querytime.strftime("%Y%m%d%H%M")

# def executequery(meter, start_d):
#     # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
#     if not os.path.exists(folder_name):
#         os.makedirs(folder_name)
#         print("Creating folder",folder_name)
#     else:
#         print("Folder is existed, using that folder instead.")
#     current_d = os.getcwd()
#     csv_d = current_d+"\\"+folder_name+"\\"
#     sql1 = """  SELECT
#                             mp.meterpointid
#                         FROM
#                             edmi.tblmeterpoints mp
#                         LEFT JOIN edmi.tbldevices dv ON
#                             mp.deviceid = dv.deviceid
#                         WHERE
#                             mp.meterpointid = :mtpid"""
#     #cursor = conn.cursor()
#     cursor.execute(sql1, mtpid=meter)
#     row = cursor.fetchone()
#     if row == None:
#         print(meter," is not found.")
#         logging.info(meter," is not found.")
#         return
#     meterpointid = row[0]
#     csv_name = "AMR_" + \
#         querytime.strftime("%Y%m%d%H%M")+"_"+str(meterpointid)+".txt"
#     print("meterpointid", str(meter), " is executed as ", csv_d+csv_name)
#     logging.info("executed = " + str(meter))
#     csv_file = open(csv_d+csv_name, "w", encoding="utf-8")
#     writer = csv.writer(csv_file, delimiter=',',
#                         lineterminator="\n", quoting=csv.QUOTE_NONE)
#     # use triple quotes if you want to spread your query across multiple lines
#     with open("execute_15.sql",encoding="utf-8") as sql_file:
#         command = sql_file.read()
#     cursor.execute(command, mtpid=meterpointid,
#                     START_DATE=start_d)
#     desc = cursor.description
#     #writer.writerow(i[0] for i in desc)
#     for row in cursor:
#         writer.writerow(row)
#     # while True:
#         #row = cursor.fetchone()
#         # if row is None:
#         # break
#         # print(row)
#         # writer.writerow(row)
#         # print (row[0], '-', row[1]) # this only shows the first two columns. To add an additional column you'll need to add , '-', row[2], etc.
#         # print(row)
#         # cursor.close()
#         # conn.close()
#     csv_file.close()
#     print("done of ", meter)

#MSSQL connection
server = 'localhost' 
database = 'testsandbox' 
username = 'sa' 
password = '1q2w3e4r' 

cnxn = pyodbc.connect('DRIVER={SQL Server};'
                        'SERVER='+server+';'
                        'DATABASE='+database+';'
                        # 'ENCRYPT=yes;'
                        'UID='+username+';'
                        'PWD='+ password+';'
                        'Trusted_Connection=yes;')
cursor = cnxn.cursor()

connection_url = URL.create(
    "mssql+pyodbc",
    username=username,
    password=password,
    host=server,
    port=1433,
    database=database,
    query={
        "driver": "ODBC Driver 18 for SQL Server",
        "Encrypt": "yes",
        "TrustServerCertificate": "yes",
    },
)
mssql_engine = create_engine(connection_url)

sql1 = '''
SELECT tb1.*, tb2.date_time FROM [testsandbox].[dbo].[tb_pea_customer] tb1
JOIN [testsandbox].[dbo].[tb_last_update] tb2 ON tb1.meter_point_id = tb2.meter_point_id
WHERE tb1.meter_point_id IS NOT NULL;'''
# cursor.execute(sql1) 

# for i in cursor:
#     print(i)

df = pd.read_sql_query(sql1, mssql_engine)
# medter_dict = df.to_dict()
# print(medter_dict)
print(df)
print(df.dtypes)
meterpointlist = df['meter_point_id'].values.tolist()
print(meterpointlist)
df['date_time'] = df['date_time'].dt.strftime("%Y-%m-%d %H:%M")
date_end = df['date_time'].values.tolist()
print(date_end)
dictionary = dict(zip(meterpointlist,date_end))
# print(dictionary)
# print(meterpointlist)

# in_vars = ','.join(':%d' % i for i in range(len(meterpointlist)))
# print(in_vars)

#Oracle
USERNAME = "pea_kritsakorn"
PASSWORD = "pea_kritsakorn"
DSN = "mdamr_srv"
ENCODING = "UTF-8"
# COMMAND_FILE = "export.sql"
CONFIG = "C:/Oracle/instantclient_19_5/network/admin"
DIALECT = 'oracle'
SQL_DRIVER = 'cx_oracle'
ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD +'@' + DSN
oracledb.init_oracle_client(lib_dir=r"C:/Oracle/instantclient_19_5")

# conn = cx_Oracle.connect(
#     user=r'pea_kritsakorn', password='pea_kritsakorn', dsn='mdamr_srv', encoding="UTF-8")
#SQLAlchemy
try:
    engine = create_engine(
        ENGINE_PATH_WIN_AUTH,
        # thick_mode=None,
        connect_args={
            "encoding": ENCODING,
            "nencoding": ENCODING,
            "events": True
        }
    )
    print("Conected.")
except exc.SQLAlchemyError as e:
    error = str(e)
    print(error)


def getandsave(meterpointlist_str, meter, date_end):
    sql_cus = '''
    SELECT mp.meterpointid, cus.customercode
    FROM edmi.tblmeterpoints mp
    LEFT JOIN edmi.tblcustomers cus ON mp.customerid = cus.customerid
    WHERE mp.meterpointid IN ({meterpointlist})
    '''.format(meterpointlist=meterpointlist_str)

    sql_whimp = '''
    SELECT
        TO_CHAR(whimp.date_m, 'YYYY-MM-DD HH24:MI') DATETIME_METER,
        whimp.meterpointid METERPOINTID,
        TRIM(RTRIM((TO_CHAR(WHIMP.QTYVALUE, 'FM99999999990.999999999999999999999999999999')), '.')) tae_kwh_imp
    FROM
        edmi.tblprofilewhimp whimp
    WHERE
        whimp.date_m BETWEEN TO_DATE(\'{datetime_start}\','YYYY-MM-DD HH24.MI') AND TO_DATE(\'{datetime_end}\','YYYY-MM-DD HH24.MI')
        AND whimp.meterpointid IN ({meterpointlist})
        '''.format(datetime_start=dt_start, datetime_end=date_end,meterpointlist=meterpointlist_str)

    sql_varimp = '''
    SELECT
        TO_CHAR(varimp.date_m, 'YYYY-MM-DD HH24:MI') DATETIME_METER,
        varimp.meterpointid METERPOINTID,
        TRIM(RTRIM((TO_CHAR(varimp.QTYVALUE, 'FM99999999990.999999999999999999999999999999')), '.')) tae_kvarh_imp
    FROM
        edmi.tblprofilevarhimp varimp
    WHERE
        varimp.date_m BETWEEN TO_DATE(\'{datetime_start}\','YYYY-MM-DD HH24.MI') AND TO_DATE(\'{datetime_end}\','YYYY-MM-DD HH24.MI')
        AND varimp.meterpointid IN ({meterpointlist})
        '''.format(datetime_start=dt_start, datetime_end=date_end,meterpointlist=meterpointlist_str)

    sql_whexp = '''
    SELECT
        TO_CHAR(whexp.date_m, 'YYYY-MM-DD HH24:MI') DATETIME_METER,
        whexp.meterpointid METERPOINTID,
        TRIM(RTRIM((TO_CHAR(WHEXP.QTYVALUE, 'FM99999999990.999999999999999999999999999999')), '.')) tae_kwh_exp
    FROM
        edmi.tblprofilewhexp whexp
    WHERE
        whexp.date_m BETWEEN TO_DATE(\'{datetime_start}\','YYYY-MM-DD HH24.MI') AND TO_DATE(\'{datetime_end}\','YYYY-MM-DD HH24.MI')
        AND whexp.meterpointid IN ({meterpointlist})
        '''.format(datetime_start=dt_start, datetime_end=date_end,meterpointlist=meterpointlist_str)

    sql_varexp = '''
    SELECT
        TO_CHAR(varexp.date_m, 'YYYY-MM-DD HH24:MI') DATETIME_METER,
        varexp.meterpointid METERPOINTID,
        TRIM(RTRIM((TO_CHAR(varexp.QTYVALUE, 'FM99999999990.999999999999999999999999999999')), '.')) tae_kvarh_exp
    FROM
        edmi.tblprofilevarhexp varexp
    WHERE
        varexp.date_m BETWEEN TO_DATE(\'{datetime_start}\','YYYY-MM-DD HH24.MI') AND TO_DATE(\'{datetime_end}\','YYYY-MM-DD HH24.MI')
        AND varexp.meterpointid IN ({meterpointlist})
        '''.format(datetime_start=dt_start, datetime_end=date_end,meterpointlist=meterpointlist_str)

    sql_pf = '''
    SELECT
        TO_CHAR(pf.date_m, 'YYYY-MM-DD HH24:MI') DATETIME_METER,
        pf.meterpointid METERPOINTID,
        TRIM(RTRIM((TO_CHAR(pf.QTYVALUE, 'FM99999999990.999999999999999999999999999999')), '.')) tae_pf
    FROM
        edmi.tblprofileinstpf pf
    WHERE
        pf.date_m BETWEEN TO_DATE(\'{datetime_start}\','YYYY-MM-DD HH24.MI') AND TO_DATE(\'{datetime_end}\','YYYY-MM-DD HH24.MI')
        AND pf.phase = 3
        AND pf.meterpointid IN ({meterpointlist})
        '''.format(datetime_start=dt_start, datetime_end=date_end,meterpointlist=meterpointlist_str)

    sql_insti = '''
    SELECT
        TO_CHAR(ia.date_m, 'YYYY-MM-DD HH24:MI') DATETIME_METER,
        ia.meterpointid,
        ia.qtyvalue tae_i_A,
        ib.qtyvalue tae_i_B,
        ic.qtyvalue tae_i_C
    FROM
        (
        SELECT
            meterpointid,
            date_m,
            qtyvalue
        FROM
            edmi.tblprofileinsti
        WHERE
            METERPOINTID IN ({meterpointlist})
            AND DATE_M BETWEEN TO_DATE(\'{datetime_start}\','YYYY-MM-DD HH24.MI') AND TO_DATE(\'{datetime_end}\','YYYY-MM-DD HH24.MI')
            AND PHASE = 0
    ) IA
    LEFT JOIN (
        SELECT
            meterpointid,
            date_m,
            qtyvalue
        FROM
            edmi.tblprofileinsti
        WHERE
            METERPOINTID IN ({meterpointlist})
            AND DATE_M BETWEEN TO_DATE(\'{datetime_start}\','YYYY-MM-DD HH24.MI') AND TO_DATE(\'{datetime_end}\','YYYY-MM-DD HH24.MI')
                AND PHASE = 1) IB ON
        ia.meterpointid = ib.meterpointid
        AND ia.date_m = ib.date_m
    LEFT JOIN (
        SELECT
            meterpointid,
            date_m,
            qtyvalue
        FROM
            edmi.tblprofileinsti
        WHERE
            METERPOINTID IN ({meterpointlist})
            AND DATE_M BETWEEN TO_DATE(\'{datetime_start}\','YYYY-MM-DD HH24.MI') AND TO_DATE(\'{datetime_end}\','YYYY-MM-DD HH24.MI')
                AND PHASE = 2) IC ON
        ia.meterpointid = ic.meterpointid
        AND ia.date_m = ic.date_m
        '''.format(datetime_start=dt_start, datetime_end=date_end,meterpointlist=meterpointlist_str)

    sql_instv = '''
    SELECT
        TO_CHAR(va.date_m, 'YYYY-MM-DD HH24:MI') DATETIME_METER,
        va.meterpointid,
        va.qtyvalue tae_v_A,
        vb.qtyvalue tae_v_B,
        vc.qtyvalue tae_v_C
    FROM
        (
        SELECT
            meterpointid,
            date_m,
            qtyvalue
        FROM
            edmi.tblprofileinstv
        WHERE
            METERPOINTID IN ({meterpointlist})
            AND DATE_M BETWEEN TO_DATE(\'{datetime_start}\','YYYY-MM-DD HH24.MI') AND TO_DATE(\'{datetime_end}\','YYYY-MM-DD HH24.MI')
            AND PHASE = 0
    ) VA
    LEFT JOIN (
        SELECT
            meterpointid,
            date_m,
            qtyvalue
        FROM
            edmi.tblprofileinstv
        WHERE
            METERPOINTID IN ({meterpointlist})
            AND DATE_M BETWEEN TO_DATE(\'{datetime_start}\','YYYY-MM-DD HH24.MI') AND TO_DATE(\'{datetime_end}\','YYYY-MM-DD HH24.MI')
                AND PHASE = 1) VB ON
        va.meterpointid = vb.meterpointid
        AND va.date_m = vb.date_m
    LEFT JOIN (
        SELECT
            meterpointid,
            date_m,
            qtyvalue
        FROM
            edmi.tblprofileinstv
        WHERE
            METERPOINTID IN ({meterpointlist})
            AND DATE_M BETWEEN TO_DATE(\'{datetime_start}\','YYYY-MM-DD HH24.MI') AND TO_DATE(\'{datetime_end}\','YYYY-MM-DD HH24.MI')
                AND PHASE = 2) VC ON
        va.meterpointid = vc.meterpointid
        AND va.date_m = vc.date_m
        '''.format(datetime_start=dt_start, datetime_end=date_end,meterpointlist=meterpointlist_str)

    sql_instang = '''
    SELECT
        TO_CHAR(anga.date_m, 'YYYY-MM-DD HH24:MI') DATETIME_METER,
        anga.meterpointid,
        anga.qtyvalue tae_ang_A,
        angb.qtyvalue tae_ang_B,
        angc.qtyvalue tae_ang_C
    FROM
        (
        SELECT
            meterpointid,
            date_m,
            qtyvalue
        FROM
            edmi.tblprofileinstang
        WHERE
            METERPOINTID IN ({meterpointlist})
            AND DATE_M BETWEEN TO_DATE(\'{datetime_start}\','YYYY-MM-DD HH24.MI') AND TO_DATE(\'{datetime_end}\','YYYY-MM-DD HH24.MI')
            AND PHASE = 0
    ) ANGA
    LEFT JOIN (
        SELECT
            meterpointid,
            date_m,
            qtyvalue
        FROM
            edmi.tblprofileinstang
        WHERE
            METERPOINTID IN ({meterpointlist})
            AND DATE_M BETWEEN TO_DATE(\'{datetime_start}\','YYYY-MM-DD HH24.MI') AND TO_DATE(\'{datetime_end}\','YYYY-MM-DD HH24.MI')
                AND PHASE = 1) ANGB ON
        anga.meterpointid = angb.meterpointid
        AND anga.date_m = angb.date_m
    LEFT JOIN (
        SELECT
            meterpointid,
            date_m,
            qtyvalue
        FROM
            edmi.tblprofileinstang
        WHERE
            METERPOINTID IN ({meterpointlist})
            AND DATE_M BETWEEN TO_DATE(\'{datetime_start}\','YYYY-MM-DD HH24.MI') AND TO_DATE(\'{datetime_end}\','YYYY-MM-DD HH24.MI')
                AND PHASE = 2) ANGC ON
        anga.meterpointid = angc.meterpointid
        AND anga.date_m = angc.date_m
        '''.format(datetime_start=dt_start, datetime_end=date_end,meterpointlist=meterpointlist_str)

    sql_thdi = '''
    SELECT
        TO_CHAR(thdia.date_m, 'YYYY-MM-DD HH24:MI') DATETIME_METER,
        thdia.meterpointid,
        thdia.qtyvalue tae_thdi_A,
        thdib.qtyvalue tae_thdi_B,
        thdic.qtyvalue tae_thdi_C
    FROM
        (
        SELECT
            meterpointid,
            date_m,
            qtyvalue
        FROM
            edmi.tblprofilethdi
        WHERE
            METERPOINTID IN ({meterpointlist})
            AND DATE_M BETWEEN TO_DATE(\'{datetime_start}\','YYYY-MM-DD HH24.MI') AND TO_DATE(\'{datetime_end}\','YYYY-MM-DD HH24.MI')
            AND PHASE = 0
    ) THDIA
    LEFT JOIN (
        SELECT
            meterpointid,
            date_m,
            qtyvalue
        FROM
            edmi.tblprofilethdi
        WHERE
            METERPOINTID IN ({meterpointlist})
            AND DATE_M BETWEEN TO_DATE(\'{datetime_start}\','YYYY-MM-DD HH24.MI') AND TO_DATE(\'{datetime_end}\','YYYY-MM-DD HH24.MI')
                AND PHASE = 1) THDIB ON
        thdia.meterpointid = thdib.meterpointid
        AND thdia.date_m = thdib.date_m
    LEFT JOIN (
        SELECT
            meterpointid,
            date_m,
            qtyvalue
        FROM
            edmi.tblprofilethdi
        WHERE
            METERPOINTID IN ({meterpointlist})
            AND DATE_M BETWEEN TO_DATE(\'{datetime_start}\','YYYY-MM-DD HH24.MI') AND TO_DATE(\'{datetime_end}\','YYYY-MM-DD HH24.MI')
                AND PHASE = 2) THDIC ON
        thdia.meterpointid = thdic.meterpointid
        AND thdia.date_m = thdic.date_m
        '''.format(datetime_start=dt_start, datetime_end=date_end,meterpointlist=meterpointlist_str)

    sql_thdv = '''
    SELECT
        TO_CHAR(thdva.date_m, 'YYYY-MM-DD HH24:MI') DATETIME_METER,
        thdva.meterpointid,
        thdva.qtyvalue tae_thdv_A,
        thdvb.qtyvalue tae_thdv_B,
        thdvc.qtyvalue tae_thdv_C
    FROM
        (
        SELECT
            meterpointid,
            date_m,
            qtyvalue
        FROM
            edmi.tblprofilethdv
        WHERE
            METERPOINTID IN ({meterpointlist})
            AND DATE_M BETWEEN TO_DATE(\'{datetime_start}\','YYYY-MM-DD HH24.MI') AND TO_DATE(\'{datetime_end}\','YYYY-MM-DD HH24.MI')
            AND PHASE = 0
    ) THDVA
    LEFT JOIN (
        SELECT
            meterpointid,
            date_m,
            qtyvalue
        FROM
            edmi.tblprofilethdv
        WHERE
            METERPOINTID IN ({meterpointlist})
            AND DATE_M BETWEEN TO_DATE(\'{datetime_start}\','YYYY-MM-DD HH24.MI') AND TO_DATE(\'{datetime_end}\','YYYY-MM-DD HH24.MI')
                AND PHASE = 1) THDVB ON
        thdva.meterpointid = thdvb.meterpointid
        AND thdva.date_m = thdvb.date_m
    LEFT JOIN (
        SELECT
            meterpointid,
            date_m,
            qtyvalue
        FROM
            edmi.tblprofilethdv
        WHERE
            METERPOINTID IN ({meterpointlist})
            AND DATE_M BETWEEN TO_DATE(\'{datetime_start}\','YYYY-MM-DD HH24.MI') AND TO_DATE(\'{datetime_end}\','YYYY-MM-DD HH24.MI')
                AND PHASE = 2) THDVC ON
        thdva.meterpointid = thdvc.meterpointid
        AND thdva.date_m = thdvc.date_m
        '''.format(datetime_start=dt_start, datetime_end=date_end,meterpointlist=meterpointlist_str)
    df_cus = pd.read_sql_query(sql_cus, engine, params=meter)
    print(df_cus)
    df_whimp = pd.read_sql_query(sql_whimp, engine, params=meter)
    print(df_whimp)
    df_varhimp = pd.read_sql_query(sql_varimp, engine, params=meter)
    print(df_varhimp)
    df_whexp = pd.read_sql_query(sql_whexp, engine, params=meter)
    print(df_whexp)
    df_varhexp = pd.read_sql_query(sql_varexp, engine, params=meter)
    print(df_varhexp)
    df_pf = pd.read_sql_query(sql_pf, engine, params=meter)
    print(df_pf)
    df_instv = pd.read_sql_query(sql_instv, engine, params=meter)
    print(df_instv)
    df_insti = pd.read_sql_query(sql_insti, engine, params=meter)
    print(df_insti)
    df_ang = pd.read_sql_query(sql_instang, engine, params=meter)
    print(df_ang)
    df_thdi = pd.read_sql_query(sql_thdi, engine, params=meter)
    # print(df_thdi)
    # df_thdv = pd.read_sql_query(sql_thdv, engine, params=meterpointlist)
    # print(df_thdv)
    df_wh = pd.merge(df_whimp, df_whexp, how="left", on=["meterpointid", "datetime_meter"],suffixes=["_whimp","_whexp"])
    print(df_wh)
    df_varh = pd.merge(df_varhimp, df_varhexp, how="left", on=["meterpointid", "datetime_meter"])
    print(df_varh)
    df_all = pd.merge(df_wh, df_instv, how="left", on=["meterpointid", "datetime_meter"])
    df_1 = pd.merge(df_insti, df_instv, how="left", on=["meterpointid", "datetime_meter"])
    df_2 = pd.merge(df_ang,df_varh,how="left", on=["meterpointid", "datetime_meter"])
    df_3 = pd.merge(df_1,df_2,how="left", on=["meterpointid", "datetime_meter"])
    df_4 = pd.merge(df_3,df_pf,how="left", on=["meterpointid", "datetime_meter"])
    df_5 = pd.merge(df_4, df_cus, how="left", on=["meterpointid"])
    df_all = pd.merge(df_5,df_wh,how="left", on=["meterpointid", "datetime_meter"])
    # df_all.to_csv("test.csv",index=False)
    df_final = df_all.replace(({np.NaN:None}))
    print(df_final)
    # df_final.info()
    #INSERT DATA TO MSSQL
    sql_update = '''
    MERGE [testsandbox].[dbo].[tb_last_update] as t
    USING (Values(?,?)) AS s(meter_point_id, date_time)
    ON t.meter_point_id = s.meter_point_id 
    WHEN MATCHED THEN UPDATE SET 
        t.meter_point_id = s.meter_point_id,
        t.date_time = s.date_time
    WHEN NOT MATCHED BY TARGET 
        THEN INSERT (meter_point_id, date_time)
            VALUES (s.meter_point_id, s.date_time);
    '''
    sql_insert = '''
    INSERT INTO [testsandbox].[dbo].[tb_amr_energy] (meter_point_id, tae_i_a, tae_i_b, tae_i_c, tae_v_a, tae_v_b, tae_v_c, tae_pf, tae_ang_a, tae_ang_b, tae_ang_c, tae_kvarh_imp, tae_kvarh_exp, tae_kwh_imp, tae_kwh_exp, tae_date, contract_account) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    '''
    for index, row in df_final.iterrows():
        print("insert row ", index, "Values = ",row.meterpointid, row.tae_i_a, row.tae_i_b, row.tae_i_c, row.tae_v_a, row.tae_v_b, row.tae_v_c, row.tae_pf, row.tae_ang_a, row.tae_ang_b, row.tae_ang_c, row.tae_kvarh_imp, row.tae_kvarh_exp, row.tae_kwh_imp, row.tae_kwh_exp, row.datetime_meter)
        cursor.execute(sql_insert, row.meterpointid, row.tae_i_a, row.tae_i_b, row.tae_i_c, row.tae_v_a, row.tae_v_b, row.tae_v_c, row.tae_pf, row.tae_ang_a, row.tae_ang_b, row.tae_ang_c, row.tae_kvarh_imp, row.tae_kvarh_exp, row.tae_kwh_imp, row.tae_kwh_exp, row.datetime_meter, row.customercode)
        cursor.execute(sql_update, row.meterpointid, row.datetime_meter)
    cnxn.commit()
    # cursor.close()

#query on by one
for device in meterpointlist:
    device = [device]
    in_vars = ','.join(':%d' % i for i in range(len(device)))
    # print(in_vars) 
    if dictionary[device[0]]:
        print("Value from list is " + str(device[0]))
        print("Value from dictionary is " + str(dictionary[device[0]]))
        #Get last update + 15 min to check if it equal to current query time
        lastest_ = datetime.strptime(dictionary[device[0]],'%Y-%m-%d %H:%M')
        log_msg = "Latest update of meterpointid "+ str(device[0]) + " = " + lastest_.strftime('%Y-%m-%d %H:%M')
        print(log_msg)
        logging.info(log_msg)
        lastest = lastest_ + timedelta(minutes=15)
        print("Time for checking is " + lastest.strftime('%Y-%m-%d %H:%M'))
        if querytime == lastest:
            log_msg = "Time using for query of meterpointid " + str(device[0]) + " is " + querytime.strftime('%Y-%m-%d %H:%M')
            logging.info(log_msg)
            print("log_msg")
            dt_end = dt_start = querytime.strftime('%Y-%m-%d %H:%M')
        else:

            log_msg = "Time using for query of meterpointid " + str(device[0]) + " is " + lastest.strftime('%Y-%m-%d %H:%M') + " to " + querytime.strftime('%Y-%m-%d %H:%M')
            logging.info(log_msg)
            print("log_msg")
            dt_start = lastest.strftime('%Y-%m-%d %H:%M')
            dt_end = querytime.strftime("%Y-%m-%d %H:%M")
    else:
        print("No latest time is found, maybe a new meter?")
        log_msg = "Using default start time for query of new meterpointid " + str(device[0]) + " = as " + default_query_time.strftime("%Y-%m-%d %H:%M")
        logging.info(log_msg)
        print("log_msg")
        dt_start = default_query_time.strftime("%Y-%m-%d %H:%M")
        dt_end = querytime.strftime("%Y-%m-%d %H:%M")
    getandsave(in_vars, device, dt_end)
cursor.close()

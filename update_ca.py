from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, exc, text, MetaData, Table
from sqlalchemy.exc import SQLAlchemyError
import cx_Oracle
import csv
import os
import sys
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

logging.basicConfig(filename="update_pea_ca.log", filemode="a",
                    format="%(process)d - %(levelname)s - %(asctime)s - %(message)s", datefmt='%d-%b-%Y %H:%M:%S', level=logging.INFO)
#today = datetime.today()
now = datetime.now()
log_msg = "Start execute task on " + now.strftime("%Y-%m-%d %H:%M:%S")
logging.info(log_msg)

conn = cx_Oracle.connect(
    user=r'pea_kritsakorn', password='pea_kritsakorn', dsn='mdamr_srv', encoding="UTF-8")
cursor = conn.cursor()

# MSSQL connection
server = 'localhost'
database = 'testsandbox'
username = 'sa'
password = '1q2w3e4r'
port = '1433'

# server = '172.16.62.31'
# database = 'ERC_SANDBOX_2'
# username = 'amruser'
# password = 'tpaamruser3e4r!'
# port = '50609'

cnxn_str = ("Driver={ODBC Driver 18 for SQL Server};"
            "Server=" + server + "," + port + ";"
            "Database=" + database + ";"
            "UID=" + username + ";"
            "PWD=" + password + ";"
            "Encrypt=no;"
            # "Trusted_Connection=yes;"
            )

print(cnxn_str)

cnxn = pyodbc.connect(cnxn_str)
cursor = cnxn.cursor()

connection_url = URL.create(
    "mssql+pyodbc",
    username=username,
    password=password,
    host=server,
    port=port,
    database=database,
    query={
        "driver": "ODBC Driver 18 for SQL Server",
        "Encrypt": "yes",
        "TrustServerCertificate": "yes",
    },
)
# create a connection to the database
try:
    mssql_engine = create_engine(connection_url, future=True)
    print("Sandbox2 Server Conected.")
except exc.SQLAlchemyError as e:
    error = str(e)
    print(error)

# create a metadata object
metadata = MetaData()
# create a table object by loading the existing table schema
sb2_customer = Table('tb_pea_customer', metadata,
                     autoload=True, autoload_with=mssql_engine)

sql1 = '''
SELECT tb1.meter_point_id FROM [dbo].[tb_pea_customer] tb1
WHERE tb1.meter_point_id IS NOT NULL
AND tb1.contract_account IS NULL;'''

with mssql_engine.connect() as conn:
    df = pd.read_sql_query(sql=text(sql1), con=conn)
    # query = sb2_customer.select([sb2_customer.columns.meter_point_id, sb2_customer.columns.contract_account]).where(
    #     and_(sb2_customer.columns.meter_point_id.is_not(None), sb2_customer.columns.contract_account.is_(None)))
    # ResultProxy = conn.execute(text(sql1))
    # ResultSet = ResultProxy.fetchall()
    # print(ResultSet)
# medter_dict = df.to_dict()
# print(medter_dict)
# print(df)
# print(df.dtypes)
meterpointlist = df['meter_point_id'].values.tolist()
# print(meterpointlist)

# Oracle
USERNAME = "pea_kritsakorn"
PASSWORD = "pea_kritsakorn"
DSN = "mdamr_srv"
ENCODING = "UTF-8"
# COMMAND_FILE = "export.sql"
CONFIG = "C:/Oracle/instantclient_19_5/network/admin"
DIALECT = 'oracle'
SQL_DRIVER = 'cx_oracle'
ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + \
    '://' + USERNAME + ':' + PASSWORD + '@' + DSN
oracledb.init_oracle_client(lib_dir=r"C:/Oracle/instantclient_19_5")

# SQLAlchemy
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
    print("AMR Conected.")
except exc.SQLAlchemyError as e:
    error = str(e)
    print(error)

finalResultDF = pd.DataFrame(columns=['meterpointid', 'customercode'])


def getandsave(meter):
    sql_cus = '''
    SELECT mp.meterpointid, cus.customercode
    FROM edmi.tblmeterpoints mp
    LEFT JOIN edmi.tblcustomers cus ON mp.customerid = cus.customerid
    WHERE mp.meterpointid = :mtpid
    '''

    # list1 = """' , '""".join([str(item) for item in meterpointlist])
    # list = tuple(meterpointlist)

    queryString = '''
    SELECT cus.customercode 
    FROM edmi.tblmeterpoints mp
    LEFT JOIN edmi.tblcustomers cus ON mp.customerid = cus.customerid
    WHERE mp.meterpointid = :mtpid
    '''
    # + str(tuple(list))
    with engine.connect() as orac_conn:
        # queryResultDF = pd.read_sql_query(
        #     sql=text(sql_cus), con=orac_conn, params={"mtpid": meter})
        ResultProxy = orac_conn.execute(text(sql_cus), mtpid = meter)
        ResultSet = ResultProxy.fetchone()
        ca = ResultSet[1]
        print("Meterpointid " + str(meter) + " is " + str(ca))
        with mssql_engine.connect() as conn:
            sql_update = '''UPDATE [dbo].[tb_pea_customer] SET contract_account = :ca WHERE meter_point_id = :mtpid'''
            # update the contract_account of a record where the meter_point_id is = meter
            # update_stmt = sb2_customer.update().where(sb2_customer.c.meter_point_id == 1).values(contract_account=ca)
            print("Setting the data....")
            # execute the update statement
            # conn.execute(update_stmt)
            # create a try-except block to catch any SQLAlchemy errors
            try:

                # update the contract_account of a record where the meter_point_id is = meter
                # update_stmt = sb2_customer.update().where(sb2_customer.c.meter_point_id == 1).values(contract_account=str(ca))
                update_stmt = text("UPDATE [dbo].[tb_pea_customer] SET contract_account = :ca WHERE meter_point_id = :mtpid")

                # bind the named parameters to the SQL statement
                bound_sql = update_stmt.bindparams(ca=ca, mtpid=meter)
                # execute the update statement
                # conn.execute(update_stmt)
                conn.execute(bound_sql)

                # commit the changes
                conn.commit()
                print("Update successful")

            except SQLAlchemyError as e:
                # rollback the transaction if an error occurs
                conn.rollback()
                print(f"Error occurred: {str(e)}")
        # print(finalResultDF)


for device in meterpointlist:
    getandsave(device)
# print(finalResultDF)
cursor.close()

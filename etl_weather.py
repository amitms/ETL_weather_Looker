# -*- coding: utf-8 -*-
"""ETL_weather.ipynb
python code to read the weather data from visual Crossing for particular City,Country and load as JSON
which is tranformed into CSV with cleanup data
and then load to IBM cloud DB2 database
pushed to google sheet which is visualized using Google Looker
executionwith Airflow data pipeline to run and schedule in batch intervals
"""
##### necessary python packages installation
# !pip install --upgrade pip
# !pip install "apache-airflow==2.9.0" \
# --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.10.txt"
# !pip install --force-reinstall ibm_db ibm_db_sa
# !pip install --upgrade ibm_db ibm_db_sa
# !pip install pandas
# check airflow docker shell packages .
# command: bash -c "pip install -r requirements.txt && airflow webserver"
# docker compose exec airflow-webserver bash
# python --version
# which python
# pip install --upgrade pip
# pip install pandas sqlalchemy ibm_db ibm_db_sa
# install pandas globally inside container
# pip install --upgrade --target=/opt/airflow/.local/lib/python3.12/site-packages pandas
# pip install gspread gspread-dataframe gspread-auth

##### importing all necessary python libraries
import urllib.request
import json
import sys
import logging
import pendulum
import pandas as pd
# import ibm_db
# import ibm_db_dbi
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
# from airflow.decorators import dag, task
# from config import Config
# import gspread
# from gspread_dataframe import set_with_dataframe


# logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # set minimum level

# Create a file handler
file_handler = logging.FileHandler('app_log.log')  # log file path
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
# logger.info("This is a log message WEATHER_DATA ETL")

# define the location and parameters for the API call default values
# if external config file is used then from config import Config  &key={Config.VISUALCROSSING_API_KEY}

############################# EXTRACT task to fetch weather data for given location from date ragne from visual Crossing API date in YYYY-MM-DD format ####
# fetching weather data from weather api of https://www.visualcrossing.com/
# define the API key and base URL
api_key ="KMRF8C6UADP84FUDRSGDYBZ2C"
base_url =  "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
# print (api_key)
city = "London,UK"
unit_group = "metric"
content_type = "json"
# Fort Worth, TX

# IBM db2 configurations
dsn_hostname = "54a2f15b-5c0f-46df-8954-7e38e612c2bd.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud" 
dsn_uid = "MGV47022"        
dsn_pwd = "VwteXwlQNaJgq8xe"    
dsn_driver = "{IBM DB2 ODBC DRIVER}"
dsn_database = "bludb"            
dsn_port = "32733" 
dsn_protocol = "TCPIP"  
dsn_security = "SSL" 
dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd,dsn_security)


############################################## functions  ############################################## 
## EXTRACT: get data from visualcrossing weather API ###
# Airflow stores XCom data in its metadata database.
def retrieve_weather(city, start_day, end_day, **context):
    # construct the request URL
    request_url = f"{base_url}{city}/{start_day}/{end_day}?unit_group={unit_group}&include=days&contentType={content_type}&key={api_key}"
    print(request_url)
    try:
      # Make the request to the visual Crossing Weather API
      with urllib.request.urlopen(request_url) as response:
        if response.status == 200:
          # Read and decode the response data
          data = response.read()
          # parse JSON data
          weather_data = json.loads(data.decode('utf-8'))     
          # with open("weather_data.json","w") as file:
          #   json.dump(weather_data, file, indent =4)
          # print(" file saved to weather_data.json")     
          context["ti"].xcom_push(key="weather_data", value=weather_data)
          print("weather_data.json will be passed to transform by **context.")
          logger.info("weather_data.json will be passed to transform by **context")
        else:
          print(f"Error: Received status code {response.state}", file = sys.stderr)
    except urllib.error.URLError as e:
      print(f"Failed to retrieve weather data: {e.reason}", file = sys.stderr)
      logger.error(f"Failed to retrieve weather data: {e.reason}", file = sys.stderr)
    return f"Loaded {len(weather_data)} rows from weather API"
###### TRANSFORM: code for cleaning and transforming the data
def transform_data(**context):
  records = context["ti"].xcom_pull(key="weather_data", task_ids="extract_weather")
  try:
    resolvedAddress = records.get("resolvedAddress", "")
    values = records.get("days", [])
    df = pd.DataFrame(
        values,
        columns=["datetime", "temp", "feelslike", "humidity", "precip", "windspeed"]
        )
    df["resolvedAddress"] = resolvedAddress
    # unit conversion
    df.columns = ["date", "temperature", "feels_like", "humidity", "precipitation", "windspeed", "resolvedAddress"]
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d") # issue is pd.to_datetime() converts the date to a Timestamp object which is not JSON serializable. Convert it back to a string before pushing to XCom:
    df["temperature"] = pd.to_numeric(df["temperature"], errors='coerce') # if value cannot be converted 'coerce' replaces with "NaN"
    df["feels_like"] = pd.to_numeric(df["feels_like"], errors='coerce')
    df["humidity"] = pd.to_numeric(df["humidity"], errors='coerce')
    df["precipitation"] = pd.to_numeric(df["precipitation"], errors='coerce')
    df["windspeed"] = pd.to_numeric(df["windspeed"], errors='coerce')

    # clean column names
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    # fill NaN with None (also not JSON serializable)
    df = df.where(pd.notnull(df), None)
    # df.to_csv("transformed_weather_data.csv", index=False)
    context["ti"].xcom_push(key="transformed_data", value= df.to_dict(orient="records"))
    print("transformed successfully ")
    logger.info("transformed successfully ")
  except KeyError as e:
    print(f"Missing expected key in data: {e}")
    logger.error(f"Missing expected key in data: {e}")
    sys.exit(1)
  except Exception as e:
    print(f"An error occurred during data transformation: {e}")
    logger.error(f"An error occurred during data transformation: {e}")
    sys.exit(1)

############################# LOAD transformed data .csv and write to database
def load_data_to_db2(table_name, **context):
  import ibm_db
  import ibm_db_dbi
  try:
    conn, engine = db2_connection()
    print ("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)
    logger.info ("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)
    server = ibm_db.server_info(conn)
    print ("DBMS_NAME: ", server.DBMS_NAME)
    print ("DBMS_VER:  ", server.DBMS_VER)
    print ("DB_NAME:   ", server.DB_NAME)
    # create table in remote IBM cloud DB2 database
    records = context["ti"].xcom_pull(key="transformed_data", task_ids="transform_weather")
    df = pd.DataFrame(records)
    df.to_sql(table_name, engine, if_exists="append", index=False,chunksize=1000)
    print(f"Data loaded into the table {table_name} successfully!")
    logger.info(f"Data loaded into the table {table_name} successfully!")
    ibm_db.close(conn)      
  except SQLAlchemyError as e:
    print(f"SQLAlchemyError : {str(e)} ", )
  except Exception as e:
    print(f"Error loading data to IBMDB2 : {e}" )
    print(f"Error loading data to ibm_db.conn_errormsg(): {ibm_db.conn_errormsg()}" )
    sys.exit(1)

# Read from IBM DB2 cloud to export to google sheets to be used by Google Looker
def read_data_db2_gs(table_name="weather_data",**context):
  import ibm_db
  import ibm_db_dbi
  import gspread
  from gspread_dataframe import set_with_dataframe
  try:
    conn, engine = db2_connection()
    print ("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)
    server = ibm_db.server_info(conn)
    print ("DBMS_NAME: ", server.DBMS_NAME)
    print ("DBMS_VER:  ", server.DBMS_VER)
    print ("DB_NAME:   ", server.DB_NAME)
    # create table in remote IBM cloud DB2 database
    df = pd.read_sql(f"SELECT * from {table_name}", engine)
    # df = pd.read_csv("transformed_weather_data.csv")
    print(f"Data read from into the table {table_name} successfully!")
    logger.info(f"Data loaded into the table {table_name} successfully!")    
  
    # push to google sheets opens browser to login with google account no crednetials.json needed
    gc = gspread.service_account(filename='/opt/airflow/dags/sapient-cycle-300703-e8b417737ae1.json') # if gcp cloud is used use service credentials    
    # gc = gspread.oauth(credentials_filename='/content/client_secret_899747170575-6vu3pmn521nsona5tih7utdpikil87sf.apps.googleusercontent.com.json')
    # gc = gspread.oauth()
    print("Connected to google sheets✅")
    logger.info("Connected to google sheets✅")
    sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1xf-vxmY9MJ7843oPXeUT0afhYCveMKCX2Kq5Ycw_T5w/edit?usp=sharing")
    set_with_dataframe(sh.sheet1, df)    
    ibm_db.close(conn)
    return f"Loaded {len(df)} rows to google sheets"
  except SQLAlchemyError as e:
    print(f"SQLAlchemyError : {str(e)} ", )
  except Exception as e:
    print(f"Error loading data to IBMDB2 : {e}" )
    print(f"Error loading data to ibm_db.conn_errormsg(): {ibm_db.conn_errormsg()}" )
    sys.exit(1)

def db2_connection():
  import ibm_db
  import ibm_db_dbi
  class PatchedConnection(ibm_db_dbi.Connection):
    @property
    def current_schema(self):
        return dsn_uid
  def get_conn():
      conn = ibm_db.connect(dsn, "", "")
      return PatchedConnection(conn)
  try:
    conn = ibm_db.connect(dsn, "", "")
    engine = create_engine("db2+ibm_db://", creator=get_conn)
    return conn, engine
  except SQLAlchemyError as e:
    print(f"SQLAlchemyError : {str(e)} ", )
  except Exception as e:
    print(f"Error loading data to IBMDB2 : {e}" )
    print(f"Error loading data to ibm_db.conn_errormsg(): {ibm_db.conn_errormsg()}" )
    sys.exit(1)

# export AIRFLOW_HOME =/content/
# echo @AIRFLOW_HOME

############################# Airflow configurations and define pipeline
# load_data_to_db2(df, table_name="weather_data")
# step1: defining DAG arguments
default_args = {
    'owner' : 'amit',
    'start_date' : pendulum.today("UTC").add(days=0), 
    'email' : 'amitms2005@hotmail.com',
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
    'email_on_failure': True    
}
# days_ago(0), datetime(2024, 1, 1)
with DAG(
  dag_id="weather_etl",
  default_args=default_args,
  schedule=timedelta(minutes=10), 
  catchup=False,
  description="Weather ETL — Visual Crossing → Transform → DB2 → Google Sheets",
) as  dag:
# schedule=None, schedule_interval=timedelta(minutes=5),
## TASKS
  t1 = PythonOperator(
    task_id="extract_weather",
    python_callable=retrieve_weather,
    op_kwargs={
      "city": "London,UK",
      "start_day":"2026-03-17",
      "end_day":"2026-03-19",
    },
  )
  # Fort Worth, TX
  t2 = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,  
  )
  t3 = PythonOperator(
    task_id="load_data_to_db2",
    python_callable=load_data_to_db2,
    op_kwargs={
      "table_name":"weather_data",
    },
  )
  t4 = PythonOperator(
    task_id="read_data_db2_gs",
    python_callable=read_data_db2_gs, 
    op_kwargs={
      "table_name":"weather_data",
    }, 
  )

# pipeline Order
  t1 >> t2 >> t3 >> t4
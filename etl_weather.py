# -*- coding: utf-8 -*-
"""ETL_weather.ipynb
python code to read the weather data from visual Crossing for particular City,Country and load as JSON
which is tranformed into CSV with cleanup data
and then load to IBM cloud DB2 database
executionwith Airflow data pipeline to run and schedule in batch intervals
"""
##### necessary python packages installation
!pip install --upgrade pip
!pip install "apache-airflow==2.9.0" \
--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.10.txt"
!pip install --force-reinstall ibm_db ibm_db_sa
!pip install --upgrade ibm_db ibm_db_sa


##### importing all necessary python libraries
import urllib.request
import json
import sys
import logging
# from config import Config
from airflow.decorators import task
import pandas as pd
import ibm_db
import ibm_db_dbi
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from datetime import timedelta
from airflow.models import DAG
from ariflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

############################# EXTRACT task to fetch weather data for given location from date ragne from visual Crossing API date in YYYY-MM-DD format ####
# fetching weather data from weather api of https://www.visualcrossing.com/
# define the API key and base URL

# logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # set minimum level

# Create a file handler
file_handler = logging.FileHandler('app_log.log')  # log file path
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.info("This is a log message WEATHER_DATA ETL")

# define the location and parameters for the API call default values
# if external config file is used then from config import Config  &key={Config.VISUALCROSSING_API_KEY}

api_key ="KMRF8C6UADP84FUDRSGDYBZ2C"
base_url =  "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
print (api_key)
location = "London,UK"
unit_group = "metric"
content_type = "json"

@tasks
def retrieve_weather(city, start_date, end_date):
    # onstruct the request URL
    request_url = f"{base_url}{city}/{start_date}/{end_date}?unit_group={unit_group}&include=days&contentType={content_type}&key={api_key}"
    print(request_url)
    try:
  # Make the request to the visual Crossing Weather API
      with urllib.request.urlopen(request_url) as response:
        if response.status == 200:
          # Read and decode the response data
          data = response.read()
          # parse JSON data
          weather_data = json.loads(data.decode('utf-8'))

          # example: print weather data
          # print("Weather data for : ",city)
          # print(json.dumps(weather_data, indent =4))
          # dump to external file weather_data.json to be consumed by tranformation.py
          with open("weather_data.json","w") as file:
            json.dump(weather_data, file, indent =4)
          print(" file saved to weather_data.json")
          logger.info(" file saved to weather_data.json")
        else:
          print(f"Error: Received status code {response.state}", file = sys.stderr)
    except urllib.error.URLError as e:
      print(f"Failed to retrieve weather data: {e.reason}", file = sys.stderr)
      logger.error(f"Failed to retrieve weather data: {e.reason}", file = sys.stderr)
    return weather_data

# retrieve_weather("London,UK","2026-03-16","2026-03-16")
# retrieve_weather("FortWorth,USA","2026-03-17","2026-03-17")

############################# TRANSFORM: code for cleaning and transforming the data
import pandas as pd
import json
import sys
# from config import config
@tasks
def transform_data(file_name = "weather_data.json"):
# load json file from extraction
  try:
    with open(file_name,"r") as file:
      data = json.load(file)
  except FileNotFoundError:
    print(f"File {file_name} not found.")
    sys.exit(1)
  except json.JSONDecodeError:
    print("Error decoding JSON from file")
    sys.exit(1)
# transform data
  try:
    resolvedAddress = data.get("resolvedAddress", "")
    # print(resolvedAddress)
    records = data.get("days", [])
    # print(records)
    df = pd.DataFrame(
        records,
        columns=["datetime", "temp", "feelslike", "humidity", "precip", "windspeed"]
        )
    df["resolvedAddress"] = resolvedAddress
    # print(df.head())
    # unit conversion
    df.columns = ["date", "temperature", "feels_like", "humidity", "precipitation", "windspeed", "resolvedAddress"]
    df["date"] = pd.to_datetime(df["date"])
    df["temperature"] = pd.to_numeric(df["temperature"], errors='coerce') # if value cannot be converted 'coerce' replaces with "NaN"
    df["feels_like"] = pd.to_numeric(df["feels_like"], errors='coerce')
    df["humidity"] = pd.to_numeric(df["humidity"], errors='coerce')
    df["precipitation"] = pd.to_numeric(df["precipitation"], errors='coerce')
    df["windspeed"] = pd.to_numeric(df["windspeed"], errors='coerce')

    # clean column names
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    df.to_csv("transformed_weather_data.csv", index=False)
    df = pd.read_csv("transformed_weather_data.csv")
    return df
  except KeyError as e:
    print(f"Missing expected key in data: {e}")
    logger.error(f"Missing expected key in data: {e}")
    sys.exit(1)
  except Exception as e:
    print(f"An error occurred during data transformation: {e}")
    logger.error(f"An error occurred during data transformation: {e}")
    sys.exit(1)

# retrieve_weather("FortWorth,USA","2026-03-17","2026-03-17")
# transformed_data = transform_data("weather_data.json")
# transformed_data.to_csv("transformed_weather_data.csv", index=False)
# print(transformed_data)

############################# LOAD transformed data .csv and write to database
@tasks
def load_data_to_db2(df, table_name="WEATHER_DATA"):
  # connect to IBM DB2 cloud db
  #Replace the placeholder values with your actual Db2 hostname, username, and password:
  dsn_hostname = "54a2f15b-5c0f-46df-8954-7e38e612c2bd.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud" # e.g.: "54a2f15b-5c0f-46df-8954-7e38e612c2bd.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud"
  dsn_uid = "MGV47022"        # e.g. "abc12345"
  dsn_pwd = "VwteXwlQNaJgq8xe"      # e.g. "7dBZ3wWt9XN6$o0J"

  dsn_driver = "{IBM DB2 ODBC DRIVER}"
  dsn_database = "bludb"            # e.g. "BLUDB"
  dsn_port = "32733"                # e.g. "32733"
  dsn_protocol = "TCPIP"            # i.e. "TCPIP"
  dsn_security = "SSL"              #i.e. "SSL"
  dsn = (
      "DRIVER={0};"
      "DATABASE={1};"
      "HOSTNAME={2};"
      "PORT={3};"
      "PROTOCOL={4};"
      "UID={5};"
      "PWD={6};"
      "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd,dsn_security)

  class PatchedConnection(ibm_db_dbi.Connection):
    @property
    def current_schema(self):
        return dsn_uid

  def get_conn():
      conn = ibm_db.connect(dsn, "", "")
      return PatchedConnection(conn)

  try:
    conn = ibm_db.connect(dsn, "", "")
    # engine = create_engine(dsn)
    # engine = create_engine(f"db2+ibm_db://{dsn_uid}:{dsn_pwd}@{dsn_hostname}:{dsn_port}/{dsn_database}?security=SSL")
    engine = create_engine("db2+ibm_db://", creator=get_conn)
    print ("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)
    server = ibm_db.server_info(conn)

    print ("DBMS_NAME: ", server.DBMS_NAME)
    print ("DBMS_VER:  ", server.DBMS_VER)
    print ("DB_NAME:   ", server.DB_NAME)
    # create table in remote IBM cloud DB2 database
    # createQuery = "create table weather_data(DATE DATE, temperature NUMERIC(10, 2), feels_like NUMERIC(10, 2), humidity NUMERIC(10, 2), windspeed NUMERIC(10, 2), resolvedaddress VARCHAR(50))"
    #Now fill in the name of the method and execute the statement
    # createStmt = ibm_db.exec_immediate(conn,createQuery)
    df.to_sql(table_name, engine, if_exists="replace", index=False,chunksize=1000)
    print(f"Data loaded into the table {table_name} successfully!")
    logger.info(f"Data loaded into the table {table_name} successfully!")
    ibm_db.close(conn)
  except SQLAlchemyError as e:
    print(f"SQLAlchemyError : {str(e)} ", )
  except Exception as e:
    print(f"Error loading data to IBMDB2 : {e}" )
    print(f"Error loading data to ibm_db.conn_errormsg(): {ibm_db.conn_errormsg()}" )
    sys.exit(1)

############################# Airflow configurations and define pipeline
load_data_to_db2(df, table_name="weather_data")

# step1: defining DAG arguments
default_args = {
    'owner' : 'amit',
    'start_date' : days_ago(0),
    'email' : 'amitms2005@hotmail.com',
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
}

# step2:  defining DAG:
dag = DAG(
    'weather_data_pipline',
    default_args=default_args,
    description='weather data pipeline for ETL from weather API to IBM cloud DB2',
    schedule_interval=timedelta(minutes=5),
)

# step3: defining tasks with @task decorators in the python methods

# creating task pipeline
retrieve_weather("FortWorth,USA","2026-03-17","2026-03-17") >> transformed_data = transform_data("weather_data.json") >> load_data_to_db2(df, table_name="weather_data")
# export AIRFLOW_HOME =/content/
# echo @AIRFLOW_HOME


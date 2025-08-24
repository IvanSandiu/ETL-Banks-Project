# Code for ETL operations on Country-GDP data
# Importing the required libraries
import sqlite3
import pandas as pd
from datetime import datetime 
from bs4 import BeautifulSoup
import requests
import numpy as np


code_name = "banks_project.py"
data_url = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
exchange_rate_csv_path = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
table_attribs_extraction = ["Name", "MC_USD_Billion"]
table_attribs = ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
output_path = "./Largest_banks_data.csv"
db_name = "Banks.db"
table_name = "Largest_banks"
log_file = "./code_log.txt"

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f:
        f.write(timestamp + ":" + message)

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    df = pd.DataFrame(columns = table_attribs_extraction)
    for row1 in rows[1:]:
        if len(row1)>0:
            row = row1.find_all('td')
            mc_raw = row[2].contents[0]
            dict1 = {
                "Name" : row[1].contents[2].contents[0], 
                "MC_USD_Billion" : float(mc_raw[:-1])
            }
            df1 = pd.DataFrame(dict1, index = [0])
            df = pd.concat([df, df1], ignore_index = True)
    log_progress("Data extraction complete. Initiating Transformation process")
    print(df)
    return df

def transform(df, csv_path):
    data = pd.read_csv(csv_path)
    exchange_rate = data.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    print(df)
    log_progress("Data transformation complete. Initiating Loading process")

    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)
    log_progress("Data saved to CSV file")

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index = False)
    log_progress("Data loaded to Database as a table, Executing queries")

def run_query(query_statement, sql_connection):
    print(query_statement)
    res= pd.read_sql(query_statement,sql_connection)
    print(res)
    log_progress("Process Complete")

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress("Preliminaries complete. Initiating ETL process")
df = extract(data_url, table_attribs_extraction)
df = transform(df, exchange_rate_csv_path)
load_to_csv(df, output_path)
conn = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")
load_to_db(df, conn, table_name)
run_query("SELECT * FROM Largest_banks", conn)
run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", conn)
run_query("SELECT Name from Largest_banks LIMIT 5",conn)
conn.close()
log_progress("Server Connection closed")
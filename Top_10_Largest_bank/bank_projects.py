import pandas as pd
import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime


URL = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
log_file = 'code_log.txt'
csv_path = 'D:/DADE/Top10Banks/Largest_Banks_Data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'

#For logging
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file,"a") as f:
        f.write(timestamp + ',' + message + '\n')


#Function for extracting data from the URL
def extract(u):
    df = pd.DataFrame(columns=["Name","MC_USD_Billion"])
    html_page = requests.get(URL).text
    data = BeautifulSoup(html_page,'html.parser')

    #find all tables by table and class
    tables = data.find('table' , {'class':'wikitable'})

    #find the rows by <tr> tag
    rows  = tables.findAll('tr')

    for row in rows:
        #findind all the data cells
        col =  row.findAll('td')

        if len(col) >= 3:
            my_data ={
                "Name" : col[1].get_text(strip=True),
                "MC_USD_Billion" : col[2].get_text(strip=True)
            }
            df1 = pd.DataFrame(my_data,index=[1])
            df = pd.concat([df1,df] , ignore_index=True)
    return df


#Function for applying transformations to the data
def transform(Data_Frame):
    new_df = pd.read_csv('D:/DADE/Top10Banks/exchange_rate.csv')
    f_df = pd.DataFrame(columns=["MC_GBP_Billion","MC_EUR_Billion","MC_INR_Billion"])
    Data_Frame["MC_USD_Billion"] = pd.to_numeric(Data_Frame["MC_USD_Billion"])
    new_df["Rate"] = pd.to_numeric(new_df["Rate"])
    for i in range(0,10):
        final = {
            "Name":Data_Frame["Name"][i],
            "MC_USD_Billion" : Data_Frame["MC_USD_Billion"][i],
            "MC_GBP_Billion" : round(Data_Frame["MC_USD_Billion"][i] * new_df["Rate"][1],2),
            "MC_EUR_Billion" : round(Data_Frame["MC_USD_Billion"][i] * new_df["Rate"][0],2),
            "MC_INR_Billion" : round(Data_Frame["MC_USD_Billion"][i] * new_df["Rate"][2],2)
        }
        fin_df = pd.DataFrame(final,index=[0]).sort_values(["MC_USD_Billion","MC_GBP_Billion","MC_EUR_Billion","MC_INR_Billion"],ascending=False)
        f_df = pd.concat([fin_df,f_df], ignore_index=True)
    return f_df


#Loads the data frame to csv 
def load_to_csv(input_df,path):
    input_df.to_csv(path,index=False)

#Loads data to database
def load_to_db(df,db,tb):
    conn = sqlite3.connect(db_name)
    d.to_sql(tb,conn,if_exists='replace',index=False)
    conn.close()

#Runs Query on Database
def run_query(query,db_name,table_name):
    conn = sqlite3.connect(db_name)
    df3 = pd.read_sql_query(query,conn)
    return df3


#Calling all the functions
log_progress("Start Extract")
Data1 = extract(URL)
log_progress("Extract done")

log_progress("Transformation Started")
d = transform(Data1)
print(d)
log_progress("Transformation Done")

# log_progress("Writing to Database")
# load_to_db(d,db_name,table_name)
# log_progress("Done Writing")

# log_progress("Write to csv")
# load_to_csv(d,csv_path)
# log_progress("csv created")

# ch = input("You want to run queries(y/n):")
# if ch == 'y':
#     log_progress("Enter Query")
#     query = input("Enter the query:")
#     final_set = run_query(query,db_name,table_name)
#     print(final_set)
#     log_progress("Extracting data")
# else:
#     log_progress("Exiting the process")
#     print("Process Completed")




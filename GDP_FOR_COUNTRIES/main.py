import pandas  as pd
import sqlite3
from bs4 import BeautifulSoup
import requests
from datetime import datetime

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
json_path = 'D:/DADE/GDP_FOR_COUNTRIES/Countries_by_GDP.json'
log_file = 'etl_project_log.txt'
df3 = pd.DataFrame(columns=["Country","GDP"])


def logger(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file,"a") as f:
        f.write(timestamp + ',' + message + '\n')


def browser(ul):
    df = pd.DataFrame(columns=["Country","GDP"])
    html_page = requests.get(url).text
    req_data = BeautifulSoup(html_page , 'html.parser')

    tables = req_data.find('table', {'class': 'wikitable'})

    rows = tables.find_all('tr')
    my_data={}
    for row in rows[1:]:
        col = row.find_all(['td'])

        if len(col)>=3:
            my_data ={
                  "Country":col[0].get_text(strip=True),
                  "GDP" : col[2].get_text(strip=True)
            } 
            df1 = pd.DataFrame(my_data,index=[0])
            df = pd.concat([df,df1],ignore_index=True)
    return df


def to_json_file(df,j_path):
    df.drop(0,inplace=True)        
    df.to_json(j_path,index=False)


def insert_to_db(d_name,t_name,df):
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name,conn,if_exists='replace',index=False)
    conn.close()

def check_data(query):
    conn = sqlite3.connect(db_name)
    df3 = pd.read_sql_query(query,conn)
    return df3


logger("Connecting to the browser")
dframe = browser(url)
logger("Url Hit success")

logger("Creating Json file")
to_json_file(dframe,json_path)
logger("Json created sucess")

logger("Creating Database")
insert_to_db(db_name,table_name,dframe)
logger("Database Created")

response = input("Do you want to fire query to table (y/n):")
if response == 'y':
    logger("Extracting sql result")
    sql = input("Enter sql query:")
    final_data = check_data(sql)
    print(final_data)
else:
    logger("Closing Program")
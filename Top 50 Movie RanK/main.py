import  pandas as pd
import sqlite3
import requests
from bs4 import BeautifulSoup

url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
csv_path = 'D:/DADE/WebScrapping/Top 50 Movie RanK/top_50_films.csv'
df = pd.DataFrame(columns=["Film" , "Year" , "Rotten Tomatoes"])
tb_name = 'movies.db'
table_name = 'Top_50_Rotten'
count = 0

html_page = requests.get(url).text
data = BeautifulSoup(html_page,'html.parser')

#Gets the all the tables from the web page
table = data.find_all('tbody')

#For getting rows of  the table
rows = table[0].find_all('tr')

#this gets the rows from the table
for row in rows:
    if  count < 50:

        #loops through the cell of tables
        col = row.find_all('td')
        if len(col)!= 0:
            movie_data = {
                          "Film":col[1].contents[0],
                          "Year": col[2].contents[0],
                          "Rotten Tomatoes": col[3].contents[0]}
            df1 = pd.DataFrame(movie_data,index=[0])
            df = pd.concat([df,df1] , ignore_index = True)
            count += 1
    else:
        break
#this line insert all the data to csv
df.to_csv(csv_path, index=False)

conn = sqlite3.connect(table_name)
df.to_sql(table_name,conn,if_exists= 'replace',index=False)
conn.close()

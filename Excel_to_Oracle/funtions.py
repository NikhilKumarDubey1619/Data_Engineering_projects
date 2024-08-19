import cx_Oracle
from sqlalchemy import create_engine



def cleaning(data):
    #For removing Kg from the from of weight class
    data['Weight Class'] = data['Weight Class'].str.replace('kg','')

    #for removing space from the data in cloumns for all columns 
    #with data type as string

    data = data.applymap(lambda x : x.strip() if isinstance(x,str) else x)

    #for checking and handling duplicates from the data frame
    duplicates = data.duplicated()
    no_of_dup = sum(duplicates)
    print("No of lines which are duplicates is:", no_of_dup)
    print("Line which is duplicated is:",data[duplicates])
    new_df = data.drop_duplicates()
    return new_df


def connecting(new):
    try:
        connection = cx_Oracle.connect(user="???", password = "???", 
                                   dsn = "??/??")
    
        cursor = connection.cursor()

        insert_query = """insert into powerlifter (ATHLETE_NAME,AGE,WEIGHT_GROUP,LIF_TYPE,LIFTED_WT) 
                          values (:1,:2,:3,:4,:5)"""
        
        data_to_insert = [tuple(x) for x in new.to_numpy()]
        cursor.executemany(insert_query,data_to_insert)

        connection.commit()
    except:
        print("Error Occured while inserting")
        



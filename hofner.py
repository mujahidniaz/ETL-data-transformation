import sqlalchemy

import numpy as np
import pandas as pd
import urllib
from xml.etree import ElementTree as ET
import time


pd.set_option('display.max_colwidth', -1)


def get_book_description(isbn):
    key="NSauNVPBhRVn804aDXbg"
    q=isbn
    requestURL = 'https://www.goodreads.com/book/isbn/'+q+'?key='+key # Request URL Contianing key and query for API Call
    root = ET.parse(urllib.request.urlopen(requestURL)).getroot() ##
    book=root[1][16]
    return book.text.replace("'", "").strip().strip('\n')

def get_book_info_by_isbn(isbn):      ## Gets info of a book from goodreads API using isbn number.
    key="NSauNVPBhRVn804aDXbg"
    q=isbn
    requestURL = 'https://www.goodreads.com/search/index.xml?'+'key='+key+'&q=' + q # Request URL Contianing key and query for API Call
    root = ET.parse(urllib.request.urlopen(requestURL)).getroot() ## Parsing the XML response from API 
    book=root[1][6][0][8] ## Index based acess to the book element inside XML object
    return {"title":book.find('title').text.replace("'", "").strip().strip('\n'),"author":book.find('author').find('name').text.replace("'", "").strip().strip('\n')} # Returning dictionary object containing title and author's name of the book

def remove_isbn_and_description_inconsistancy(engine):
     engine.execute("UPDATE bestsellers set isbn=b.isbn from bestsellers b where b.isbn is not null and b.title=bestsellers.title")
     engine.execute("UPDATE bestsellers set description=b.description from bestsellers b where b.description is not null and b.isbn=bestsellers.isbn")
     engine.execute("UPDATE bestsellers set description=trim(both from description)")
     
def create_table_with_published_date_data(published_date,engine):
    print('Creating new table _'+time.strftime('%Y_%M_%d',published_date)+"_bestsellers...\n")
    engine.execute("CREATE TABLE IF NOT EXISTS _"+time.strftime('%Y_%M_%d',published_date)+"_bestsellers AS SELECT * FROM bestsellers WHERE date(published_date) = '"+time.strftime('%M-%d-%Y',published_date)+"'")
    print("Table created successfully...\n")
    print("Deleting records from Bestsellers table...\n")
    engine.execute("DELETE FROM bestsellers WHERE date(published_date) = '"+time.strftime('%M-%d-%Y',published_date)+"'")
    print("Records deleted successfully!!...\n")

def fix_column_inconsistancy(column_name,table_name,engine):
    print('Trying to remove null values in column '+column_name+' from table table '+table_name+' Using Other columns...\n')
    engine.execute("UPDATE "+table_name+" set "+column_name+"=b."+column_name+" from bestsellers b where b."+column_name+" is not null and b.isbn="+table_name+".isbn")
    print("verifying table for null values and Updating from GoodReads api!!...\n")
    null_values = pd.read_sql("select * from "+table_name+" where "+column_name+" is null or "+column_name+"=''", engine)
    updated_from_good_reads=0
    if column_name=='description':
         for index, row in null_values.iterrows():
            desc=get_book_description(row["isbn"])
            engine.execute("UPDATE "+table_name+" set description='"+desc+"' where isbn='"+row["isbn"]+"'") ## updating the values of current table using goodreads API
            engine.execute("UPDATE bestsellers set description='"+desc+"' where isbn='"+row["isbn"]+"'") ## updating the master table for future references to avoid API Calls.
            updated_from_good_reads=updated_from_good_reads+1
    else:
        for index, row in null_values.iterrows():
            res=get_book_info_by_isbn(row["isbn"])
            engine.execute("UPDATE "+table_name+" set title='"+res["title"]+"', author='"+res["author"]+"' where isbn='"+row["isbn"]+"'") ## updating the values of current table using goodreads API
            engine.execute("UPDATE bestsellers set title='"+res["title"]+"', author='"+res["author"]+"' where isbn='"+row["isbn"]+"'") ## updating the master table for future references to avoid API Calls.
            updated_from_good_reads=updated_from_good_reads+1
    print("Updated "+str(updated_from_good_reads)+" records in Column "+column_name+" updated from GoodReads API\n")
    
def get_table_date(table_name,engine):
    table_values = pd.read_sql("select title,author,publisher,description,to_char(published_date, 'YYYY-MM-DD') as published_date,rank,isbn,amazon_url from "+table_name, engine)
    response=[]
    
    for index, row in table_values.iterrows():
        response.append({"ISBN":row["isbn"],"Title":row["title"],"Author":row["author"],"Publisher":row["publisher"],"Published Date":row["published_date"],"Rank":row["rank"],"Amazon URL":row["amazon_url"],"Description":row["description"]})
    return response

if __name__ == '__main__':
   
    SQLALCHEMY_DATABASE_URI = 'postgresql://{user}:{password}@{host}/{database}'.format(host='postgres', 
                                                                                        port=5432, 
                                                                                        user='postgres', 
                                                                                        password='ovFPCzqbDN2XI0Ax', 
                                                                                        database='nyt')
    engine = sqlalchemy.create_engine(SQLALCHEMY_DATABASE_URI, echo=False)
    remove_isbn_and_description_inconsistancy(engine)
    published_date=""
    while published_date!="exit":
       published_date = input("\nEnter a Published Date to create Cleaned Table (format: yyyy-MM-dd) or type 'exit': ")
       published_date=time.strptime(published_date, '%Y-%M-%d')
       table_name="_"+time.strftime('%Y_%M_%d',published_date)+"_bestsellers"
       create_table_with_published_date_data(published_date,engine)
       fix_column_inconsistancy("title",table_name,engine)
       fix_column_inconsistancy("author",table_name,engine)
       fix_column_inconsistancy("description",table_name,engine)
       
           
       
        
   
    
    

    #random_sample = pd.read_sql("select * from bestsellers order by random() limit 5", engine)
    #print('random sample', random_sample)


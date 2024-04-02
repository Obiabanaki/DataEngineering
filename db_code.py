import sqlite3
'''Note: SQLite3 is a software library that implements
a self-contained, serverless, zero-configuration, 
transactional SQL database engine. SQLite is the 
most widely deployed SQL database engine in the 
world. SQLite3 comes bundled with Python and does 
not require installation.'''

import pandas as pd

conn = sqlite3.connect('STAFF.db')

table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

file_path = '/home/project/INSTRUCTOR.csv'
df = pd.read_csv(file_path, names = attribute_list) # the name of columns are given by names

#pandas add data to sql database with connection and table name
df.to_sql(table_name, conn, if_exists = 'replace', index =False)
print('Table is ready')

query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)
'''
adding second table to same database
'''
#for appending, each item should be a list
data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}
data_append = pd.DataFrame(data_dict)

data_append.to_sql(table_name, conn, if_exists = 'append', index =False)
print('Data appended successfully')

query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

table_name = 'Departments'
attribute_list = ['DEPT_ID', 'DEP_NAME', 'MANAGER_ID', 'LOC_ID']

file_path = '/home/project/Departments.csv'

df=pd.read_csv(file_path, names = attribute_list)
df.to_sql(table_name, conn, if_exists='replace',index =True)

dict_append={'DEPT_ID':['9'],
            'DEP_NAME':['Quality Assurance'],
         'MANAGER_ID':['30010'],
             'LOC_ID':['L0010']}

data_append=pd.DataFrame(dict_append)             
data_append.to_sql(table_name,conn, if_exists='append', index=True)

query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

#important
conn.close()
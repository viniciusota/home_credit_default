import sqlite3
import os
import pandas as pd 

# Create database with sqlite3:
conn = sqlite3.connect('home_credit_default.db')

#Create list with file names in home-credit-default-risk folder:
csv_file = [ file_name  for file_name in os.listdir('home-credit-default-risk')]

# create list with database names:
dataset_name = [database_name.split('.')[0] for database_name in csv_file]

for import_file , database_name in zip(csv_file,dataset_name):
    print('Import file {} \n'.format( import_file ) )
    
    path = os.path.join('home-credit-default-risk',import_file)
    data = pd.read_csv( path )
    print(database_name)
    data.to_sql( name = database_name , con = conn , if_exists = 'append' , index = False )
    
    print("Create table {}".format(database_name))

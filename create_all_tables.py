import boto3
import botocore
import csv
import operator
import os
from aws_module.createTable import *
from aws_module.deleteTable import delete_table
from aws_module.load_records import load_csv
def parse_econmoic_data(dynamodb,table):
    try:
        db = dynamodb.Table(table)
        file1 = 'shortlist_curpop.csv'
        file2 = 'shortlist_gdppc.csv'
        reader1 = csv.reader(open(file1))
        reader2 = csv.reader(open(file2))
        next(reader1)
        next(reader2)
        data1 = sorted(reader1,key=operator.itemgetter(0))
        data2 = sorted(reader2,key=operator.itemgetter(0))
        header = ['Country','Currency','GDPPC']
        f = open('combine_economic.csv','w')
        writer = csv.writer(f)
        writer.writerow(header)
        for (row1,row2) in zip(data1,data2):
            dict1 = {}
            year = 1970
            country_name = row1[0]
            currency = row1[1]
            for col in row2:
                if col.isdigit() or col=='':
                    dict1[str(year)] = col
                    year+=1
            row  = [country_name,currency,dict1]
            writer.writerow(row)
        f.close()
        load_csv(dynamodb,table,'combine_economic.csv')
        os.remove('combine_economic.csv')
    except:
        answer = input('Would you like to try again? (Yes/No):')
        if answer =='Yes':
            parse_econmoic_data(dynamodb,table)
def upload_country_data(dynamodb,table):
    try:
        db = dynamodb.Table(table)
        file1 = 'shortlist_capitals.csv'
        file2 = 'shortlist_languages.csv'
        file3  = 'shortlist_area.csv'
        reader1 = csv.reader(open(file1))
        reader2 = csv.reader(open(file2))
        reader3 = csv.reader(open(file3))
        next(reader1)
        next(reader2)
        next(reader3)
        data1 = sorted(reader1,key=operator.itemgetter(0))
        data2 = sorted(reader2,key=operator.itemgetter(0))
        data3 = sorted(reader3,key=operator.itemgetter(0))
        header = ['ISO3','Country','Capital','Area','Languages']
        f = open('combine_country.csv', 'w')
        writer = csv.writer(f)
        writer.writerow(header)
        for (row1,row2,row3) in zip(data1,data2,data3):
            newList = row2[2:]
            row = [row1[0],row1[1],row1[2],row3[2],newList]
            writer.writerow(row)
        f.close()
        load_csv(dynamodb,table,'combine_country.csv')
        os.remove('combine_country.csv')
    except os.error as e:
        print(e)
        answer = input('Would you like to try again? (Yes/No):')
        if answer =='Yes':
            upload_country_data(dynamodb,table)
def parse_population_data(dynamodb,table):
    try:
        db = dynamodb.Table(table)
        file1 = 'shortlist_area.csv'
        file2 = 'shortlist_curpop.csv'
        reader1 = csv.reader(open(file1))
        reader2 = csv.reader(open(file2))
        next(reader1)
        data1 = sorted(reader1,key=operator.itemgetter(1))
        next(reader2)
        data2 = sorted(reader2,key=operator.itemgetter(0))
        header = ['Country','ISO3','Area','Population_data']
        f = open('combine_population.csv', 'w')
        writer = csv.writer(f)
        writer.writerow(header)
        for (row1,row2) in zip(data1,data2):
            year = 1970
            iso3 = row1[0]
            area = row1[2]
            countryName = row1[1]
            dict1 = {}
            for col in row2:
                dict2 = {}
                if col.isdigit() or col=='':
                    if(col==''):
                        density = 0
                    else:
                        density = int(col)/int(area)
                        density = "{:.2f}".format(density)
                    dict2['Population'] = str(col)
                    dict2['Density'] = str(density)
                    dict1[str(year)] = dict2
                    year+=1
            row = [countryName,iso3,area,dict1]
            writer.writerow(row)
        f.close()
        load_csv(dynamodb,table,'combine_population.csv')
        os.remove('combine_population.csv')
    except:
        answer = input('Would you like to try again? (Yes/No):')
        if answer =='Yes':
            parse_population_data(dynamodb,table)
def decorate():
    print('________________________________________________________________________________________')

def decorate2():
    print('****************************************************************************************')

try:
    dynamodb = boto3.resource('dynamodb', region_name='ca-central-1') 
    dynamodb_client = boto3.client('dynamodb', region_name='ca-central-1')
    existing_tables = dynamodb_client.list_tables()['TableNames']
except botocore.exceptions.ClientError as error :
    print(error)

print('Welcome to the AWS Dynamodb , we are about to construct three Tables in the database.')
# # Creating the first table
print('Creating the table for economic data (suggest <username>_economic_data as a table name)')
table_name_economic = create_table(dynamodb,existing_tables,'Country')
# Uploading economic data
parse_econmoic_data(dynamodb,table_name_economic)
decorate()
# #Creating the second table
print('Creating the table for country data (suggest <username>_country_data as a table name)')
table_name_country = create_table(dynamodb,existing_tables,'ISO3')
#uploading country data_
upload_country_data(dynamodb,table_name_country)
decorate()
print('Creating the table for population data (suggest <username>_population_data as a table name)')
#Creating the third table
table_name_population = create_table(dynamodb,existing_tables,'Country')
parse_population_data(dynamodb,table_name_population)
# deleteing a table
decorate2()
answer  = input('Would you like to delete a table? (Yes/No):')
while(answer=='Yes' or answer =='yes'):
    table_name = input('Enter the name of the table that you want to delete:')
    delete_table(dynamodb,table_name)
    answer  = input('Would you like to delete a table? (Yes/No):')
    decorate()

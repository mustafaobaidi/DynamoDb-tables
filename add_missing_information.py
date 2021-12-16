from os import error
import boto3
import botocore
from boto3.dynamodb.conditions import Key, Attr
from aws_module.add_individual_records import add_record
from aws_module.delete_individual_record import delete_record
import json
try:
    dynamodb = boto3.resource('dynamodb', region_name='ca-central-1') 
    dynamodb_client = boto3.client('dynamodb', region_name='ca-central-1')
    existing_tables = dynamodb_client.list_tables()['TableNames']
except botocore.exceptions.ClientError as error :
    print(error)
answerQ = 1
while(answerQ):
    table_name = input('Please Enter the name of the table that you would like to add the information to:')
    if table_name not in existing_tables:
        print('This table does not exist, please try again!')
    else:
        try:
            listKeys = []
            db = dynamodb.Table(table_name)
            response = db.scan()
            for item in response['Items']:
                for item2 in item:
                    if item2 not in listKeys:
                        listKeys.append(item2)
            if 'Languages' in listKeys:
                Iso3 = input('Please enter the Iso3 (country code) here:')
                language = input('Please enter the new language that you would like to add here:')
                response2  = db.query(KeyConditionExpression=Key('ISO3').eq(Iso3))
                items = response2['Items']
                for item in items:
                    for key in item:
                        if(key == 'Languages'):
                            tmp = item[key].replace("'","")    
                            newLanguages = tmp.strip('[]').split(', ')
                            newLanguages.append(language)
                        if(key == 'Country'):
                            country = item[key]
                        if(key == 'Capital'):
                            capital = item[key]
                        if(key == 'Area'):
                            area = item[key]
                        if(key == 'ISO3'):
                            iso3 = item[key]
                newLanguages  = str(newLanguages)
                Item = {}
                Item['ISO3'] = iso3
                Item['Area'] = area
                Item['Capital'] = capital
                Item['Country'] = country
                Item['Languages'] = str(newLanguages)
                delete_record(db,'ISO3',Iso3)
                add_record(dynamodb,table_name,Item)
            if 'Population_data' in listKeys:
                countryName = input('Please enter the country name here:')
                year = input('Please enter the year of the population here:')
                population = input('Please enter the population that corresponds to the year ' + year +' here:')
                response3  = db.query(KeyConditionExpression=Key('Country').eq(countryName))
                items = response3['Items']
                for item in items:
                    for key in item:
                        if key == 'Country':
                            country = item[key]
                        if key =='ISO3':
                            iso3 = item[key]
                        if key =='Area':
                            area = item[key]
                        if key == 'Population_data' :
                            population_data = item[key].replace("'",'"')
                            population_data_Dict = json.loads(population_data) # convert the string to a dictionary
                            for key in population_data_Dict:
                                if key == year:
                                    innerDict = population_data_Dict[key]
                                    for key1 in innerDict:
                                        if key1 =='Population':
                                            innerDict[key1] = population
                                        if key1 =='Density':
                                            density = int(population)/int(area)
                                            density = "{:.2f}".format(density)
                                            density = str(density)
                                            innerDict[key1] = density
                                    population_data_Dict[key] = innerDict
                Item = {}
                Item['Country'] = country
                Item['ISO3'] = iso3
                Item['Area'] = area
                Item['Population_data'] = str(population_data_Dict)
                delete_record(db,'Country',countryName)
                add_record(dynamodb,table_name,Item)
            if 'GDPPC' in listKeys:
                countryName = input('Please enter the country name here:')
                year = input('Please enter the year gdppc entry here:')
                gdppc = input('Please enter the GDPPC value that corresponds to the year ' + year +' here:')
                response3  = db.query(KeyConditionExpression=Key('Country').eq(countryName))
                items = response3['Items']
                for item in items:
                    for key in item:
                        if key == 'Country':
                            country = item[key]
                        if key == 'Currency':
                            currency = item[key]
                        if key == 'GDPPC' :
                            gdppc_string = item[key].replace("'",'"')
                            gdppc_dict = json.loads(gdppc_string) # convert the string to a dictionary
                            for key in gdppc_dict:
                                if key == year:
                                    gdppc_dict[key] = gdppc
                Item = {}
                Item['Country'] = country
                Item['Currency'] = currency
                Item['GDPPC'] = str(gdppc_dict)
                delete_record(db,'Country',countryName)
                add_record(dynamodb,table_name,Item)
        except botocore.exceptions.ClientError as error:
            print(error)
            print('Failure to add the missing info for table ' + table_name)
    
    answer  = input('Would you like to add other information(Yes/No)?:')
    if answer == 'Yes' or answer =='yes':
        answerQ = 1
    else:
        answerQ = 0
                



from os import readlink
import boto3
import botocore
from prettytable import PrettyTable
import operator
from boto3.dynamodb.conditions import Key, Attr
import json
from aws_module.query_and_scan import *
from PyPDF2 import PdfFileReader
from PyPDF2 import PdfFileWriter
from PyPDF2.generic import RectangleObject
import collections
population_pretty_table = PrettyTable()
economic_pretty_table = PrettyTable()
country_pretty_table = PrettyTable()
currency = ''
area_global_pretty_table = PrettyTable()
population_global_pretty_table = PrettyTable()
density_global_pretty_table = PrettyTable()
gdppc_global_pretty_table1 = PrettyTable()
gdppc_global_pretty_table2 = PrettyTable()
gdppc_global_pretty_table3 = PrettyTable()
gdppc_global_pretty_table4 = PrettyTable()
gdppc_global_pretty_table5 = PrettyTable()

def country_table_design(dynamodb,table,country_name):
    try:
        area = ''
        official_name = ''
        languages = ''
        country_pretty_table.clear_rows()
        area_list = []
        scan_response = scan_all_items(dynamodb,table)
        for item in scan_response:
            for key in item:
                if key == 'Area':
                    area_list.append(int(item[key]))
        area_list.sort(reverse = True) # sort in decsinding order
        items = query_item(dynamodb,table,iso3save,'ISO3')
        for item in items:
            for key in item:
                if key == 'Area':
                    area = item[key]
                if key =='Languages':
                    languages = item[key]
                    languages = languages.strip('[]').replace("'","")
                if key =='Capital':
                    capital = item[key]
        items2 = query_item(dynamodb,'UN_country_codes',iso3save,'iso3')
        for item in items2:
            for key in item:
                if key == 'officialname':
                    official_name = item[key]
        country_pretty_table.field_names = ["Official Name: "+ official_name]
        country_area_rank = area_list.index(int(area)) + 1
        country_pretty_table.add_row(["Area: " + area + " sq km " +"(" + str(country_area_rank)+")"])
        country_pretty_table.add_row(["Official Languages: " + languages])
        country_pretty_table.add_row(["Capital City: " + capital])
        country_pretty_table.hrules = 1
    except:
        print('Failure to construct the country table in the report')

def economic_table_design(dynamodb,table,country_name):
    try:
        global currency
        economic_pretty_table.clear_rows()
        gdppc_list = []
        gdppc_rank = []
        year_list = []
        economic_pretty_table.field_names = ["      Year      ", "      GDPPC       ", "        Rank        "]
        items  = query_item(dynamodb,table,country_name,'Country')
        for item in items:
            for key in item:

                if key =='Country':
                    country = item[key]
                if key == 'Currency':
                    currency = item[key]
                if key == 'GDPPC':
                    gdppc_string = item[key].replace("'",'"')
                    gdppc_dict = json.loads(gdppc_string) # convert the string to a dictionary
                    for key in gdppc_dict:
                        year = key
                        year_list.append(year)
                        innerDict = gdppc_dict[key]
                        if(innerDict == ''):
                            if len(gdppc_list)>0:
                                gdppc_list.append(0)
                                gdppc_rank.append(0)
                            else:
                                year_list.pop()
                        else:
                            gdpccValue = int(innerDict)
                            gdppc_list.append(gdpccValue)
                            gdppc_rank.append(gdpccValue)
        value = gdppc_list[-1]
        while value == 0:
            year_list.pop()
            gdppc_list.pop()
            gdppc_rank.pop()
            value = gdppc_list[-1]
        gdppc_rank.sort(reverse=True)
        for item1,item2,item3 in zip(year_list,gdppc_list,gdppc_rank):
            index_gdppc_rank = gdppc_rank.index(item2)+1
            economic_pretty_table.add_row([item1,item2,index_gdppc_rank])
        economic_pretty_table.hrules = 1
    except:
        print('Failure to construct the economic table in the report')
def Population_table_design(dyanmodb,table,country_name):
    try:
        population_pretty_table.clear_rows()
        global iso3save
        population_list = []
        population_density_list = []
        population_rank = []
        population_density_rank = []
        year_list = []
        population_pretty_table.field_names = ["Year", "Population", "Population Rank", "Population Desnity(people/sq km)","Population Density Rank"]
        items = query_item(dyanmodb,table,country_name,'Country')
        for item in items:
            for key in item:
                if key =='ISO3':
                    iso3save = item[key]
                if key == 'Population_data':
                    population_data = item[key].replace("'",'"')
                    population_data_Dict = json.loads(population_data) # convert the string to a dictionary
                    for key in population_data_Dict:
                            year = key
                            year_list.append(year)
                            innerDict = population_data_Dict[key]
                            for key1 in innerDict:
                                if key1 =='Population':
                                    if(innerDict[key1] == ''):
                                        if len(population_list) >0:
                                            population_list.append(0)
                                            population_rank.append(0)
                                        else:
                                            year_list.pop()
                                    else:        
                                        population = int(innerDict[key1])
                                        population_list.append(population)
                                        population_rank.append(population)
                                if key1 =='Density':
                                    if innerDict[key1] == '0':
                                        if len(population_density_list) > 0:
                                            population_density_list.append(0.00)
                                            population_density_rank.append(0.00)
                                    else:
                                        density = float(innerDict[key1])
                                        population_density_list.append(density)
                                        population_density_rank.append(density)
        value = population_list[-1]
        while value == 0:
            year_list.pop()
            population_list.pop()
            population_density_list.pop()
            population_rank.pop()
            population_density_rank.pop()
            value = population_list[-1]
        population_rank.sort(reverse=True)
        population_density_rank.sort(reverse=True)
        
        for item1,item2,item3,item4,item5 in zip(year_list,population_list,population_rank,population_density_list,population_density_rank):
            index_population_rank = population_rank.index(item2)+1
            index_population_density_rank = population_density_rank.index(item4)+1
            population_pretty_table.add_row([item1,item2,index_population_rank,item4,index_population_density_rank])
        population_pretty_table.hrules = 1
    except:
        print('Failure to construct the population table in the report')


def create_reportA(dynamodb,existing_tables,country_table,population_table,economic_table):
    global currency
    print('________________________________________________________________________________________')

    country_table = country_table.strip()
    economic_table = economic_table.strip()
    population_table = population_table.strip()
    if country_table not in existing_tables or economic_table not in existing_tables or population_table not in existing_tables:
        print('Error! Cannot produce the report because one of the tables or more does not exist!')
    else:
        # listKeys = []
        country_name = input('Please enter the name of the country that you want to generate the report for:')
        #Population_data table
        Population_table_design(dynamodb,population_table,country_name)
        #Economics table
        economic_table_design(dynamodb,economic_table,country_name)

        country_table_design(dynamodb,country_table,country_name)
        reportName = 'ReportA_' + country_name +'.txt'
        
        pretty_table_2_string = str(country_pretty_table)
        if country_name in pretty_table_2_string:
            with open(reportName,'w') as f:
                f.write('Report A - Country Level Report')
                f.write('\n\n')
                f.write(country_pretty_table.get_string(title=country_name))
                f.write('\n\n')
                f.write('<Population>')
                f.write('\n')
                f.write('Table of Population, Population Density, and their respective world ranking for that year,ordered by year:')
                f.write('\n')
                f.write(str(population_pretty_table))
                f.write('\n\n')
                f.write('<Economics>')
                f.write('\n')
                f.write('Currency: ' + currency)
                f.write('\n')
                f.write('Table of GDP per capita (GDPCC) <from earliest year to latest year> and rank within the world for that year')
                f.write('\n')
                f.write(str(economic_pretty_table))
        else:
            print('Falied to construct the report, ' + country_name + ' is not recognized in the countries database!')


def Create_population_density_area_tables(dynamodb,population_table_name,year):
    try:
        area_global_pretty_table.field_names = ["Country Name", "Area (sq km)", "Rank"]
        population_global_pretty_table.field_names = ["Country Name","Population","Rank"]
        density_global_pretty_table.field_names = ["Country Name","Density (people/ sq km)","Rank"]
        population_dict = {}
        density_dict = {}
        db = dynamodb.Table(population_table_name)
        area_list = []
        population_list = []
        density_list = []
        countries_list = []
        countries_list2 = []
        scan_response = scan_all_items(dynamodb,population_table_name)
        for item in scan_response:
            for key in item:
                if key =='Country':
                    country = item[key]
                    if(population!=0):
                        population_dict[population] = country
                    countries_list.append(item[key])
                    if(density != 0.00):
                        density_dict[density]  = country
                if key == 'Area':
                    area_list.append(int(item[key]))
                if key =='Population_data':
                    population_data = item[key].replace("'",'"')
                    population_data_Dict = json.loads(population_data) # convert the string to a dictionary
                    for key in population_data_Dict:
                        if key == year:
                            innerDict = population_data_Dict[key]
                            for key1 in innerDict:
                                if key1 =='Population':
                                    if(innerDict[key1] != ''):
                                        population = int(innerDict[key1])
                                        population_list.append(population)
                                    else:
                                        population = 0
                                if key1 =='Density':
                                    if innerDict[key1] != '0':
                                        density = float(innerDict[key1])
                                        density_list.append(density)
                                    else:
                                        density = 0.00
        # add information to the global area table
        area_list.sort(reverse=True)
        length = len(area_list)
        for i in range (length):
            items = db.scan(FilterExpression=Attr('Area').eq(str(area_list[i])))
            response = items['Items']
            for item in response:
                for key in item:
                    if(key == 'Country'):
                        countries_list2.append(item[key])
        for country,area in zip(countries_list2,area_list):
            area_global_pretty_table.add_row([country,area,area_list.index(area)+1])
        area_global_pretty_table.hrules = 1
        #add information to the global population table
        orderd_population_dict = sorted(population_dict.items(),reverse=True)
        rank = 1
        for key in orderd_population_dict:
            country_name = key[1]
            population = key[0]
            population_global_pretty_table.add_row([country_name,population,rank])
            rank +=1
        population_global_pretty_table.hrules =1
        #add information to the global density table
        ordered_density_dict = sorted(density_dict.items(),reverse=True)
        rank = 1
        for key in ordered_density_dict:
            country_name = key[1]
            density = key[0]
            density_global_pretty_table.add_row([country_name,density,rank])
            rank+=1
        density_global_pretty_table.hrules  = 1
    except:
        print('Failure to construct the population, area and density tables in the global report')
def create_global_GDPPC(dynamodb,table):
    try:
        num_countryies = 0
        dict1 = {} #1970-1979
        dict2 = {} #1980-1989
        dict3 = {} #1990-1999
        dict4 = {} #2000-2009
        dict5 = {} #2010-2019
        gdppc_global_pretty_table1.field_names = ["Country Name","1970","1971","1972","1973","1974","1975","1976","1977","1978","1979"]
        gdppc_global_pretty_table2.field_names = ["Country Name","1980","1981","1982","1983","1984","1985","1986","1987","1988","1989"]
        gdppc_global_pretty_table3.field_names = ["Country Name","1990","1991","1992","1993","1994","1995","1996","1997","1998","1999"]
        gdppc_global_pretty_table4.field_names = ["Country Name","2000","2001","2002","2003","2004","2005","2006","2007","2008","2009"]
        gdppc_global_pretty_table5.field_names = ["Country Name","2010","2011","2012","2013","2014","2015","2016","2017","2018","2019"]

        db = dynamodb.Table(table)
        scan_response = scan_all_items(dynamodb,table)
        for item in scan_response:
            list1 = []
            list2 = []
            list3 = []
            list4 = []
            list5 = []
            for key in item:
                if key =='Country':
                    num_countryies+=1
                    country = item[key]
                if key == 'GDPPC':
                    gdppc_string = item[key].replace("'",'"')
                    gdppc_dict = json.loads(gdppc_string) # convert the string to a dictionary
                    for key in gdppc_dict:
                        year = int(key)
                        if year in range(1970,1980):
                            list1.append(gdppc_dict[key])
                        elif year in range(1980,1990):
                            list2.append(gdppc_dict[key])
                        elif year in range(1990,2000):
                            list3.append(gdppc_dict[key])
                        elif year in range(2000,2010):
                            list4.append(gdppc_dict[key])
                        else:
                            list5.append(gdppc_dict[key])
            gdppc_global_pretty_table1.add_row([country,list1[0],list1[1],list1[2],list1[3],list1[4],list1[5],list1[6],list1[7],list1[8],list1[9]])
            gdppc_global_pretty_table2.add_row([country,list2[0],list2[1],list2[2],list2[3],list2[4],list2[5],list2[6],list2[7],list2[8],list2[9]])
            gdppc_global_pretty_table3.add_row([country,list3[0],list3[1],list3[2],list3[3],list3[4],list3[5],list3[6],list3[7],list3[8],list3[9]])
            gdppc_global_pretty_table4.add_row([country,list4[0],list4[1],list4[2],list4[3],list4[4],list4[5],list4[6],list4[7],list4[8],list4[9]])
            gdppc_global_pretty_table5.add_row([country,list5[0],list5[1],list5[2],list5[3],list5[4],list5[5],list5[6],list5[7],list5[8],list5[9]])
            gdppc_global_pretty_table1.hrules = 1
            gdppc_global_pretty_table2.hrules = 1
            gdppc_global_pretty_table3.hrules = 1
            gdppc_global_pretty_table4.hrules = 1
            gdppc_global_pretty_table5.hrules = 1
        return num_countryies
    except:
        print('Failure to construct the GDP Per Capita tables')
        return 0

def create_reportB(dynamodb,existing_tables,population_table_name,economic_table_name):
    try:
        print('________________________________________________________________________________________')
        year = input('Please enter the year for the global report:')
        Create_population_density_area_tables(dynamodb,population_table_name,year)
        reportName = 'ReportB_Global_'+ year +'.txt'
        number_of_countries= create_global_GDPPC(dynamodb,economic_table_name)
        if(number_of_countries!=0):
            with open(reportName,'w') as f:
                f.write('Report B - Global Report')
                f.write('\n\n')
                f.write('Year: ' + year)
                f.write('\n')
                f.write('Number of Countries:' + str(number_of_countries))
                f.write('\n\n')
                f.write('Table of Countries Ranked by Population (largest to smallest)')
                f.write('\n')
                f.write(str(population_global_pretty_table))
                f.write('\n\n')
                f.write('Table of Countries Ranked by Area (largest to smallest)')
                f.write('\n')
                f.write(str(area_global_pretty_table))
                f.write('\n\n')
                f.write('Table of Countries Ranked by Density (largest to smallest)')
                f.write('\n')
                f.write(str(density_global_pretty_table))
                f.write('\n\n')
                f.write('GDP Per Capita for all Countries')
                f.write('\n')
                f.write(gdppc_global_pretty_table1.get_string(title = "1970's Table"))
                f.write('\n\n')
                f.write(gdppc_global_pretty_table2.get_string(title = "1980's Table"))
                f.write('\n\n')
                f.write(gdppc_global_pretty_table3.get_string(title = "1990's Table"))
                f.write('\n\n')
                f.write(gdppc_global_pretty_table4.get_string(title = "2000's Table"))
                f.write('\n\n')
                f.write(gdppc_global_pretty_table5.get_string(title = "2010's Table"))
        else:
            print('Falied to create the global report')
    except:
        print('Falied to create the global report')       


iso3save = ''
try:
    dynamodb = boto3.resource('dynamodb', region_name='ca-central-1') 
    dynamodb_client = boto3.client('dynamodb', region_name='ca-central-1')
    existing_tables = dynamodb_client.list_tables()['TableNames']
except botocore.exceptions.ClientError as error :
    print(error)
else:

    loop = 1
    while(loop):
        country_table = input('Please enter the name of your table that contains the countries data, in the form <username>_country_data:')
        economic_table = input('Please enter the name of your table that contains the countries data, in the form <username>_economic_data:')
        population_table = input('Please enter the name of your table that contains the countries data, in the form <username>_population_data:') 
        reportN = input('Please Enter (A) to produce the Country Level Report or Enter (B) to produce Global Report or (C) for both reports:')
        if reportN == 'A':
            create_reportA(dynamodb,existing_tables,country_table,population_table,economic_table)
        elif reportN == 'B':
            create_reportB(dynamodb,existing_tables,population_table,economic_table)
        elif reportN =='C':
            create_reportA(dynamodb,existing_tables,country_table,population_table,economic_table)
            create_reportB(dynamodb,existing_tables,population_table,economic_table)
        else:
            print('Wrong entry ' + reportN +' is not an option!')
        print('________________________________________________________________________________________')
        answer = input('Would you like to produce another report? Enter (yes) if so:')
        if answer =='Yes' or answer =='yes':
            loop = 1
        else:
            loop = 0
    
    

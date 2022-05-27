
DynamoDb:
The purpose of this project is to design tables so that we can update them by adding more countries to the database, as well as years of data. I designed the tables so that economic and non-economic data are not in the same table. Moreover, I made a python module that helped me accomplish the tasks I needed to do in dynamo db the module is called aws_module and includes the following:

aws_module: contains modules to help creating tables, deleteing tables, loading csv file to a table, adding indvidual record, deleteing indvidual record, as well as query and scan tables

Programs:

1- create_all_tables.py:
This programs create all the tables and load all the csv files to the dynamodb. You will be asked to enter 3 table names, please make sure that your table name is as specied in the terminal instruction. All tables will start with <username>table_name. At the end of the program you will have the option to delete some tables if you would like.

2. add_missing_information.py:
In this program you can add infromation to the existing tables. If you have not created the tables this program won't work and will give you an error. At the start of the program, you will be asked to enter the table name that you want to add information to. Then the program will ask for other inputs in order to add the record.

3. create_reports.py:
This program creates 2 reports in a (txt file) format. At the start of the program, you will be asked to enter the 3 table names that you have in dynamodb. After that you will have a choice to produce Report, you can enter 'A' to create Country Level Report, or 'B' for Global Report, or 'C' for both reports. The program will ask for inputs in order to produce the reports. After the program is done, the reports will be ready in the same directory as the 3 programs(create_all_tables.py,create_reports.py,add_missing_information.py)


Note: The csv files must be in the same directory as the 3 programs(create_all_tables.py,create_reports.py,add_missing_information.py)

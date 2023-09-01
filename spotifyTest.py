# -*- coding: utf-8 -*-
"""
Created on 26/02/2023

@author: JD
"""


import json
import pandas as pd
import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
from pathlib import Path
import re
import requests
import warnings
warnings.filterwarnings('ignore')   # stops the warning messages from pandas, regexp


### these lines are useful to read any pandas dataframes in the terminal, as the number of
### rows tends to be restricted for viewing reaQsons

#pd.set_option('display.max_columns', 10)
#pd.set_option('display.max_rows', 50)

#pd.options.display.max_columns = 10
#pd.options.display.max_rows = 50




class ingestor:
    def __init__(self, sf_username, sf_password, sf_database, sf_schema, sf_account, sf_warehouse, sf_role, destination_table, access_token):
        self.sf_username = sf_username
        self.sf_password = sf_password
        self.sf_database = sf_database
        self.sf_schema = sf_schema
        self.sf_account = sf_account
        self.sf_warehouse = sf_warehouse
        self.sf_role = sf_role
        self.destination_table = destination_table
        # self.directory = directory
        self.access_token = access_token

##### improve this by iterating the files in, rather than batch load #####
##### skip files if possible by note it down, use accept #####

    def request(self, endpoint, access_token):
        
        get_data = requests.get('https://api.hubapi.com/forms/v2/forms/{}'.format(endpoint)
                                , headers = {"Authorization": "Bearer {}".format(access_token)} # expires in a year
                                )
        
        return get_data


    def json_packer(self, data):
    
        """ Packaging the data from the GET and adds the extract date """
        
        to_str = json.loads(data.text) # parses the data into a str
        to_str = {"data": to_str}

        df = pd.json_normalize(to_str["data"])
        # normalises the data into a dataframe
            
        # df.columns = df.columns.str.upper()
        df.columns = df.columns.str.replace('[#,@,&,.,?]', '_')
        df.columns = df.columns.str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True).str.upper()
            
        
        extract_time = datetime.now().timestamp()
        extract_time = datetime.utcfromtimestamp(
            extract_time).strftime('%Y-%m-%d'+'T'+'%H:%M:%S'+'Z')
        # converts the unix timestamp into the format we like
        extract_list = []
        
        for i in range(len(df.index)):
            extract_list.append(extract_time)
        
        extract_list = pd.DataFrame(extract_list, 
                                    columns = ['_KLEENE_EXTRACT_DATE'])
        
        batch = pd.concat([df, extract_list], axis = 1, join ='outer')
        
        return batch


    def json_reader(self, directory): # function to read one json file into a pandas dataframe
        """looks at a directory and imports the json into Python"""

        studentsList = []
        concat_data = pd.DataFrame()


        pathlist = Path(directory).glob('**/*.json') # takes the input directory and searches all downstream folders for .json files


        for path in pathlist: # for each path leading to a .json in pathlist

            path_in_str = str(path) # because path is object, not string so force it to be string

            with open(r'{}'.format(path_in_str)) as f: # open each .json
                for jsonObj in f: # takes each json object in the .json file
                    studentDict = json.loads(jsonObj) # loading the json object
                    studentsList.append(studentDict) # appending the json into a list


            df = pd.DataFrame(studentsList) # list of json to a pandas DataFrame

            count_of_splitter = str.count(path_in_str, re.escape('\\')) # counts the number of characters that will split the text

            splitter = re.split(re.escape('\\'), path_in_str, count_of_splitter) # splits the text into a list

            splitter_element_total = len(splitter) - 1 # zero indexes the list length

            df['fileName'] = splitter[splitter_element_total] # assumes the file name is always the last element
            df['folderName'] = splitter[splitter_element_total-1] # assumes the folder name is always the penultimate element

            df = concat_data.append(df)

        return df # returns just the json in DataFrame



    def packaging(self, data):
        """ Packaging the data from the ingest and adds the extract timestamp """

        extract_time = datetime.now().timestamp() # gets the current system timestamp
        extract_time = datetime.utcfromtimestamp(
            extract_time).strftime('%Y-%m-%d'+' '+'%H:%M:%S'+' +0000') # turns the current timestamp into yyyy-mm-ddThr:mi:ss+S.SSS format

        data['_extractTimestamp'] = extract_time # data extract time into Python

        return data



    def data_to_snowflake(self, data, sf_username, sf_password, sf_database, sf_schema, sf_account, sf_warehouse, sf_role, destination_table):
        """ pushes data up into snowflake """

        cnn = sf.connect(user=sf_username, password=sf_password, role=sf_role,
                        database=sf_database, schema=sf_schema, account=sf_account, warehouse=sf_warehouse) # opens the Snowflake connectiona

        cnn.cursor().execute("CREATE SCHEMA IF NOT EXISTS {}.{}".format(sf_database, sf_schema))
        cnn.cursor().execute("USE SCHEMA {}.{}".format(sf_database, sf_schema))

        hello = []                              # empty list to append into

        for col in data:                        # bringing the column names in with
            #col = col.replace('.','_')          # replaces any '.' with '_' to prevent errors in column names
            hello.append('"'+col+'"' + " varchar")      # the correct data types 
                                                        # all column names are double quoted to handle any spaces or special characters
                                                        # this can be improved to be regexed out at some point
        fields = ', '.join(hello)   # comma separates field names in the hello list

        cnn.cursor().execute(
            "CREATE TABLE IF NOT EXISTS {}.{}.{}".format(sf_database, sf_schema, destination_table) + " " +
            "({})".format(fields))

        success, nchunks, nrows, _ = write_pandas(
            conn = cnn, df = data, table_name = destination_table.upper()) # sends the data from the pandas DataFrame input into Snowflake

        print('status: ', success) # self-explanatory, hopefully
        print('chunk size: ', nchunks) # data sent in n chunks
        print('number of rows: ', nrows) # number of rows in the destination table

        cnn.close()

        return


    # below activates all the functions


    def start(self):
        

        raw_data = self.request(endpoint = '', access_token = self.access_token)
        json_data = self.json_packer(data = raw_data)


        self.data_to_snowflake( data = json_data
                        , sf_username = self.sf_username
                        , sf_password = self.sf_password
                        , sf_database = self.sf_database
                        , sf_schema = self.sf_schema
                        , sf_account = self.sf_account
                        , sf_warehouse = self.sf_warehouse
                        , sf_role = self.sf_role
                        , destination_table = self.destination_table
                        )
        return


#
def main():
    sf_username         = 'eltuser'
    sf_password         = 'sf_password'
    sf_database         = 'PROD'
    sf_schema           = ''
    sf_account          = ''
    sf_warehouse        = 'ELT'
    sf_role             = 'ELT'
    destination_table   = ''
    access_token        = ''
    ingestorgator = ingestor(sf_username, sf_password, sf_database, sf_schema, sf_account, sf_warehouse, sf_role, destination_table, access_token)
    return ingestorgator.start()


if __name__ == '__main__':
    main()
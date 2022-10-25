##############################################################################
# Import necessary modules and files
##############################################################################


import pandas as pd
import os
import sqlite3
from sqlite3 import Error


###############################################################################
# Define the function to build database
###############################################################################

def build_dbs():
    '''
    This function checks if the db file with specified name is present 
    in the /Assignment/01_data_pipeline/scripts folder. If it is not present it creates 
    the db file with the given name at the given path. 


    INPUTS
        db_file_name : Name of the database file 'utils_output.db'
        db_path : path where the db file should be '   


    OUTPUT
    The function returns the following under the conditions:
        1. If the file exsists at the specified path
                prints 'DB Already Exsists' and returns 'DB Exsists'

        2. If the db file is not present at the specified loction
                prints 'Creating Database' and creates the sqlite db 
                file at the specified path with the specified name and 
                once the db file is created prints 'New DB Created' and 
                returns 'DB created'


    SAMPLE USAGE
        build_dbs()
    '''
    if os.path.isfile(db_path +db_file_name):
        print('DB Already Exsists')
        return 'DB Exsists'
    else:
        print('Creating Database')
        conn = None
        try:
            #sqlite3.connect will create a database if does not exist
            conn = sqlite3.connect(db_path + db_file_name)
            print('New DB Created , Version', sqllite3.version)
        except Error as ex:
            print('Error in creating Database', ex)
        finally:
            if conn:
                conn.close()
                return('DB Created')

###############################################################################
# Define function to load the csv files to data Array
###############################################################################
 
def load_data(file_path_list):
    data = []
    for file in file_path_list:
        data.append(pd.read_csv(file))
    return data

###############################################################################
# Define function to load the csv file to the database
###############################################################################

def load_data_into_db():
    '''
    Thie function loads the data present in datadirectiry into the db
    which was created previously.
    It also replaces any null values present in 'toal_leads_dropped' and
    'referred_lead' with 0.


    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        data_directory : path of the directory where 'leadscoring.csv' 
                        file is present
        

    OUTPUT
        Saves the processed dataframe in the db in a table named 'loaded_data'.
        If the table with the same name already exsists then the function 
        replaces it.


    SAMPLE USAGE
        load_data_into_db()
    '''
    cnx = sqlite3.connect(db_path + db_file_name)
    leadscoringData = load_data([f"{data_directory}{lead_scoring_file}"])[0]
    
    #leadscoringData.reset_index(drop=True)
    
    #It also replaces any null values present in 'toal_leads_dropped' and 'referred_lead' with 0.
    leadscoringData['total_leads_droppped'] = leadscoringData['total_leads_droppped'].fillna(0)
    leadscoringData['referred_lead'] = leadscoringData['referred_lead'].fillna(0)
    
    print(leadscoringData.isnull().sum())
    
    #replace DB Table if already exist & drop the index while writting to SQL Table
    leadscoringData.to_sql(name=table_name,con=cnx,if_exists='replace',index=False)
    
    #close db connection
    cnx.close()
    
    print(lead_scoring_file, ' is written to SQLLite Database, Table Name : ', table_name)

###############################################################################
# Define function to map cities to their respective tiers
###############################################################################

    
def map_city_tier():
    '''
    This function maps all the cities to their respective tier as per the
    mappings provided in /mappings/city_tier_mapping.py file. If a
    particular city's tier isn't mapped in the city_tier_mapping.py then
    the function maps that particular city to 3.0 which represents
    tier-3.


    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        city_tier_mapping : a dictionary that maps the cities to their tier

    
    OUTPUT
        Saves the processed dataframe in the db in a table named
        'city_tier_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_city_tier()

    '''
    cnx = sqlite3.connect(db_path + db_file_name)
    df_lead_scoring = pd.read_sql("select * from loaded_data",cnx)
    
    print('columns of datatable' , df_lead_scoring.columns.to_list())
    
    #map city to 1,2,3 based on mapping file
    df_lead_scoring["city_tier"] = df_lead_scoring["city_mapped"].map(city_tier_mapping)
    
    #fill all null value in city as level 3
    df_lead_scoring["city_tier"] = df_lead_scoring["city_tier"].fillna(3.0)
    
    #drop the column city_mapped
    df_lead_scoring.drop(["city_mapped"],axis=1,inplace=True)
    
    print('columns of datatable after droping ' , df_lead_scoring.columns.to_list())
    
    print(df_lead_scoring["city_tier"].head())
    
    #dump to DB Table
    df_lead_scoring.to_sql(name="city_tier_mapped",con=cnx,if_exists='replace',index=False)
    
    #close connection
    cnx.close()
    print("city_tier_mapped Table Created")
    
###############################################################################
# Define function to map insignificant categorial variables to "others"
###############################################################################


def map_categorical_vars():
    '''
    This function maps all the unsugnificant variables present in 'first_platform_c'
    'first_utm_medium_c' and 'first_utm_source_c'. The list of significant variables
    should be stored in a python file in the 'significant_categorical_level.py' 
    so that it can be imported as a variable in utils file.
    

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        list_platform : list of all the significant platform.
        list_medium : list of all the significat medium
        list_source : list of all rhe significant source

        **NOTE : list_platform, list_medium & list_source are all constants and
                 must be stored in 'significant_categorical_level.py'
                 file. The significant levels are calculated by taking top 90
                 percentils of all the levels. For more information refer
                 'data_cleaning.ipynb' notebook.
  

    OUTPUT
        Saves the processed dataframe in the db in a table named
        'categorical_variables_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_categorical_vars()
    '''
    cnx = sqlite3.connect(db_path+db_file_name)
    df_lead_scoring = pd.read_sql("select * from city_tier_mapped",cnx)
    
    print('columns of datatable' , df_lead_scoring.columns.to_list())
    
    # all the levels below 90 percentage are assgined to a single level called others
    new_df = df_lead_scoring[~df_lead_scoring['first_platform_c'].isin(list_platform)] 
    # get rows for levels which are not present in list_platform
    new_df['first_platform_c'] = "others" # replace the value of these levels to others
    old_df = df_lead_scoring[df_lead_scoring['first_platform_c'].isin(list_platform)] 
    # get rows for levels which are present in list_platform
    df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe
    
    
    # all the levels below 90 percentage are assgined to a single level called others
    new_df = df[~df['first_utm_medium_c'].isin(list_medium)] 
    # get rows for levels which are not present in list_medium
    new_df['first_utm_medium_c'] = "others" 
    # replace the value of these levels to others
    old_df = df[df['first_utm_medium_c'].isin(list_medium)] 
    # get rows for levels which are present in list_medium
    df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe
    
    
    # all the levels below 90 percentage are assgined to a single level called others
    new_df = df[~df['first_utm_source_c'].isin(list_source)] # get rows for levels which are not present in list_source
    new_df['first_utm_source_c'] = "others" # replace the value of these levels to others
    old_df = df[df['first_utm_source_c'].isin(list_source)] # get rows for levels which are present in list_source
    df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe
    
    #dump to sql db
    df.to_sql(name="categorical_variables_mapped",con=cnx,if_exists='replace',index=False)
    
    #close connection
    cnx.close()
    
    return("categorical_variables_mapped Table Created")

##############################################################################
# Define function that maps interaction columns into 4 types of interactions
##############################################################################
def interactions_mapping():
    '''
    This function maps the interaction columns into 4 unique interaction columns
    These mappings are present in 'interaction_mapping.csv' file. 


    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        interaction_mapping_file : path to the csv file containing interaction's
                                   mappings
        index_columns : list of columns to be used as index while pivoting and
                        unpivoting
        NOTE : Since while inference we will not have 'app_complete_flag' which is
        our label, we will have to exculde it from our index_columns. It is recommended 
        that you use an if loop and check if 'app_complete_flag' is present in 
        'categorical_variables_mapped' table and if it is present pass a list with 
        'app_complete_flag' in it as index_column else pass a list without 'app_complete_flag'
        in it.

    
    OUTPUT
        Saves the processed dataframe in the db in a table named 
        'interactions_mapped'. If the table with the same name already exsists then 
        the function replaces it.
        
        It also drops all the features that are not requried for training model and 
        writes it in a table named 'model_input'

    
    SAMPLE USAGE
        interactions_mapping()
    '''
    cnx = sqlite3.connect(db_path+db_file_name)
    df = pd.read_sql("select * from categorical_variables_mapped",cnx)
    print("columns===>>>",df.columns)
    
    df.drop(columns=['index'], inplace=True, axis=1, errors='ignore')
    df = df.drop_duplicates()   
    df_event_mapping = load_data([f"{interaction_mapping}interaction_mapping.csv",])[0]
    
    # unpivot the interaction columns and put the values in rows
    df_unpivot = pd.melt(df, id_vars = index_columns, var_name='interaction_type', value_name='interaction_value')
    
    df_unpivot['interaction_value'] = df_unpivot['interaction_value'].fillna(0)
    df = pd.merge(df_unpivot, df_event_mapping, on='interaction_type', how='left')
    df = df.drop(['interaction_type'], axis=1)
    
    df_pivot = df.pivot_table(values='interaction_value', index=index_columns, columns='interaction_mapping',aggfunc='sum')
    df_pivot = df_pivot.reset_index()
    
    #print first 5 rows
    print(df.head())
 
    #dump to DB
    df_pivot.to_sql(name='interactions_mapped', con=cnx, if_exists='replace',index=False)
    
    #close connection
    cnx.close()
    
    return "interactions_mapped Table Created"
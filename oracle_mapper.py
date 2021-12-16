''' *********************************************************************************************************************

* Product: Snowflake Migration Platform

* Utility: "Mapper" which utilises the mapping input file to generate the datatype mapping of the Oracle metatables 

* Date: Jun 2021

* Company: Dattendriya Data Science Solutions

************************************************************************************************************************* '''

import sqlite3
import pandas as pd
import re
import xlsxwriter
import os

#Get the path of the current working directory
path = os.getcwd()

#Read the excel spreadsheet containing the metadata tables and drop the duplicate values
meta = pd.read_excel(path+'\\oracle_meta_info.xlsx', 'ORA_META_TABLE')      #CHANGED: xlsx to csv defintion
meta = meta.apply(lambda x: x.astype(str).str.lower())
meta = meta[['TABLE_NAME','COLUMN_NAME','DATA_TYPE','CHAR_LENGTH','PRECISION','IS_NULLABLE','CONSTRAINT_TYPE','CONSTRAINT_NAME']]#CHANGED: Added precision,character_maximum_length to char_length, case of column names
meta = meta.drop_duplicates()

#Connect to the sqlite database using meta_dmp as the database
conn = sqlite3.connect('oracle_meta_dmp.db')            #CHANGED: db name to oracle_meta_dmp.db
c = conn.cursor()

#Insert the values of a dataframe into the sql table META_TABLES
meta.to_sql('META_TABLES', conn, if_exists='replace', index = False)  
c.execute('''SELECT * FROM META_TABLES ''')
#CHANGED: psg_table_name to oracle_table_name
df = pd.DataFrame(c.fetchall(),columns = ['oracle_table_name','oracle_column_name','oracle_data_type','oracle_size','oracle_precision','oracle_isnullable','oracle_constraint_type','oracle_constraint_name'])
print(df) # To display the results after an insert query

#Read the mapping file and adding a new index column for easy access
datatype_map = pd.read_csv (path+'\\oracle2snow_xlat.csv')
#datatype_map = datatype_map.dropna()
print(datatype_map)

#NEW METHOD: Convert datatype_map dataframe to a dictionary
#CHANGED: PostgreSQL to Oracle
datatype_map['Oracle_Data_Type'] = datatype_map['Oracle_Data_Type'].str.lower()
datatype_map['Snowflake_Data_Type'] = datatype_map['Snowflake_Data_Type'].str.lower()
datatype_map['Oracle_Data_Type'] = datatype_map['Oracle_Data_Type'].str.split('/')
datatype_map=datatype_map.explode('Oracle_Data_Type')
datatype_map['Oracle_Data_Type'] = datatype_map['Oracle_Data_Type'].str.strip()
datatype_map['Snowflake_Data_Type'] = datatype_map['Snowflake_Data_Type'].str.strip()
datatype_map['idx'] = range(0, len(datatype_map))
datatype_map.set_index('idx',inplace = True) 
export_csv = datatype_map.to_csv (path+'\\temporary_datatype_map.csv', index = None, header=True)  #addednow
datatype_map_dict = dict(zip(datatype_map.Oracle_Data_Type, datatype_map.Snowflake_Data_Type))
print(datatype_map_dict)

#The table_name, column_name columns are the same in postgres and snowflake
#So values from postgres are copied to snowflake columns
#CHANGED: psg to oracle in 3 lines
df['oracle_data_type'] = df['oracle_data_type'].str.lower()
df[['sflake_table_name']] = df[['oracle_table_name']]
df[['sflake_column_name']] = df[['oracle_column_name']]
df=pd.DataFrame(df)

#New Method: Mapping function
final_datatype_map = pd.read_csv (path+'\\temporary_datatype_map.csv')

#NEWLY ADDED
if (df['oracle_data_type'].str.contains(r'\(([A-Za-z0-9_,]+)\)')).any():
    str_within_parentheses="("+df['oracle_data_type'].str.extract(r'\(([A-Za-z0-9_,]+)\)')+")"
    str_parentheses = str_within_parentheses[str_within_parentheses[0].notnull()]
    index_str_parentheses = str_within_parentheses[str_within_parentheses[0].notnull()].index.tolist()
        
for i, (k, v) in enumerate(datatype_map_dict.items()):
    print(i, k, v)

    #NEWLY ADDED
    mod_dt = df['oracle_data_type'].str.replace(r'\(([A-Za-z0-9_,]+)\)', '')     #Another pattern: r'\([^()]+\)'
    
    #CHANGED: psg to oracle
    #pos = df[df['oracle_data_type'] == k].index.tolist()
    #NEWLY ADDED
    pos = df[mod_dt == k].index.tolist()        #to match the input and the mapping dataset exactly
    print('Positions for ',k,': ',pos)
    df.loc[pos,['sflake_data_type']] = v
    df.loc[pos,['comments']] = final_datatype_map.loc[i,'Comments']

#NEWLY ADDED
if (df['oracle_data_type'].str.contains(r'\(([A-Za-z0-9_,]+)\)')).any():
    print(str_within_parentheses)
    print(mod_dt)
    
#The size, constraint type and constraint name columns are the same in postgres and snowflake
#So values from postgres are copied to snowflake columns
#CHANGED: psg to oracle in 3 lines
df[['sflake_size']] = df[['oracle_size']]
df[['sflake_precision']] = df[['oracle_precision']]
df[['sflake_isnullable']] = df[['oracle_isnullable']]
df[['sflake_constraint_type']] = df[['oracle_constraint_type']]
df[['sflake_constraint_name']] = df[['oracle_constraint_name']]
print(df[['oracle_column_name','oracle_data_type','sflake_column_name','sflake_data_type']].head(15))

#NEWLY ADDED
if (df['oracle_data_type'].str.contains(r'\(([A-Za-z0-9_,]+)\)')).any():
    new_df=pd.DataFrame(df.loc[index_str_parentheses,'sflake_data_type'])
    print(new_df)
    str_parentheses=pd.DataFrame(str_parentheses)
    str_parentheses=str_parentheses.rename(columns = {0: 'precision'}, inplace = False)
    print(str_parentheses)
    newest_df=pd.DataFrame()
    newest_df=new_df.join(str_parentheses)
    newest_df['dt']=newest_df['sflake_data_type'].map(str) + newest_df['precision'].map(str)
    print(newest_df)
    df.loc[index_str_parentheses,'sflake_data_type']=newest_df.loc[index_str_parentheses,'dt']

#CHANGED: Comments
format_cells_list = df[df['comments'].notnull()].index.tolist()
print(format_cells_list)

# To export the results to an excel file
#CHANGED: psg to oracle 
writer = pd.ExcelWriter(path+'\\oracle2snow_src_tgt_mapping.xlsx', engine='xlsxwriter')
export_excel = df.to_excel (writer, sheet_name = "source_target_mapping",index = None, header=True)
#CHANGED: psg to oracle 
df.to_sql('oracle2snow_src_tgt_mapping', conn, if_exists='replace', index = False)
print(pd.read_sql('SELECT * FROM oracle2snow_src_tgt_mapping', conn))
conn.commit()
conn.close()

# Get workbook
workbook = writer.book
# Get Sheet
worksheet = writer.sheets['source_target_mapping']
# To format cells
cell_format1 = workbook.add_format({'bg_color': '#E3E4FA'})
cell_format1.set_border(1)

worksheet.set_column(0,8, 27, cell_format1)
cell_format2 = workbook.add_format({'bg_color': '#FFDFDD'})
cell_format2.set_border(1)
worksheet.set_column(8,16, 27, cell_format2)
cell_format3 = workbook.add_format({'font_color': 'red'})
cell_format3.set_border(1)
for i in format_cells_list:
    worksheet.set_row(i+1, None, cell_format3)
writer.close()
# To pop up the output excel file
os.startfile(path+'\\oracle2snow_src_tgt_mapping.xlsx')



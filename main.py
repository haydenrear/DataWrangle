import os
import pandas as pd
import django
from django.utils.text import slugify

filespath = '/Users/hayde/IdeaProjects/drools/data/src/main/resources/data/before/one/'

def breakexcels():
    for file in os.listdir(filespath):
        if '.xlsx' in file:
            thisfilename = slugify(file.split(".")[0])
            filepath = filespath+thisfilename+'dir'
            print(filepath)
            if os.path.exists(filepath) == False:
                os.mkdir(filepath)
            df = pd.read_excel(filespath + file)
            multi_index_and_recurse(df, file, filepath)

def getMax(columns):
    max = 0
    for column in columns:
        if 'Sub-Sector' in column:
            for word in column.split(" "):
                try:
                    if int(word) > max:
                        max = int(word)
                except:
                    continue
    return max

def createLocations(columns):
    return createLocationMax(getMax(columns))

def createLocationMax(max):
    return createLocationString(max)

def createLocationString(num):
    if num == 0:
        return 'Sub-Sector'
    else:
        return 'Sub-Sector Level ' + str(num)

def get_columns_with_one_unique(df):
    col_with_one_unique = []
    for col in df.columns:
        if len(df[col].unique()) == 1:
            col_with_one_unique.append(col)
    return col_with_one_unique

import uuid

def save_to_file(df, filename, filepath):
    columns = []
    filename = slugify(filename)
    for col in get_columns_with_one_unique(df):
        filename += slugify(col)
        columns.append(col)
    df=df.drop(columns=columns)
    df.to_csv(filepath+"/"+slugify(filename+str(uuid.uuid4()))+".csv")

def get_column_with_least_unique(columns, df, date_column, value_column):
    [print(column) for column in columns]
    least = len(df)+10
    column_name = []
    for column in columns:
        if column != date_column and column != value_column:
            length_of_unique = len(df[column].unique())
            if length_of_unique != 1:
                if length_of_unique == least:
                    column_name.append(column)
                elif length_of_unique < least:
                    least = length_of_unique
                    column_name.clear()
                    column_name.append(column)
    return tuple(column_name, least)

class tuple:
    num_unique = 0
    columns = []
    def __init__(self, columns, num_unique):
        self.num_unique=num_unique
        self.columns=columns

def create_index_with_least_unique(df, column, unique_value):
    return df.loc[df.index.get_level_values(column) == unique_value]

def multi_index_and_recurse(df, filename, filepath):
    thisfile = filename.split(".")[0]
    index = pd.MultiIndex.from_frame(df)
    df.set_index(index, inplace=True)
    recurse([df], thisfile, filepath, df)

def get_unique_values(df, col):
    return df[col].unique()

def recurse(dfs, filename, filepath, original):
    # base case
    # least number of unique values not all same is equal to number of rows
    for index,df in enumerate(dfs):
        tuple = get_column_with_least_unique(df.columns, df, "Date", "Value")
        if len(tuple.columns) == 0:
            for df_final in find_all_unique(dfs):
                save_to_file(df_final, filename, filepath)
            return
        else:
            for col in tuple.columns:
                for unique_value in get_unique_values(df, col):
                    new_df = create_index_with_least_unique(df, col, unique_value)
                    dfs.insert(0, new_df)
    recurse(dfs, filename, filepath, original)

def find_all_unique(dfs):
    final_dfs = []
    for df in dfs:
        if len(get_column_with_least_unique(df.columns, df, "Date", "Value").columns) == 0:
           final_dfs.append(df)
    return final_dfs

breakexcels()
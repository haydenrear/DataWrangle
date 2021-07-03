import os
import pandas as pd
from django.utils.text import slugify
import uuid
import remove_columns
import threading

datadirectory = '/Users/hayde/IdeaProjects/drools/data/src/main/resources/data/'
new_dir_folder = 'new_dir/'
new_dir = datadirectory + new_dir_folder
stripdirectory = datadirectory + 'to_strip/'
output_final = datadirectory + "final_after/"


class MultiIndexAndRecurse(threading.Thread):
    def __init__(self, threadID, name, df, file, new_strip):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.df = df
        self.file = file
        self.new_strip = new_strip

    def run(self) -> None:
        multi_index_and_recurse(self.df, self.file, self.new_strip)


class StripColumns(threading.Thread):
    def __init__(self, threadID, name, df, dirname):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.df = df
        self.dirname = dirname

    def run(self) -> None:
        strip_columns_and_save(self.df, self.dirname)


def breakexcels():
    threads = []
    for file in os.listdir(datadirectory + new_dir_folder):
        if '.xlsx' in file:
            thisfilename = slugify(file.split(".")[0])
            new_strip = stripdirectory + thisfilename + "_dir"
            if not os.path.exists(new_strip):
                os.mkdir(new_strip)
            df = pd.read_excel(new_dir + "/" + file)
            df = delete_unique_and_empty(df)
            thread = MultiIndexAndRecurse(thisfilename, thisfilename, df, file, new_strip)
            thread.start()
            threads.insert(0, thread)
    [thread.join() for thread in threads]
    threads.clear()
    for file in os.listdir(stripdirectory):
        if os.path.isdir(stripdirectory+file):
            print(file)
            for csv_file in os.listdir(stripdirectory + file):
                if '.csv' in csv_file:
                    thisfilename = slugify(file.split(".")[0])
                    print(thisfilename)
                    df = pd.read_csv(stripdirectory + file + "/" + csv_file)
                    print(df.head())
                    print(file)
                    strip = StripColumns(thisfilename, thisfilename, df, file)
                    strip.start()
                    threads.insert(0, strip)
    [thread.join() for thread in threads]


def strip_columns_and_save(df, dir_name):
    output_final_stripped_file_directory = output_final + dir_name
    if not os.path.isdir(output_final_stripped_file_directory):
        os.mkdir(output_final_stripped_file_directory)
    col_removal = remove_columns.RemoveColumns(df,
                                               output_final_stripped_file_directory)
    col_removal.remove_redundant_columns_and_rename_file()


def delete_unique_and_empty(df):
    cols_to_drop = []
    for col in df.columns:
        if len(df[col].unique()) == 1 or len(df[col].unique()) == 0:
            cols_to_drop.append(col)
    return df.drop(columns=cols_to_drop)


def get_columns_with_one_unique(df):
    col_with_one_unique = []
    for col in df.columns:
        if len(df[col].unique()) == 1:
            col_with_one_unique.append(col)
    return col_with_one_unique


def save_to_file(df, filename, strip_dir):
    columns = []
    filename = slugify(filename)
    for col in get_columns_with_one_unique(df):
        filename += slugify(col)
        columns.append(col)
    df = df.drop(columns=columns)
    df.to_csv(strip_dir + "/" + slugify(filename + str(uuid.uuid4()))[0:240] + ".csv")


def get_column_with_least_unique(columns, df, date_column, value_column):
    least = len(df) + 10
    column_name = []
    for column in columns:
        if date_column.lower() not in column.lower() and column.lower() != value_column.lower():
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
        self.num_unique = num_unique
        self.columns = columns


def create_index_with_least_unique(df, column, unique_value):
    return df.loc[df.index.get_level_values(column) == unique_value]


def multi_index_and_recurse(df, filename, strip_dir):
    thisfile = filename.split(".")[0]
    index = pd.MultiIndex.from_frame(df)
    df.set_index(index, inplace=True)
    recurse(df, thisfile, strip_dir)


def get_unique_values(df, col):
    return df[col].unique()


def recurse(df, filename, filepath):
    tuple = get_column_with_least_unique(df.columns, df, "Date", "Value")
    if len(tuple.columns) == 0:
        save_to_file(df, filename, filepath)
    else:
        for column in tuple.columns:
            for unique_value in get_unique_values(df, column):
                recurse(create_index_with_least_unique(df, column, unique_value), filename, filepath)


def find_all_unique(dfs):
    final_dfs = []
    for df in dfs:
        if len(get_column_with_least_unique(df.columns, df, "Date", "Value").columns) == 0:
            final_dfs.append(df)
    return final_dfs


breakexcels()
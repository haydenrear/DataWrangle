import os
import pandas as pd
from django.utils.text import slugify

filespath = '/Users/hayde/IdeaProjects/drools/data/src/main/resources/data/one/'


def remove_letters(cols_to_drop, num_letters_remove_each_col):
    filename = ""
    for col in cols_to_drop:
        filename += col[0:len(col) - (num_letters_remove_each_col + 1)]
    print(filename + " is new filename")
    return filename


def remove_redundant_columns_and_rename_file():
    for file in os.listdir(filespath):
        if os.path.isdir(filespath + file):
            for csv in os.listdir(filespath + file):
                if '.csv' in file:
                    df = pd.read_csv(filespath + file + "/" + csv)
                    cols_to_drop = get_column_names_to_drop(df)
                    num_letters = get_num_letters(cols_to_drop)
                    num_letters_remove_each_col = 0
                    if num_letters > 235:
                        ratio = num_letters / 235
                        num_letters_to_remove = 235 * (1 / ratio)
                        num_letters_remove_each_col = num_letters_to_remove / len(cols_to_drop)
                    filename = remove_letters(cols_to_drop, num_letters_remove_each_col)
                    print(str(cols_to_drop) + " are the cols to drop")
                    df = df.drop(columns=cols_to_drop)
                    df.to_csv(filespath+file+"/"+slugify(filename))
                    os.remove(filespath+file+"/"+csv)

def get_column_names_to_drop(df):
    columns_to_drop = []
    for column in df.columns:
        if len(df[column].unique()) == 1:
            columns_to_drop.append(column)
    return columns_to_drop

def get_num_letters(cols_to_drop):
    sum = 0
    for col in cols_to_drop:
        sum += len(col)
    return sum

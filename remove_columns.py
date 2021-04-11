import os
import pandas as pd
from django.utils.text import slugify
import numpy as np


class RemoveColumns:


    def __init__(self, df, output_filepath):
        self.df = df
        self.cols_to_name = []
        self.num_letters_remove_each_col = 0
        self.filename = ""
        self.output_filepath = output_filepath

    def remove_letters(self):
        filename = ""
        for col in self.cols_to_name:
            try:
                col = str(col)
                filename += col[0:len(col) - (self.num_letters_remove_each_col + 1)]
            except:
                continue
        return filename

    def remove_redundant_columns_and_rename_file(self):
        self.filename = self.get_filename()
        self.df = self.df.drop(columns=self.get_cols_to_drop())
        same_cols = self.duplicate_columns()
        self.df = self.df.drop(columns=same_cols)
        print(self.output_filepath + "/" + slugify(self.filename)+".csv")
        self.df.to_csv(self.output_filepath + "/" + slugify(self.filename) + ".csv")

    def get_filename(self):
        self.cols_to_name = self.get_names_to_add_to_file()
        num_letters = self.get_num_letters()
        if num_letters > 235:
            ratio = num_letters / 235
            num_letters_to_remove = 235 * (1 / ratio)
            self.num_letters_remove_each_col = num_letters_to_remove / len(self.cols_to_name)
        filename = self.remove_letters()
        return filename

    def duplicate_columns(self):
        groups = self.df.columns.to_series().groupby(self.df.dtypes).groups
        dups = []

        for t, v in groups.items():
            cs = self.df[v].columns
            vs = self.df[v]
            lcs = len(cs)

            for i in range(lcs):
                ia = vs.iloc[:, i].values
                for j in range(i + 1, lcs):
                    ja = vs.iloc[:, j].values
                    if np.array_equal(ia, ja):
                        dups.append(cs[j])
                        break
        return dups

    def get_names_to_add_to_file(self):
        name_to_add = []
        for column in self.df.columns:
            if len(self.df[column].unique()) == 1:
                name_to_add.append(self.df[column].unique()[0])
        return name_to_add

    def get_cols_to_drop(self):
        columns_to_drop = []
        for column in self.df.columns:
            if len(self.df[column].unique()) == 1:
                columns_to_drop.append(column)
        return columns_to_drop

    def get_num_letters(self):
        sum = 0
        for col in self.cols_to_name:
            try:
                col = str(col)
            except:
                sum += 3
                continue
            sum += len(col)
        return sum

import os
import pandas as pd
import django
from django.utils.text import slugify

filespath = '/Users/hayde/IdeaProjects/drools/data/src/main/resources/good/'

def breakexcels():
    for file in os.listdir(filespath):
        if '.xlsx' in file:
            thisfilename = slugify(file.split(".")[0])
            filepath = filespath+thisfilename+'dir'
            print(filepath)
            if os.path.exists(filepath) == False:
                os.mkdir(filepath)
            df = pd.read_excel(filespath + file)
            multiIndexAndSave(df, file, filepath)

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

def saveToFile(max, uniqueDataSet, filename, filepath):
    filename = saveOriginal(max, uniqueDataSet, filename, filepath)
    df = pd.read_csv(filepath+"/"+slugify(filename)+".csv", header=0, index_col="Date")
    columns = []
    for item in df.columns:
        if (item != "Date" and item != "Value" and item != "Unit" and item != "Location"):
            columns.append(item)
    df=df.drop(columns=columns)
    os.remove(filepath+"/"+slugify(filename)+".csv")
    df.to_csv(filepath+"/"+slugify(filename)+".csv")

def saveOriginal(max, unique_data, filename, filepath):
    locationMax = createLocationMax(0)
    innerMostValue = unique_data[locationMax].unique()
    innerMostValue = innerMostValue[0]
    filename += innerMostValue
    if max == 0:
        unique_data.to_csv(filepath+"/"+slugify(filename)+".csv")
        return filename
    for i in range(2, max+1):
        uniqueValueList = unique_data[createLocationString(i)].unique()
        filename += uniqueValueList[0]
    unique_data.to_csv(filepath+"/"+slugify(filename)+".csv")
    print(filename)
    return filename


def multiIndexAndSave(df, filename, filepath):
    thisfile = filename.split(".")[0]
    index = pd.MultiIndex.from_frame(df)
    df.set_index(index, inplace=True)
    max = getMax(df.columns)
    location = createLocations(df.columns)
    uniqueValues = df[location].unique()
    for uniqueValue in uniqueValues:
        filename = thisfile
        uniqueDataSet = df.loc[df.index.get_level_values(location) == uniqueValue]
        saveToFile(max, uniqueDataSet, filename, filepath)

breakexcels()
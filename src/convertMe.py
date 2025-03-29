"""
convertMe.py
~~~

Converted convertMe.r file to pyspark
"""
from pyspark.sql import SparkSession
import utils
import pathlib

# To use within functions to get yearly info and aggregate sales info
# dfDayDetails = funDayDetails(year, monthsales)


def main():
    sparksesh = SparkSession.builder.getOrCreate()
    data_location = pathlib.Path(utils.get_proj_root(), 'tests', 'data')
    
    data_loader = utils.FileLoader(sparksesh, data_location)
    return data_loader

if __name__=='__main__':
    dataloader = main()
    df1 = dataloader.load_file('dfReinvestmentProjects.csv')
    df2 = dataloader.load_file('dfSalesDaysFuture.csv')
    print(df1.limit(10).show())
    print('\n')
    print(df2.limit(10).show())
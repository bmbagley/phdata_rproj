"""
convertMe.py
~~~

Converted convertMe.r file to pyspark
"""
from pyspark.sql import SparkSession
import utils
import pathlib


sparksesh = SparkSession.builder.getOrCreate()

# Load in data using new class
pathDataLocation = pathlib.Path(utils.get_proj_root(), 'tests', 'data')
classDataLoader = utils.FileLoader(sparksesh, pathDataLocation)



## Replace Montly Sales aggregation with enrichment, flags and inequalities instead of hard coded rules
# NOTE The stored reporting table/data from previous runs can be used to remove already-aggregagted data in previous years
def funCreateSalesMonthly():
    dfSalesMonthly = classDataLoader.load_file('dfMonthlySales.csv')
    classSalesEnrich = utils.DateUtils(dfSalesMonthly, 'month')
    # [Potential improvement] Remove previous years of aggregations already stored if CONDITIONS
    dfSalesMonthly = classSalesEnrich.add_month_info_columns()
    dfSalesMonthly = classSalesEnrich.add_year_info_columns()
    dfSalesMonthly = classSalesEnrich.update_leap_year_info()
    return dfSalesMonthly, True


## 


def main():
    # Load in sales data and aggregate data (this can be incremental)    
    dfSalesMonthly, blnLiveForecast = funCreateSalesMonthly()
    


    print('main complted successfully')
    return True

if __name__=='__main__':
    main()
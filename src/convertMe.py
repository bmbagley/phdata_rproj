"""
convertMe.py
~~~

Converted convertMe.r file to pyspark
"""

from pyspark.sql import SparkSession, DataFrame
import utils
import pathlib


sparksesh = SparkSession.builder.getOrCreate()

# Load in data using new class
pathDataLocation = pathlib.Path(utils.funProjRoot(), "tests", "data")
classDataLoader = utils.FileLoader(sparksesh, pathDataLocation)


## Replace Montly Sales aggregation with enrichment, flags and inequalities instead of hard coded rules
# NOTE The stored reporting table/data from previous runs can be used to remove already-aggregagted data in previous years
def funCreateSalesMonthly(dfSalesMonthly: DataFrame) -> DataFrame:
    # dfSalesMonthly =
    classSalesEnrich = utils.DateUtils(dfSalesMonthly, "month")
    # [Potential improvement] Remove previous years of aggregations already stored if CONDITIONS
    dfSalesMonthly = classSalesEnrich.add_month_info_columns()
    dfSalesMonthly = classSalesEnrich.add_year_info_columns()
    dfSalesMonthly = classSalesEnrich.update_leap_year_info()
    return dfSalesMonthly, True


def funCreateSalesFuture(dfForecast: DataFrame) -> DataFrame:

    dfFuture = utils.funForecastUtils(dfForecast)

    # Add yearly date info using utilities
    # classFutureEnrich = utils.DateUtils(dfFuture, 'pred_month')
    # dfFuture = classFutureEnrich.add_year_info_columns()
    # dfFuture = classFutureEnrich.add_month_info_columns()
    # dfFuture = classFutureEnrich.update_leap_year_info()
    return dfFuture


def funCreateReinvestmentProj(dfReinvestment: DataFrame) -> DataFrame:
    # Enrich, explode by month of shutdown into rows
    dfReinvest = utils.funReinvestmentProjUtils(dfReinvestment)

    return dfReinvest


# def funCreatePredFuture(dfForecast: DataFrame, dfSalesMonthly: DataFrame, dfReinvestment: DataFrame) -> DataFrame:
#     # Join dfForecast, dfSalesMonthly, dfReinvestment on the leap_year, month, and jan1 columns(????)
#     # Use existing conditions to join dataframes, and filter out bad data like leap_year issues
#     # Explode the forecast data into multiple rows by prediction month, join with InflationRate frame
#     # dfFuture = utils.funSalesFutureBuild(dfFuture)

#     return dfPredFuture


def main():
    # Load in sales data and aggregate data (this can be incremental)
    dfSalesMonthly, blnLiveForecast = funCreateSalesMonthly(
        classDataLoader.load_file("dfMonthlySales.csv")
    )
    # dfSalesMonthly.show(truncate=True)

    # Load in Prediction data and enrich
    dfSalesDaysFuture = funCreateSalesFuture(
        classDataLoader.load_file("dfSalesDaysFuture.csv")
    )
    # dfSalesDaysFuture.show(truncate=True)

    # Load in Reinvestment data and enrich
    dfReinvestment = funCreateReinvestmentProj(
        classDataLoader.load_file("dfReinvestmentProjects.csv")
    )
    # dfReinvestment.show(truncate=True)

    # # Build Incremental Inflation df for joining by prediction month
    dfIncrementalInflation = utils.funBuildIncrementInflation(
        sparksesh, 120, utils.valAnnualizedInflation
    )
    # dfIncrementalInflation.show(truncate=True)

    # Join sales & future
    # dfsalesMonthly groupby loc_num, get total number of rows of history (filter <12 mo)

    print("main complted successfully")
    return True


if __name__ == "__main__":
    main()

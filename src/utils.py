"""
spark.py
~~

Module containing functions for use with spark
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions  as F
from pyspark.sql.types import DateType
from pathlib import Path
import logging
# from datetime import datetime
# import calendar
# from dateutil.relativedelta import relativedelta

valActualsReportDay = 16
valOpeningVision = 12
valClosingVision = 12
valReinvestmentVision = 12
valLongRunInflation = 1.025

def funProjRoot() -> Path:
    """
    Function for getting the project root via pathlib

    Returns:
        pathlib.path: location to project root
    """
    return Path(__file__).parent.parent


class FileLoader:
    """
    A class that loads files into a PySpark DataFrame with error handling.

    Attributes:
        spark (SparkSession): The PySpark SparkSession instance.
        file_path (str): The path to the directory containing the files to be loaded.

    Methods:
        initialize(file_path): Initializes the FileLoader class with the file path.
        load_file(filename): Loads the file into a PySpark DataFrame.
    """

    def __init__(self, spark, file_path):
        self.spark = spark
        self.file_path = file_path

    def load_file(self, filename: str, schema=None):
        """
        Loads the file into a PySpark DataFrame.

        Args:
            filename (str): The filename with extension to be loaded.

        Returns:
            pyspark.sql.DataFrame: A PySpark DataFrame containing the loaded file.
        """
        try:
            # Determine the file extension and load the file accordingly
            file_path = Path(self.file_path) / filename
            file_extension = file_path.suffix.lower()

            if file_extension == ".csv":
                if schema:
                    df = self.spark.read.csv(str(file_path), header=True, schema=schema)
                else:
                    df = self.spark.read.csv(
                        str(file_path), header=True, inferSchema=True
                    )
            elif file_extension == ".parquet":
                df = self.spark.read.parquet(str(file_path))
            elif file_extension == ".json":
                df = self.spark.read.json(str(file_path))
            elif file_extension == ".orc":
                df = self.spark.read.orc(str(file_path))
            else:
                raise ValueError(f"Unsupported file extension: {file_extension}")

            return df
        except Exception as e:
            print(f"Error loading file {filename}: {e}")
            return None


class DateUtils:
    def __init__(self, df: DataFrame, date_col: str):
        """
        Initialize the custom DateUtils class of a DataFrame.

        Parameters:
        df (DataFrame): The input DataFrame which should have a 'date' column of type DateType.
        """
        if date_col not in df.columns:
            raise ValueError(f"The DataFrame must contain the date column {date_col}.")
        if df.schema[date_col].dataType != DateType():
            raise ValueError("The 'date' column must be of type DateType.")
        else:
            self.df = df
            self.date_col = date_col

    def add_year_info_columns(self) -> DataFrame:
        """
        Add yearly date-related information columns to the DataFrame.

        Returns:
        DataFrame: The DataFrame with additional year-related columns.
        """
        self.df = (self.df
            .withColumn("year_integer", F.year(F.col(self.date_col)))
            .withColumn("is_leap_year", ((F.year(F.col(self.date_col)) % 4 == 0) & ((F.year(self.date_col)) % 100 != 0) | (F.year(self.date_col) % 400 == 0)))
            .withColumn("first_day_of_year", F.date_format(F.trunc(self.date_col, 'year'), 'EEEE'))
            .withColumn("total_days_in_month", F.dayofmonth(F.last_day(self.date_col))))
        return self.df
    
    def add_month_info_columns(self) -> DataFrame:
        """
        Add monthly date-related information columns to the DataFrame.

        Returns:
        DataFrame: The DataFrame with additional month-related columns.
        """
        self.df = (self.df
              .withColumn("month_integer", F.month(F.col(self.date_col)))
              .withColumn("total_days_in_month", F.dayofmonth(F.last_day(F.col(self.date_col)))))
        return self.df
    
    def update_leap_year_info(self) -> DataFrame:
        """
        Update leap year, days_in_month, and first_day_of_year based on business rules.

        Returns:
        DataFrame: The DataFrame with updated leap year related columns.
        """
        cond1 = F.col('month_integer') <= 2
        cond2 = F.col('is_leap_year')==False
        cond3 = F.col('first_day_of_year') == 'Wednesday'
        cond4 = F.col('month_integer') > 2
        cond5 = F.col('first_day_of_year') == 'Thursday'

        # <=Feb Leap Year update when Wednesday
        self.df = self.df.withColumn('is_leap_year', F.when(cond1 & cond2 & cond3, True).otherwise(self.df.is_leap_year))
        self.df = self.df.withColumn('total_days_in_month', F.when(cond1 & cond2 & cond3, self.df.total_days_in_month + 1).otherwise(self.df.total_days_in_month))

        # >Mar Leap Year upate when Thursday
        self.df = self.df.withColumn('is_leap_year', F.when(cond4 & cond2 & cond5, True).otherwise(self.df.is_leap_year))
        self.df = self.df.withColumn('first_day_of_year', F.when(cond1 & cond2 & cond3, 'Wednesday').otherwise(self.df.first_day_of_year))

        return self.df


def funAnnualizedInflation(blnLiveForecast=True) -> float:
    if blnLiveForecast:
        return 1.01
    else:
        return 1.05222


def funForecastUtils(dfFuture: DataFrame) -> DataFrame:
    """
    Create enrichment columns in the Forecast data.

    Returns:
    DataFrame: The DataFrame with additional forecast-related columns.
    """
    # try:
    if dfFuture.filter(F.col("open_date").isNull()).count() > 0:
        print("There are Null values in Future open_date col, exiting")
        # logging.error("There are Null values in Future open_date col, exiting")
        return

    # except Exception as e:
    #     # logging.error(f"An error occurred in the my_function: {e}")
    #     raise

    # Conditions to build cols
    cond1 = F.dayofmonth(F.col("date_forecast")) < valActualsReportDay
    cond2 = F.col("open_date").isNull()
    cond3 = F.col('date_forecast').isNotNull()
    cond4 = F.add_months(F.col('date_forecast'), valClosingVision) > F.col('close_date')

    # Build enriched dataframe
    dfFuture = dfFuture.withColumn(
        "nMonthStart", F.when(cond1, -1).otherwise(0)
    ).withColumn(
        "strOpenMonth",
        F.when(cond2, "1900-01-01").otherwise(F.trunc(F.col("open_date"), 'month'))
    ).withColumn(
        "strCloseMonth",
        F.when(cond3 & cond4, F.trunc(F.col('close_date'), 'month')).otherwise('2200-01-01')
    )


    return dfFuture
# def funLocationFuture(SalesDayFuture:Data) -> DataFrame:


# def funMonthsBetween(startMonth: datetime, endMonth: datetime) -> int:
#     return (endMonth.year - startMonth.year) * 12 + endMonth.month - startMonth.month



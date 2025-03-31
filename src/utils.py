"""
spark.py
~~

Module containing functions for use with spark
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, StructType, StructField, IntegerType
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
valAnnualizedInflation = 1.0522
valAnnualizedInflationBLNLive = 1.01


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

        Attributes:
            df (DataFrame): The input DataFrame which should have a 'date' column of type DateType.
        
        Methods:
            add_year_info_columns(): appends columns for leap_year and firstdayofyear
            add_month_info_columns(): appends columns for monthly number of days
            update_leap_year_info(): Reformats the leap_year and max_days based on biz constraints
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
        self.df = (
            self.df.withColumn("year_integer", F.year(F.col(self.date_col)))
            .withColumn(
                "is_leap_year",
                (
                    (F.year(F.col(self.date_col)) % 4 == 0)
                    & ((F.year(self.date_col)) % 100 != 0)
                    | (F.year(self.date_col) % 400 == 0)
                ),
            )
            .withColumn(
                "first_day_of_year",
                F.date_format(F.trunc(self.date_col, "year"), "EEEE"),
            )
        )
        return self.df

    def add_month_info_columns(self) -> DataFrame:
        """
        Add monthly date-related information columns to the DataFrame.

        Returns:
        DataFrame: The DataFrame with additional month-related columns.
        """
        self.df = self.df.withColumn(
            "month_integer", F.month(F.col(self.date_col))
        ).withColumn(
            "total_days_in_month", F.dayofmonth(F.last_day(F.col(self.date_col)))
        )
        return self.df

    def update_leap_year_info(self) -> DataFrame:
        """
        Update leap year, days_in_month, and first_day_of_year based on business rules.

        Returns:
        DataFrame: The DataFrame with updated leap year related columns.
        """
        cond1 = F.col("month_integer") <= 2
        cond2 = F.col("is_leap_year") == False
        cond3 = F.col("first_day_of_year") == "Wednesday"
        cond4 = F.col("month_integer") > 2
        cond5 = F.col("first_day_of_year") == "Thursday"

        # <=Feb Leap Year update when Wednesday
        self.df = self.df.withColumn(
            "is_leap_year",
            F.when(cond1 & cond2 & cond3, True).otherwise(self.df.is_leap_year),
        )
        self.df = self.df.withColumn(
            "total_days_in_month",
            F.when(cond1 & cond2 & cond3, self.df.total_days_in_month + 1).otherwise(
                self.df.total_days_in_month
            ),
        )

        # >Mar Leap Year upate when Thursday
        self.df = self.df.withColumn(
            "is_leap_year",
            F.when(cond4 & cond2 & cond5, True).otherwise(self.df.is_leap_year),
        )
        self.df = self.df.withColumn(
            "first_day_of_year",
            F.when(cond1 & cond2 & cond3, "Wednesday").otherwise(
                self.df.first_day_of_year
            ),
        )

        return self.df


def funExplodeSeq(df: DataFrame, length_col: str) -> DataFrame:
    dfExplode = (
        df.withColumn("startval", F.lit(0))
        .withColumn("seq_list", F.sequence(start="startval", stop=length_col))
        .withColumn("seq_row", F.explode("seq_list"))
        .drop("seq_list", "startval")
    )

    return dfExplode


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
    try:
        if dfFuture.filter(F.col("open_date").isNull()).count() > 0:
            print("There are Null values in Future open_date col, exiting")
            logging.error("There are Null values in Future open_date col, exiting")
            return

    except Exception as e:
        logging.error(f"An error occurred in the my_function: {e}")
        raise

    # Conditions to build cols
    cond1 = F.dayofmonth(F.col("date_forecast")) < valActualsReportDay
    cond2 = F.col("open_date").isNull()
    cond3 = F.col("date_forecast").isNotNull()
    cond4 = F.add_months(F.col("date_forecast"), valClosingVision) > F.col("close_date")

    # Build enriched dataframe
    dfFuture = (
        dfFuture.withColumn("nMonthStart", F.when(cond1, -1).otherwise(0))
        .withColumn(
            "OpenMonth",
            F.when(cond2, "1900-01-01").otherwise(F.trunc(F.col("open_date"), "month")),
        )
        .withColumn(
            "CloseMonth",
            F.when(cond3 & cond4, F.trunc(F.col("close_date"), "month")).otherwise(
                "2200-01-01"
            ),
        )
        .withColumn("intMonthsBetween", F.col("months_predict") - F.col("nMonthStart"))
        # Calculate days between beginning and open, or close and lastdayofmonth
        .withColumn(
            "days_before_open", F.datediff(F.col("open_date"), F.col("OpenMonth"))
        )
        .withColumn(
            "days_after_close",
            F.datediff(F.last_day(F.col("CloseMonth")), F.col("close_date")),
        )
    )

    return dfFuture


def funSalesFutureBuild(dfForecastEnriched: DataFrame) -> DataFrame:
    """
    Creates exploded Future data by prediction month.

    Returns:
    DataFrame: The DataFrame with a row for each prediction month, and all other forecast-related columns.
    """
    # Add filter conditions for the row generation
    filtercond1 = F.col("pred_month") <= F.col("CloseMonth")
    filtercond2 = F.col("pred_month") <= F.col("OpenMonth")

    # Create a new DataFrame with X rows based on the number of months to predict by id
    dfSalesFuture = (  # TODO: Replace 3 lines with with funSeqExplode
        dfForecastEnriched.withColumn("startval", F.lit(0))
        .withColumn("month_list", F.sequence(start="startval", stop="intMonthsBetween"))
        .withColumn("month_row", F.explode("month_list"))
        .withColumn(
            "pred_month",
            F.add_months(F.trunc(F.current_date(), "month"), F.col("month_row")),
        )
        .drop("month_list", "startval")
        .filter(filtercond1 & filtercond2)
    )

    return dfSalesFuture


def funReinvestmentProjUtils(dfReinvestment: DataFrame) -> DataFrame:
    """
    Performs various transformations on a DataFrame containing reinvestment data.

    Args:
        dfReinvestment (pyspark.sql.DataFrame): The input DataFrame containing reinvestment data.

    Returns:
        pyspark.sql.DataFrame: The transformed DataFrame with the following columns:
            - month_shutdown: The month of the shutdown date.
            - month_reopen: The month of the reopen date.
            - month_between_openshut: The number of months between the shutdown and reopen dates.
            - openshut_month_between: The month between the shutdown and reopen dates.
    """    
    dfReinvest = (
        dfReinvestment.withColumn("month_shutdown", F.trunc(F.col("shutdown"), "month"))
        .withColumn("month_reopen", F.trunc(F.col("reopen"), "month"))
        .withColumn(
            "month_between_openshut",
            F.months_between(F.col("month_reopen"), F.col("month_shutdown")).cast(
                "integer"
            ),
        )
    )

    # explode months between into rows
    dfReinvest = funExplodeSeq(dfReinvest, "month_between_openshut")
    dfReinvest = dfReinvest.withColumn(
        "openshut_month_between",
        F.add_months(F.col("month_shutdown"), F.col("seq_row")),
    ).drop("seq_row")

    return dfReinvest


def funBuildIncrementInflation(
    spark: SparkSession, prediction_forecast_months: int, annualizedInflation: float
) -> DataFrame:
    """
    Builds a DataFrame with incremental inflation factors based on the provided parameters.

    Args:
        spark (pyspark.sql.SparkSession): The SparkSession instance.
        prediction_forecast_months (int): The number of months for the prediction forecast.
        annualizedInflation (float): The annualized inflation rate.

    Returns:
        pyspark.sql.DataFrame: A DataFrame with the following columns:
            - prediction_forecast_months: The number of months for the prediction forecast.
            - current_date: The current date.
            - prediction_month: The month for the prediction.
            - begin_inflation_rate: The beginning inflation rate.
            - begin_inflation_rate_monthly: The monthly beginning inflation rate.
            - end_inflation_rate: The end inflation rate.
            - slope: The slope of the inflation rate.
            - intermediate_inflation_rate_init: The initial intermediate inflation rate.
            - intermediate_inflation_rate: The intermediate inflation rate.
            - intermediate_inflation_rate_monthly: The monthly intermediate inflation rate.
            - incremental_time_inflation: The cumulative product of the intermediate inflation rates.
    """    
    # Define inflation factors and variables from biz 
    begin_inflation_cutoff_date = "2025-05-01"
    intermediate_inflation_cutoff_date = "2026-01-01"

    # Define the schema for the DataFrame
    schema = StructType(
        [StructField("prediction_forecast_months", IntegerType(), nullable=False)]
    )
    newdata = [prediction_forecast_months]
    df = spark.createDataFrame([newdata], schema)
    df = df.withColumn("current_date", F.current_date())

    df = funExplodeSeq(df, "prediction_forecast_months")
    df = (
        df.withColumn(
            "prediction_month",
            F.add_months(F.trunc(F.col("current_date"), "month"), F.col("seq_row")),
        )
        .withColumn(
            "begin_inflation_rate",
            F.when(
                F.col("prediction_month") < begin_inflation_cutoff_date, 1.0
            ).otherwise(annualizedInflation),
        )
        .withColumn(
            "begin_inflation_rate_monthly",
            F.when(
                F.col("seq_row") > 0, F.col("begin_inflation_rate") ** (1 / 12)
            ).otherwise(1.0),
        )
        .withColumn("end_inflation_rate", F.lit(valLongRunInflation))
        .withColumn(
            "slope", (F.col("begin_inflation_rate") - F.col("end_inflation_rate")) / 12
        )
        .withColumn(
            "intermediate_inflation_rate_init",
            (
                F.col("begin_inflation_rate")
                + F.col("slope")
                * F.months_between(
                    F.col("current_date"), F.lit(intermediate_inflation_cutoff_date)
                ).cast("integer")
                - F.col("seq_row")
            ),
        )
        .withColumn(
            "intermediate_inflation_rate",
            F.least(
                F.greatest(
                    F.col("intermediate_inflation_rate_init"),
                    F.col("begin_inflation_rate"),
                ),
                F.col("end_inflation_rate"),
            ),
        )
        .withColumn(
            "intermediate_inflation_rate_monthly",
            F.when(
                F.col("seq_row") >= 1, F.col("intermediate_inflation_rate") ** (1 / 12)
            ).otherwise(1.0),
        )
    )

    # Add cumulative product of intermediate_inflation_rate_monthly column
    window_cumprod = Window.partitionBy('current_date').orderBy("prediction_month").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    # Add running cumulative product column
    df = df.withColumn(
        "incremental_time_inflation",
        F.product("intermediate_inflation_rate_monthly").over(window_cumprod),
    )

    return df

"""
spark.py
~~

Module containing functions for use with spark
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pathlib import Path
from datetime import datetime
import calendar
from dateutil.relativedelta import relativedelta

valReportDay = 16
valOpeningVision = 12
valClosingVision = 12
valReinvestmentVision = 12
valLongRunInflation = 1.025

def funAnnualizedInflation(blnLiveForecast=True) -> float:
    if blnLiveForecast:
        return 1.01
    else:
        return 1.05222

def funProjRoot() -> Path:
    """
    Function for getting the project root via pathlib
    
    Returns:
        pathlib.path: location to project root
    """
    return Path(__file__).parent.parent


class YearInfo:
    def __init__(self, year):
        self.year = year

    def is_leap_year(self):
        return (self.year % 4 == 0 and self.year % 100 != 0) or (self.year % 400 == 0)

    def first_day_of_year(self):
        return datetime(self.year, 1, 1).strftime("%A")

    def total_days_in_month(self):
        return [calendar.monthrange(self.year, month)[1] for month in range(1, 13)]

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

    def load_file(self, filename:str, schema=None):
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
                    df = self.spark.read.csv(str(file_path), header=True, inferSchema=True)
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

def funMonthsBetween(startMonth:datetime, endMonth:datetime) -> int:
    return (endMonth.year - startMonth.year) * 12 + endMonth.month - startMonth.month


def funIsLeapYear(year:int) -> bool:
    """
    Return binary if the year is a leap year

    :year int:year
    :return: bool is leap year
    """
    febend = datetime(year=year, month=2, day=28)
    marstart = datetime(year=year, month=3, day=1)
    return (marstart - febend).days > 1

def funFirstDayOfYear(year:int) -> str:
    """
    This function takes a date string, extracts the year,
    and returns the day of the week for the first day of that year.

    :year date_string: String representing a date (e.g., "2023-03-15")
    :return: Day of the week for January 1st of that year
    """
    return datetime(year, 1, 1).strftime('%A')



def month_aggregation(monthsales:DataFrame) -> DataFrame:
    monthsales

    return montagg
# def year_enrich(year:int) -> DataFrame:

#     df = 
#     return df

def funDayDetails(year:int, monthsales:DataFrame) -> DataFrame:

    



"""
spark.py
~~

Module containing functions for use with spark
"""

from pyspark.sql import SparkSession
import pathlib

def get_proj_root() -> pathlib.Path:
    return pathlib.Path(__file__).parent.parent



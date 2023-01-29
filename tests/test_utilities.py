import os, uuid, sys, traceback
import warnings
import pyspark
from pyspark.sql.types import *
from pyspark.sql import *
import  pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.context import SparkConf, SparkContext
from pyspark import StorageLevel
from pyspark.sql import SparkSession
import logging
from logging import StreamHandler
import html
import time
import pytest
import datetime
from src._run_scripts_ import *

warnings.filterwarnings('ignore')

def create_spark_session():

    """
    Create spark session

    Returns
    -------
    SparkSession
        Spark session.
    """

    spark = (
        SparkSession
        .builder
        .appName("unittesting")
        .config('spark.sql.parquet.int96RebaseModeInWrite', 'LEGACY')
        .config("spark.sql.legacy.timeParserPolicy","LEGACY")
        .config("spark.sql.caseSensitive","true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", True)
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.default.parallelism", 2)
        .getOrCreate()
        )

    return spark

def custom_logger(app_name):

    """
    Create python logger handler

    Returns
    -------
    logger
        logger handler.
    """

    # Define python logger
    logger = logging.getLogger(app_name)

    # Define logg pattern
    formatter = logging.Formatter(fmt='%(asctime)s %(name)s %(levelname)s: %(message)s (%(filename)s:%(lineno)d)',datefmt='%Y-%m-%d %H:%M:%S')
    logger.setLevel(logging.INFO)
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    return logger

def df_equality(df1, df2):

    """
    Check equality of spark datafarmes

    Parameter
    ---------
    df1 : pyspark.sql.dataframe.DataFrame
        spark dataframe.

    df2 : pyspark.sql.dataframe.DataFrame
        spark dataframe.

    Returns
    -------
    boolean
        Boolean state of data frames equality.
    """

    # Check dataframes schemas
    if df1.schema != df2.schema:
        return False
    
    # Check dataframes values
    if df1.collect() != df2.collect():
        return False
        
    return True
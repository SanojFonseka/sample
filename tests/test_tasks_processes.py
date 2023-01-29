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

logger = custom_logger(app_name = 'HelloFresh | Data Engineer | Unit Tests | Sanoj Fonseka | ')

@pytest.fixture(scope="session")
def create_test_input_data_for_unit_testing(spark):

    """
    Create spark dataframe for input data for unit testing

    Parameter
    ---------
    spark : SparkSession
        spark session.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        spark dataframe.
    """

    # Define schema for input data
    input_schema = StructType([
        StructField("cookTime", StringType(), True),
        StructField("datePublished", StringType(), True),
        StructField("description", StringType(), True),
        StructField("image", StringType(), True),
        StructField("ingredients", StringType(), True),
        StructField("name", StringType(), True),
        StructField("prepTime", StringType(), True),
        StructField("recipeYield", StringType(), True),
        StructField("url", StringType(), True)
        ])

    # Define test data for input data
    input_data_dict = [
        {"cookTime": "PT90M", "datePublished": '2011-06-17', "description": "Test description1", "image": '', "ingredients": "Beef", "name": "Test name1", "prepTime": "PT90M", "recipeYield": "5", "url": "http://thepioneerwoman.com/cooking/2011/06/spicy-pasta-salad-with-smoked-gouda-tomatoes-and-basil/"},
        {"cookTime": "", "datePublished": '2011-06-17', "description": "Test description2", "image": '', "ingredients": "Beef", "name": "Test name2", "prepTime": "", "recipeYield": "8", "url": "http://thepioneerwoman.com/cooking/2011/06/spicy-pasta-salad-with-smoked-gouda-tomatoes-and-basil/"},
        {"cookTime": "PT10M", "datePublished": '2011-06-17', "description": "Test description3", "image": '', "ingredients": "Chicken", "name": "Test name3", "prepTime": "PT10M", "recipeYield": "", "url": "http://thepioneerwoman.com/cooking/2011/06/spicy-pasta-salad-with-smoked-gouda-tomatoes-and-basil/"},
        {"cookTime": "PT90H", "datePublished": '2011-06-17', "description": "Test description4", "image": '', "ingredients": "beef", "name": "Test name4", "prepTime": "PT90H", "recipeYield": "1 -2 persons", "url": "http://thepioneerwoman.com/cooking/2011/06/spicy-pasta-salad-with-smoked-gouda-tomatoes-and-basil/"},
        {"cookTime": "PT10M", "datePublished": '2011-06-17', "description": "Test description5", "image": '', "ingredients": "Beef", "name": "Test name5", "prepTime": "PT10M", "recipeYield": "", "url": "http://thepioneerwoman.com/cooking/2011/06/spicy-pasta-salad-with-smoked-gouda-tomatoes-and-basil/"},
        {"cookTime": "PT20M", "datePublished": '2011-06-17', "description": "Test description6", "image": '', "ingredients": "beef and chicken", "name": "Test name6", "prepTime": "PT20M", "recipeYield": "3", "url": "http://thepioneerwoman.com/cooking/2011/06/spicy-pasta-salad-with-smoked-gouda-tomatoes-and-basil/"},
        {"cookTime": "PT20M", "datePublished": '2011-06-17', "description": "Test description7", "image": '', "ingredients": "Beef", "name": "Test name7", "prepTime": "PT20M", "recipeYield": "", "url": "http://thepioneerwoman.com/cooking/2011/06/spicy-pasta-salad-with-smoked-gouda-tomatoes-and-basil/"},
    ]

    # Create spark datafarme
    input_df = spark.createDataFrame(input_data_dict, input_schema)

    return input_df

def create_pre_processed_for_unit_testing(spark):

    """
    Create spark dataframe for pre processed data for unit testing

    Parameter
    ---------
    spark : SparkSession
        spark session.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        spark dataframe.
    """

    # Define schema for expected data
    pre_processed_schema = StructType([
        StructField("cookTime", DoubleType(), True),
        StructField("datePublished", DateType(), True),
        StructField("description", StringType(), True),
        StructField("image", StringType(), True),
        StructField("ingredients", StringType(), True),
        StructField("name", StringType(), True),
        StructField("prepTime", DoubleType(), True),
        StructField("recipeYield", StringType(), True),
        StructField("url", StringType(), True),
        StructField("totalCookTime", DoubleType(), True),
        StructField("executionDate", DateType(), False)
        ])

    # Define expected data
    pre_processed_data_dict = [
        {"cookTime": 90.0, "datePublished": datetime.datetime.strptime('2011-06-17', "%Y-%m-%d").date(), "description": "test description1", "image": None, "ingredients": "beef", "name": "test name1", "prepTime": 90.0, "recipeYield": "5", "url": "http://thepioneerwoman.com/cooking/2011/06/spicy-pasta-salad-with-smoked-gouda-tomatoes-and-basil/", "totalCookTime":180.0, "executionDate": datetime.date.today()},
        {"cookTime": 0.0, "datePublished": datetime.datetime.strptime('2011-06-17', "%Y-%m-%d").date(), "description": "test description2", "image": None, "ingredients": "beef", "name": "test name2", "prepTime": 0.0, "recipeYield": "8", "url": "http://thepioneerwoman.com/cooking/2011/06/spicy-pasta-salad-with-smoked-gouda-tomatoes-and-basil/", "totalCookTime":0.0, "executionDate": datetime.date.today()},
        {"cookTime": 10.0, "datePublished": datetime.datetime.strptime('2011-06-17', "%Y-%m-%d").date(), "description": "test description3", "image": None, "ingredients": "chicken", "name": "test name3", "prepTime": 10.0, "recipeYield": None, "url": "http://thepioneerwoman.com/cooking/2011/06/spicy-pasta-salad-with-smoked-gouda-tomatoes-and-basil/", "totalCookTime":20.0, "executionDate": datetime.date.today()},
        {"cookTime": 5400.0, "datePublished": datetime.datetime.strptime('2011-06-17', "%Y-%m-%d").date(), "description": "test description4", "image": None, "ingredients": "beef", "name": "test name4", "prepTime": 5400.0, "recipeYield": "1 -2 persons", "url": "http://thepioneerwoman.com/cooking/2011/06/spicy-pasta-salad-with-smoked-gouda-tomatoes-and-basil/", "totalCookTime":10800.0, "executionDate": datetime.date.today()},
        {"cookTime": 10.0, "datePublished": datetime.datetime.strptime('2011-06-17', "%Y-%m-%d").date(), "description": "test description5", "image": None, "ingredients": "beef", "name": "test name5", "prepTime": 10.0, "recipeYield": None, "url": "http://thepioneerwoman.com/cooking/2011/06/spicy-pasta-salad-with-smoked-gouda-tomatoes-and-basil/", "totalCookTime":20.0, "executionDate": datetime.date.today()},
        {"cookTime": 20.0, "datePublished": datetime.datetime.strptime('2011-06-17', "%Y-%m-%d").date(), "description": "test description6", "image": None, "ingredients": "beef and chicken", "name": "test name6", "prepTime": 20.0, "recipeYield": "3", "url": "http://thepioneerwoman.com/cooking/2011/06/spicy-pasta-salad-with-smoked-gouda-tomatoes-and-basil/", "totalCookTime":40.0, "executionDate": datetime.date.today()},
        {"cookTime": 20.0, "datePublished": datetime.datetime.strptime('2011-06-17', "%Y-%m-%d").date(), "description": "test description7", "image": None, "ingredients": "beef", "name": "test name7", "prepTime": 20.0, "recipeYield": None, "url": "http://thepioneerwoman.com/cooking/2011/06/spicy-pasta-salad-with-smoked-gouda-tomatoes-and-basil/", "totalCookTime":40.0, "executionDate": datetime.date.today()}
        ]

    # Create spark dataframe
    pre_processed_df = spark.createDataFrame(pre_processed_data_dict,  pre_processed_schema)

    return pre_processed_df

def create_aggregated_data_for_unit_testing(spark):

    """
    Create spark dataframe for aggregated data for unit testing

    Parameter
    ---------
    spark : SparkSession
        spark session.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        spark dataframe.
    """

    # Define schema for aggregated data
    aggregate_schema = StructType([
        StructField("difficulty", StringType(), True),
        StructField("avg_total_cooking_time", DoubleType(), True),
        ])

    # Define test data for aggregated data
    aggregate_data_dict = [
        {"difficulty": "hard", "avg_total_cooking_time": 5490.0},
        {"difficulty": "medium", "avg_total_cooking_time": 40.0},
        {"difficulty": "easy", "avg_total_cooking_time": 20.0},
    ]

    # Create spark datafarme
    aggregate_df = spark.createDataFrame(aggregate_data_dict, aggregate_schema)

    return aggregate_df

def test_pre_process_raw_data(spark, logger):

    """
    Unit testing for pre propcess raw data function

    Parameter
    ---------
    spark : SparkSession
        spark session.
    
    logger : logger
        Configured python logg handler.
    """

    # Define dataframes for unit testoing
    input_data = create_test_input_data_for_unit_testing(spark)
    expected_result = create_pre_processed_for_unit_testing(spark)
    
    # Execute main function with testing data
    result = pre_process_raw_data(input_data, logger)

    # Assersion of result and expected results
    assert(df_equality(result, expected_result))

def test_aggregate_pre_processed_data(spark, logger):

    """
    Unit testing for pre propcess raw data function

    Parameter
    ---------
    spark : SparkSession
        spark session.
    
    logger : logger
        Configured python logg handler.
    """

    # Define dataframes for unit testoing
    processed_data = create_pre_processed_for_unit_testing(spark)
    expected_result = create_aggregated_data_for_unit_testing(spark)
    
    # Execute main function with testing data
    result  = aggregate_pre_processed_data(processed_data, logger)

    # Assersion of result and expected results
    assert(df_equality(result, expected_result))
from src.utilities import *

def html_unescape(input_string):

    """
    Unescape html characters

    Parameter
    ---------
    input_string : str
        Input string.

    Returns
    -------
    str
        Output string.
    """

    output_string = html.unescape(input_string)

    return output_string
  
html_unescape_udf = f.udf(html_unescape, StringType()) # Spark user defined function to process data


def for_each_bacth_raw_data_writer(dataframe, batchid, corrupted_data_path, validated_data_path):

    """
    Write corrupted and validated data to relevant locations

    Parameter
    ---------
    dataframe : SparkDataFrame
        Datafarem.
        
    corrupted_data_path : str
        Path contains corrupted data.
        
    validated_data_path : str
        Path contains valildated data.

    Returns
    -------
    parquet
        Persist precessed data to disk using parquet format.
    """
  
    dataframe.persist() # Cache dataframe
    
    # Filter corrupted data from the dataframe
    corrupted_df = (dataframe
    .filter(
        (f.col("cookTime").isNull()) & 
        (f.col("datePublished").isNull()) & 
        (f.col("description").isNull()) & 
        (f.col("image").isNull()) & 
        (f.col("ingredients").isNull()) & 
        (f.col("name").isNull()) & 
        (f.col("prepTime").isNull()) & 
        (f.col("recipeYield").isNull()) & 
        (f.col("url").isNull())
        )
        )

    # Write corrupted data to the location
    corrupted_df.write.mode("append").partitionBy("executionDate").parquet(corrupted_data_path)
    
    # Filter uncorrupted data from the dataframe
    validated_df = (dataframe
    .filter(~(
        (f.col("cookTime").isNull()) & 
        (f.col("datePublished").isNull()) & 
        (f.col("description").isNull()) & 
        (f.col("image").isNull()) & 
        (f.col("ingredients").isNull()) & 
        (f.col("name").isNull()) & 
        (f.col("prepTime").isNull()) & 
        (f.col("recipeYield").isNull()) & 
        (f.col("url").isNull()))
        )
        )
    
    # Write validated data to the location
    validated_df.write.mode("append").partitionBy("executionDate").parquet(validated_data_path)
    
    dataframe.unpersist() # Remove dataframe cache

def read_raw_data(spark, logger, input_data_path):

    """
    Read raw data from json files

    Parameter
    ---------
    spark : SparkSession
        Configured spark session.
        
    logger : logger
        Configured python logg handler.
        
    input_data_path : str
        Path contains input json files.
    
    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        spark dataframe containes raw data.
    """

    try:
        logger.info("read raw data")
        
        # Define schema for recipes data
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
        
        # Read data as stream from inout folder
        input_df = (
            spark
            .readStream
            .format("json")
            .load(input_data_path, schema=input_schema)
            )

        logger.info("read raw data completed")

        return input_df

    except Exception:

        logger.error(msg = "raw data read failed", exc_info = True)

def pre_process_raw_data(input_df, logger):

    """
    Preprocess recipies raw data

    Parameter
    ---------
    input_df : pyspark.sql.dataframe.DataFrame
        spark dataframe.

    logger : logger
        Configured python logg handler.
        
    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        processed spark dataframe.
    """

    try:

        logger.info("pre processing")
        
        # Pre process data
        pre_pros_df = (input_df
        .withColumn("cookTime", 
                f.coalesce(f.regexp_extract("cookTime", r"(\d+)H", 1).cast("int"), f.lit(0)) * 60 + 
                f.coalesce(f.regexp_extract("cookTime", r"(\d+)M", 1).cast("int"), f.lit(0)) + 
                f.coalesce(f.regexp_extract("cookTime", r"(\d+)S", 1).cast("int"), f.lit(0)) / 60
                ) # Convert ISO formatted string into minutes
        .withColumn("prepTime", 
                f.coalesce(f.regexp_extract("prepTime", r"(\d+)H", 1).cast("int"), f.lit(0)) * 60 + 
                f.coalesce(f.regexp_extract("prepTime", r"(\d+)M", 1).cast("int"), f.lit(0)) + 
                f.coalesce(f.regexp_extract("prepTime", r"(\d+)S", 1).cast("int"), f.lit(0)) / 60
                ) # Convert ISO formatted string into minutes

        .withColumn("datePublished", f.to_date("datePublished", "yyyy-MM-dd")) # Convert date string to datetype

        .withColumn("description", f.col("description").cast("string")) # Type cast values to string
        .withColumn("description", f.trim("description")) # Trim leading and trailing white spaces
        .withColumn("description", f.regexp_replace("description", "  +", "")) # Replace multiple white spaces with empty string
        .withColumn("description", f.lower("description")) # Convert all characters to lower case
        .withColumn("description", html_unescape_udf("description")) # Unscape html characters

        .withColumn("ingredients", f.col("ingredients").cast("string")) # Type cast values to string
        .withColumn("ingredients", f.trim("ingredients")) # Trim leading and trailing white spaces
        .withColumn("ingredients", f.regexp_replace("ingredients", "  +", "")) # Replace multiple white spaces with empty string
        .withColumn("ingredients", f.lower("ingredients")) # Convert all characters to lower case
        .withColumn("ingredients", html_unescape_udf("ingredients")) # Unscape html characters
        
        .withColumn("name", f.col("name").cast("string")) # Type cast values to string
        .withColumn("name", f.trim("name")) # Trim leading and trailing white spaces
        .withColumn("name", f.regexp_replace("name", "  +", "")) # Replace multiple white spaces with empty string
        .withColumn("name", f.lower("name")) # Convert all characters to lower case
        .withColumn("name", html_unescape_udf("name")) # Unscape html characters
        
        .withColumn("recipeYield", f.col("recipeYield").cast("string")) # Type cast values to string
        .withColumn("recipeYield", f.trim("recipeYield")) # Trim leading and trailing white spaces
        .withColumn("recipeYield", f.regexp_replace("recipeYield", "  +", "")) # Replace multiple white spaces with empty string
        .withColumn("recipeYield", f.lower("recipeYield")) # Convert all characters to lower case
        .withColumn("recipeYield", html_unescape_udf("recipeYield")) # Unscape html characters

        .withColumn("url", f.col("url").cast("string")) # Type cast values to string
        
        .withColumn("totalCookTime", (f.col("cookTime") + f.col("prepTime"))) # Calculate total cooking time
        
        .replace("",None) # Replace empty strings with null value
        
        .withColumn("executionDate", f.current_date()) # Create new column with execution date
        )

        logger.info("pre processing complted")

        return pre_pros_df

    except Exception:

            logger.error(msg = "pre processing failed", exc_info = True)

def write_pre_processed_data(output_df, logger, checkpoint_path, corrupted_data_path, validated_data_path):

    """
    Validate, preprocess recipies data and persist to parquet table as a stream

    Parameter
    ---------
    logger : logger
        Configured python logg handler.
        
    checkpoint_path : str
        Path contains streaming query checkpoint.
        
    corrupted_data_path : str
        Path contains corrupted data.
        
    validated_data_path : str
        Path contains valildated data.

    Returns
    -------
    parquet
        Persist precessed data to disk using parquet format.
    """

    try:

        logger.info("write processed data")
        
        # Write data to disk
        (output_df.writeStream
        .format("parquet") # Output format
        .foreachBatch(lambda dataframe, batchid: for_each_bacth_raw_data_writer(dataframe, batchid, corrupted_data_path, validated_data_path)) # process micro batches
        .outputMode("append") # Write mode
        .option("mergeSchema","true") # Schema enforcement
        .option("checkpointLocation", checkpoint_path) # Degine checkpoint location to keep track of offset
        .trigger(once=True) # Trigger only once to stream process to stop itselt
        .start() # Start the process
        .awaitTermination() # Awaits untill the process is complted
        )

        logger.info("write processed data completed")

    except Exception:

        logger.error(msg = "write processed failed", exc_info = True)

def etl(spark, logger, input_data_path, checkpoint_path, corrupted_data_path, validated_data_path):
  
    """
    Validate, preprocess recipies raw data and persist to parquet table as a stream

    Parameter
    ---------
    spark : SparkSession
        Configured spark session.
        
    logger : logger
        Configured python logg handler.
        
    input_data_path : str
        Path contains input json files.
        
    checkpoint_path : str
        Path contains streaming query checkpoint.
        
    corrupted_data_path : str
        Path contains corrupted data.
        
    validated_data_path : str
        Path contains valildated data.

    Returns
    -------
    parquet
        Persist precessed data to disk using parquet format.
    """
    try:

        logger.info("etl load raw data start")

        input_df = read_raw_data(spark, logger, input_data_path)

        logger.info("etl load raw data complete")

        try:

            logger.info("elt pre process data start")

            output_df = pre_process_raw_data(input_df, logger)

            logger.info("etl pre process data complete")

            try:

                logger.info("etl write output data start")
                
                write_pre_processed_data(output_df, logger, checkpoint_path, corrupted_data_path, validated_data_path)

                logger.info("etl write output data complete")

            except Exception:

                logger.error(msg = "elt write output data failed", exc_info = True)

        except Exception:

            logger.error(msg = "etl pre process data failed", exc_info = True)

    except Exception:

        logger.error(msg = "etl load raw data failed", exc_info = True)
    
def read_pre_processed_data(spark, logger, pre_processed_data_path):

    """
    Read processed data from etl function

    Parameter
    ---------
    spark : SparkSession
        Configured spark session.
        
    logger : logger
        Configured python logg handler.
        
    pre_processed_data_path : str
        Path contains pre processed data.
        
    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        spark datafarme.
    """

    try:

        logger.info("read processed data")
        
        # Read pre processed data from previouse step
        processed_df = (spark.read.parquet(pre_processed_data_path))

        logger.info("read processed data complete")

        return processed_df

    except Exception:

        logger.error(msg = "read processed data failed", exc_info = True)

def aggregate_pre_processed_data(processed_df, ingredient, logger):

    """
    Calculate average cooking based on difficulty for the recipies with beef as an ingredient

    Parameter
    ---------
    logger : logger
        Configured python logg handler.
        
    processed_df : pyspark.sql.dataframe.DataFrame
        spark dataframe with processed data.

    ingredient : str
        Any ingredient that wanted to filter out.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        spark dataframe.
    """
    
    try:

        logger.info("label dificulty level")

        # Converts input ingredient to lower case
        ingredient = ingredient.lower()

        # Define difficulty level
        processed_df = (processed_df
                            .filter((f.col("ingredients").contains(ingredient)) & (f.col("totalCookTime") != 0)) # Filter only the recipies that has input ingredient and totalCookTime != 0
                            .withColumn("difficultyLevel", 
                            f.when(f.col("totalCookTime") < 30, f.lit("easy"))
                            .when((f.col("totalCookTime") >= 30) & (f.col("totalCookTime") <= 60), f.lit("medium"))
                            .when(f.col("totalCookTime") > 60, f.lit("hard"))
                            .otherwise(None)) # Define difficulty level based on tootal cooking time in minutes
                            )

        logger.info("label dificulty level complete")

        logger.info("calculate average cooking time")

        # Calculate average cooking time
        output_df = (processed_df
                .groupby(f.col("difficultyLevel").alias("difficulty")) # Group by defficulty level
                .agg(f.round((f.avg("totalCookTime")), 2).alias("avg_total_cooking_time")) # Aggregate average cooking time
                .sort(f.desc('avg_total_cooking_time')) # Sort data by average cooking time
                )

        logger.info("calculate average cooking time complete")

        return output_df

    except Exception:

            logger.error(msg = "calculate avg cooking time failed", exc_info = True)

def write_csv_file(output_df, csv_file, logger):

    """
    Calculate average cooking based on difficulty for the recipies with beef as an ingredient

    Parameter
    ---------
    logger : logger
        Configured python logg handler.
        
    output_df : pyspark.sql.dataframe.DataFrame
        Aggregated spark datafarme.
        
    csv_file : str
        Path contains csv file.

    Returns
    -------
    csv
        Persist precessed data to disk using csv format.
    """

    try:

        logger.info("generate csv")
        
        # Write csv file
        (output_df
        .write
        .mode("overwrite")
        .option("header","true")
        .csv(csv_file))
        
        logger.info("generate csv completed")

    except Exception:

        logger.error(msg = "generate csv failed", exc_info = True)

def csv_etl(spark, logger, pre_processed_data_path, ingredient, csv_file):

    """
    Calculate average cooking based on difficulty for the recipies with beef as an ingredient

    Parameter
    ---------
    spark : SparkSession
        Configured spark session.
        
    logger : logger
        Configured python logg handler.
        
    pre_processed_data_path : str
        Path contains pre processed data.

    ingredient : str
        Any ingredient that wanted to filter out.
        
    csv_file : str
        Path contains csv file.

    Returns
    -------
    csv
        Persist precessed data to disk using csv format.
    """
  
    try:

        logger.info("csv etl load processed data start")
        
        # Read pre processed data from previouse step
        processed_df = read_pre_processed_data(spark, logger, pre_processed_data_path)

        logger.info("csv etl read processed data completed")

        try:

            logger.info("csv etl calculate avg cooking time")

            # calculate average cooking time in minutes
            output_df = aggregate_pre_processed_data(processed_df, ingredient, logger)

            logger.info("csv etl calculate avg cooking time completed")

            try:

                logger.info("csv etl write csv")
                
                # Write csv file
                write_csv_file(output_df, csv_file, logger)

                logger.info("csv etl write csv completed")

            except Exception:

                logger.error(msg = "csv etl write csv failed", exc_info = True)

        except Exception:

            logger.error(msg = "csv etl calculate avg cooking time failed", exc_info = True)

    except Exception:

        logger.error(msg = "csv etl read processed data failed", exc_info = True)
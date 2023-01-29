# import libraries and functions to the python application
from src._run_scripts_ import *

# Define python logger
logger = custom_logger(app_name = 'HelloFresh | Data Engineer | Application | Sanoj Fonseka | ')

logger.info("Start application")

try:

    # Define default path
    logger.info("Define default path")
    path = '/app'

    try:

        # Create spark session for the application
        logger.info("Create dspark session")
        spark = create_spark_session()

        try:

            # Run pre processing function
            etl(
                spark = spark, 
                logger = logger, 
                input_data_path = f'{path}/input', 
                checkpoint_path = f'{path}/output/pre_process/checkpoint', 
                corrupted_data_path = f'{path}/output/pre_process/corrupted_data', 
                validated_data_path = f'{path}/output/pre_process/validated_data'
                )
            logger.info("Complted pre processing function")

            try:

                # Generate csv file with average cooking time for recipies with beef as an ingredient
                csv_etl(
                    spark = spark, 
                    logger = logger, 
                    pre_processed_data_path = f'{path}/output/pre_process/validated_data', 
                    ingredient = 'beef',
                    csv_file = f'{path}/output/avg_total_cooking_time.csv')

                logger.info("Complted csv file generation")

            except Exception:

                logger.error(msg = "calculate_avg_cooking_time_with_beef function failed", exc_info = True)

        except Exception:

            logger.error(msg = "pre_process_raw_json_data function failed", exc_info = True)

    except Exception:

        logger.error(msg = "Spark session creation failed", exc_info = True)

except Exception:

    logger.error(msg = "Default path creation failed", exc_info = True)

logger.info("End application")
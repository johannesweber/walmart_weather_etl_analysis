import logging
from helper import util
from helper.etl_jobs import Jobs
from config import constants

logger = logging.getLogger(__name__)
util.setup_logging()

test_mode = False


def main():
    job = Jobs()

    if test_mode:
        output_path = constants.DB_PATH_TEST
        input_path = constants.SOURCE_FILE_TEST
    else:
        output_path = constants.DB_PATH
        input_path = constants.SOURCE_FILE

    logger.info('Extract...')
    logger.info('Creating DB Scheme...')
    job.init_db_connection(output_path)
    job.create_star_scheme()

    logger.info('Reading Excel File...')
    job.extract_from_xlsx(input_path)

    logger.info('Transform...')
    logger.info('Renaming Columns...')
    job.rename_columns()

    logger.info('Removing Rows with NULL values in xxx_id  column...')
    job.drop_null_ids()

    # creating store, station, item
    logger.info('Creating Dimension Dataframes/Tables...')
    job.create_dimension_tables()

    logger.info('Creating Fact Dataframes/Tables...')
    logger.info('Creating Sales Fact...')
    job.create_sales_fact_table()

    logger.info('Creating Weather Fact...')
    job.create_weather_fact_table()

    logger.info('Load...')
    job.load_into_database()


if __name__ == '__main__':
    main()

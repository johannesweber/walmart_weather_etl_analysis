import logging
import pandas as pd

import util
import helper
from helper import JobHelper
from config import constants

logger = logging.getLogger(__name__)
util.setup_logging()
job_helper = JobHelper(constants.SOURCE_FILE)


def main():
    # job_helper.create_star_scheme()
    df = extract()
    dict_of_dfs = transform(df)
    load(dict_of_dfs)
    job_helper.close_db_connection()


def extract():
    logger.info('Reading Excel File...')
    df = job_helper.read_xlsx()
    return df


def transform(source_dataframe):
    result = {}  # = dict of dfs; key is table name and value is dataframe

    #  rename columns
    logger.info('Renaming Columns...')
    source_dataframe.rename(index=str, inplace=True, columns={
        'store_nbr': 'store_id',
        'item_nbr': 'item_id',
        'units': 'units_sold',
        'station_nbr': 'station_id',
        'tmax': 'temperature_max',
        'tmin': 'temperature_min',
        'tavg': 'temperature_avg',
        'wetbulb': 'wetbulb_temperature',
        'preciptotal': 'preciptation_total',
        'stnpressure': 'station_pressure',
        'avgspeed': 'avg_speed',
    })

    # remove rows with NULL ids
    logger.info('Renaming Rows with NULL values in xxx_id  column...')
    df_wo_na_ids = source_dataframe.dropna(subset=['store_id', 'station_id', 'item_id'])

    # deactivate SettingWithCopyWarning:
    pd.options.mode.chained_assignment = None

    # create dimension dataframes (store, station, item)
    logger.info('Creating Dimension Dataframes/Tables...')
    logger.info('Creating Date Dimension...')
    date_dimension_series = df_wo_na_ids['date'].drop_duplicates()
    date_dimension_df = date_dimension_series.to_frame()
    surr_id = pd.Series(range(date_dimension_df.shape[0]))
    date_dimension_df = date_dimension_df.assign(surr_id=surr_id.values)

    result['date'] = date_dimension_df
    
    logger.info('Creating Store Dimension...')
    store_dimension_series = df_wo_na_ids['store_id'].drop_duplicates()
    store_dimension_df = store_dimension_series.to_frame()
    surr_id = pd.Series(range(store_dimension_df.shape[0]))
    store_dimension_df = store_dimension_df.assign(surr_id=surr_id.values)

    result['store'] = store_dimension_df

    logger.info('Creating Station Dimension...')
    station_dimension_series = df_wo_na_ids['station_id'].drop_duplicates()
    station_dimension_df = station_dimension_series.to_frame()
    surr_id = pd.Series(range(station_dimension_df.shape[0]))
    station_dimension_df = station_dimension_df.assign(surr_id=surr_id.values)

    result['station'] = station_dimension_df

    logger.info('Creating Item Dimension...')
    item_dimension_series = df_wo_na_ids['item_id'].drop_duplicates()
    item_dimension_df = item_dimension_series.to_frame()
    surr_id = pd.Series(range(item_dimension_df.shape[0]))
    item_dimension_df = item_dimension_df.assign(surr_id=surr_id.values)

    result['item'] = item_dimension_df

    # create dimension dataframes (weather, units)
    logger.info('Creating Fact Dataframes/Tables...')
    logger.info('Creating Unit Fact...')
    unit_fact_df = df_wo_na_ids[['date', 'store_id', 'item_id', 'units_sold']]

    # find and set surrogate ids instead date value
    unit_fact_df['item_id'] = unit_fact_df.apply(func=helper.find_surrogate_id, axis=1, args=('item_id', item_dimension_df))
    unit_fact_df['store_id'] = unit_fact_df.apply(func=helper.find_surrogate_id, axis=1, args=('store_id', store_dimension_df))
    unit_fact_df['date'] = unit_fact_df.apply(func=helper.find_surrogate_id, axis=1, args=('date', date_dimension_df))

    result['sales'] = unit_fact_df

    logger.info('Creating Weather Fact...')
    # A value is trying to be set on a copy of a slice from a DataFrame.
    weather_fact_df = df_wo_na_ids[['date',
                                    'item_id',
                                    'store_id',
                                    'station_id',
                                    'temperature_min',
                                    'temperature_max',
                                    'temperature_avg',
                                    'dewpoint',
                                    'wetbulb_temperature',
                                    'snowfall',
                                    'preciptation_total',
                                    'station_pressure',
                                    'sealevel',
                                    'avg_speed']]

    # calculate humidity
    weather_fact_df['humidity'] = weather_fact_df.apply(func=helper.calculate_relative_humidity, axis=1)
    # find and set surrogate ids instead date value
    weather_fact_df['item_id'] = weather_fact_df.apply(func=helper.find_surrogate_id, axis=1, args=('item_id', item_dimension_df))
    weather_fact_df['station_id'] = weather_fact_df.apply(func=helper.find_surrogate_id, axis=1, args=('station_id', station_dimension_df))
    weather_fact_df['date'] = weather_fact_df.apply(func=helper.find_surrogate_id, axis=1, args=('date', date_dimension_df))
    weather_fact_df['store_id'] = weather_fact_df.apply(func=helper.find_surrogate_id, axis=1,args=('store_id', store_dimension_df))

    result['weather'] = weather_fact_df

    return result


def load(dict_of_dfs):
    logger.info('Saving dataframes in Database...')
    for key, value in dict_of_dfs.items():
        job_helper.load_into_db(value, key)


if __name__ == '__main__':
    main()

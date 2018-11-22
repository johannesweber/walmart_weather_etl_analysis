from config import constants
from helper.helper import DatabaseHelper, Helper
import pandas as pd


class Jobs:
    db_helper = None
    sheet_name = constants.XLSX_SHEET
    df = None
    result = {}
    # deactivate SettingWithCopyWarning:
    pd.options.mode.chained_assignment = None

    def init_db_connection(self, db_path):
        self.db_helper = DatabaseHelper()
        self.db_helper.connect_to_db(db_path)

    def create_star_scheme(self):
        self.db_helper.drop_tables()
        self.db_helper.create_star_scheme()

    def extract_from_xlsx(self, input_path):
        self.df = pd.read_excel(input_path, sheet_name=self.sheet_name)

    def rename_columns(self):
        self.df.rename(index=str, inplace=True, columns={
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

    def drop_null_ids(self):
        self.df.dropna(subset=['store_id', 'station_id', 'item_id'], inplace=True)

    def create_dimension_tables(self):
        dimensions = {'date': 'date', 'store': 'store_id', 'station': 'station_id', 'item': 'item_id'}

        for key, value in dimensions.items():
            dimension_series = self.df[value].drop_duplicates()
            dimension_df = dimension_series.to_frame()
            surr_id_series = pd.Series(range(dimension_df.shape[0]))
            dimension_df = dimension_df.assign(surr_id=surr_id_series.values)

            self.result[key] = dimension_df

    def create_sales_fact_table(self):
        unit_fact_df = self.df[['date', 'store_id', 'item_id', 'units_sold']]

        facts = {'date': 'date', 'store': 'store_id', 'item': 'item_id'}

        for key, value in facts.items():
            # find and set surrogate ids
            unit_fact_df[value] = unit_fact_df.apply(func=Helper.find_surrogate_id, axis=1,
                                                     args=(value, self.result[key]))

        self.result['sales'] = unit_fact_df

    def create_weather_fact_table(self):
        weather_fact_df = self.df[['date',
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

        dimensions = {'date': 'date', 'store': 'store_id', 'station': 'station_id', 'item': 'item_id'}

        for key, value in dimensions.items():
            # find and set surrogate ids
            weather_fact_df[value] = weather_fact_df.apply(func=Helper.find_surrogate_id, axis=1,
                                                           args=(value, self.result[key]))

        # calculate and add humidity
        weather_fact_df['humidity'] = weather_fact_df.apply(func=Helper.calculate_relative_humidity, axis=1)

        self.result['weather'] = weather_fact_df

    def load_into_database(self):
        for key, value in self.result.items():
            self.db_helper.load_into_db(value, key)

        self.db_helper.close_db_connection()


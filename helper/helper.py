import sqlite3


class DatabaseHelper:

    conn = None
    cursor = None

    def connect_to_db(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

    def create_star_scheme(self):
        # dimensions
        create_station_query = '''CREATE TABLE station ( station_id INTEGER, surr_id INTEGER PRIMARY KEY)'''
        create_date_query = '''CREATE TABLE date ( date INTEGER, surr_id INTEGER PRIMARY KEY)'''
        create_item_query = '''CREATE TABLE item ( item_id INTEGER, surr_id INTEGER PRIMARY KEY)'''
        create_store_query = '''CREATE TABLE store ( store_id INTEGER, surr_id INTEGER PRIMARY KEY)'''

        # facts
        create_sales_query = '''CREATE TABLE sales 
        ( date INTEGER,
          store_id INTEGER,
          item_id INTEGER,
          units_sold INTEGER,
          FOREIGN KEY(item_id) REFERENCES item(surr_id)
          FOREIGN KEY(store_id) REFERENCES store(surr_id)
          FOREIGN KEY(date) REFERENCES date(surr_id)
          PRIMARY KEY (date, store_id, item_id))'''

        create_weather_query = '''CREATE TABLE weather 
        ( date INTEGER, 
          item_id INTEGER,
          store_id INTEGER,
          station_id INTEGER,
          temperature_min INTEGER,
          temperature_max INTEGER,
          temperature_avg INTEGER,
          dewpoint INTEGER,
          wetbulb_temperature INTEGER,
          snowfall REAL,
          preciptation_total REAL,
          station_pressure REAL,
          sealevel REAL,
          avg_speed REAL,
          humidity REAL,
          units_sold INTEGER,
          FOREIGN KEY(item_id) REFERENCES item(surr_id)
          FOREIGN KEY(station_id) REFERENCES station(surr_id)
          FOREIGN KEY(date) REFERENCES date(surr_id)
          FOREIGN KEY(store_id) REFERENCES store(surr_id)
          PRIMARY KEY (date, station_id, item_id, store_id))
          '''

        self.conn.execute(create_station_query)
        self.conn.execute(create_date_query)
        self.conn.execute(create_item_query)
        self.conn.execute(create_store_query)

        self.conn.execute(create_sales_query)
        self.conn.execute(create_weather_query)

    def load_into_db(self, dataframe, table_name):
        if table_name in ['station', 'date', 'store', 'item']:
            index = 'surr_id'
        elif table_name == 'sales':
            index = 'store_id, item_id'
        else:
            index = 'date, item_id, station_id'
        dataframe.to_sql(table_name, self.conn, if_exists="replace", index=False, index_label=index)

    def close_db_connection(self):
        self.conn.close()


class Helper:

    @staticmethod
    def calculate_relative_humidity(row):
        relative_humidity = 0

        # formula from https://www.1728.org/relhum.htm
        dry_bulb_temperature = row['temperature_avg']
        wet_bulb_temperature = row['wetbulb_temperature']

        if wet_bulb_temperature != 0 and \
                dry_bulb_temperature != 0 and \
                (dry_bulb_temperature != wet_bulb_temperature) and \
                wet_bulb_temperature < dry_bulb_temperature:
            dry_bulb_temperature_celsius = Helper._calculate_celsius_from_fahrenheit(dry_bulb_temperature)
            wet_bulb_temperature_celsius = Helper._calculate_celsius_from_fahrenheit(wet_bulb_temperature)

            e_dry = 6.112 * 2.71828182845904 ** ((17.502 * dry_bulb_temperature_celsius) /
                                                 (240.97 + dry_bulb_temperature_celsius))
            e_wet = 6.112 * 2.71828182845904 ** ((17.502 * wet_bulb_temperature_celsius) /
                                                 (240.97 + wet_bulb_temperature_celsius))

            relative_humidity = ((e_wet - (0.6687451584 * ((1 + 0.00115 * wet_bulb_temperature_celsius) * (
                    dry_bulb_temperature_celsius - wet_bulb_temperature_celsius)))) / e_dry) * 100
        return relative_humidity

    @staticmethod
    def find_surrogate_id(row, key, dataframe):
        surr_id = dataframe[dataframe[key] == row[key]]['surr_id'].values[0]
        return surr_id

    @staticmethod
    def _calculate_celsius_from_fahrenheit(fahrenheit):
        return (fahrenheit - 32) * 5 / 9

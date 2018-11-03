import pandas as pd
import logging
import sqlite3
import constants

logger = logging.getLogger(__name__)


class JobHelper:

    source_file = None
    conn = None
    cursor = None

    def __init__(self, filename):
        self.source_file = filename
        self.conn = sqlite3.connect(constants.DB_NAME)
        self.cursor = self.conn.cursor()

    def read_xlsx(self):
        df = pd.read_excel(self.source_file, sheet_name=constants.XLSX_SHEET)
        return df

    def load_into_db(self, dataframe, table_name):
        dataframe.to_sql(table_name, self.conn, if_exists="replace")


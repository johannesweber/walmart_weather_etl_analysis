import logging
import util
from helper import JobHelper
import constants

logger = logging.getLogger(__name__)
util.setup_logging()
helper = JobHelper(constants.SOURCE_FILE)


def main():
    df = extract()
    # dict_of_dfs = transform(dataframe)
    # load(dict_of_dfs)


def extract():
    df = helper.read_xlsx()
    logger.debug(df.columns)
    print(df.columns)
    return df


def transform(dataframe):
    result = {}  # = dict of dfs; key is table name and value is dataframe
    return result


def load(dict_of_dfs):
    for key, value in dict_of_dfs.items():
        helper.load_into_db(value, key)


if __name__ == '__main__':
    main()
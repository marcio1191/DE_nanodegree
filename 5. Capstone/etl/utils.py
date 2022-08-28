
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


class CleanPandasDf(pd.DataFrame):

    @staticmethod
    def drop_empty_rows(df):
        logger.info('Droping all empty rows ...')
        df.dropna(axis=0, how='all', inplace=True) # drop rows with all missing values

    @staticmethod
    def drop_empty_cols(df):
        logger.info('Droping all empty cols ...')
        df.dropna(axis=1, how='all', inplace=True)

    @staticmethod
    def drop_duplicated_rows(df, keep='last'):
        logger.info(f'Droping duplicated rows and keeping {keep} record...')
        df.drop_duplicates(keep=keep, inplace=True)

    @staticmethod
    def drop_col_if_missing_data_more_than_perc(df, col_name, perc=10):
        """ Drops column ``col_name`` if percentage of NAN values is more than a threshold ``perc``.

        Parameters:
            col_name (str) : column name to be inspected.
            perc (float | int) : value such that percetange NANs above that value, column is dropped
        """
        if col_name not in df.columns:
            raise ValueError(f'col_name = {col_name} does not exist in dataframe ..')

        number_rows = len(df.index)
        number_col_nan = df[col_name].isna().sum()
        number_col_nan_perc = 100 * number_col_nan / number_rows
        logger.debug(f'% NAN in column "{col_name}" = {number_col_nan_perc:.1f} %...')

        if number_col_nan_perc > perc:
            logger.info(f'Dropping column "{col_name}" because it contains {number_col_nan_perc:.1f} % NAN values (> {perc} %)')
            df.drop(col_name, axis=1, inplace=True)

    @staticmethod
    def drop_all_cols_if_missing_data_is_more_than_perc(df, exclude_cols = tuple(), perc=10):
        """Drop all columns in dataframe if percentage of NANs is more than ``perc``.
        NOTE: All columns in exclude_cols are ignored.

        Parameters:
            exclude_cols (tuple | list) : tuple/list of columns to be excluded
            perc (float | int) : value such that percetange NANs above that value, columns are dropped
        """
        logger.info(f'Droping all cols where number NAN is above {perc} % ...')
        for col in df.columns:
            if col not in exclude_cols:
                CleanPandasDf.drop_col_if_missing_data_more_than_perc(df, col_name=col, perc=perc)

    @staticmethod
    def split_column_based_on_separator(df, col_name,  list_new_cols, separator=',', force_types=False, new_cols_types=tuple()):
        if force_types:
            if not new_cols_types:
                raise ValueError('Please provide "new_cols_types" when using force_types=True')
            if len(new_cols_types) != len(list_new_cols):
                raise ValueError('Size of "new_cols_types" array must be the same as size of "list_new_cols"')
        df[list_new_cols] = df[col_name].str.split(separator, expand=True)
        if force_types:
            df[list_new_cols].astype(*new_cols_types)

        return df

    @staticmethod
    def strip_white_text_from_all_cells(df):
       df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
       df.replace(r'^\s*$', np.nan, regex=True, inplace=True) # replace all empty space with NAN

    @staticmethod
    def lower_case_all_cells(df):
        df = df.apply(lambda x: x.astype(str).str.lower())
        df.replace(r'^\s*$', np.nan, regex=True, inplace=True)


def load_csv_data_with_spark(spark, input_dir):
    """ Loads csv data into memory using spark
    Parameters:
        spark : spark session
        input_dir (str) : directory with csv files or csv file

    Returns:
        Spark dataframe with immigration data
    """
    return spark.read.format('csv').option('header','true').load(input_dir)

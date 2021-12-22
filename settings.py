import pandas as pd


def set_df_max_row():
    pd.set_option('display.max_rows', None)


def set_df_max_column():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', -1)
    pd.set_option('display.width', None)


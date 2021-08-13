import os
import glob
import pandas as pd
import dask.dataframe as dd
import pathlib


def read_data(dir_name, usecols, dtype):
    """
    Return a dataframe with concatenated data.
    Set timestamp as index.

    Parameters:
        dir_name (str): directory name
        usecols (list-like): selected columns
        dtype (dict): data type for columns
    """

    filenames = [filename for filename in glob.iglob(dir_name, recursive=True)]
    filenames.sort()
    df = dd.read_csv(filenames,
                     sep=r'\s+',
                     usecols=usecols,
                     dtype=dtype)
    df = df.compute()
    df['DATE_TIME'] = pd.to_datetime(df['DATE'] + ' ' + df['TIME'])
    df.drop(['DATE', 'TIME'], axis=1)

    return df


def save_24h(df, path, file_id, level):
    """
    Save 24-hour files

    Parameters:
        df (pandas DataFrame): dataframe
        path (str): path to save output files
        file_id (str): analyzer serial number
        level (str): data processing level
    """
    for day in df.index.dayofyear.unique():
        df_24h = df[(df.index.dayofyear == day)]
        year = str(df_24h.index[0].strftime('%Y'))
        month = str(df_24h.index[0].strftime('%m'))
        full_path = path + '/' + year + '/' + month
        pathlib.Path(full_path).mkdir(parents=True, exist_ok=True)
        file_name = full_path +\
                    '/' + file_id + '-' +\
                    df_24h.index[0].strftime('%Y%m%d') + '-' + \
                    'Z-DataLog_User_' + level + '.csv'
        if os.path.isfile(file_name):
            df_24h.to_csv(file_name, mode='a', header=False)
        else:
            df_24h.to_csv(file_name)


def resample_data(df, t, my_cols):
    """
    Returns a dataframe with resampled data [mean, std, count].

    Parameters:
        df (pandas DataFrame): dataframe
        t ('T', 'H', 'D') : minute, hour or day
        my_cols (list-like): selected columns
    """
    df_mean = df[my_cols].resample(t).mean()
    df_std = df[my_cols].resample(t).std()
    df_count = df[my_cols].resample(t).count()
    return df_mean.join(df_std, rsuffix='_std').join(df_count, rsuffix='_count')


def gantt_data(path, var, pos):
    """
    Returns a dataframe with data availability info.

    Parameters:
        path (str): file name
        var (str): selected variable
        pos (int): position in the graph (from bottom to top)
    """
    df = pd.read_csv(path)
    df = df.set_index('DATE_TIME')
    df.index = pd.to_datetime(df.index)
    df['avail'] = df[var].isnull()  # look for null values
    df['avail'] = df['avail'].map({False: pos})  # poputlate with graph position
    return df

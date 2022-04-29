import os
import glob
import pandas as pd
import dask.dataframe as dd
import pathlib
from datetime import datetime, timedelta


def read_data(dir_name, usecols, dtype, raw_data=True, date_range=None):
    """
    Return a dataframe with concatenated data.
    Set timestamp as index.

    Parameters:
        dir_name (str): directory name
        usecols (list-like): selected columns
        dtype (dict): data type for columns
        raw_data (bool): if True, combine date and time columns,
        and set sep to r'\s+', otherwise ',', default is True
        date_range (list of str): list with initial and final date 'yyyy/mm/dd'
    """
    filenames = [filename for filename in glob.iglob(dir_name + '**/*.dat', recursive=True)]
    filenames.sort()
    if date_range:
        idx0 = filenames.index([x for x in filenames if date_range[0] in x][0])
        if idx0 != 0:
            idx0 -= 1
        idx1 = filenames.index([x for x in filenames if date_range[-1] in x][-1]) + 1
        filenames = filenames[idx0:idx1]
    if raw_data:
        sep = r'\s+'
    else:
        sep = ','
    df = dd.read_csv(filenames,
                     sep=sep,
                     usecols=usecols,
                     dtype=dtype)
    df = df.compute()
    if raw_data:
        df['DATE_TIME'] = pd.to_datetime(df['DATE'] + ' ' + df['TIME'])
        df = df.drop(['DATE', 'TIME'], axis=1)
    df = df.set_index('DATE_TIME')

    if date_range:
        return df.loc[(df.index >= date_range[0]) &
                      (df.index < datetime.strptime(date_range[-1], "%Y/%m/%d") + timedelta(days=1))]
    else:
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
        file_name = full_path + \
            '/' + file_id + '-' + \
            df_24h.index[0].strftime('%Y%m%d') + \
            'Z-DataLog_User_' + level + '.csv'
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

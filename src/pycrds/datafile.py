import calendar
import glob
import warnings
from datetime import datetime, timedelta, timezone
from typing import Tuple, List, Dict

import dask.dataframe as dd
import numpy as np
import pandas as pd
import xarray as xr


def get_filenames(path: str,
                  date_range: Tuple[str, str] or str,
                  serial_number: str) -> Tuple[List[str], str, str]:
    """
    Generates a list of .dat filenames in a directory matching a date range.
    Check if all files selected have the serial number provided and if there
    are any Sync files.

    Parameters
    ----------
    path : str
        Directory path where the .dat files are located. For example:
        '/raw-data/iag/G2301_CFADS2502/DataLog_User'.
    date_range : tuple of str or str
        Date range to filter filenames. Can be a tuple in the format ('YYYY-MM-DD',
        'YYYY-MM-DD') or a string in the format 'YYYY-MM'.
        If a tuple, it should contain two date strings ('YYYY-MM-DD'), representing
        the start and end dates. If a string, it should be in 'YYYY-MM' format,
        representing an entire month.
    serial_number : str
        Serial number of the CRDS being read as written in the input file. For
        example: 'CFADS2502'.

    Returns
    -------
    tuple of (list of str, str, str)
        A tuple containing:
        - A list of filenames that match the specified date range.
        - The start date as a string in the format 'YYYY-MM-DD'.
        - The end date as a string in the format 'YYYY-MM-DD'.

    """

    if isinstance(date_range, tuple):
        if len(date_range) != 2 or not all(isinstance(date, str) for date in date_range):
            raise ValueError("Tuple must contain exactly two string elements.")
        try:
            start_date = datetime.strptime(date_range[0], '%Y-%m-%d')
            end_date = datetime.strptime(date_range[1], '%Y-%m-%d')
        except ValueError:
            raise ValueError("Invalid date format in tuple. Expected 'YYYY-MM-DD'.")
        if start_date > end_date:
            raise ValueError("End date must be greater than or equal start date")
    elif isinstance(date_range, str):
        try:
            start_date = datetime.strptime(date_range, '%Y-%m')
        except ValueError:
            raise ValueError("Invalid date format in tuple. Expected 'YYYY-MM-DD'.")
        last_day = calendar.monthrange(start_date.year, start_date.month)[1]
        end_date = datetime(start_date.year, start_date.month, last_day)
    else:
        raise TypeError("date_range must be a tuple or a string.")

    previous_day = start_date - timedelta(days=1)
    next_day = end_date + timedelta(days=1)
    if previous_day.year == next_day.year and previous_day.month != 1 and next_day.month != 12:
        filenames = [filename for filename in glob.iglob(path + f'/{previous_day.year}/**/*.dat', recursive=True)]
    else:
        filenames = [filename for filename in glob.iglob(path + '/**/*.dat', recursive=True)]
    filenames.sort()

    try:
        idx0_candidates = [x for x in filenames if previous_day.strftime('%Y%m%d') in x]
        idx0_candidates.sort()
        idx0 = filenames.index(idx0_candidates[-1])
    except:
        try:
            idx0_candidates = [x for x in filenames if start_date.strftime('%Y%m%d') in x]
            idx0_candidates.sort()
            idx0 = filenames.index(idx0_candidates[0])
        except:
            try:
                idx0_candidates = [x for x in filenames if start_date.strftime('%Y%m') in x]
                idx0_candidates.sort()
                idx0 = filenames.index(idx0_candidates[0])
            except:
                raise ValueError("No files found for the first date of the date range.")
    try:
        idx1_candidates = [x for x in filenames if next_day.strftime('%Y%m%d') in x]
        idx1_candidates.sort()
        idx1 = filenames.index(idx1_candidates[0])
    except:
        try:
            idx1_candidates = [x for x in filenames if end_date.strftime('%Y%m%d') in x]
            idx1_candidates.sort()
            idx1 = filenames.index(idx1_candidates[-1])
        except:
            try:
                idx1_candidates = [x for x in filenames if start_date.strftime('%Y%m') in x]
                idx1_candidates.sort()
                idx1 = filenames.index(idx1_candidates[-1])
            except:
                raise ValueError("No files found for the last date of the date range.")
    filenames = filenames[idx0:idx1 + 1]
    filenames.sort()

    filenames_to_check = [filename.split('/')[-1] for filename in filenames]
    if not all('Sync' not in name for name in filenames_to_check):
        raise ValueError("There are Sync files in the path and date range provided")
    serial_numbers = [filename.split('-')[0] for filename in filenames_to_check]
    serial_numbers = list(set(serial_numbers))
    if len(serial_numbers) == 1 and serial_numbers[0] != serial_number:
        raise ValueError("Files do not correspond to the serial number provided")
    elif len(serial_numbers) == 2:
        if serial_number in serial_numbers:
            other = [name for name in serial_numbers if name != serial_number][0]
            if other.isalpha():
                warnings.warn(f"There are one or more incomplete serial number in file names: {other}")
        else:
            raise ValueError("Not all files correspond to the same serial number provided")
    elif len(serial_numbers) >= 3:
        raise ValueError("Check files serial number")

    return filenames, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')


def read_raw_data(path: str,
                  date_range: Tuple[str, str] or str,
                  serial_number: str,
                  usecols: List[str],
                  dtype: Dict) -> pd.DataFrame:
    """
    Reads data from .dat files in a directory matching a date range and serial number.

    Parameters
    ----------
    path : str
        Directory path where the .dat files are located. For example:
        '/raw-data/iag/G2301_CFADS2502/DataLog_User'.
    date_range : tuple of str or str
        Date range to filter filenames. Can be a tuple in the format ('YYYY-MM-DD',
        'YYYY-MM-DD') or a string in the format 'YYYY-MM'.
        If a tuple, it should contain two date strings ('YYYY-MM-DD'), representing
        the start and end dates. If a string, it should be in 'YYYY-MM' format,
        representing an entire month.
    serial_number : str
        Serial number of the CRDS being read as written in the input file. For
        example: 'CFADS2502'
    usecols : list of str
        A list of column names to read from the .dat files.
    dtype : dict
        A dictionary specifying the data type for each column.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the data read from the .dat files.

    """

    filenames, start_date, end_date = get_filenames(path, date_range, serial_number)

    df = dd.read_csv(filenames,
                     sep=r'\s+',
                     usecols=usecols,
                     dtype=dtype,
                     engine='c',
                     )
    df = df.compute()

    # ATTENTION: Is CO2 always species 1? Should it be a user input?
    df = df[df.species == 1]
    df = df.drop(['species'], axis=1)

    df['DATE_TIME'] = pd.to_datetime(df['DATE'] + ' ' + df['TIME'])
    df = df.drop(['DATE', 'TIME'], axis=1)
    df = df.set_index('DATE_TIME')

    df = df[(df.index >= start_date) & (df.index <= f'{end_date} 23:59:59.999')]

    return df


def save_dataset_level_0(df, config):

    ds = xr.Dataset.from_dataframe(df)
    ds = ds.rename({'DATE_TIME': 'time'})
    ds['CAL'] = ds['CAL'].astype(np.int32)
    ds['FA'] = ds['FA'].astype(np.int32)
    ds['FM'] = ds['FM'].astype(np.int32)

    global_attrs = config['global_attrs']
    current_utc_time = datetime.now(timezone.utc)
    current_utc_time = current_utc_time.strftime("%Y-%m-%d %H:%M:%S UTC")
    global_attrs['processed_date'] = current_utc_time
    ds.attrs = global_attrs

    variable_attrs = config['variable_attrs']
    for var in variable_attrs.keys():
        ds[var].attrs = variable_attrs[var]

    start_date = str(df.index.min())[0:10].replace('-', '')
    end_date = str(df.index.max())[0:10].replace('-', '')
    name_to_save = f'{config["file_serial_number"]}-{start_date}-{end_date}-DataLog_User-level_0.nc'

    path_to_save = config['path_to_save']
    ds.to_netcdf(f'{path_to_save}/{name_to_save}')
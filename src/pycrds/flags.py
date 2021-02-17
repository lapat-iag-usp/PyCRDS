import pandas as pd
import numpy as np


def read_logbook(path):
    """
    Return a dataframe with the logbook data.
    Read events from the logbook file (.csv).
    Drop incorrect timestamp.
    Drop empty lines.

    Logbook columns: Event | Start_Event | End_Event | Flags

    Parameters:
        path (str): path of .csv logbook
    """
    lines = open(path).readlines()
    lines_skip = lines.index('Event,Start_Event,End_Event,Flags\n')
    df = pd.read_csv(path, sep=',', skiprows=lines_skip)
    for line in range(len(df['Event'])):
        try:
            df.Start_Event[line] = pd.to_datetime(df.Start_Event[line], format='%Y/%m/%d %H:%M')
            df.End_Event[line] = pd.to_datetime(df.End_Event[line], format='%Y/%m/%d %H:%M')
        except:
            df = df.drop(index=line)
    df = df.dropna(how='all')
    df = df[df.Flags.notna()]
    df = df.reset_index(drop=True)
    return df


def insert_manual_flags(df, df_logbook):
    """
    Return the dataframe with a column flag for manual control quality.
    Use regex to remove duplicated characters for simultaneous events.

    Parameters:
        df (pandas DataFrame): data dataframe
        df_logbook (pandas DataFrame): logbook dataframe
    """
    df['FLAGS'] = np.nan
    lines_logbook = range(df_logbook['Start_Event'].shape[0])
    for line in lines_logbook:
        flag_range = (df.index >= df_logbook['Start_Event'][line]) & (df.index <= df_logbook['End_Event'][line])
        df.loc[flag_range, ['FLAGS']] = df.loc[flag_range, ['FLAGS']].fillna('') + df_logbook['Flags'][line]
    df.FLAGS = df.FLAGS[df.FLAGS.notna()].apply(lambda x: x[::-1]).replace(to_replace='(.)(?=.*\\1)', value='',
                                                                           regex=True)
    df.FLAGS = df.FLAGS[df.FLAGS.notna()].apply(lambda x: x[::-1])
    return df

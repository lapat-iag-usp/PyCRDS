import sqlite3

import pandas as pd


def apply_automatic_flags(df: pd.DataFrame, config) -> pd.DataFrame:

    df['FA'] = 0
    compiled_conditions = [(rule, compile(rule, '<string>', 'eval')) for rule in
                           config['automatic_flags']]

    # Iterar sobre as regras compiladas e aplicar cada uma vetorizadamente
    for condition, compiled_condition in compiled_conditions:
        mask = df.apply(lambda row: eval(compiled_condition, {"row": row}), axis=1)
        df.loc[mask, 'FA'] = 1

    return df


def apply_manual_flags(df: pd.DataFrame,
                       config) -> pd.DataFrame:

    # Banco de dados
    conn = sqlite3.connect(config['database_path'])
    query = "SELECT * FROM dashboard_Event"
    log = pd.read_sql_query(query, conn)
    conn.close()

    # Logbook
    # ATTENTION: Is it better to use the logbook id?
    log = log[log['name'].str.contains(config['logbook_name'], na=False)]
    log = log.reset_index(drop=True)
    log.loc[:, 'event_date'] = pd.to_datetime(log['event_date'])
    log.loc[:, 'start_date'] = pd.to_datetime(log['start_date'])
    log.loc[:, 'end_date'] = pd.to_datetime(log['end_date'])
    log = log[(log.invalid == 1)]  # apenas eventos que invalidam os dados
    log = log.sort_values(by='start_date')
    log = log[(log['end_date'] >= df.index[0]) & (log['start_date'] <= df.index[-1])]

    log['key'] = 1
    df['key'] = 1

    merged_df = pd.merge(df.reset_index(), log[['start_date', 'end_date', 'key']], on='key')
    merged_df = merged_df[(merged_df['DATE_TIME'] >= merged_df['start_date']) &
                          (merged_df['DATE_TIME'] <= merged_df['end_date'])]

    df['FM'] = df.index.isin(merged_df['DATE_TIME']).astype(int)
    df.drop(columns=['key'], inplace=True)

    return df


def apply_calibration_flags(df: pd.DataFrame,
                            config) -> pd.DataFrame:

    for calib_period in config['calibration']:
        start, end, _ = calib_period
        if end == '':
            end = df.index.max()
        mask = (df.index >= start) & (df.index <= end)
        # ATTENTION: Will solenoid_valves be the only method?
        df.loc[mask, 'CAL'] = df.loc[mask, 'solenoid_valves'].apply(lambda x: 0 if x == 1 else 1)
    df['CAL'] = df['CAL'].fillna(0)
    last_indices = []
    last_one_idx = -1
    for i in range(len(df)):
        if df['CAL'].iloc[i] == 1:
            last_one_idx = i
        else:
            if last_one_idx != -1:
                last_indices.append(last_one_idx)
                last_one_idx = -1
    if last_one_idx != -1:
        last_indices.append(last_one_idx)
    for idx in last_indices:
        df.iloc[idx:idx + 50, df.columns.get_loc('CAL')] = 1

    return df


def apply_calibration_id(df, config):

    for calib_period in config['calibration']:
        start, end, cal_id = calib_period
        mask = (df.index >= start) & (df.index <= end) & (df['CAL'] == 1)
        df.loc[mask, 'CAL_ID'] = cal_id

        return df

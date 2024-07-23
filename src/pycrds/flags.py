import sqlite3

import pandas as pd


def apply_automatic_flags(df: pd.DataFrame,
                          automatic_flags: list) -> pd.DataFrame:
    """
    Apply automatic flags to a DataFrame based on specified conditions.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to which the flags will be applied.
    automatic_flags : list
        List of conditions to apply the flags, for example:
        [
            "row['CavityTemp'] < 44.98",
            "any(row[value] == 0 for value in ['CO2', 'CO2_dry', 'CH4', 'CH4_dry'])"
        ]

    Returns
    -------
    pd.DataFrame
        DataFrame with the 'FA' column updated with automatic flags.
        'FA' column values:
            - 1: Condition is true (flag is applied)
            - 0: Condition is false (flag is not applied)

    Notes
    -----
    - The conditions in `automatic_flags` must be strings evaluable as Python
      expressions that return boolean values.
    - The parameter automatic_flags may be defined in the campaign config file.

    """

    df['FA'] = 0
    compiled_conditions = [(rule, compile(rule, '<string>', 'eval')) for rule in automatic_flags]

    for condition, compiled_condition in compiled_conditions:
        mask = df.apply(lambda row: eval(compiled_condition, {"row": row}), axis=1)
        df.loc[mask, 'FA'] = 1

    return df


def apply_manual_flags(df: pd.DataFrame,
                       database_path: str,
                       logbook_name: str) -> pd.DataFrame:
    """
    Applies manual flags to a DataFrame based on events from a database.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to which the manual flags will be applied.

    database_path : str
        Path to the SQLite database containing event data.

    logbook_name : str
        Name or identifier of the logbook events to consider.

    Returns
    -------
    pd.DataFrame
        DataFrame with the 'FM' column updated with manual flags:
            - 1: Data point falls within a logged event period
            - 0: Data point does not fall within any logged event period

    Notes
    -------
    - The parameters database_path and logbook_name may be defined in the
      campaign config file.
    - Only events that invalidate data (where invalid=True) are considered.
    - Date intervals are closed on both sides.

    """

    # Banco de dados
    conn = sqlite3.connect(database_path)
    query_flag = "SELECT * FROM dashboard_Flag"
    flag = pd.read_sql_query(query_flag, conn)
    query_log = "SELECT * FROM dashboard_Event"
    log = pd.read_sql_query(query_log, conn)
    conn.close()

    # Logbook
    flag_id = flag[flag.flag == 'M'].id.values[0]
    log = log[log['name'].str.contains(logbook_name, na=False)]
    log = log[(log.invalid == 1)]
    log = log[(log.flags_id == flag_id)]
    log = log.reset_index(drop=True)
    log.loc[:, 'event_date'] = pd.to_datetime(log['event_date'])
    log.loc[:, 'start_date'] = pd.to_datetime(log['start_date'])
    log.loc[:, 'end_date'] = pd.to_datetime(log['end_date'])
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
                            calibration_periods: list) -> pd.DataFrame:
    """
    Apply calibration flags to a DataFrame.

    The function iterates through `df` rows and applies calibration flags and ID
    based on the specified `calibration_periods`. It also handles cases where 'FM'
    (manual flags) invalidate the calibration flags ('CAL').

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with calibration data.
    calibration_periods : list
        List of calibration periods, each represented as a list with the format:
        [start_time, end_time, value, id, method].
        Example:
        [
            ["2020-12-18 16:00", "2021-10-25 15:00", 4, "D311113", "solenoid_valves"],
            ["2021-10-25 19:00", "2022-10-05", 4, "D289341", "solenoid_valves"],
            ["2021-10-18 19:00", "2022-10-05", 5, "CC339517", "solenoid_valves"],
            ["2022-10-13", "", 4, "CC339517", "MPVPosition"]
        ]

    Returns
    -------
    pd.DataFrame
        DataFrame with applied calibration flags and ID:
        - 'CAL': Calibration flag (1 for calibration periods, 0 otherwise).
        - 'CAL_ID': ID of the gas tank used during calibration periods.

    Notes
    -------
    - The parameter calibration_periods may be defined in the campaign config file.
    - Requires a column 'FM' (manual flags) in `df`. If 'FM' == 1, 'CAL' is set to 0.

    """


    def apply_flag(row):
        for period in calibration_periods:
            _, _, value, _, method = period
            if row[method] == value:
                return value
        return 0


    df['CAL'] = df.apply(apply_flag, axis=1)

    # If there is a manual flag (FM equals 1), it invalidates the calibration
    # flag (CAL will be set to 0)
    if 'FM' not in df.columns:
        raise KeyError("The column 'FM' does not exist in the DataFrame.")
    df['CAL'] = df.apply(lambda row: 0 if row['FM'] == 1 else row['CAL'], axis=1)

    sequences = []
    start_idx = -1
    last_non_zero_idx = 0
    for i in range(len(df)):
        if df['CAL'].iloc[i] != 0:
            if start_idx == -1:
                start_idx = i
            last_non_zero_idx = i
        else:
            if start_idx != -1:
                sequences.append((start_idx, last_non_zero_idx))
                start_idx = -1
    if start_idx != -1:
        sequences.append((start_idx, last_non_zero_idx))

    for sequence in sequences:
        start_idx = max(0, sequence[0] - 2)
        end_idx = min(len(df), sequence[1] + 50)
        df.iloc[start_idx:end_idx, df.columns.get_loc('CAL')] = df.iloc[sequence[0], df.columns.get_loc('CAL')]

    df = _apply_calibration_id(df, calibration_periods)

    # Is this the best way to do it?
    df.loc[df['CAL_ID'].isna(), 'CAL'] = 0

    return df


def _apply_calibration_id(df: pd.DataFrame,
                         calibration_periods: list) -> pd.DataFrame:

    for period in calibration_periods:
        start, end, value, cal_id, _ = period
        if end == '':
            end = df.index.max()
        mask = (df.index >= start) & (df.index <= end) & (df['CAL'] == value)
        df.loc[mask, 'CAL_ID'] = cal_id
    df['CAL'] = df['CAL'].apply(lambda x: 1 if x != 0 else 0)

    return df

import sqlite3

import pandas as pd


def apply_automatic_flags(df: pd.DataFrame,
                          config) -> pd.DataFrame:

    def _automatic_flags(row):
        """
        Se mais de uma flag se aplica ao mesmo registro, apenas a primeira flag é aplicada
        """
        for rule in config['automatic_flags']:
            if eval(rule['condition'], {"row": row}):
                return rule['flag']
        return 0

        # if row['CavityTemp'] < 44.98:
        #     return 1
        # elif any(row[value] == 0 for value in config['zero_value_flag_columns']):
        #     return 2
        # else:
        #     return 0

    df['FA'] = df.apply(_automatic_flags, axis=1)

    return df


def apply_manual_flags(df: pd.DataFrame,
                       config) -> pd.DataFrame:

    def _manual_flags(row):
        for index, log_row in log.iterrows():
            if log_row['start_date'] <= row.name <= log_row['end_date']:
                return 1
        return 0

    # Banco de dados
    conn = sqlite3.connect(config['database_path'])
    query = "SELECT * FROM dashboard_Event"
    log = pd.read_sql_query(query, conn)
    conn.close()

    # Logbook
    # ATTENTION: Melhor usar o nome ou o logbook id?
    log = log[log['name'].str.contains(config['logbook_name'], na=False)]
    log = log.reset_index(drop=True)
    log.loc[:, 'event_date'] = pd.to_datetime(log['event_date'])
    log.loc[:, 'start_date'] = pd.to_datetime(log['start_date'])
    log.loc[:, 'end_date'] = pd.to_datetime(log['end_date'])
    log = log[(log.invalid == 1)]  # apenas eventos que invalidam os dados
    log = log.sort_values(by='start_date')
    log = log[(log['end_date'] >= df.index[0]) & (log['start_date'] <= df.index[-1])]

    df['FM'] = df.apply(_manual_flags, axis=1)

    return df

import pandas as pd


def rename_columns(df):
    col_names = {'well': '', 'top': '', 'bot': '', 'soil': ''}
    for column in df.columns.values:
        if 'кваж' in column:
            col_names['well'] = column
        elif ('нач' in column) or ('кров' in column):
            col_names['top'] = column
        elif ('кон' in column) or ('подошв' in column):
            col_names['bot'] = column
        elif 'нефтенас' in column:
            col_names['soil'] = column
    df.rename(columns={col_names['bot']: 'bot', col_names['top']: 'top',
                       col_names['well']: 'well', col_names['soil']: 'soil'}, inplace=True)


def perf_reader(perf_path):
    perf_df = pd.read_excel(perf_path, engine='openpyxl')
    perf_df.rename(columns=lambda x: x.lower().strip(), inplace=True)
    rename_columns(perf_df)
    perf_ints = {}
    for well in perf_df['well'].unique():
        well_df = perf_df[perf_df['well'] == well][['top', 'bot']]
        perf_ints[well] = well_df.to_dict(orient='records')
    return perf_ints


def fes_reader(fes_path):
    fes_df = pd.read_excel(fes_path, engine='openpyxl')
    fes_df.rename(columns=lambda x: x.lower().strip(), inplace=True)
    rename_columns(fes_df)
    return fes_df

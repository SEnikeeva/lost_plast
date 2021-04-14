import pandas as pd


def rename_columns(df):
    col_names = {'well': '', 'top': '', 'bot': '',
                 'soil': '', 'date': '', 'type': ''}
    for column in df.columns.values:
        if 'кваж' in column:
            col_names['well'] = column
        elif ('нач' in column) or ('кров' in column):
            col_names['top'] = column
        elif ('кон' in column) or ('подошв' in column):
            col_names['bot'] = column
        elif 'нефтенас' in column:
            col_names['soil'] = column
        elif 'дата_перф' in column:
            col_names['date'] = column
        elif 'цель' in column:
            col_names['type'] = column
    df.rename(columns={col_names['bot']: 'bot', col_names['top']: 'top',
                       col_names['well']: 'well', col_names['soil']: 'soil',
                       col_names['date']: 'date', col_names['type']: 'type'},
              inplace=True)


def perf_reader(perf_path):
    perf_df = pd.read_excel(perf_path, engine='openpyxl')
    perf_df.rename(columns=lambda x: x.lower().strip(), inplace=True)
    rename_columns(perf_df)
    perf_df.sort_values(by=['well', 'date'], inplace=True)
    perf_df = perf_df.round({'top': 1, 'bot': 1})
    perf_df['type'] = perf_df['type'].apply(get_type)
    perf_ints = {}
    for well in perf_df['well'].unique():
        well_df = perf_df[perf_df['well'] == well][['top', 'bot', 'type', 'date']]
        perf_ints[well] = well_df.to_dict(orient='records')
    return perf_ints, perf_df


def fes_reader(fes_path):
    fes_df = pd.read_excel(fes_path, engine='openpyxl')
    fes_df.rename(columns=lambda x: x.lower().strip(), inplace=True)
    rename_columns(fes_df)
    return fes_df


def get_type(type_str):
    if ('отключ' in type_str) \
            or (('ищоляц' in type_str) and ('раб' in type_str)):
        return 0
    elif '' in type_str:
        return -1
    return 1

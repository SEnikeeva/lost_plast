import pandas as pd


def rename_columns(df):
    col_names = {'well': '', 'top': '', 'bot': '',
                 'soil': '', 'date': '', 'type': ''}
    for column in df.columns.values:
        if 'скв' in column:
            col_names['well'] = column
        elif ('нач' in column) or ('кров' in column) or ('верх' in column):
            col_names['top'] = column
        elif ('кон' in column) or ('подош' in column) or ('низ' in column):
            col_names['bot'] = column
        elif ('нефтенас' in column) or ('н_нас' in column):
            col_names['soil'] = column
        elif 'дата_перф' in column:
            col_names['date'] = column
        elif 'цель' in column:
            col_names['type'] = column
    df.rename(columns={col_names['bot']: 'bot', col_names['top']: 'top',
                       col_names['well']: 'well', col_names['soil']: 'soil',
                       col_names['date']: 'date', col_names['type']: 'type'},
              inplace=True)
    col_names_set = set(df.columns)
    df.drop(columns=list(col_names_set.difference(col_names.keys())),
            inplace=True)


def perf_reader(perf_path):
    print('started reading perf xl')
    perf_df = pd.read_excel(perf_path, engine='openpyxl', skiprows=1)
    print('done reading perf xl and started processing perf data')
    perf_df.rename(columns=lambda x: x.lower().strip(), inplace=True)
    rename_columns(perf_df)
    perf_df['date'] = perf_df['date'].dt.date
    perf_df.sort_values(by=['well', 'date'], ascending=False, inplace=True)
    # определение вида перфорации
    perf_df['type'] = perf_df['type'].apply(get_type)
    perf_df.drop(perf_df[perf_df['type'] == -1].index, inplace=True)
    # переименовка скважин (удаление слэша)
    perf_df['well'] = perf_df['well'].apply(well_renaming)

    perf_df.set_index('well', inplace=True)
    perf_ints = perf_df.groupby(level=0, sort=False) \
        .apply(lambda x: [{'type': e[0],
                           'date': e[1],
                           'top': e[2],
                           'bot': e[3]}
                          for e in x.values]) \
        .to_dict()

    return perf_ints


def fes_reader(fes_path):
    print('started reading fes xl')
    fes_df = pd.read_excel(fes_path, engine='openpyxl', skiprows=1)
    print('done reading fes xl')
    fes_df.rename(columns=lambda x: x.lower().strip(), inplace=True)
    rename_columns(fes_df)
    fes_df['well'] = fes_df['well'].apply(well_renaming)
    fes_df.dropna(inplace=True)
    fes_df.set_index('well', inplace=True)

    fes_dict = fes_df.groupby(level=0, sort=False) \
        .apply(lambda x: [{'top': e[0],
                           'bot': e[1],
                           'soil': e[2]}
                          for e in x.values]) \
        .to_dict()
    print('done processing data')
    return fes_dict


def get_type(type_str):
    if (type(type_str) is not str) or 'спец' in type_str.lower():
        return -1
    elif ('отключ' in type_str.lower()) \
            or (
            ('изоляц' in type_str.lower()) and ('раб' in type_str.lower())):
        return 0
    return 1


def well_renaming(w_name):
    if type(w_name) is not str:
        return w_name
    if '/' in w_name:
        return w_name.split('/')[0].lower()
    else:
        return w_name.lower()

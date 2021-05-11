import pandas as pd
import json
from dateutil import parser


class DataReader:
    def __init__(self):
        self.sl_wells = set()
        self.perf_wells = []
        self.rigsw_wells = []
        self.perf_df = None
        self.frs_df = None

    def perf_reader(self, perf_path):
        print('started reading perf xl')
        perf_df = read_df(perf_path)
        print('done reading perf xl and started processing perf data')
        perf_df.rename(columns=lambda x: x.lower().strip(), inplace=True)
        rename_columns(perf_df)
        try:
            perf_df['date'] = perf_df['date'].dt.date
        except:
            perf_df['date'] = perf_df['date'].apply(
                lambda str_date: parser.parse(str_date).date())
        perf_df.sort_values(by=['well', 'date'], ascending=False, inplace=True)
        # определение вида перфорации
        perf_df['type'] = perf_df['type'].apply(get_type)
        perf_df.drop(perf_df[perf_df['type'] == -1].index, inplace=True)
        self.perf_df = perf_df
        # переименовка скважин (удаление слэша)
        perf_df['well'] = perf_df['well'].apply(self.well_renaming)
        self.perf_wells = perf_df['well'].unique()
        perf_df.set_index('well', inplace=True)
        # перестановка столбцов для сохранения установленного порядка
        perf_df = perf_df.reindex(['type', 'date', 'top', 'bot'], axis=1)
        # преобразование датафрейма в словарь
        perf_ints = perf_df.groupby(level=0, sort=False) \
            .apply(lambda x: [{'type': e[0],
                               'date': e[1],
                               'top': e[2],
                               'bot': e[3]}
                              for e in x.values]) \
            .to_dict()

        return perf_ints

    def fes_reader(self, fes_path):
        print('started reading fes xl')
        fes_df = read_df(fes_path)
        print('done reading fes xl')
        fes_df.rename(columns=lambda x: x.lower().strip(), inplace=True)
        rename_columns(fes_df)
        self.frs_df =fes_df
        fes_df['well'] = fes_df['well'].apply(self.well_renaming)
        fes_df.dropna(inplace=True)
        self.rigsw_wells = fes_df['well'].unique()
        fes_df.set_index('well', inplace=True)
        # перестановка столбцов для сохранения установленного порядка
        fes_df = fes_df.reindex(['top', 'bot', 'soil'], axis=1)
        # преобразование датафрейма в словарь
        fes_dict = fes_df.groupby(level=0, sort=False) \
            .apply(lambda x: [{'top': e[0],
                               'bot': e[1],
                               'soil': e[2]}
                              for e in x.values]) \
            .to_dict()
        print('done processing data')
        return fes_dict

    def well_diff(self):
        return list(set(self.perf_wells).difference(self.rigsw_wells)),\
               list(set(self.rigsw_wells).difference(self.perf_wells))

    def well_renaming(self, w_name):
        if type(w_name) is not str:
            return w_name
        if '/' in w_name:
            self.sl_wells.add(w_name)
            return w_name.split('/')[0].lower().strip()
        else:
            return w_name.lower().strip()


def rename_columns(df):
    col_names = {'well': '', 'top': '', 'bot': '',
                 'soil': '', 'date': '', 'type': ''}
    for column in df.columns.values:
        if ('скв' in column) or ('skw_nam' in column) or (column == 'skw'):
            col_names['well'] = column
        elif ('verh' in column) or ('krow' == column) or ('верх' in column):
            col_names['top'] = column
        elif ('niz' in column) or ('podosh' in column) or ('низ' in column):
            col_names['bot'] = column
        elif ('nnas' in column) or ('н_нас' in column):
            col_names['soil'] = column
        elif ('дата_перф' in column) or (column == 'dat'):
            col_names['date'] = column
        elif ('цель' in column) or ('_cel' in column):
            col_names['type'] = column
    df.rename(columns={col_names['bot']: 'bot', col_names['top']: 'top',
                       col_names['well']: 'well', col_names['soil']: 'soil',
                       col_names['date']: 'date', col_names['type']: 'type'},
              inplace=True)
    col_names_set = set(df.columns)
    df.drop(columns=list(col_names_set.difference(col_names.keys())),
            inplace=True)


def get_type(type_str):
    if (type(type_str) is not str) or 'спец' in type_str.lower():
        return -1
    elif ('отключ' in type_str.lower()) \
            or (
            ('изоляц' in type_str.lower()) and ('раб' in type_str.lower())):
        return 0
    return 1


def read_df(df_path):
    if '.xl' in df_path:
        return pd.read_excel(df_path, engine='openpyxl', skiprows=1)
    elif '.json' in df_path:
        with open(df_path, 'r') as f:
            data = json.dumps(json.load(f))
        return pd.read_json(data, orient='records')
    else:
        return None

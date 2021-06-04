import pandas as pd
import json
from dateutil import parser
import numpy as np


def is_contains(w, a):
    for v in a:
        if v in w:
            return True
    return False


def rename_columns(df):
    col_names = {'well': '', 'top': '', 'bot': '',
                 'soil': '', 'date': '', 'type': '', 'type_perf': '',
                 'layer': '', 'well_id': '', 'field': ''}
    for column in df.columns.values:
        if type(column) is not str:
            continue
        if ('скв' in column) or ('skw_nam' in column) or (column == 'skw'):
            col_names['well'] = column
        elif ('verh' in column) or ('krow' == column) or ('верх' in column) or ('кров' in column):
            col_names['top'] = column
        elif ('niz' in column) or ('podosh' in column) or ('низ' in column) or ('подош' in column):
            col_names['bot'] = column
        elif ('nnas' in column) or ('н_нас' in column):
            col_names['soil'] = column
        elif ('дата_перф' in column) or (column == 'dat'):
            col_names['date'] = column
        elif ('цель' in column) or ('_cel' in column):
            col_names['type'] = column
        elif ('tip_perf' in column) or ('тип' in column):
            col_names['type_perf'] = column
        elif ('plast_nam' in column) or ('пласт' in column):
            col_names['layer'] = column
        elif ('skw_id' in column):
            col_names['well_id'] = column
        elif ('obr_nam' in column):
            col_names['field'] = column
    df.rename(columns={col_names['bot']: 'bot', col_names['top']: 'top',
                       col_names['well']: 'well', col_names['soil']: 'soil',
                       col_names['date']: 'date', col_names['type']: 'type',
                       col_names['type_perf']: 'type_perf',
                       col_names['layer']: 'layer',
                       col_names['well_id']: 'well_id', col_names['field']: 'field'},
              inplace=True)
    col_names_set = set(df.columns)
    df.drop(columns=list(col_names_set.difference(col_names.keys())),
            inplace=True)


def read_df(df_path):
    if '.xl' in df_path:
        df = pd.read_excel(df_path, engine='openpyxl', skiprows=0)
        df.rename(
            columns=lambda x: x if type(x) is not str else x.lower().strip(),
            inplace=True)
        if ('скваж' not in df.columns) and ('skw_nam' not in df.columns) \
                and ('skw_nam' not in df.columns) and ('скв' not in df.columns):
            return pd.read_excel(df_path, engine='openpyxl', skiprows=1)
        else:
            return df
    elif '.json' in df_path:
        with open(df_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        return pd.DataFrame(json_data)
    else:
        return None


class DataReader:
    def __init__(self):
        self.sl_wells = set()
        self.perf_wells = []
        self.rigsw_wells = []
        self.rigsw_wells_none = []
        self.rigsw_wells_okay = []
        self.perf_df = None
        self.frs_df = None
        self.unique_perf_wells = []
        self.key_words = {'-1': ['спец', 'наруш', 'циркуляц'],
                          '2': ['ый мост', 'пакером', 'гпш', 'рппк', 'шлипс', 'прк(г)'],
                          'd0': ['d0', 'd_0', 'д0', 'д_0']}

    def perf_reader(self, perf_paths):
        count = 1
        all_perf_df = pd.DataFrame(columns=['type', 'date', 'top', 'bot', 'layer'])
        for perf_path in perf_paths:
            print('started reading perf{} xl'.format(count))
            perf_df = read_df(perf_path)
            print('done reading perf xl and started processing perf data')
            perf_df.rename(columns=lambda x: x if type(x) is not str else x.lower().strip(), inplace=True)
            rename_columns(perf_df)
            perf_df['well'] = perf_df['well'].apply(self.well_renaming)
            if 'well_id' not in perf_df.columns:
                perf_df['well_id'] = ''
            if 'field' not in perf_df.columns:
                perf_df['field'] = ''
            self.perf_wells.extend(list(perf_df['well'].unique()))
            self.get_unique_wells(perf_df)

            try:
                perf_df['date'] = perf_df['date'].dt.date
            except:
                perf_df['date'] = perf_df['date'].apply(
                    lambda str_date: parser.parse(str_date).date())
            perf_df.sort_values(by=['well', 'date'], ascending=True, inplace=True, kind='mergesort')
            perf_df.reset_index(drop=True, inplace=True)
            perf_df = perf_df[::-1]
            if 'layer' not in perf_df.columns:
                perf_df['layer'] = ''
            # определение вида перфорации
            perf_df['type'] = perf_df.apply(
                lambda x: self.get_type(x['type'], x['type_perf'], x['layer']), axis=1)
            perf_df.drop(perf_df[perf_df['type'] == -1].index, inplace=True)
            self.perf_df = perf_df
            # переименовка скважин (удаление слэша)
            all_perf_df = all_perf_df.append(perf_df, ignore_index=True)
            count += 1
        all_perf_df = all_perf_df.drop_duplicates()
        all_perf_df.set_index('well', inplace=True)
        # перестановка столбцов для сохранения установленного порядка
        all_perf_df = all_perf_df.reindex(['type', 'date', 'top', 'bot', 'layer', 'well_id', 'field'], axis=1)
        # преобразование датафрейма в словарь
        perf_ints = all_perf_df.groupby(level=0, sort=False) \
            .apply(lambda x: [{'type': e[0],
                               'date': e[1],
                               'top': e[2],
                               'bot': e[3],
                               'layer': e[4],
                               'well_id': e[5],
                               'field': e[6]}
                              for e in x.values]) \
            .to_dict()

        return perf_ints

    def fes_reader(self, fes_paths):
        all_fes_df = pd.DataFrame(columns=['well', 'top', 'bot', 'soil', 'layer', 'well_id'])
        count = 1
        for fes_path in fes_paths:
            print('started reading fes{} xl'.format(count))
            fes_df = read_df(fes_path)
            print('done reading fes xl')
            fes_df.rename(columns=lambda x: x if type(x) is not str else x.lower().strip(), inplace=True)
            rename_columns(fes_df)
            self.frs_df = fes_df
            if 'layer' not in fes_df.columns:
                fes_df['layer'] = ''
            if 'well_id' not in fes_df.columns:
                fes_df['well_id'] = ''
            fes_df['well'] = fes_df['well'].apply(self.well_renaming)
            self.rigsw_wells.extend(list(fes_df['well'].unique()))
            self.rigsw_wells_none.extend(list(fes_df[fes_df['soil'].isna()]['well'].unique()))
            fes_df.dropna(inplace=True)
            all_fes_df = all_fes_df.append(fes_df, ignore_index=True)
            count += 1
        self.rigsw_wells_okay = all_fes_df['well'].unique()
        all_fes_df = all_fes_df.drop_duplicates()
        all_fes_df.set_index('well', inplace=True)
        # перестановка столбцов для сохранения установленного порядка
        all_fes_df = all_fes_df.reindex(['top', 'bot', 'soil', 'layer', 'well_id'], axis=1)
        # преобразование датафрейма в словарь
        fes_dict = all_fes_df.groupby(level=0, sort=False) \
            .apply(lambda x: [{'top': e[0],
                               'bot': e[1],
                               'soil': e[2],
                               'layer': e[3],
                               'well_id': e[4]}
                              for e in x.values]) \
            .to_dict()
        print('done processing data')
        return fes_dict

    def well_diff(self):
        perf = list(set(self.perf_wells).difference(set(self.rigsw_wells)))
        rigsw = list(set(self.rigsw_wells).difference(set(self.perf_wells)))
        rigsw_none = list(set(self.rigsw_wells_none).difference(set(self.rigsw_wells_okay)))
        max_len = max(len(perf), len(rigsw), len(rigsw_none))
        for i in range(max_len - len(perf)):
            perf.append(None)
        for i in range(max_len - len(rigsw)):
            rigsw.append(None)
        for i in range(max_len - len(rigsw_none)):
                rigsw_none.append(None)
        diff_well_df = pd.DataFrame(columns=['нет в перф', 'нет в ригис', 'отсут. н-нас.'])
        diff_well_df['отсут. н-нас.'] = rigsw_none
        diff_well_df['нет в перф'] = rigsw
        diff_well_df['нет в ригис'] = perf
        return perf, rigsw, diff_well_df

    def well_renaming(self, w_name):
        if type(w_name) is not str:
            return w_name
        if '/' in w_name:
            self.sl_wells.add(w_name)
            return w_name.split('/')[0].lower().strip()
        else:
            return w_name.lower().strip()

    def get_type(self, type_str, type_perf, layer=''):
        """

        :param type_str: цель перфорации
        :param type_perf: тип перфорации
        :param layer: название пласта
        :return: 1 - открытый, 0 - закрытый,
         3 - бурение бокового ствола, 2 - тип закрытого, который перекрывает нижележащие

        """
        if (type(type_str) is not str) or \
                is_contains(type_str.lower(), self.key_words['-1']):
            return -1
        elif ('отключ' in type_str.lower()) or \
                (('изоляц' in type_str.lower()) and (
                        'раб' in type_str.lower())):
            if (type(type_perf) is not str) or\
                    ((type_perf.lower().strip() == 'изоляция пакером') and
                     (layer.lower().strip() in self.key_words['d0'])):
                return 0
            elif is_contains(type_perf, self.key_words['2']):
                return 2
            else:
                return 0
        elif ('бок' in type_str.lower()) and ('ств' in type_str.lower()):
            return 3
        return 1

    def get_unique_wells(self, perf_df):
        for well in perf_df['well'].unique():
            well_df = perf_df[perf_df['well'] == well]
            dupl_by_id = well_df.loc[~well_df.duplicated(subset='well_id')]
            dupl_by_field = well_df.loc[~well_df.duplicated(subset='field')]
            if len(dupl_by_id) > 1:
                dict_ = dupl_by_id.to_dict(orient='records')
                dict_none = []
                for e in dict_:
                    if (type(e['well_id']) is float) and (np.isnan(e['well_id'])) or (e['well_id'] is None):
                        continue
                    else:
                        e['comment'] = 'Неуникальное название скважины относительно id'
                        dict_none.append(e)
                if len(dict_none) > 1:
                    self.unique_perf_wells.extend(dict_none)
            if len(dupl_by_field) > 1:
                dict_ = dupl_by_field.to_dict(orient='records')
                dict_none = []
                for e in dict_:
                    if (type(e['field']) is float) and (np.isnan(e['field'])) or (e['field'] is None):
                        continue
                    else:
                        e['comment'] = 'Неуникальное название скважины относительно месторождения'
                        dict_none.append(e)
                if len(dict_none) > 1:
                    self.unique_perf_wells.extend(dict_none)


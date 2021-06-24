import json

import numpy as np
import pandas as pd


def is_contains(w, a):
    for v in a:
        if v in w:
            return True
    return False


def rename_columns(df):
    col_names = {'well': '', 'top': '', 'bot': '',
                 'soil': '', 'date': '', 'type': '', 'type_perf': '',
                 'layer': '', 'well_id': '', 'field': '', 'trunk': ''}
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
        elif ('ствол' in column):
            col_names['trunk'] = column
    df.rename(columns={col_names['bot']: 'bot', col_names['top']: 'top',
                       col_names['well']: 'well', col_names['soil']: 'soil',
                       col_names['date']: 'date', col_names['type']: 'type',
                       col_names['type_perf']: 'type_perf',
                       col_names['layer']: 'layer',
                       col_names['well_id']: 'well_id', col_names['field']: 'field', col_names['trunk']: 'trunk'},
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


def fes_wells_renaming(well, trunk):
    if (trunk == 0) or ('/' in well):
        return well
    else:
        return well + f'/{int(trunk)}'


def well_renaming(w_name):
    try:
        w_name = str(w_name)
    except:
        return w_name
    return w_name.lower().strip()


class DataReader:
    def __init__(self):
        self.sl_wells = set()
        self.perf_wells = []
        self.rigsw_wells = []
        self.rigsw_wells_none = pd.DataFrame(columns=['well', 'well_id'])
        self.rigsw_wells_okay = pd.DataFrame(columns=['well', 'well_id'])
        self.perf_df = None
        self.fes_df = None
        self.fes_id = False
        self.perf_id = False
        self.unique_perf_wells = []
        self.key_words = {'-1': ['спец', 'наруш', 'циркуляц'],
                          '2': ['ый мост', 'пакером', 'гпш', 'рппк', 'шлипс', 'прк(г)'],
                          'd0': ['d0', 'd_0', 'д0', 'д_0']}

    def perf_reader(self, perf_paths):
        count = 1
        all_perf_df = pd.DataFrame(columns=['type', 'top', 'bot', 'layer'])
        for perf_path in perf_paths:
            print('started reading perf{} xl'.format(count))
            perf_df = read_df(perf_path)
            print('done reading perf xl and started processing perf data')
            perf_df.rename(columns=lambda x: x if type(x) is not str else x.lower().strip(), inplace=True)
            rename_columns(perf_df)
            perf_df['well'] = perf_df['well'].apply(well_renaming)
            if 'trunk' in perf_df.columns:
                perf_df['well'] = perf_df['well'].apply(lambda x: x if ('/' in x) or (type(x) != str) else x + '/1')
            if 'well_id' not in perf_df.columns:
                perf_df['well_id'] = ''
            else:
                self.perf_id = True
            if 'field' not in perf_df.columns:
                perf_df['field'] = ''
            self.perf_wells.extend(list(perf_df['well'].unique()))
            if 'layer' not in perf_df.columns:
                perf_df['layer'] = ''
            # определение вида перфорации
            perf_df['type'] = perf_df.apply(
                lambda x: self.get_type(x['type'], x['type_perf'], x['layer']), axis=1)
            perf_df.drop(perf_df[perf_df['type'] == -1].index, inplace=True)
            # переименовка скважин (удаление слэша)
            all_perf_df = all_perf_df.append(perf_df, ignore_index=True)
            count += 1
        all_perf_df = all_perf_df.drop_duplicates()
        all_perf_df.sort_values(by=['well_id'], inplace=True)
        all_perf_df.reset_index(drop=True, inplace=True)
        self.get_unique_wells(all_perf_df, 'Перфорации')
        all_perf_df['well_id'] = all_perf_df['well_id'].astype(int)
        all_perf_df['well_id'] = all_perf_df['well_id'].astype(str)
        self.perf_df = all_perf_df.copy()
        return all_perf_df

    def df_to_dict(self, perf_df):
        index = 'well_id' if self.fes_id and self.perf_id else 'well'
        perf_df.set_index(index, inplace=True)
        field = 'well' if self.fes_id and self.perf_id else 'well_id'
        perf_df = perf_df.reindex(['type', 'top', 'bot', 'layer', 'field', field], axis=1)
        # преобразование датафрейма в словарь
        perf_ints = perf_df.groupby(level=0, sort=False) \
            .apply(lambda x: [{'type': e[0],
                               'top': e[1],
                               'bot': e[2],
                               'layer': e[3],
                               'field': e[4],
                               'well': e[5]}
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
            if 'layer' not in fes_df.columns:
                fes_df['layer'] = ''
            if 'well_id' not in fes_df.columns:
                fes_df['well_id'] = ''
            else:
                self.fes_id = True
            if 'trunk' not in fes_df.columns:
                fes_df['trunk'] = 0
            fes_df['well'] = fes_df['well'].apply(well_renaming)
            fes_df['trunk'].fillna(0, inplace=True)
            fes_df['well'] = fes_df.apply(lambda x: fes_wells_renaming(x['well'], x['trunk']), axis=1)
            self.rigsw_wells = self.rigsw_wells.append(fes_df[['well', 'well_id']].drop_duplicates())
            self.rigsw_wells_none = self.rigsw_wells_none.append(fes_df[fes_df['soil'].isna()][['well', 'well_id']]
                                                                 .drop_duplicates())
            fes_df.dropna(subset=['soil', 'top', 'bot'], inplace=True)
            all_fes_df = all_fes_df.append(fes_df, ignore_index=True)
            count += 1
        self.rigsw_wells_okay = all_fes_df[['well', 'well_id']]
        all_fes_df = all_fes_df.drop_duplicates()
        all_fes_df.sort_values(by='well_id', inplace=True)
        all_fes_df.reset_index(drop=True, inplace=True)
        self.get_unique_wells(all_fes_df, 'РИГИС')
        all_fes_df['well_id'] = all_fes_df['well_id'].astype(int)
        all_fes_df['well_id'] = all_fes_df['well_id'].astype(str)
        index = 'well_id' if self.fes_id and self.perf_id else 'well'
        field = 'well' if self.fes_id and self.perf_id else 'well_id'
        self.fes_df = all_fes_df.copy()
        all_fes_df.set_index(index, inplace=True)
        # перестановка столбцов для сохранения установленного порядка
        all_fes_df = all_fes_df.reindex(['top', 'bot', 'soil', 'layer', field], axis=1)
        # преобразование датафрейма в словарь
        fes_dict = all_fes_df.groupby(level=0, sort=False) \
            .apply(lambda x: [{'top': e[0],
                               'bot': e[1],
                               'soil': e[2],
                               'layer': e[3],
                               'well': e[4]}
                              for e in x.values]) \
            .to_dict()
        print('done processing data')
        return fes_dict, self.fes_id and self.perf_id

    def well_diff(self):
        type_diff = 'название скважины'
        perf = []
        rigsw = []
        rigsw_none = []
        self.rigsw_wells_none['well'] = self.rigsw_wells_none['well'].astype(str)
        self.rigsw_wells_none['well_id'] = self.rigsw_wells_none['well_id'].astype(str)
        self.rigsw_wells_okay['well'] = self.rigsw_wells_okay['well'].astype(str)
        self.rigsw_wells_okay['well_id'] = self.rigsw_wells_okay['well_id'].astype(str)
        if self.fes_id and self.perf_id:

            rigsw_none_id = list(set(self.rigsw_wells_none['well_id'].tolist())\
                .difference(set(self.rigsw_wells_okay['well_id'].tolist())))
            type_diff = 'id'
            df1 = self.perf_df['well_id']
            df2 = self.fes_df['well_id']
            perf_id = list(set(df1).difference(set(df2).union(rigsw_none_id)))
            rigsw_id = list(set(df2).difference(set(df1)))
            for p_id in perf_id:
                perf.append(self.perf_df[self.perf_df['well_id'] == p_id]['well'].unique()[0])
            for r_id in rigsw_id:
                rigsw.append(self.fes_df[self.fes_df['well_id'] == r_id]['well'].unique()[0])
            for n_id in rigsw_none_id:
                rigsw_none.append(self.rigsw_wells_none[self.rigsw_wells_none['well_id'] == n_id]['well'].unique()[0])
            rigsw = list(set(rigsw))
        else:
            rigsw_none_id = list(set(self.rigsw_wells_none['well'].tolist())
                              .difference(set(self.rigsw_wells_okay['well'].tolist())))
            perf = list(set(self.perf_wells).difference(set(self.rigsw_wells)))
            perf = list(set(perf).difference(set(rigsw_none_id)))
            rigsw = list(set(self.rigsw_wells).difference(set(self.perf_wells)))
            rigsw_none = list(set(rigsw_none_id))
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
        return diff_well_df, type_diff

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

    def get_unique_wells(self, perf_df, source):
        for well in perf_df['well'].unique():
            well_df = perf_df[perf_df['well'] == well]
            dupl_by_id = well_df.loc[~well_df.duplicated(subset='well_id')]
            dupl_by_field = well_df.loc[~well_df.duplicated(subset='field')] if 'field' in perf_df.columns else []
            if len(dupl_by_id) > 1:
                dict_ = dupl_by_id.to_dict(orient='records')
                dict_none = []
                for e in dict_:
                    if (type(e['well_id']) is float) and (np.isnan(e['well_id'])) or (e['well_id'] is None):
                        continue
                    else:
                        e['comment'] = f'{source}. Неуникальное название скважины относительно id'
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
                        e['comment'] = f'{source}. Неуникальное название скважины относительно месторождения'
                        dict_none.append(e)
                if len(dict_none) > 1:
                    self.unique_perf_wells.extend(dict_none)


import json
from tqdm import tqdm

import numpy as np
import pandas as pd
import warnings

warnings.filterwarnings(action='ignore')


def is_contains(w, a):
    for v in a:
        if v in w:
            return True
    return False


def rename_columns(df):
    col_names = {'well': '', 'top': '', 'bot': '',
                 'soil': '', 'date': '', 'type': '', 'type_perf': '',
                 'layer': '', 'well_id': '', 'field': '', 'trunk': '', 'ngdu': '', 'area': ''}
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
        elif ('ствол' in column) or ('stv' == column):
            col_names['trunk'] = column
        elif ('ngdu' in column):
            col_names['ngdu'] = column
        elif ('cex' in column):
            col_names['area'] = column
    df.rename(columns={col_names['bot']: 'bot', col_names['top']: 'top',
                       col_names['well']: 'well', col_names['soil']: 'soil',
                       col_names['date']: 'date', col_names['type']: 'type',
                       col_names['type_perf']: 'type_perf',
                       col_names['layer']: 'layer',
                       col_names['well_id']: 'well_id',
                       col_names['field']: 'field',
                       col_names['trunk']: 'trunk',
                       col_names['ngdu']: 'ngdu',
                       col_names['trunk']: 'trunk',
                       col_names['area']: 'area'},
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
        self.fes_dict = {}
        self.del_ids = set()
        self.sl_wells = set()
        self.perf_wells = []
        self.perf_ids = []
        self.perf_ints = {}
        self.perf_ints_cl = {}
        self.rigsw_wells = []
        self.rigsw_wells_none = pd.DataFrame(columns=['well', 'well_id'])
        self.rigsw_wells_okay = pd.DataFrame(columns=['well', 'well_id'])
        self.perf_df = None
        self.fes_df = None
        self.fes_id = False
        self.perf_id = False
        self.key_words = {'-1': ['спец', 'наруш', 'циркуляц'],
                          '2': ['ый мост', 'пакером', 'гпш', 'рппк', 'шлипс', 'прк(г)'],
                          'd0': ['d0', 'd_0', 'д0', 'д_0']}
        self.warn_wells = set()

    def get_perf_id(self, fes_w_name, w_id, ngdu=None, area=None):
        is_same_well = False
        if fes_w_name in self.perf_ints.keys():
            if ngdu is not None:
                is_same_well = self.perf_ints[fes_w_name][0]['ngdu'] == ngdu
            if area is not None:
                is_same_well = self.perf_ints[fes_w_name][0]['area'] == area
            if is_same_well:
                if w_id != self.perf_ints[fes_w_name][0]['well']:
                    self.warn_wells.add(f'Скважина {fes_w_name} (id - {w_id}) из РИГИ'
                                        f'С сопоставилась с перфорациями по названию, нгду и площади')
                return self.perf_ints[fes_w_name][0]['well']
        else:
            one_trunk = True
            w_name = fes_w_name.split('/')[0]
            perf_ints = self.perf_ints_cl
            if perf_ints.get(w_name) is not None:
                st_v = perf_ints[w_name][0]['trunk']
                if st_v != 1:
                    one_trunk = False
                else:
                    for ints in perf_ints[w_name]:
                        if ints['trunk'] != st_v:
                            one_trunk = False
                if one_trunk:
                    if ngdu is not None:
                        is_same_well = perf_ints[w_name][0]['ngdu'] == ngdu
                    if area is not None:
                        is_same_well = perf_ints[w_name][0]['area'] == area
                    if is_same_well:
                        if w_id != perf_ints[w_name][0]['well']:
                            self.warn_wells.add(f'Скважина {fes_w_name} (id - {w_id}) из РИГИС сопоставилась'
                                                f' с перфорациями по названию, нгду и площади без учета ствола')
                        return perf_ints[w_name][0]['well']

        return w_id

    def perf_reader(self, perf_paths):
        count = 1
        all_perf_df = pd.DataFrame(columns=['type', 'top', 'bot', 'layer'])
        for perf_path in perf_paths:
            print('started reading perf{} xl'.format(count))
            perf_df = read_df(perf_path)
            print('done reading perf xl and started processing perf data')
            perf_df.rename(columns=lambda x: x if type(x) is not str else x.lower().strip(), inplace=True)
            rename_columns(perf_df)
            if 'well_id' not in perf_df.columns:
                perf_df['well_id'] = -1
            else:
                self.perf_id = True
            if 'field' not in perf_df.columns:
                perf_df['field'] = ''
            if 'layer' not in perf_df.columns:
                perf_df['layer'] = ''
            perf_df['well_id'] = perf_df['well_id'].astype(int)
            perf_df['well_id'] = perf_df['well_id'].astype(str)
            perf_df['well'] = perf_df['well'].apply(well_renaming)
            if 'trunk' in perf_df.columns:
                perf_df['well'] = perf_df['well'].apply(lambda x: x if ('/' in x) or (type(x) != str) else x + '/1')
            else:
                perf_df['trunk'] = -1

            self.perf_wells.extend(list(perf_df['well'].unique()))

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
        self.perf_df = all_perf_df.copy()
        self.perf_ids = list(all_perf_df['well_id'].unique())
        self.perf_ints = self.df_to_dict(all_perf_df, 'well', 'well_id')
        self.perf_ints_cl = {k.split('/')[0]: v for k, v in self.perf_ints.items()}
        self.non_unique_wells(self.perf_ints, 'Перфорации')
        return all_perf_df

    def df_to_dict(self, perf_df, index=None, field=None):
        if index is None:
            index = 'well_id' if self.fes_id and self.perf_id else 'well'
            field = 'well' if self.fes_id and self.perf_id else 'well_id'
        perf_df.set_index(index, inplace=True)
        perf_df = perf_df.reindex(['type', 'top', 'bot', 'layer', 'field', 'ngdu', 'area', 'trunk', field], axis=1)
        # преобразование датафрейма в словарь
        perf_ints = perf_df.groupby(level=0, sort=False) \
            .apply(lambda x: [{'type': e[0],
                               'top': e[1],
                               'bot': e[2],
                               'layer': e[3],
                               'field': e[4],
                               'ngdu': e[5],
                               'area': e[6],
                               'trunk': e[7],
                               'well': e[8]}
                              for e in x.values]) \
            .to_dict()
        return perf_ints

    def find_match(self, f_id):
        if f_id in self.perf_ids:
            return 1
        else:
            return 0

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
            if 'ngdu' not in fes_df.columns:
                fes_df['ngdu'] = np.nan
            if 'area' not in fes_df.columns:
                fes_df['area'] = np.nan
            if 'well_id' not in fes_df.columns:
                fes_df['well_id'] = -1
            else:
                self.fes_id = True
            if 'trunk' not in fes_df.columns:
                fes_df['trunk'] = 0
            fes_df['well'] = fes_df['well'].apply(well_renaming)
            fes_df['trunk'].fillna(0, inplace=True)
            fes_df['well'] = fes_df.apply(lambda x: fes_wells_renaming(x['well'], x['trunk']), axis=1)
            fes_df['well_id'] = fes_df['well_id'].astype(int)
            fes_df['well_id'] = fes_df['well_id'].astype(str)
            fes_df['is_match'] = fes_df['well_id'].apply(self.find_match)
            fs = fes_df[['well', 'well_id', 'is_match']]
            fs.drop_duplicates(inplace=True)
            fs.sort_values(by='well', inplace=True)
            fs['well'] = fs['well'].apply(lambda x: x.split('/')[0])
            for w in tqdm(fs['well'].unique()):
                wd = fs[fs['well'] == w]
                w_names = [w]
                if (len(wd) == 3) and (len(fes_df[fes_df['well'] == w]) > 0)\
                        and (fes_df[fes_df['well'] == w]['is_match'].unique()[0] == 1):
                    try:
                        f1 = fes_df[fes_df['well'] == w + '/1']
                        f2 = fes_df[fes_df['well'] == w + '/2']
                        if (len(f1['well_id'].unique()) > 0)\
                                and (w + '/1' not in self.perf_wells)\
                                and (f1['is_match'].unique() == 0):
                            w_names.append(w + '/1')
                            self.del_ids.add(f1['well_id'].unique()[0])
                        if (len(f2['well_id'].unique()) > 0) \
                                and (w + '/2' not in self.perf_wells) \
                                and (f2['is_match'].unique() == 0):
                            w_names.append(w + '/2')
                            self.del_ids.add(f2['well_id'].unique()[0])
                        fes_df.loc[fes_df['well'].isin(w_names), 'well_id'] = \
                            fes_df[fes_df['well'] == w]['well_id'].unique()[0]
                        fes_df.loc[fes_df['well'].isin(w_names), 'is_match'] = 1
                    except:
                        continue
            f_dict = fes_df.to_dict(orient='records')
            for i in range(len(f_dict)):
                if f_dict[i]['is_match'] == 0:
                    f_dict[i]['well_id'] = self.get_perf_id(f_dict[i]['well'], f_dict[i]['well_id'],
                                                            f_dict[i]['ngdu'], f_dict[i]['area'])
                    if f_dict[i]['well_id'] in self.perf_ids:
                        f_dict[i]['is_match'] = 1
            fes_df = pd.DataFrame(f_dict)
            all_wells = fes_df['well'].unique()
            fs = fes_df[['well', 'well_id', 'is_match']]
            fs.drop_duplicates(inplace=True)
            fs.sort_values(by='well', inplace=True)
            fs.reset_index(drop=True, inplace=True)
            for i in tqdm(range(len(fs))):
                w_name = fs.loc[i, 'well']
                if w_name + '/1' in all_wells:
                    f_part = fes_df[fes_df['well'] == w_name + '/1']
                    if (f_part['is_match'].unique()[0] == 1) and (f_part['well_id'].unique()[0] > fs.loc[i, 'well_id']):
                        fes_df.loc[fes_df['well'] == w_name, 'well_id'] = f_part['well_id'].unique()[0]

            self.rigsw_wells.extend(fes_df['well'].unique())
            self.rigsw_wells_none = self.rigsw_wells_none.append(fes_df[fes_df['soil'].isna()][['well', 'well_id']]
                                                                 .drop_duplicates())
            all_fes_df = all_fes_df.append(fes_df, ignore_index=True)
            count += 1

        all_fes_df = all_fes_df.drop_duplicates()
        all_fes_df.sort_values(by='well_id', inplace=True)
        all_fes_df.reset_index(drop=True, inplace=True)
        index = 'well_id' if self.fes_id and self.perf_id else 'well'
        field = 'well' if self.fes_id and self.perf_id else 'well_id'

        self.fes_none_df = all_fes_df.copy()
        all_fes_df.dropna(subset=['soil', 'top', 'bot'], inplace=True)
        all_fes_df.reset_index(drop=True, inplace=True)
        self.rigsw_wells_okay = all_fes_df[['well', 'well_id']]
        self.fes_df = all_fes_df.copy()
        self.fes_dict = self.df_to_dict(all_fes_df.copy(), 'well', 'well_id')
        all_fes_df.set_index(index, inplace=True)
        # перестановка столбцов для сохранения установленного порядка
        all_fes_df = all_fes_df.reindex(['top', 'bot', 'soil', 'layer', 'ngdu', 'area', field], axis=1)
        # преобразование датафрейма в словарь
        fes_dict = all_fes_df.groupby(level=0, sort=False) \
            .apply(lambda x: [{'top': e[0],
                               'bot': e[1],
                               'soil': e[2],
                               'layer': e[3],
                               'ngdu': e[4],
                               'area': e[5],
                               'well': e[6]}
                              for e in x.values]) \
            .to_dict()
        self.non_unique_wells(self.fes_dict, 'РИГИС')
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

            rigsw_none_id = list(set(self.rigsw_wells_none['well_id'].tolist()) \
                                 .difference(set(self.rigsw_wells_okay['well_id'].tolist())).difference(self.del_ids))
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
                                 .difference(set(self.rigsw_wells_okay['well'].tolist())).difference())
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
            if (type(type_perf) is not str) or \
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

    def non_unique_wells(self, data, source):
        for k, v in data.items():
            is_id = False
            is_field = False
            field = v[0]['field']
            w_id = v[0]['well']
            for e in v:
                if (type(e['field']) is float) and (np.isnan(e['field'])) or (e['field'] is None):
                    continue
                else:
                    if e['field'] != field:
                        is_id = True
                        self.warn_wells.add(f'{source}.'
                                            f' Неуникальное название скважины относительно месторождения: {k}')
                if (type(e['well']) is float) and (np.isnan(e['well'])) or (e['well'] is None):
                    continue
                else:
                    if e['well'] != w_id:
                        is_field = True
                        self.warn_wells.add(f'{source}.'
                                            f' Неуникальное название скважины относительно id: {k}')
                if is_field and is_id:
                    break

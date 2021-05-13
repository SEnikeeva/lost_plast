import json
import logging
import os
import sys
from datetime import date
import pandas as pd

from data_reader import DataReader
from finder import find_layers
from actual_perf import get_actual_perf
from writexl import write_layers, write_act_perf


# конвертация путей файлов в зависимости от системы
def replace_slash(file_path):
    platform = sys.platform
    slash_map = {'win32': '\\',
                 'cygwin': '\\',
                 'darwin': '/',
                 'linux2': '/'}
    if platform not in slash_map.keys(): platform = 'linux2'
    return file_path.replace('\\', slash_map[platform])


# очистка папки output_folder
def clear_out_folder(output_folder):
    files = os.listdir(output_folder)
    for f in files:
        path_dir = replace_slash(output_folder + "\\" + f)
        os.remove(path_dir)


def get_year(conf_perf_year):
    if conf_perf_year == '':
        return None
    try:
        return date(year=int(conf_perf_year), month=1, day=1)
    except:
        return None


def df_to_dict(df):
    df.set_index('well', inplace=True)
    # перестановка столбцов для сохранения установленного порядка
    perf_df = df.reindex(['top', 'bot', 'layer'], axis=1)
    # преобразование датафрейма в словарь
    act_perf_ints = perf_df.groupby(level=0, sort=False) \
        .apply(lambda x: [{'top': e[0],
                           'bot': e[1],
                           'layer': e[2]}
                          for e in x.values]) \
        .to_dict()

    return act_perf_ints


if __name__ == '__main__':

    input_folder = "input_data"
    out_folder = "output_data"
    output_path_l = replace_slash(out_folder + "\\" + "non_perf_layers.xlsx")
    output_path_a = replace_slash(out_folder + "\\" + "act_perf.xlsx")

    if not os.path.exists(out_folder):
        os.makedirs(out_folder)
    else:
        clear_out_folder(out_folder)

    out_path_log = replace_slash(out_folder + "\\" + "Report.txt")
    logging.basicConfig(format=u'%(levelname)-8s : %(message)s', filename=out_path_log, filemode='w')

    with open("config.json", 'r') as f:
        try:
            conf = json.load(f)
            SOIL_CUT = float(conf["SOIL_CUT"])
            perf_path = conf["perf_path"]
            fes_path = conf["fes_path"]
            act_perf_year = get_year(conf["act_perf_year"])
        except BaseException as e:
            logging.error("Error loading config file. " + str(e))
            sys.exit()
    perf_path = replace_slash(input_folder + '\\' + perf_path)
    fes_path = replace_slash(input_folder + '\\' + fes_path)

    dr = DataReader()
    try:
        perf_ints = dr.perf_reader(perf_path)
    except BaseException as e:
        logging.error("Error loading perf file. " + str(e))
        sys.exit()
    try:
        fes_dict = dr.fes_reader(fes_path)
    except BaseException as e:
        logging.error("Error loading fes file. " + str(e))
        sys.exit()

    perf_rig_diff, rig_perf_diff, diff_well_df = dr.well_diff()
    if len(perf_rig_diff) > 0:
        logging.warning("These wells in perf file are absent in rigis "
                        + str(perf_rig_diff))
    if len(rig_perf_diff) > 0:
        logging.warning("These wells in rigis file are absent in perf "
                        + str(rig_perf_diff))
    diff_well_df.to_excel(replace_slash(out_folder + '\\' + 'wells_diff.xlsx'), index=False)

    try:
        act_perf = get_actual_perf(perf_ints, act_perf_year)
    except BaseException as e:
        logging.error("Error while getting the actual perforation " + str(e))
        sys.exit()

    act_perf_ints = df_to_dict(pd.read_json(json.dumps(act_perf)))

    try:
        lost_layers = find_layers(perf_ints, fes_dict, SOIL_CUT)
    except BaseException as e:
        logging.error("Error while finding layers " + str(e))
        sys.exit()
    # сохранение данных
    try:
        write_layers(output_path_l, lost_layers)
        write_act_perf(output_path=output_path_a, act_perf=act_perf)
    except BaseException as e:
        logging.error("Error while writing results " + str(e))
        sys.exit()

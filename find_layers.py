import json
import logging
import os
import sys
from datetime import date

from data_reader import fes_reader, perf_reader
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


# очистка папке output_folder
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
            STEP = float(conf["step"])
            perf_path = conf["perf_path"]
            fes_path = conf["fes_path"]
            act_perf_year = get_year(conf["act_perf_year"])
        except BaseException as e:
            logging.error("Error loading config file. " + str(e))
            sys.exit()
    perf_path = replace_slash(input_folder + '\\' + perf_path)
    fes_path = replace_slash(input_folder + '\\' + fes_path)
    try:
        perf_df = perf_reader(perf_path)
    except BaseException as e:
        logging.error("Error loading perf file. " + str(e))
        sys.exit()
    try:
        fes_df = fes_reader(fes_path)
    except BaseException as e:
        logging.error("Error loading fes file. " + str(e))
        sys.exit()

    try:
        lost_layers = find_layers(perf_df, fes_df, SOIL_CUT)
    except BaseException as e:
        logging.error("Error while finding layers " + str(e))
        sys.exit()

    try:
        act_perf = get_actual_perf(perf_df, act_perf_year)
    except BaseException as e:
        logging.error("Error while getting the actual perforation " + str(e))
        sys.exit()

    # сохранение данных
    try:
        write_layers(output_path_l, lost_layers)
        write_act_perf(output_path=output_path_a, act_perf=act_perf)
    except BaseException as e:
        logging.error("Error while writing results " + str(e))
        sys.exit()

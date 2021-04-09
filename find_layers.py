import json
import logging
import os

import sys
from data_reader import fes_reader, perf_reader
from finder import find_layers
from writexl import write


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


if __name__ == '__main__':

    input_folder = "input_data"
    out_folder = "output_data"
    output_path = replace_slash(out_folder + "\\" + "non_perf_layers.xlsx")
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
        except BaseException as e:
            logging.error("Error loading config file. " + str(e))
            sys.exit()
    perf_path = replace_slash(input_folder + '\\Перфорация.xlsx')
    fes_path = replace_slash(input_folder + '\\ФЕС.xlsx')
    try:
        perf_ints = perf_reader(perf_path)
    except BaseException as e:
        logging.error("Error loading perf file. " + str(e))
        sys.exit()
    try:
        fes_df = fes_reader(fes_path)
    except BaseException as e:
        logging.error("Error loading fes file. " + str(e))
        sys.exit()

    try:
        lost_layers = find_layers(perf_ints, fes_df, SOIL_CUT)
    except BaseException as e:
        logging.error("Error while finding layers " + str(e))
        sys.exit()

    # сохранение данных
    try:
        write(output_path, lost_layers)
    except BaseException as e:
        logging.error("Error while writing results " + str(e))
        sys.exit()


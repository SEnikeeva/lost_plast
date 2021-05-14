import json
import logging
import os
import sys

from data_reader import DataReader
from finder import find_layers
from writexl import write_layers


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


if __name__ == '__main__':

    input_folder = "input_data"
    out_folder = "output_data"
    output_path_l = replace_slash(out_folder + "\\" + "non_perf_layers.xlsx")

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
        lost_layers = find_layers(perf_ints, fes_dict, SOIL_CUT)
    except BaseException as e:
        logging.error("Error while finding layers " + str(e))
        sys.exit()
    # сохранение данных
    try:
        write_layers(output_path_l, lost_layers)
    except BaseException as e:
        logging.error("Error while writing results " + str(e))
        sys.exit()

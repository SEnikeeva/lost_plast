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

    with open("config.json", 'r', encoding='utf-8') as f:
        try:
            conf = json.load(f)
            SOIL_CUT = float(conf["SOIL_CUT"])
            perf_paths = conf["perf_path"]
            fes_paths = conf["fes_path"]
        except BaseException as e:
            logging.error("Error loading config file. " + str(e))
            sys.exit()
    perf_paths = [replace_slash(input_folder + '\\' + perf_path) for perf_path in perf_paths]
    fes_paths = [replace_slash(input_folder + '\\' + fes_path) for fes_path in fes_paths]

    dr = DataReader()
    try:
        perf_df = dr.perf_reader(perf_paths)
    except BaseException as e:
        logging.error("Ошибка при чтении файла с перфорациями. " + str(e))
        sys.exit()

    try:
        fes_dict, is_id = dr.fes_reader(fes_paths)
        perf_ints = dr.perf_ints
    except BaseException as e:
        logging.error("Ошибка при чтении файла с РИГИС. " + str(e))
        sys.exit()
    for well in dr.warn_wells:
        logging.warning(well)

    diff_well_df, type_diff = dr.well_diff()
    logging.warning(f"Скважины сопоставлялись по следующему полю: {type_diff}")
    diff_well_df.to_excel(replace_slash(out_folder + '\\' + 'wells_diff.xlsx'), index=False)

    try:
        lost_layers = find_layers(perf_ints, fes_dict, SOIL_CUT, is_id)
    except BaseException as e:
        logging.error("Ошибка при поиске пропущенных пластов " + str(e))
        sys.exit()
    # сохранение данных
    try:
        write_layers(output_path_l, lost_layers)
    except BaseException as e:
        logging.error("Ошибка при записи результатов " + str(e))
        sys.exit()

    bs_s = ''
    for w in dr.bs:
        bs_s += w[0] + ' ' + w[1] + ' ' + str(w[2]) + '\n'
    with open('bs.txt', 'w') as f:
        f.write(bs_s)
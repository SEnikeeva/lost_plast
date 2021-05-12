import pandas as pd
import json


def write_layers(output_path, lost_layers):
    pd.read_json(json.dumps(lost_layers)).to_excel(output_path,
                                                   header=['Скважина',
                                                           'Кровля',
                                                           'Подошва',
                                                           'Нефтенасыщенность'],
                                                   index=False)


def write_act_perf(output_path, act_perf):
    pd.read_json(json.dumps(act_perf)).to_excel(output_path,
                                                header=['Скважина', 'Кровля',
                                                        'Подошва',
                                                        'Состояние',
                                                        'Пласт'],
                                                index=False)

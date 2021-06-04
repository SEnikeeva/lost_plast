import pandas as pd
import json


def write_layers(output_path, lost_layers):
    pd.read_json(json.dumps(lost_layers)).to_excel(output_path,
                                                   header=['Скважина',
                                                           'Кровля',
                                                           'Подошва',
                                                           'Нефтенасыщенность по РИГИС',
                                                           'Пласт по РИГИС', 'id скважины по РИГИС'],
                                                   index=False)

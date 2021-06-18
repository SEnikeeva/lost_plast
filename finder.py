from tqdm import tqdm


# проверка пласта на перфорированность
def is_perf(top, bot, ints):
    if ints is None:
        return False
    for int_perf in ints:
        if (top <= int_perf['bot']) and (int_perf['top'] <= bot):
            return True
    return False


def find_layers(perf_ints, fes_dict, soil_cut, is_id):
    lost_layers = []
    for well in tqdm(fes_dict.keys()):
        ints = perf_ints.get(well)
        for row in fes_dict[well]:
            top = row['top']
            bot = row['bot']
            soil = row['soil']
            layer = row['layer']
            if soil < soil_cut:
                continue
            if not is_perf(top, bot, ints):
                if is_id:
                    lost_layers.append({'ID по РИГИС': well, 'Кровля': top,
                                        'Подошва': bot, 'Нефтенасыщенность': soil, 'Пласт': layer,
                                        'Номер скважины по РИГИС': row['well'].split('/1')[0]})
                else:
                    lost_layers.append({'Номер скважины по РИГИС': well.split('/1')[0], 'Кровля': top,
                                        'Подошва': bot, 'Нефтенасыщенность': soil, 'Пласт': layer,
                                        'ID по РИГИС': row['well']})

    return lost_layers

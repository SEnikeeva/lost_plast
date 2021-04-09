import pandas as pd

from tqdm import tqdm


def is_perf(top, bot, ints):
    if ints is None:
        return False
    for int_perf in ints:
        if (top <= int_perf['top'] <= bot) or (top <= int_perf['bot'] <= bot):
            return True
        elif (top >= int_perf['top']) and (int_perf['bot'] >= bot):
            return True
    return False


def find_layers(perf_ints, fes_df, soil_cut):
    lost_layers = pd.DataFrame(columns=['well', 'top', 'bot', 'soil', 'is_perf'])
    for well in tqdm(fes_df['well'].unique()):
        well_df = fes_df[fes_df['well'] == well][['well', 'top', 'bot', 'soil']]
        well_df.dropna(inplace=True)
        ints = perf_ints.get(well)
        well_df['is_perf'] = well_df.apply(lambda x: is_perf(x['top'], x['bot'], ints), axis=1)
        non_perf = well_df[(well_df['is_perf'] == False) & (well_df['soil'] > soil_cut)]
        lost_layers = lost_layers.append(non_perf, ignore_index=True)
    return lost_layers


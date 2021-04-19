import pandas as pd

from tqdm import tqdm


# проверка пласта на перфорированность
def is_perf(top, bot, ints):
    if ints is None:
        return False
    for row in ints.itertuples():
        if (top < row.bot) and (row.top < bot):
            return True
    return False


def find_layers(perf_df, fes_df, soil_cut):
    lost_layers = pd.DataFrame(columns=['well', 'top', 'bot', 'soil', 'is_perf'])
    for well in tqdm(fes_df['well'].unique()):
        well_df = fes_df[fes_df['well'] == well][['well', 'top', 'bot', 'soil']]
        well_df.dropna(inplace=True)
        if len(well_df) == 0:
            continue
        ints = perf_df[perf_df['well'] == well][['top', 'bot']]
        well_df['is_perf'] = well_df\
            .apply(lambda x: is_perf(x['top'], x['bot'], ints), axis=1)
        non_perf = well_df[(~well_df['is_perf']) &
                           (well_df['soil'] > soil_cut)]
        lost_layers = lost_layers.append(non_perf, ignore_index=True)
    return lost_layers



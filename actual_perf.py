import numpy as np
from tqdm import tqdm

import bisect


def get_actual_perf_old(perf_df, step):
    act_perf = {}
    for well in tqdm(perf_df['well'].unique()):
        well_df = perf_df[perf_df['well'] == well]
        size = int((well_df['bot'].max() - well_df['top'].min()) / step + 1)
        min_dept = well_df['top'].min() * 10
        line = np.full(size, np.nan)
        for date in well_df['date'].unique():
            date_df = well_df[well_df['date'] == date]
            for top, bot, perf_type in date_df[['top', 'bot', 'type']].values:
                size = int((bot - top)) * 10
                try:
                    np.put(line, list(range(int(top * 10 - min_dept), int(bot * 10 - min_dept), 1)),
                           np.full(size, perf_type))
                except BaseException as e:
                    print(e)
        act_perf[well] = {'line': line, 'min_dept': min_dept}

    return act_perf


def collect_ints(act_perf):
    for well in act_perf.keys():
        pass


def get_actual_perf(perf_df, act_perf_year):
    act_perf = []
    if act_perf_year is not None:
        perf_df.drop(perf_df[perf_df['date'] > act_perf_year].index,
                     inplace=True)
    for well in tqdm(perf_df['well'].unique()):
        well_df = perf_df[perf_df['well'] == well]
        act_perf_well = []
        for date in well_df['date'].unique():
            date_df = well_df[well_df['date'] == date]
            for top, bot, perf_type in date_df[['top', 'bot', 'type']].values:
                idx = bisect.bisect_left([t['bot'] for t in act_perf_well], top)
                if idx == len(act_perf_well):
                    act_perf_well.append({'well': well, 'top': top, 'bot': bot, 'perf_type': perf_type})
                else:
                    shift = 1
                    if act_perf_well[idx]['top'] > top:
                        if act_perf_well[idx]['perf_type'] == perf_type:
                            act_perf_well[idx]['top'] = top
                        else:
                            act_perf_well.insert(idx, {'well': well, 'top': top,
                                                       'bot': bot if act_perf_well[idx]['top'] > bot else
                                                       act_perf_well[idx]['top'],
                                                       'perf_type': perf_type})
                            shift += 1
                    if act_perf_well[idx]['bot'] < bot:
                        if act_perf_well[idx]['perf_type'] == perf_type:
                            act_perf_well[idx]['bot'] = bot
                        else:
                            act_perf_well.insert(idx + shift, {'well': well, 'top': act_perf_well[idx]['bot'], 'bot': bot,
                                                           'perf_type': perf_type})

        act_perf.extend(act_perf_well)
    return act_perf

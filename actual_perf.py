from tqdm import tqdm
import datetime


# бинарный поиск индекса для вставки элемента в отсортированный массив
def bisect_left(a, x, lo=0, hi=None, param='bot'):
    if lo < 0:
        raise ValueError('lo must be non-negative')
    if hi is None:
        hi = len(a)
    while lo < hi:
        mid = (lo + hi) // 2
        if a[mid][param] < x:
            lo = mid + 1
        else:
            hi = mid
    return lo


def get_actual_perf(perf_ints, act_perf_year=None):
    act_perf = []
    if act_perf_year is None:
        act_perf_year = datetime.datetime.now().date()
    for well in tqdm(perf_ints.keys()):
        act_perf_well = []
        for row in perf_ints[well]:
            if row['date'] > act_perf_year:
                continue
            top = row['top']
            bot = row['bot']
            perf_type = row['type']

            idx = bisect_left(act_perf_well, top)
            if idx == len(act_perf_well):
                act_perf_well.append({'well': well, 'top': top, 'bot': bot,
                                      'perf_type': perf_type})
            else:
                shift = 1
                if act_perf_well[idx]['top'] > top:
                    if act_perf_well[idx]['perf_type'] == perf_type:
                        act_perf_well[idx]['top'] = top
                    else:
                        act_perf_well.insert(idx,
                                             {'well': well, 'top': top,
                                              'bot': bot if
                                              act_perf_well[idx][
                                                  'top'] > bot else
                                              act_perf_well[idx]['top'],
                                              'perf_type': perf_type})
                        shift += 1
                if act_perf_well[idx]['bot'] < bot:
                    if act_perf_well[idx]['perf_type'] == perf_type:
                        act_perf_well[idx]['bot'] = bot
                    else:
                        act_perf_well.insert(idx + shift,
                                             {'well': well,
                                              'top': act_perf_well[idx]['bot'],
                                              'bot': bot,
                                              'perf_type': perf_type})

        act_perf.extend(act_perf_well)
    return act_perf

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

            idx_t = bisect_left(act_perf_well, top)
            if idx_t == len(act_perf_well):
                act_perf_well.append({'well': well, 'top': top, 'bot': bot,
                                      'perf_type': perf_type})
            else:
                shift = 0
                if act_perf_well[idx_t]['top'] > top:
                    if act_perf_well[idx_t]['perf_type'] == perf_type:
                        if act_perf_well[idx_t]['top'] < bot:
                            act_perf_well[idx_t]['top'] = top
                        else:
                            act_perf_well.insert(idx_t,
                                                 {'well': well, 'top': top,
                                                  'bot': bot,
                                                  'perf_type': perf_type})
                            shift += 1
                    else:
                        act_perf_well.insert(idx_t,
                                             {'well': well, 'top': top,
                                              'bot': bot if
                                              act_perf_well[idx_t]['top'] > bot else
                                              act_perf_well[idx_t]['top'],
                                              'perf_type': perf_type})
                        shift += 1
                idx_b = bisect_left(act_perf_well, bot, param='top')
                idx_t += shift
                to_append = []
                shift = 1
                if act_perf_well[idx_t]['bot'] < bot:
                    end = idx_b if idx_b < len(act_perf_well) else len(act_perf_well)
                    for i in range(idx_t, end):
                        if act_perf_well[i]['perf_type'] == perf_type:
                            if i == end - 1:
                                if bot > act_perf_well[i]['bot']:
                                    act_perf_well[i]['bot'] = bot
                            else:
                                act_perf_well[i]['bot'] = act_perf_well[i + 1]['top']
                        else:
                            if i == end - 1:
                                if bot > act_perf_well[i]['bot']:
                                    to_append.append(dict(well=well, top=act_perf_well[i]['bot'],
                                                          bot=bot, perf_type=perf_type, idx=i + shift))
                                    shift += 1

                            else:
                                to_append.append(dict(well=well, top=act_perf_well[i]['bot'],
                                                      bot=act_perf_well[i + 1]['top'],
                                                      perf_type=perf_type, idx=i + shift))
                                shift += 1
                    for el in to_append:
                        act_perf_well.insert(el['idx'],
                                             {'well': el['well'],
                                              'top':  el['top'],
                                              'bot':  el['bot'],
                                              'perf_type': el['perf_type']})
                    if (idx_b == len(act_perf_well)) and (act_perf_well[-1]['bot'] < bot):
                        if act_perf_well[-1]['perf_type'] == perf_type:
                            act_perf_well[-1]['bot'] = bot
                        else:
                            act_perf_well.append({'well': well,
                                                  'top': act_perf_well[-1]['bot'],
                                                  'bot': bot,
                                                  'perf_type': perf_type})
        act_perf.extend(act_perf_well)
    return act_perf

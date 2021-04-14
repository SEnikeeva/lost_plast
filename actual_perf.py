class Slice:
    def __init__(self, dept, state):
        self.dept = dept
        self.state=state


def get_actual_perf(perf_df, step):
    act_perf = {}
    for well in perf_df['well'].unique():
        well_df = perf_df[perf_df['well'] == well]
        act_perf[well] = []
        for date in well_df['date'].unique():
            pass


    pass

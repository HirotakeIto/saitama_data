import pandas as pd

def values_not_in_list_to_nan(series: pd.Series, nan_values):
    series.loc[~series.isin(nan_values)] = pd.np.NaN
    return series

class SchoolNameMixin:
    @staticmethod
    def assign_sch_name_to_sch_id(dfx: pd.DataFrame, sch_name_col='school_name', sch_id_col ='school_id'):
        school_master = (
            pd.read_csv('data/db/info/school_master.csv')
            .set_index('school_name')
            ['school_id'].to_dict()
        )
        dfx[sch_id_col] = (
            dfx[sch_name_col]
            .replace({'戸田市立': '', '学校': ''}, regex=True)
            .replace(school_master)
            .pipe(values_not_in_list_to_nan, nan_values=list(school_master.values()))
        )
        return dfx

    @staticmethod
    def assign_sch_num_to_sch_id(dfx: pd.DataFrame, sch_name_col='school_num', sch_id_col ='school_id'):
        school_master = (
            pd.read_csv('data/db/info/school_master.csv')
            .set_index('school_num')
            ['school_id'].to_dict()
        )
        dfx[sch_id_col] = (
            dfx[sch_name_col]
            .replace(school_master)
            .pipe(values_not_in_list_to_nan, nan_values=list(school_master.values()))
        )
        return dfx


import pandas as pd
import numpy as np
from importlib import reload
from saitama_data.lib.read_config import ReadConfigTodashi20162017
from saitama_data.datasetup.makedata.lib.validation import DataFrameGetter
from saitama_data.datasetup.models.todashi.toda_old_tch_noncog import model
from saitama_data.datasetup.models.todashi.engineer_mixin import school_name_mixin
[reload(a) for a in [model, school_name_mixin]]
TodaOldTchNoncog = model.TodaOldTchNoncog
SchoolNameMixin = school_name_mixin.SchoolNameMixin


class TodashiQesNoncog(DataFrameGetter):
    need_cols = ['teacher_id', 'school_name', 'school_num', 'year', 't_grade', 't_class']
    convert_list = {'teacher_id': float, 'school_num': float}


class TodashiQesNoncog2016(TodashiQesNoncog):
    path_seito_id = '/todashi_data/hininchi_todashi.csv'

    def __init__(self, c):
        self.path = c.downpath + self.path_seito_id

    def build(self):
        data = self.read()
        data = self.engineer(data)
        self.valid_columnn_check(data, self.need_cols)
        self.convert_columns_type(data, self.convert_list)
        self.data = data
        return self

    def read(self):
        path = self.path
        data = pd.read_csv(path, encoding='sjis')
        return data

    def engineer(self, data):
        if type(data) != pd.DataFrame:
            raise TypeError
        # rename columns
        columns_mapper = {'タイムスタンプ': 'timestamp',
                          'コード': 'teacher_id',
                          '免許の種類': 'licence',
                          '最終学歴の学校名': 'graduation',
                          '年齢': 'age',
                          '担任の学年とクラス': 't_grade_class',
                          '役職': 'position',
                          '学校名': 'school_name',
                          '学校ID': 'school_num',
                          '性別': 'sex'
                          }
        data = data.rename(columns=columns_mapper)
        # extract grade
        data['tmp'] = data['t_grade_class'].str.split('年')
        data['t_grade'] = data['tmp'].apply(lambda x: float(x[0]))
        slicing = data['t_grade'] <= 3
        data.loc[slicing, 't_grade'] += 6  # 中１を7にする
        #  extract class
        data['tmp'] = data['t_grade_class'].str.find('組')

        def get_class(x):
            t = x['tmp']
            if t == -1:
                return np.nan
            else:
                s = x['t_grade_class'][t - 1:t]
                try:
                    return float(s)
                except:
                    return np.nan

        data['t_class'] = data.apply(get_class, axis=1)
        data = data.drop('tmp', axis=1)
        # give year
        data['year'] = 2015  # 確か2015年度の担当だから
        # add to convert list
        self.add_convert_list(data)
        return data

    def add_convert_list(self, data):
        astype = {c: float for c in data.columns if c.count('t_k')}
        astype = {c: float for c in astype if c not in ['t_k1', 't_k2', 't_k3', 't_k4', 't_k5']}
        self.convert_list.update(astype)


def seed():
    c = ReadConfigTodashi20162017(path='saitama_data/setting.ini')
    c.get_setting()
    data = (
        TodashiQesNoncog2016(c).build()
        .data
        .pipe(SchoolNameMixin.assign_sch_num_to_sch_id)
    )
    totn = TodaOldTchNoncog(data)
    totn.validate()
    totn.save()
    return data

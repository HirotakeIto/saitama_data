import pandas as pd
from importlib import reload
from src.lib.read_config import ReadConfigTodashi20162017
from src.datasetup.makedata.lib.validation import DataFrameGetter
from src.datasetup.models.todashi.toda_old_tch_qes import model
from src.datasetup.models.todashi.engineer_mixin import school_name_mixin
[reload(a) for a in [model, school_name_mixin]]
TodaOldTchQes = model.TodaOldTchQes
SchoolNameMixin = school_name_mixin.SchoolNameMixin


class TodashiKyouinQes(DataFrameGetter):
    need_cols = ['school_num', 'teacher_id']
    # memo:t_grade, t_classは必ずしも数値じゃないが、そいつはfetchモジュールに入れとく
    convert_list = {'teacher_id': float, 'school_num': float, 't_grade': float}


class TodashiKyouinQes2016(TodashiKyouinQes):
    path_seito_id = '/todashi_data/kyouin_qes_todashi.csv'

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
        data = pd.read_csv(path, encoding='shift-jis')
        return data

    def engineer(self, data):
        if type(data) != pd.DataFrame:
            raise TypeError
        columns_mapper = {'学校ID': 'school_num',
                          '学校名': 'school_name',
                          '個人ID': 'teacher_id',
                          '付箋': 'husen',
                          '担任学年': 't_grade',  # 確か2015年度の担当
                          '担任学級': 't_class',  # 確か2015年度の担当
                          '担当教科': 'subject'
                          }
        data = data.rename(columns=columns_mapper)
        data['year'] = 2015  # 確か2015年度の担当だから
        self.add_convert_list(data)
        return data

    def add_convert_list(self, data):
        astype = {c: float for c in data.columns if c.count('t_q')}
        self.convert_list.update(astype)


def main():
    c = ReadConfigTodashi20162017(path='src/setting.ini')
    c.get_setting()
    data = (
        TodashiKyouinQes2016(c).build()
        .data
        .pipe(SchoolNameMixin.assign_sch_num_to_sch_id)
    )
    totq = TodaOldTchQes(data)
    totq.validate()
    totq.save()

import pandas as pd
import re
import numpy as np
import os
from src.lib.read_config import ReadConfigTodashi20162017
from src.datasetup.makedata.lib.validation import DataFrameGetter
from src.datasetup.models.todashi.toda_teacher_qes.model import TodaTeacherQes
from src.datasetup.models.todashi.engineer_mixin.school_name_mixin import SchoolNameMixin

class QuestionList:
    # path_question_list = '/todashi_data/2017/todashi_qeslist.xlsx'
    path_question_list = 'todashi_data/2018/todashi_qeslist.xlsx'

    def __init__(self, c):
        self.path = os.path.join(c.downpath, self.path_question_list)

    def build(self):
        data = self.read()
        qlist = self.engineer(data)
        self.qlist = qlist
        return self

    def read(self):
        path = self.path
        data = pd.read_excel(path, sheetname='2017')
        return data

    def engineer(self, data: pd.DataFrame):
        qlist = {}
        for school in ['小学校', '中学校']:
            dt = data[['colnames', school]].copy()
            dt = dt.dropna()
            _qlist = {x[school]: x['colnames'] for x in dt.to_dict(orient='record')}
            qlist.update({school: _qlist})
        return qlist


class QuestionList2018:
    path_question_list = 'todashi_data/2018/todashi_qeslist.xlsx'

    def __init__(self, c):
        self.path = os.path.join(c.downpath, self.path_question_list)

    def build(self):
        data = self.read()
        qlist = self.engineer(data)
        self.qlist = qlist
        return self

    def read(self):
        path = self.path
        data = pd.read_excel(path, sheetname='2018')
        return data

    def engineer(self, data: pd.DataFrame):
        qlist = {}
        for school in ['小学校', '中学校']:
            dt = data[['colnames', school]].copy()
            dt = dt.dropna()
            _qlist = {x[school]: x['colnames'] for x in dt.to_dict(orient='record')}
            qlist.update({school: _qlist})
        return qlist


class TodashiKyouinQes():
    need_cols = ['school_name', 'teacher_id']
    # memo:t_grade, t_classは必ずしも数値じゃないが、そいつはfetchモジュールに入れとく
    convert_list = {'teacher_id': float}


class TodashiKyouinQes2017(TodashiKyouinQes):
    path_seito_id = '/todashi_data/2017/教員質問紙【rawdata・教員特定済】.xlsx'

    def __init__(self, c):
        self.path = c.downpath + self.path_seito_id
        self.year = 2016

    def build(self, qlist):
        sheets = self.read()
        data = self.engieer(sheets, qlist)
        self.valid_columnn_check(data, self.need_cols)
        self.convert_columns_type(data, self.convert_list)
        self.data = data
        return self

    def read(self):
        path = self.path
        sheets = pd.read_excel(path, sheetname=None)
        return sheets

    def engieer(self, sheets, qlist):
        result = pd.DataFrame()
        for sheet in ['小学校', '中学校']:
            data = sheets[sheet]
            data = data.rename(columns=qlist[sheet])

            def get_grade_from_string(x: str):
                if type(x) != str:
                    return np.nan
                if len(re.findall('^(第\d学年)*', x)) != 0:
                    return float(x[1])
                else:
                    return np.nan

            def get_class_from_string(x: str):
                if type(x) != str:
                    return np.nan
                if x.count('組') > 0:
                    return float(x[0])
                else:
                    return np.nan

            data['t_grade_original'] = data['t_grade']
            data['t_class_original'] = data['t_class']
            data['t_grade'] = data['t_grade'].apply(lambda x: get_grade_from_string(x))
            data['t_class'] = data['t_class'].apply(lambda x: get_class_from_string(x))
            if sheet == '中学校':
                data['t_grade'] += 6
            data['year'] = self.year  # 確か2015年度の担当だから
            result = pd.concat([result, data], axis=0)
        return result


class TodashiKyouinQes2018(TodashiKyouinQes2017):
    def __init__(self):
        self.path_p = 'data/original_data/todasi/todashi_data/2018/【小】教員質問紙結果（送付用）.xlsx'
        self.path_j = 'data/original_data/todasi/todashi_data/2018/【中】教員質問紙結果（送付用）.xlsx'
        self.year = 2017

    def read(self):
        sheets = {}
        sheets.update({'小学校': pd.read_excel(self.path_p)})
        sheets.update({'中学校': pd.read_excel(self.path_j)})
        return sheets

    def engieer(self, sheets, qlist):
        result = pd.DataFrame()
        for sheet in ['小学校', '中学校']:
            data = sheets[sheet]
            data = data.rename(columns=qlist[sheet])

            def get_grade_from_string(x: str):
                if type(x) != str:
                    return np.nan
                if len(re.findall('^(第\d学年)+', x)) != 0:
                    return float(x[1])
                else:
                    return np.nan

            def get_class_from_string(x: str):
                if type(x) != str:
                    return np.nan
                if x.count('組') > 0:
                    return float(x[0])
                else:
                    return np.nan
            data['t_grade_original'] = data['t_grade']
            data['t_class_original'] = data['t_class']
            data['t_grade'] = data['t_grade'].apply(lambda x: get_grade_from_string(x))
            data['t_class'] = data['t_class'].apply(lambda x: get_class_from_string(x))
            if sheet == '中学校':
                data['t_grade'] += 6
            data['year'] = self.year  # 確か2015年度の担当だから
            result = pd.concat([result, data], axis=0)
        return result


def seed():
    c = ReadConfigTodashi20162017(path='src/setting.ini')
    c.get_setting()
    qlist = QuestionList(c).build().qlist
    qlist2018 = QuestionList2018(c).build().qlist
    data1 = TodashiKyouinQes2017(c).build(qlist).data
    data2 = TodashiKyouinQes2018().build(qlist2018).data
    SchoolNameMixin()
    data = (
        pd.concat([data1,data2], axis=0)
        .pipe(SchoolNameMixin().assign_sch_name_to_sch_id, sch_name_col='school_name', sch_id_col ='school_id')
    )
    model = TodaTeacherQes(data)
    model.validate()
    model.save()
    return data

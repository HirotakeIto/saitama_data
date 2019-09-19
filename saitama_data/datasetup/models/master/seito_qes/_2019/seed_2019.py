import pandas as pd
import numpy as np
from saitama_data.datasetup.models.info.correspondence.model import Correspondence
from saitama_data.lib.safe_path import safe_path


class SeitoQes2018:
    path_seitoqes = './data/original_data/H31データ/H31データ/H31データ/児童生徒質問紙回答データ/scoring_info_feedback_2019.csv'
    need_col = ['year', 'id', 'grade']
    need_original_col = ['answer_form_num', 'testset_code',
                         'question_id', 'mark_value']
    mapper = {'answer_form_num': 'id', 'testset_code': 'grade'}

    def __init__(self):
        super(SeitoQes2018, self).__init__()
        self.path = self.path_seitoqes

    def build(self, correspondence):
        data = self.read()
        self.data = data
        self.data = self.engineer(data, correspondence)
        return self

    def read(self):
        # parametor
        path = self.path
        need_original_col = self.need_original_col
        # start
        data = pd.read_csv(safe_path(path))
        return data[need_original_col]

    def engineer(self, data, correspondence):
        # parametor
        mapper = self.mapper
        # start
        data = data.rename(columns=mapper)
        data = pd.merge(data, correspondence, on='question_id', how='left')
        print("面倒臭いから、変な奴は全部nanに。これで大丈夫なことは別途確認する。")
        data['mark_value'] = pd.to_numeric(data['mark_value'], errors='coerce')
        # q138以外作成
        seito = data.loc[~data['qes'].isin(['q138'])].copy()
        seito_qes = seito.pivot(index='id', columns='qes',
                                values='mark_value').reset_index()
        # q138
        birth = data.loc[data['qes'].isin(['q138']), :].copy()
        birth['mark_value'] = birth['mark_value'].replace(0, np.nan)
        slicing = birth['question_id'].isin(
            ['Q00000001031', 'Q00000002051', 'Q00000002951', 'Q00000003811', 'Q00000004781', 'Q00000005801']
        )
        birth.loc[slicing, 'q138'] = birth['mark_value']
        slicing = birth['question_id'].isin(
            ['Q00000001032', 'Q00000002052', 'Q00000002952', 'Q00000003812', 'Q00000004782', 'Q00000005802']
        )
        birth.loc[slicing, 'q138'] = birth['mark_value'] + 4
        slicing = birth['question_id'].isin(
            ['Q00000001033', 'Q00000002053', 'Q00000002953', 'Q00000003813', 'Q00000004783', 'Q00000005803']
        )
        birth.loc[slicing, 'q138'] = birth['mark_value'] + 8
        birth = birth.groupby('id')['q138'].sum().reset_index()
        # grade
        grade = data[['id', 'grade']].copy()
        mapper_grade = {'Q1': 4, 'Q2': 5, 'Q3': 6, 'Q4': 7, 'Q5': 8, 'Q6': 9}
        grade['grade'] = grade['grade'].replace(mapper_grade)
        grade = grade.dropna().drop_duplicates(subset='id')
        # merge
        print('Before birth merge:', seito_qes.shape)
        seito_qes = pd.merge(seito_qes, birth, on='id', how='left')
        print('After birth merge:', seito_qes.shape)
        print('null birth:', seito_qes['q138'].isnull().sum())
        seito_qes = pd.merge(seito_qes, grade, on='id', how='left')
        print('After grade merge:', seito_qes.shape)
        print('null grade:', seito_qes['grade'].isnull().sum())
        # add data
        seito_qes['year'] = 2019
        return seito_qes


def main2019(profileing=False):
    cc = Correspondence().read().get_correspondence_a_year(year=2019)
    s = SeitoQes2018().build(cc)
    _qlist = s.data.columns
    qlist = ['q' + str(i) for i in range(0, 1000) if 'q' + str(i) in _qlist]
    data = s.data[['id', 'year', 'grade'] + qlist]
    if profileing is True:
        import pandas_profiling
        profile = pandas_profiling.ProfileReport(data)
        profile.to_file('./tmp/seito_tmp.html')
    return data

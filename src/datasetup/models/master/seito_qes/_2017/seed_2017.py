import pandas as pd
import numpy as np
from src.lib.read_config import ReadConfig2017, ReadConfig2016


class Correspondence:
    """
    質問番号と質問内容の対応表を作成する
    """
    path_correspondence = '/seito_2017.csv'
    need_col = ['qes', 'question_id']
    need_original_col = ['カラム名2', '小4', '小5', '小6', '中1', '中2', '中3']

    def __init__(self, c):
        super(Correspondence, self).__init__()
        self.path = c.info + self.path_correspondence

    def build(self):
        data = self.read()
        self.data = self.engineer(data)
        return self

    def read(self):
        # parametor
        path = self.path
        need_original_col = self.need_original_col
        # start
        data = pd.read_csv(path)
        return data[need_original_col]

    def engineer(self, data):
        # engineer
        correspondence = pd.DataFrame()
        for g in ['小4', '小5', '小6', '中1', '中2', '中3']:
            correspondence_t = data[['カラム名2', g]]
            correspondence_t = correspondence_t.rename(columns={'カラム名2': 'qes', g: 'question_id'})
            correspondence = pd.concat([correspondence, correspondence_t], axis=0)
            correspondence = correspondence.dropna(how='any', subset=['question_id'])
        correspondence = correspondence.dropna(how='any', subset=['question_id'])
        return correspondence


class SeitoQes2017:
    path_seitoqes = '/児童生徒質問紙回答データ/scoring_info_feedback_2017.csv'
    need_col = ['year', 'id', 'grade']
    need_original_col = ['answer_form_num', 'testset_code',
                         'question_id', 'mark_value']
    mapper = {'answer_form_num': 'id', 'testset_code': 'grade'}

    def __init__(self, c):
        super(SeitoQes2017, self).__init__()
        self.path = c.downpath + self.path_seitoqes

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
        data = pd.read_csv(path)
        return data[need_original_col]

    def engineer(self, data, correspondence):
        # parametor
        mapper = self.mapper
        # start
        data = data.rename(columns=mapper)
        data = pd.merge(data, correspondence, on='question_id', how='left')
        # # 面倒臭いから、変な奴は全部nanに。これで大丈夫なことは別途確認する。
        data['mark_value'] = pd.to_numeric(data['mark_value'], errors='coerce')
        # q138以外作成
        seito = data.loc[~data['qes'].isin(['q138'])].copy()
        seito_qes = seito.pivot(index='id', columns='qes',
                                values='mark_value').reset_index()
        # q138
        birth = data.loc[data['qes'].isin(['q138']), :].copy()
        birth['mark_value'] = birth['mark_value'].replace(0, np.nan)
        slicing = birth['question_id'].isin(
            ['Q00000000881', 'Q00000001721', 'Q00000002561', 'Q00000003451', 'Q00000004401', 'Q00000005351'])
        birth.loc[slicing, 'q138'] = birth['mark_value']
        slicing = birth['question_id'].isin(
            ['Q00000000882', 'Q00000001722', 'Q00000002562', 'Q00000003452', 'Q00000004402', 'Q00000005352'])
        birth.loc[slicing, 'q138'] = birth['mark_value'] + 6
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
        seito_qes['year'] = 2017
        return seito_qes


def main2017():
    c = ReadConfig2017(path='src/setting.ini')
    c.get_setting()
    cc = Correspondence(c).build()
    s = SeitoQes2017(c).build(cc.data)
    _qlist = s.data.columns
    qlist = ['q' + str(i) for i in range(0, 1000) if 'q' + str(i) in _qlist]
    data = s.data[['id', 'year', 'grade'] + qlist]
    return data


def main20152016():
    c = ReadConfig2016(path='src/setting.ini')
    c.get_setting()
    return pd.read_pickle(c.workpath + '/20152016_from_raw.pkl')


def main():
    seito_qes = main2017()
    seito_qes2 = main20152016()
    data = pd.concat([seito_qes, seito_qes2], axis=0)
    print(data.shape, seito_qes.shape, seito_qes2.shape)
    # data.to_pickle('./data/original_data/work_data/seito_qes_2017.pkl')
    return data

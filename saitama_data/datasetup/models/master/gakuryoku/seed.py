import pandas as pd
import numpy as np
import subprocess
from saitama_data.lib.read_config import config, ReadConfig2015, ReadConfig2017, ReadConfig2016, ReadConfig2018
from saitama_data.datasetup.models.master.gakuryoku.model import Gakuryoku
from saitama_data.lib.safe_path import safe_path


class Gakuryoku2016:
    path_gakuryoku = '/IRT分析結果/SKP_IRT分析結果_2016.csv'
    need_original_col = ['year', 'grade', 'answer_form_num', 'subject', 'theta']
    mapper = {'answer_form_num': 'id'}
    mapper_subject = {'A': 'kokugo_level', 'C': 'math_level', 'E': 'eng_level'}

    def __init__(self, c):
        super(Gakuryoku2016, self).__init__()
        self.path = c.downpath + self.path_gakuryoku

    def build(self):
        data = self.read()
        data = self.engineer(data)
        # self.valid_columnn_check(data, self.need_col)
        # self.data = self.convert_columns_type(data, self.convert_list)
        self.data = data
        return self

    def read(self):
        # parametor
        path =self.path
        need_original_col = self.need_original_col
        # start
        data = pd.read_csv(safe_path(path))
        # self.valid_columnn_check(data, need_original_col)
        return data[need_original_col]

    def engineer(self, data):
        # parametor
        mapper = self.mapper
        mapper_subject = self.mapper_subject
        # start
        data = data.rename(columns=mapper)
        data['subject'] = data['subject'].replace(mapper_subject)
        # data.loc[~data['theta'].between(-10,10), 'theta'] = np.nan # 満点の人は999と−999で入っている。
        data.loc[(data['theta'] > 10), 'theta'] = 5.8
        data.loc[(data['theta'] < -10), 'theta'] = -5.8
        data = data.pivot_table(index=['year', 'grade', 'id'], columns='subject', values='theta').reset_index()
        return data


class Gakuryoku2017(Gakuryoku2016):
    path_gakuryoku = '/IRT分析結果/SKP_IRT分析結果_2017.csv'


class Gakuryoku2018(Gakuryoku2016):
    path_gakuryoku = '/IRT分析結果/SKP_IRT分析結果_2018.csv'

class Gakuryoku2019(Gakuryoku2016):
    path_gakuryoku = '/IRT分析結果/SKP_IRT分析結果_2019.csv'

    def __init__(self):
        self.path = './data/original_data/H31データ/H31データ/H31データ' + self.path_gakuryoku

class Gakuryoku2020(Gakuryoku2016):
    def __init__(self):
        self.path = config.path2020.gakuryoku

class Gakuryoku2015:
    need_original_col = ['H27個人番号', 'H27\n学年', 'kokugo_H27平均値', 'math_H27平均値', 'eng_H27平均値']
    mapper = {
        'H27個人番号': 'id',
        'H27\n学年': 'grade',
        'kokugo_H27平均値': 'kokugo_level',
        'math_H27平均値': 'math_level',
        'eng_H27平均値': 'eng_level',
    }
    path_gakuryoku = '/H28gakuryoku.csv'

    def __init__(self, c):
        super(Gakuryoku2015, self).__init__()
        self.path = c.workpath + self.path_gakuryoku

    def build(self):
        data = self.engineer()
        # self.valid_columnn_check(data, self.need_col)
        # self.data = self.convert_columns_type(data, self.convert_list)
        self.data = data
        return self

    def engineer(self):
        # parametor
        need_original_col = self.need_original_col
        mapper = self.mapper
        path = self.path
        # start
        data = pd.read_csv(safe_path(path))[need_original_col]
        data = data.rename(columns=mapper)
        data = data.replace('-', np.nan)
        replace_target = ['kokugo_level', 'math_level', 'eng_level']
        data = data.astype(float)
        # 5.8などの変換はしない
        # for t in replace_target:
        #     data.loc[~data[t].between(-5.799, 5.799), t] = np.nan
        data = data.dropna(how='all')
        data['year'] = 2015
        return data



def seed(save_dry = True):
    c = ReadConfig2015(path='saitama_data/setting.ini')
    c.get_setting()
    g2015 = Gakuryoku2015(c)
    c = ReadConfig2016(path='saitama_data/setting.ini')
    c.get_setting()
    g2016 = Gakuryoku2016(c)
    c = ReadConfig2017(path='saitama_data/setting.ini')
    c.get_setting()
    g2017 = Gakuryoku2017(c)
    c = ReadConfig2018(path='saitama_data/setting.ini')
    c.get_setting()
    g2018 = Gakuryoku2018(c)
    g2019 = Gakuryoku2019()
    g2020 = Gakuryoku2020()
    data = pd.DataFrame()
    for g in [g2015, g2016, g2017, g2018, g2019, g2020]: # これはこれで別メソッドとして切り分けるべきなのかもなー
        g.build()
        data = pd.concat([data, g.data], axis=0)
    gs = Gakuryoku(data)
    gs.validate()
    if save_dry is False:
        gs.save()
    # res = subprocess.call(["pytest", "tests/test.py::test_gakuryoku"])
    # if res != 0:
    #     raise ValueError("Testに失敗しました")
    return data


# def main():
#     # めっちゃ時間かかるけえ、あとでチェック
#     validater = GakuryokuSchema()
#     data = pd.DataFrame()
#     for f in [main201520162017, main2018]:
#         tmp = f()
#         tmp = validater._adjust_schema(tmp)
#         validater._convert(tmp)
#         data = pd.concat([data, tmp], axis=0)
#     validater._validate(data)
#     return data
# 全部一気につく
# refresh　→　１から作る。
# 保存したところだけやる → 実施記録を残しておいて、その続きからやる？
# とりま今後開発すべき機能だな。メモだけissueに残しておこう。
# 欲しい機能ー手元のスキーマと実際のDBを比較して違ったら、変更を加える関数
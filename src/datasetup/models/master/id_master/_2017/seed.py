"""
MasterID : 'mst_id', 'id', 'grade'のデータフレーム
SeitoInfo: 各々の生徒の情報を集めてくる
IdMaster: idmasterの最終テーブル
"""
import pandas as pd
import numpy as np
# from ..model import IdMaster
from src.datasetup.models.master.id_master.model import IdMaster
from src.lib.read_config import ReadConfig2016, ReadConfig2017
from src.datasetup.models.master.school_converter.seed_2017 import ClassIdSchoolId


def get_uniqueness(data, unique_key):
    data = data.sort_values(['sex'])
    data = data.groupby(unique_key).head(1)
    return data


class MasterId:
    """
        connfig の中にある所定のファイルを入れると、結果が帰ってくる。
        元々のデータのschool_idのところには、クラスのidが入っているらしい。。。
        これは
         sch_id : class_is
        を作るクラス
    """
    path_id_master = '/IDマスタ/id_master_grade_skp29.csv'
    need_original_col = ['mst_stuid', 'stuid_4', 'stuid_5', 'stuid_6', 'stuid_7', 'stuid_8', 'stuid_9']
    need_col = ['mst_id', 'id', 'grade']

    def __init__(self, c):
        super(MasterId, self).__init__()
        self.path = c.master + self.path_id_master

    def build(self):
        """
            目的となるDataFrameをビルドする
        """
        # parametor
        need_col = self.need_col
        # build
        data = self.read()
        data = self.engineer(data)
        self.data = data
        return self

    def read(self):
        # parametor
        path = self.path
        need_original_col = self.need_original_col
        #
        dt = pd.read_csv(path)
        return dt

    def engineer(self, dt):
        grades = ['stuid_4', 'stuid_5', 'stuid_6', 'stuid_7', 'stuid_8', 'stuid_9']
        master_id = pd.DataFrame(columns=['mst_id', 'id', 'grade'])
        for grade in grades:
            dt_ = dt[['mst_stuid', grade]].copy()
            dt_ = dt_.rename(columns={'mst_stuid': 'mst_id', grade: 'id'})
            dt_ = dt_.replace('-', np.nan)
            dt_ = dt_.dropna()
            dt_['grade'] = int(grade[-1])
            dt_['id'] = dt_['id'].astype(int)
            master_id = pd.concat([master_id, dt_], axis=0)
        return (
            master_id.drop_duplicates()
            .astype(
                { 'id': float, 'grade':  float}
            )
        )


class SeitoInfo:
    """
    SeitoInfoは明示的にデーベースとかに格納していないが、教則研の持っているテーブルをイメージしている。
    SeitoInfo は id や　school_codeなど教則研のコードも多いので、後々振り直す必要がある。
    """
    need_col = ['class', 'grade', 'id', 'school_code', 'sex', 'year']
    pass


class SeitoInfo2017(SeitoInfo):
    """
    2017年度の生徒質問紙の情報から、SeitoInfoオブジェクトを作るもの。

    """
    path_seito_info = '/児童生徒の解答（回答）用紙情報/student_info_2017.csv'
    need_original_col = ['answer_form_num', 'school_code', 'grade', 'class_num', 'gender']
    mapper = {'answer_form_num': 'id', 'school_code': 'school_code',
              'grade': 'grade', 'class_num': 'class', 'gender': 'sex'}

    def __init__(self, c):
        super(SeitoInfo2017, self).__init__()
        self.path = c.downpath + self.path_seito_info

    def build(self):
        """
        目的となるDataFrameをビルドする
        """
        # parametor
        need_col = self.need_col
        # build
        data = self.read()
        data = self.engineer(data)
        self.data = data
        return self

    def read(self):
        # read parametor
        path = self.path
        need_original_col = self.need_original_col
        # read
        data = pd.read_csv(path)
        data = data[need_original_col]
        return data

    def engineer(self, data):
        # read parametor
        mapper = self.mapper
        # engineer
        data = data.drop_duplicates()
        data['year'] = 2017
        data = (
            data
            .rename(columns=mapper)
            .astype(
                { 'id': float, 'grade':  float, 'class':  float, 'sex':  float}
            )
        )
        return data


class SeitoInfo2016(SeitoInfo):
    """
    2016年度の生徒質問紙の情報から、SeitoInfoオブジェクトを作るもの。
    2017と「たまたま」同じ形をしているっぽいので、そのママ転用

    """
    path_seito_info = '/児童生徒の解答（回答）用紙情報/student_info_2016.csv'
    need_original_col = ['answer_form_num', 'school_code', 'grade', 'class_num', 'gender']
    mapper = {'answer_form_num': 'id', 'school_code': 'school_code',
              'grade': 'grade', 'class_num': 'class', 'gender': 'sex'}

    def __init__(self, c):
        super(SeitoInfo2016, self).__init__()
        self.path = c.downpath + self.path_seito_info

    def build(self):
        """
        目的となるDataFrameをビルドする
        """
        # parametor
        need_col = self.need_col
        # build
        data = self.read()
        data = self.engineer(data)
        self.data = data
        return self

    def read(self):
        # read parametor
        path = self.path
        need_original_col = self.need_original_col
        # read
        data = pd.read_csv(path)
        data = data[need_original_col]
        return data

    def engineer(self, data):
        # read parametor
        mapper = self.mapper
        # engineer
        data = data.drop_duplicates()
        data['year'] = 2016
        data = (
            data
            .rename(columns=mapper)
            .astype(
                { 'id': float, 'grade':  float, 'class':  float, 'sex':  float}
            )
        )
        return data


class IdMaster2016(IdMaster):
    def __init__(self, sources: dict):
        super(IdMaster2016, self).__init__()
        self.class_id_school_id = sources['class_id_school_id']
        self.master_id = sources['master_id']
        self.seito_info = sources['seito_info']
        self.need_col = [col.name for col in self.schema.columns]

    def build(self):
        self.read()
        data = self.engineer()
        data = get_uniqueness(data, ['mst_id', 'year'])
        self.data = data
        return self

    def read(self):
        """
        本当はデータフレームを引数にとるの脆弱だから嫌だなあ。メモリも食うし。
        そのデータフレームがvalidなデータフレームかどうかわからない。。。
        本当はvisitorパターンで書くべき。
        """
        pass

    def engineer(self):
        # read parametor
        seito_info = self.seito_info
        class_id_school_id = self.class_id_school_id
        master_id = self.master_id
        need_col = self.need_col
        # engineer
        id_master = seito_info.copy()
        print('Init: data shape is {shape}'.format(shape=id_master.shape))
        id_master = pd.merge(id_master, master_id, on=['id', 'grade'], how='left')
        print('after left join master_id, data shape is {shape}'.format(shape=id_master.shape))
        id_master = pd.merge(id_master, class_id_school_id, left_on='school_code', right_on='class_id', how='left')
        print('after left join class_id_school_id data shape is {shape}'.format(shape=id_master.shape))
        return id_master[need_col]


class IdMaster2017(IdMaster2016):
    pass


class IdMaster2015(IdMaster):
    """
    IdMasterの2015年度版を作る。
    """

    gakuryoku_path = 'data/original_data/work_data/H28gakuryoku.csv'
    seito_qes_path = 'data/original_data/work_data/2015_from_raw.pkl'


    def __init__(self, master_id):
        super(IdMaster2015, self).__init__()
        self.master_id = master_id

    def build(self):
        self.read()
        data = self.engineer()
        self.data = data
        return self

    def read(self):
        """
        """

    def engineer(self):
        master_id = self.master_id
        id_master = self.get_data1()
        print('Init: data shape is {shape}'.format(shape=id_master.shape))
        id_master = pd.merge(id_master, master_id, on=['id', 'grade'], how='left')
        print('after left join master_id, data shape is {shape}'.format(shape=id_master.shape))
        return id_master

    def get_data1(self):
        # parametor
        gakuryoku_path = self.gakuryoku_path
        seito_qes_path = self.seito_qes_path
        # get use columns
        path = gakuryoku_path
        need_original_col = ['市町村教育委員会', '学校コード']
        mapper = {'市町村教育委員会': 'city_id', '学校コード': 'school_id'}
        data = pd.read_csv(path)[need_original_col]
        data = data.rename(columns=mapper)
        school_city = data[['school_id', 'city_id']].drop_duplicates()
        # next
        """
        ここポイントだった
        20170817現在、以下のコードをidmasterは1対1対応しない。
        何故ならば、H27個人番号とH28個人番号は違うため。
        従来のidmasterテーブルは学力と生徒質問から作っていたが、前者はH28idだけで後者はH27だけで作っていた。
        H28とH27でidが変わったような人間に対しては、多重登録になっていたので、従来テーブルの方が数が多い。
        """
        path = gakuryoku_path
        need_original_col = ['H27学校コード', 'H27個人番号', 'H27\n学年', 'H27\n組', 'H27\n性別']
        mapper = {'H27学校コード': 'school_id', 'H27個人番号': 'id',
                  'H27\n学年': 'grade', 'H27\n組': 'class', 'H27\n性別': 'sex'}
        dt = pd.read_csv(path)[need_original_col]
        dt = dt.dropna(how='all')
        dt = dt.rename(columns=mapper)
        dt = dt.replace('-', np.nan)
        data = pd.DataFrame()
        data = pd.concat([data, dt], axis=0)
        # next
        path = seito_qes_path
        need_original_col = ['id', 'grade', 'school_id', 'sex', 'class']
        dt = pd.read_pickle(path)[need_original_col]
        dt = dt.dropna(how='all')
        dt['grade'] = pd.to_numeric(dt['grade'])
        dt.loc[dt['grade'] <= 3, 'grade'] += 6
        dt = dt.loc[~dt['id'].isin(data['id'].unique()), :]
        # 微調整
        data = pd.concat([data, dt], axis=0)
        data = data.dropna(subset=list(data.columns[~data.columns.isin(['id', 'year'])]), how='all')  # 歯抜けは殺す
        data = data.replace(np.nan, 99999999).astype(int).replace(99999999, np.nan)
        data = data.drop_duplicates()
        data = pd.merge(data, school_city, on='school_id', how='left')
        data['year'] = 2015
        data['id'] = data['id'].astype(int)
        data['grade'] = data['grade'].astype(int)
        return data


def main2017():
    c = ReadConfig2017(path='src/setting.ini')
    c.get_setting()
    # data setup
    class_id_school_id = ClassIdSchoolId().build().data
    master_id = MasterId(c).build().data
    seito_info = SeitoInfo2017(c).build().data
    i = IdMaster2017({'class_id_school_id': class_id_school_id,
                      'master_id': master_id,
                      'seito_info': seito_info})
    i.build()
    id_master = i.data
    print(id_master.isnull().agg(['sum', 'mean']))
    return id_master
    # id_master.loc[id_master['school_id'].isnull(), :]


def main2016():
    c = ReadConfig2016(path='src/setting.ini')
    c.get_setting()
    # data setup
    class_id_school_id = ClassIdSchoolId().build().data
    master_id = MasterId(c).build().data
    seito_info = SeitoInfo2016(c).build().data
    i = IdMaster2016({'class_id_school_id': class_id_school_id,
                      'master_id': master_id,
                      'seito_info': seito_info})
    i.build()
    id_master = i.data
    print(id_master.isnull().agg(['sum', 'mean']))
    return id_master
    # id_master.loc[id_master['school_id'].isnull(), :]


def main2015():
    c = ReadConfig2017(path='src/setting.ini')
    c.get_setting()
    # data setup
    master_id = MasterId(c).build().data
    i = IdMaster2015(master_id=master_id)
    i.build()
    id_master = i.data
    print(id_master.isnull().agg(['sum', 'mean']))
    return id_master


"""
後でメモっとく
同じ人なんに複数レコードがある。
"""
# path = 'data/raw/H29提供データ【一式】/290810 過年度(H29,H28)データ_特別対応後/H28データ/児童生徒の解答（回答）用紙情報/student_info_2016.csv'
# need_original_col = ['answer_form_num', 'school_code', 'grade', 'class_num', 'gender']
# mapper = {'answer_form_num': 'id', 'school_code': 'school_code',
#           'grade': 'grade', 'class_num': 'class', 'gender': 'sex'}
# a = pd.read_csv(path)
# a = a[need_original_col]
# a = a.rename(columns=mapper)
# a = a.loc[a['sex']!=3, :]
# a = a.drop_duplicates()
# a[a['id'].duplicated(keep=False)]

import pandas as pd
import numpy as np
from src.datasetup.models.master.id_master.model import IdMaster
from src.lib.read_config import ReadConfig2018
# from src.datasetup.models.info.classid_schoolid.trash.seed_2018 import ClassIdSchoolId
from src.datasetup.models.info import ClassIdSchoolId


def convert_columns_type(data, convert_list: dict, errors='raise'):
    for c in convert_list.keys():
        if convert_list[c] == float:
            data[c] = pd.to_numeric(data[c], errors=errors)
        if convert_list[c] == int:
            data[c] = data[c].astype(int)
    return data


def get_uniqueness(data, unique_key):
    data = data.sort_values(['sex'])
    data = data.groupby(unique_key).head(1)
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
        data = self.convert_columns_type(data, self.convert_list)
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


class MasterId:
    """
        connfig の中にある所定のファイルを入れると、結果が帰ってくる。
        元々のデータのschool_idのところには、クラスのidが入っているらしい。。。
        これは
         sch_id : class_is
        を作るクラス
    """
    path_id_master = '/IDマスタ/2018/id_master_grade_skp30.csv'
    need_original_col = ['mst_stuid', 'stuid_4', 'stuid_5', 'stuid_6', 'stuid_7', 'stuid_8', 'stuid_9']
    need_col = ['mst_id', 'id', 'grade']

    def __init__(self, c):
        super(MasterId, self).__init__()
        self.path = c.id_master

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


class SeitoInfo2018(SeitoInfo):
    """
    2017年度の生徒質問紙の情報から、SeitoInfoオブジェクトを作るもの。

    """
    path_seito_info = '/児童生徒の解答（回答）用紙情報/student_info_2018.csv'
    need_original_col = ['answer_form_num', 'school_code', 'grade', 'class_num', 'gender']
    mapper = {'answer_form_num': 'id', 'school_code': 'school_code',
              'grade': 'grade', 'class_num': 'class', 'gender': 'sex'}

    def __init__(self, c):
        super(SeitoInfo2018, self).__init__()
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
        data['year'] = 2018
        data = (
            data
            .rename(columns=mapper)
            .astype(
                { 'id': float, 'grade':  float, 'class':  float, 'sex':  float}
            )
        )
        return data


class IdMaster2018(IdMaster2016):
    pass


def main2018(profileing=False):
    c = ReadConfig2018(path='src/setting.ini')
    c.get_setting()
    # data setup
    class_id_school_id = ClassIdSchoolId().read().data
    master_id = MasterId(c).build().data
    seito_info = SeitoInfo2018(c).build().data
    i = IdMaster2018({'class_id_school_id': class_id_school_id,
                      'master_id': master_id,
                      'seito_info': seito_info})
    i.build()
    id_master = IdMaster()._validate_convert(i.data)
    print(id_master.isnull().agg(['sum', 'mean']))
    print(id_master.groupby(['year', 'grade']).agg(['count', 'mean']))
    if profileing is True:
        import pandas_profiling
        profile = pandas_profiling.ProfileReport(id_master)
        profile.to_file('./tmp/id_master.html')
    return id_master
    # id_master.loc[id_master['school_id'].isnull(), :]



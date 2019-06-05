import pandas as pd
import numpy as np
from saitama_data.lib.read_config import ReadConfig2017


class SchoolClass:
    """
    '/過年度学校マスタ.csv' からデータを作る

    元々のデータのschool_idのところには、クラスのidが入っているらしい。。。
    これは
     sch_id : class_is
    を作るクラス
    """
    path_school_master = '/学校マスタ/過年度学校マスタ.csv'

    def __init__(self, c):
        path = c.master + self.path_school_master
        self.school_class = self.get_school_class(path)

    @staticmethod
    def get_school_class(path):
        school = pd.read_csv(path)
        school = school[['sch_id', '年度', 'grad_4', 'grad_5', 'grad_6', 'grad_7', 'grad_8', 'grad_9']]
        school_class = pd.DataFrame(columns=['sch_id', 'year', 'class_id'])
        for grad in ['grad_4', 'grad_5', 'grad_6', 'grad_7', 'grad_8', 'grad_9']:
            school_class_tmp = school[['sch_id', '年度', grad]]
            school_class_tmp = school_class_tmp.rename(columns={'sch_id': 'sch_id', '年度': 'year', grad: 'class_id'})
            school_class_tmp = school_class_tmp.replace('-', np.nan)
            school_class_tmp = school_class_tmp.dropna()
            school_class = pd.concat([school_class, school_class_tmp], axis=0)
        return school_class


class SchidSchoolid:
    """
    '/replacelist.xlsx' からデータを作る
    sch_id（教則研コード） と school_id（埼玉データの統一id）の対応表
    """
    path_replace_school = '/replacelist.xlsx'

    def __init__(self, c):
        path = c.master + self.path_replace_school
        self.path = path
        self.schid_schoolid = self.get_replacement(path)

    def get_replacement(self, path):
        schid_schoolid = self.get_school_replacemet(path)
        replace_city = self.get_city_replacemet(path)
        print('before left join replace_city, data shape is {shape}'.format(shape=schid_schoolid.shape))
        schid_schoolid = pd.merge(schid_schoolid, replace_city, on='city_code', how='left')
        print('after left join replace_city, data shape is {shape}'.format(shape=schid_schoolid.shape))
        return schid_schoolid[['sch_id', 'school_id', 'city_id']]

    @staticmethod
    def get_school_replacemet(path):
        replace_school_p = pd.read_excel(path, sheetname='1_小学校マスタ')
        replace_school_p = replace_school_p[['学校コード', '市町村教育委員会コード', 'Unnamed: 9']]
        replace_school_p = replace_school_p.rename(columns={'学校コード': 'sch_id',
                                                            '市町村教育委員会コード': 'city_code', 'Unnamed: 9': 'school_id'})
        replace_school_j = pd.read_excel(path, sheetname='2_中学校マスタ')
        replace_school_j = replace_school_j[['学校コード', '市町村教育委員会コード', 'Unnamed: 9']]
        replace_school_j = replace_school_j.rename(columns={'学校コード': 'sch_id',
                                                            '市町村教育委員会コード': 'city_code', 'Unnamed: 9': 'school_id'})
        replacement = pd.concat([replace_school_p, replace_school_j], axis=0)
        replacement = replacement.loc[pd.to_numeric(replacement['school_id'], errors='coerce').notnull(), :]
        return replacement

    @staticmethod
    def get_city_replacemet(path):
        replace_city = pd.read_excel(path, sheetname='市町村教育委員会')
        replace_city = replace_city[['市町村教育委員会コード', '市町村教育委員会乱数']]
        replace_city = replace_city.rename(columns={'市町村教育委員会コード': 'city_code', '市町村教育委員会乱数': 'city_id'})
        return replace_city


def get_2017_class_id_school_id():
    c = ReadConfig2017(path='saitama_data/setting.ini')
    c.get_setting()
    schid_schoolid = SchidSchoolid(c).schid_schoolid
    school_class = SchoolClass(c).school_class
    school_class = (
        school_class.loc[school_class['year'] == 'H29', :]
        .assign(
            sch_id = lambda dfx: dfx['sch_id'].astype(float)
        )
    )
    class_id_school_id = pd.merge(schid_schoolid, school_class, on='sch_id', how='left').dropna()
    class_id_school_id = class_id_school_id[['class_id', 'school_id', 'city_id']]
    class_id_school_id = class_id_school_id.astype(float)
    return class_id_school_id


class ClassIdSchoolId(object):
    def __init__(self):
        self.data = get_2017_class_id_school_id()

    def build(self):
        return self

# class_id_school_id = get_2017_class_id_school_id()
#
# data = pd.read_csv('data/raw/H29提供データ【一式】/290810 過年度(H29,H28)データ_特別対応後/H29データ/児童生徒の解答（回答）用紙情報/student_info_2017.csv')
# data = pd.merge(data, class_id_school_id, left_on='school_code', right_on='class_id', how='left')
# data.loc[data['school_id'].isnull(), 'school_code'].unique()
# #

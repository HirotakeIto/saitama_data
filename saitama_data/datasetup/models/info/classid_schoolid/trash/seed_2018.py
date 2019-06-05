import pandas as pd
from saitama_data.lib.read_config import ReadConfig2018
from saitama_data.datasetup.models.info.classid_schoolid.school_class.model import SchoolClassSchema
from saitama_data.datasetup.models.info.classid_schoolid.schid_schoolid.model import SchidSchoolidSchema



def convert_columns_type(data, convert_list: dict, errors='raise'):
    for c in convert_list.keys():
        if convert_list[c] == float:
            data[c] = pd.to_numeric(data[c], errors=errors)
        if convert_list[c] == int:
            data[c] = data[c].astype(int)
    return data


class SchoolClass:
    """
    '/過年度学校マスタ.csv' からデータを作る

    元々のデータのschool_idのところには、クラスのidが入っているらしい。。。
    これは
     sch_id : class_is
    を作るクラス
    """
    path_config_attribute = 'school_master'
    encoding = 'sjis'
    need_col = ['sch_id', 'class_id']
    convert_list = {n: float for n in need_col}

    def __init__(self, c):
        path = getattr(c, self.path_config_attribute)
        data = self.get_school_class(path)
        self.data = self.engineer(data)

    def engineer(self, data):
        return convert_columns_type(data[self.need_col], self.convert_list)

    def get_school_class(self, path):
        school = pd.read_csv(path, encoding=self.encoding)
        school = school[['sch_id', '年度', 'grad_4', 'grad_5', 'grad_6', 'grad_7', 'grad_8', 'grad_9']]
        school_class = pd.DataFrame(columns=['sch_id', 'year', 'class_id'])
        for grad in ['grad_4', 'grad_5', 'grad_6', 'grad_7', 'grad_8', 'grad_9']:
            school_class_tmp = school[['sch_id', '年度', grad]]
            school_class_tmp = school_class_tmp.rename(columns={'sch_id': 'sch_id', '年度': 'year', grad: 'class_id'})
            school_class_tmp = school_class_tmp.replace('-', pd.np.nan)
            school_class_tmp = school_class_tmp.dropna()
            school_class = pd.concat([school_class, school_class_tmp], axis=0)
        return school_class.astype(float)


class SchidSchoolid:
    """
    '/replacelist.xlsx' からデータを作る
    sch_id（教則研コード） と school_id（埼玉データの統一id）の対応表
    """
    need_col = ['sch_id', 'school_id', 'city_id']
    convert_list = {n: float for n in need_col}


    def __init__(self, c):
        self.path = c.school_code
        data = self.get_replacement(self.path)
        self.data = self.engineer(data)

    def engineer(self, data):
        return convert_columns_type(data[self.need_col], self.convert_list)

    def get_replacement(self, path):
        schid_schoolid = self.get_school_replacemet(path)
        replace_city = self.get_city_replacemet(path)
        print('before left join replace_city, data shape is {shape}'.format(shape=schid_schoolid.shape))
        schid_schoolid = pd.merge(schid_schoolid, replace_city, on='city_code', how='left')
        print('after left join replace_city, data shape is {shape}'.format(shape=schid_schoolid.shape))
        return schid_schoolid.astype(float)

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

    @property
    def mapper_sch_id_school_id(self):
        return {x[0]: x[1] for x in self.data[['sch_id', 'school_id']].dropna().astype(int).values}


def get_2018_class_id_school_id():
    c = ReadConfig2018(path='saitama_data/setting.ini').get_setting()
    school_class = SchoolClass(c).data.drop_duplicates()
    schid_schoolid = SchidSchoolid(c).data.dropna()
    school_class = SchoolClassSchema(data=school_class).validate_convert().data
    schid_schoolid = SchidSchoolidSchema(data=schid_schoolid).validate_convert().data
    class_id_school_id = pd.merge(school_class, schid_schoolid, on='sch_id', how='left')
    class_id_school_id = class_id_school_id[['class_id', 'school_id', 'city_id']]
    return class_id_school_id


class ClassIdSchoolId(object):
    def __init__(self):
        self.data = get_2018_class_id_school_id()

    def build(self):
        return self
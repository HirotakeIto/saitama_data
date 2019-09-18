"""
TODO: あとで修正するときのメモ
replacelist.xlsxは毎年形式が変わったちゃうから自分で作る

作業手順
1. まず元々のコードを入れ替えても通じるかどうかチェック
2. 次に、replacelist.xlsxを2019年度版に変更してやり直す。


"""
import pandas as pd
from saitama_data.datasetup.models.info.classid_schoolid.schid_schoolid.model import SchidSchoolid

__path__ = 'data/original_data/マスタデータ/replacelist/replacelist.xlsx'

def convert_columns_type(data, convert_list: dict, errors='raise'):
    for c in convert_list.keys():
        if convert_list[c] == float:
            data[c] = pd.to_numeric(data[c], errors=errors)
        if convert_list[c] == int:
            data[c] = data[c].astype(int)
    return data


class SchidSchoolidSeed:
    """
    '/replacelist.xlsx' からデータを作る
    sch_id（教則研コード） と school_id（埼玉データの統一id）の対応表
    """
    need_col = ['sch_id', 'school_id', 'city_id']
    convert_list = {n: float for n in need_col}


    def __init__(self):
        self.path = __path__
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



def seed(save_dry = True):
    sss = SchidSchoolidSeed()
    ss = SchidSchoolid(sss.data)
    ss.validate()
    if save_dry is False:
        ss.save()

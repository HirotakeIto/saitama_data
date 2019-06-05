import pandas as pd
from saitama_data.datasetup.models.info.classid_schoolid.school_class.model import SchoolClass

__path__ = 'data/original_data/マスタデータ/学校マスタ/2018/過年度学校マスタ_2018.csv'
__path2__ = 'data/original_data/マスタデータ/学校マスタ/過年度学校マスタ.csv'


def convert_columns_type(data, convert_list: dict, errors='raise'):
    for c in convert_list.keys():
        if convert_list[c] == float:
            data[c] = pd.to_numeric(data[c], errors=errors)
        if convert_list[c] == int:
            data[c] = data[c].astype(int)
    return data


def read_data_concat():
    school = (
        pd.read_csv(__path__, encoding='sjis')
        [['sch_id', '年度', 'grad_4', 'grad_5', 'grad_6', 'grad_7', 'grad_8', 'grad_9']]
    )
    school2 = (
        pd.read_csv(__path2__)
        [['sch_id', '年度', 'grad_4', 'grad_5', 'grad_6', 'grad_7', 'grad_8', 'grad_9']]
    )
    return (
        school.append(school2)
    )


class SchoolClassSeed:
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

    def __init__(self):
        path = __path__
        data = self.get_school_class(path)
        self.data = self.engineer(data)

    def engineer(self, data):
        return (
            data
            .pipe(lambda dfx: convert_columns_type(dfx[self.need_col], self.convert_list))
            .astype(float)
            .drop_duplicates()
        )

    def get_school_class(self, path):
        school = read_data_concat()
        school_class = pd.DataFrame(columns=['sch_id', 'year', 'class_id'])
        for grad in ['grad_4', 'grad_5', 'grad_6', 'grad_7', 'grad_8', 'grad_9']:
            school_class_tmp = school[['sch_id', '年度', grad]]
            school_class_tmp = school_class_tmp.rename(columns={'sch_id': 'sch_id', '年度': 'year', grad: 'class_id'})
            school_class_tmp = school_class_tmp.replace('-', pd.np.nan)
            school_class_tmp = school_class_tmp.dropna()
            school_class = pd.concat([school_class, school_class_tmp], axis=0)
        return school_class



def seed():
    sc_seed = SchoolClassSeed()
    sc = SchoolClass(sc_seed.data)
    sc.validate()
    sc.save()

# def check():
#     from saitama_data.datasetup.models.info.school_converter.seed_2017 import SchoolClass as SchoolClass2017
#     from saitama_data.lib.read_config import ReadConfig2017
#     sc = SchoolClassSeed()
#     c = ReadConfig2017(path='saitama_data/setting.ini')
#     c.get_setting()
#     school_class = (
#         SchoolClass2017(c)
#         .school_class
#         [['class_id', 'sch_id']]
#         .drop_duplicates()
#         .astype(float)
#     )
#     aaa = pd.merge(sc.data, school_class, on = 'class_id', how='left', suffixes=('_new', '_old'))
#     print("Matchしていないもの", aaa[aaa['sch_id_new'] != aaa['sch_id_old']])

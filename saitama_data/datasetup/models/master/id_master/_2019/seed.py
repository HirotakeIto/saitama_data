from saitama_data.datasetup.models.master.id_master.model import IdMaster
from saitama_data.datasetup.models.master.id_master._2018.seed import MasterId, SeitoInfo2018, IdMaster2016
from saitama_data.datasetup.models.info import ClassIdSchoolId
IdMaster2019 = IdMaster2016

class MasterId2019(MasterId):
    path_id_master = './data/original_data/H31データ/H31データ/IDマスタ/id_master_grade_skp31.csv'

    def __init__(self):
        self.path = self.path_id_master

class SeitoInfo2019(SeitoInfo2018):
    path_seito_info = './data/original_data/H31データ/H31データ/H31データ/児童生徒の解答（回答）用紙情報/student_info_2019.csv'
    year_value = 2019

    def __init__(self):
        self.path = self.path_seito_info


def main2019(profileing=False):
    class_id_school_id = ClassIdSchoolId().read().data
    master_id = MasterId2019().build().data
    seito_info = SeitoInfo2019().build().data
    i = IdMaster2019({
        'class_id_school_id': class_id_school_id,
        'master_id': master_id,
        'seito_info': seito_info
    }).build()
    id_master = IdMaster()._validate_convert(i.data)
    from pandas import set_option
    set_option('display.max_columns', 500)
    print(id_master.isnull().agg(['sum', 'mean']))
    print(id_master.groupby(['year', 'grade']).agg(['count', 'mean']))
    return id_master

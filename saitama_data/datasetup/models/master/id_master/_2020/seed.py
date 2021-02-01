from saitama_data.lib.read_config import config
from saitama_data.datasetup.models.master.id_master.model import IdMaster
from saitama_data.datasetup.models.master.id_master._2018.seed import MasterId, SeitoInfo2018, IdMaster2016
from saitama_data.datasetup.models.info import ClassIdSchoolId
IdMaster2020 = IdMaster2016
PATH_ID_MASTER = config.path2020.id_master
PATH_SEITO_INFO = config.path2020.seito_info

class MasterId2020(MasterId):
    path_id_master = PATH_ID_MASTER

    def __init__(self):
        self.path = self.path_id_master

class SeitoInfo2020(SeitoInfo2018):
    path_seito_info = PATH_SEITO_INFO
    year_value = 2020

    def __init__(self):
        self.path = self.path_seito_info


def main2020(profileing=False):
    class_id_school_id = ClassIdSchoolId().read().data
    master_id = MasterId2020().build().data
    seito_info = SeitoInfo2020().build().data
    i = IdMaster2020({
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

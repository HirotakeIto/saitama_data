import subprocess
from saitama_data.datasetup.models.master.id_master.model import IdMaster
from saitama_data.datasetup.models.master.id_master._2017.seed import main2015, main2016, main2017
from saitama_data.datasetup.models.master.id_master._2018.seed import main2018
from saitama_data.datasetup.models.master.id_master._2019.seed import main2019


def seed(save_dry=True):
    def save(data, dry_save = False, if_exists='replace'):
        model = IdMaster(data)
        model.validate_convert()
        if dry_save is False:
            model.save(if_exists)
        print('NUMBER OF DUPLICATED ID: ', data.duplicated(subset=['grade', 'year', 'id'], keep=False).sum())

    save(main2015(), dry_save=save_dry, if_exists='replace')
    save(main2016(), dry_save=save_dry, if_exists='append')
    save(main2017(), dry_save=save_dry, if_exists='append')
    save(main2018(), dry_save=save_dry, if_exists='append')
    save(main2019(), dry_save=save_dry, if_exists='append')
    # res = subprocess.call(["pytest", "tests/test.py::test_idmaster"])
    # if res != 0:
    #     raise ValueError("Testに失敗しました")

    # # check
    # aa = main2018()
    # model = IdMaster(aa)
    # import json
    # from dftest import InfosDataFrame, test_infos
    # from saitama_data.datasetup.test.test import get_object_name, get_testinfo_path
    # model_name = get_object_name(model)
    # with open(get_testinfo_path(model_name), mode='r', encoding='utf8') as f:
    #     res = json.load(f)
    # bb = InfosDataFrame.read_from_csv(res['save_path'])
    # aa = InfosDataFrame.read_from_df(model.data, **res['args'])
    # test_infos(aa, bb)

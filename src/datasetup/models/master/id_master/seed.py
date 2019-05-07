import pandas as pd
import numpy as np
from src.datasetup.models.master.id_master.model import IdMaster
from src.datasetup.models.master.id_master._2017.seed import main2015, main2016, main2017
from src.datasetup.models.master.id_master._2018.seed import main2018

def seed():
    # めっちゃ時間かかるけえ、あとでチェック
    validater = IdMaster()
    id_master = pd.DataFrame()
    for f in [main2015, main2016, main2017, main2018]:
        tmp = f()
        tmp = validater._adjust_schema(tmp)
        validater._convert(tmp)
        id_master = pd.concat([id_master, tmp], axis=0)
    validater._validate(id_master)
    model = IdMaster(id_master)
    model.save()
    print(id_master.groupby(['grade', 'year'])['id'].agg(['mean', 'count']))
    return id_master

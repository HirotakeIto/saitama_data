import pandas as pd
import numpy as np
from src.datasetup.models.master.id_master.model import IdMaster
from src.datasetup.models.master.id_master._2017.seed import main2015, main2016, main2017
from src.datasetup.models.master.id_master._2018.seed import main2018

def seed():
    def save(data, if_exists='replace'):
        model = IdMaster(data)
        model.validate_convert()
        model.save(if_exists)
        print('NUMBER OF DUPLICATED ID: ', data.duplicated(subset=['grade', 'year', 'id'], keep=False).sum())

    save(main2015(), if_exists='replace')
    save(main2016(), if_exists='append')
    save(main2017(), if_exists='append')
    save(main2018(), if_exists='append')

    # # めっちゃ時間かかるけえ、あとでチェック
    # validater = IdMaster()
    # id_master = pd.DataFrame()
    # for f in [main2015, main2016, main2017, main2018]:
    #     tmp = f()
    #     tmp = validater._adjust_schema(tmp)
    #     validater._convert(tmp)
    #     id_master = pd.concat([id_master, tmp], axis=0)
    # validater._validate(id_master)
    # model = IdMaster(id_master)
    # model.save()
    # print(id_master.groupby(['grade', 'year'])['id'].agg(['mean', 'count']))
    # Debugしてただけだけど、将来こんな感じでvalidチェックをしたい
    # aaa = (
    #     IdMaster()
    #     .read()
    #     .data
    #     # .drop_duplicates(subset=['grade', 'year', 'id'])
    # )
    # aaa.groupby(['grade', 'year']).size()
    # bbb = aaa.groupby(['grade', 'year'])['id'].agg(['mean', 'count'])
    # ccc = id_master.groupby(['grade', 'year'])['id'].agg(['mean', 'count'])
    # ccc - bbb
    # (
    #     id_master.groupby(['grade','year', 'id']).size()
    #     .pipe(lambda dfx: dfx[dfx!=1])
    # )
    # aaa.groupby(['grade', 'year'])[['school_id', 'city_id']].agg(['count', 'size']) - id_master.groupby(['grade', 'year'])[['school_id', 'city_id']].agg(['count', 'size'])
    return

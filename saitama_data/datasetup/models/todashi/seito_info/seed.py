import pandas as pd
from saitama_data.datasetup.models.todashi.seito_info._seed._2015 import main as seed_2015
from saitama_data.datasetup.models.todashi.seito_info._seed._2018 import main as seed_2018
from saitama_data.datasetup.models.todashi.seito_info._seed._2019 import main as seed_2019
from saitama_data.datasetup.models.todashi.seito_info.model import SeitoInfo


def seed(save_dry = True):
    df = pd.DataFrame()
    for func in [seed_2015, seed_2018, seed_2019]:
        df = df.append(func())
    gs = SeitoInfo(df)
    gs.convert()
    gs.validate()
    if save_dry is False:
        gs.save()
    return df
    
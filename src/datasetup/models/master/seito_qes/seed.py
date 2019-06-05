import pandas as pd
from src.datasetup.models.master.seito_qes._2017.seed_2017 import main2017, main20152016
from src.datasetup.models.master.seito_qes._2018.seed_2018 import main2018
from src.datasetup.models.master.seito_qes.model import SeitoQes

def seed():
    def save(data, if_exists='replace'):
        model = SeitoQes(data).adjust_schema().convert()
        model.validate_convert()
        model.save(if_exists)
        print('NUMBER OF DUPLICATED ID: ', data.duplicated(subset=['grade', 'year', 'id'], keep=False).sum())
    save(main20152016(), if_exists='replace')
    save(main2017(), if_exists='append')
    save(main2018(), if_exists='append')


# def seed():
#     # めっちゃ時間かかるけえ、あとでチェック
#     validater = SeitoQes()
#     data = pd.DataFrame()
#     for f in [main20152016, main2017, main2018]:
#         tmp = f()
#         tmp = validater._adjust_schema(tmp)
#         data = pd.concat([data, tmp], axis=0)
#     validater._convert(tmp)
#     validater._validate(data)
#     model = SeitoQes(data)
#     model.validate()
#     model.save()
#     # ToDo: 昔格納したやつはそこを対処してたりしたけど、それはあとでやる
#     print('q138は本当はmin１max１２だけど、これはそうなっていない\n',
#           '昔格納したやつはそこを対処してたりしたけど、それはあとでやる')
#     return data
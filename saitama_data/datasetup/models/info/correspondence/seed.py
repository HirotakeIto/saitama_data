import pandas as pd
from saitama_data.datasetup.models.info.correspondence.model import Correspondence
from saitama_data.lib.safe_path import safe_path

def seed_builder(path, year):
    # path = './data/info/seito_2017.csv'
    # path = './data/info/seito_2018.csv'
    # year = 2018
    need_col = ['qes', 'question_id']
    need_original_col = ['カラム名2', '小4', '小5', '小6', '中1', '中2', '中3']
    data = (
        pd.read_csv(safe_path(path))
        [need_original_col]
    )
    correspondence = pd.DataFrame()
    for g in ['小4', '小5', '小6', '中1', '中2', '中3']:
        correspondence_t = data[['カラム名2', g]]
        correspondence_t = correspondence_t.rename(columns={'カラム名2': 'qes', g: 'question_id'})
        correspondence = pd.concat([correspondence, correspondence_t], axis=0)
        correspondence = correspondence.dropna(how='any', subset=['question_id'])
    correspondence = (
        correspondence
        .dropna(how='any', subset=['question_id'])
        [need_col]
        .assign(year = year)
    )
    return correspondence


def seed(save_dry=True):
    builder_info_list = [
        {'path': './data/info/seito_qes/seito_2017.csv', 'year': 2017},
        {'path': './data/info/seito_qes/seito_2018.csv', 'year': 2018},
        {'path': './data/info/seito_qes/seito_2019.csv', 'year': 2019},
    ]
    df = pd.DataFrame()
    for builder_info in builder_info_list:
        df = df.append(
            seed_builder(path=builder_info['path'], year=builder_info['year'])
        )
    corres = Correspondence(df)
    corres.validate()
    if save_dry is False:
        corres.save()

#
#
# class Correspondence:
#     """
#     質問番号と質問内容の対応表を作成する
#     """
#     path_correspondence = '/seito_2017.csv'
#     need_col = ['qes', 'question_id']
#     need_original_col = ['カラム名2', '小4', '小5', '小6', '中1', '中2', '中3']
#
#     def __init__(self, c):
#         super(Correspondence, self).__init__()
#         self.path = c.info + self.path_correspondence
#
#     def build(self):
#         data = self.read()
#         self.data = self.engineer(data)
#         return self
#
#     def read(self):
#         # parametor
#         path = self.path
#         need_original_col = self.need_original_col
#         # start
#         data = pd.read_csv(safe_path(path))
#         return data[need_original_col]
#
#     def engineer(self, data):
#         # engineer
#         correspondence = pd.DataFrame()
#         for g in ['小4', '小5', '小6', '中1', '中2', '中3']:
#             correspondence_t = data[['カラム名2', g]]
#             correspondence_t = correspondence_t.rename(columns={'カラム名2': 'qes', g: 'question_id'})
#             correspondence = pd.concat([correspondence, correspondence_t], axis=0)
#             correspondence = correspondence.dropna(how='any', subset=['question_id'])
#         correspondence = correspondence.dropna(how='any', subset=['question_id'])
#         return correspondence

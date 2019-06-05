import pandas as pd
from tqdm import tqdm
from src.datasetup.models.info.school_converter.seed_2018 import SchidSchoolid
from src.datasetup.models.master.school_qes.model import SchoolQes
from src.lib.read_config import ReadConfig2018
import os


_index_mapper = {
    'ユーザーID': 'inputter_id',
    '回答ステータス': 'status',
    '記入日': 'date_register',
    '入力した学校コード': 'sch_id',
    '入力した学校名': 'school_name',
    '対象名': 'inputter_name'
}

def seed():
    c = ReadConfig2018(path='src/setting.ini').get_setting()
    def get_path_list():
        _file_list = []
        for _year in ['2016', '2017', '2018']:
            file_path = {
                'root': 'data/original_data',
                '2018': 'H30データ/学校質問紙回答データ',
                '2017': 'H29データ/学校質問紙回答データ',
                '2016': 'H28データ/学校質問紙回答データ'
            }
            folder = os.path.join(file_path['root'], file_path[_year])
            for _file in os.listdir(folder):
                if _file.count('xlsx') > 0:
                    if _file.count('小学校') > 0:
                        _school_type = '小学校'
                    elif _file.count('中学校') > 0:
                        _school_type = '中学校'
                    else:
                        raise ValueError
                    file_choice = [[_year, _school_type, os.path.join(folder, _file)]]
                    _file_list.extend(file_choice)
        return _file_list

    df = pd.DataFrame()
    file_list = get_path_list()
    for year, school_type, file in tqdm(file_list):
        # file = file_list[0]
        df_raw = pd.read_excel(file, header=[0, 1, 2])
        columns = df_raw.columns.get_values()
        columns_rename = ['col{0}'.format(i) for i in range(len(columns))]
        columns_zipped = list(zip(columns_rename, columns))
        columns_content = [x for x, y in columns_zipped if y[2]=='回答内容']
        columns_no_content = [x for x, y in columns_zipped if x not in columns_content]
        columns_content_mapper_key1 = {col: val[0] for col, val in columns_zipped if col in columns_content}
        columns_content_mapper_key2 = {col: val[1] for col, val in columns_zipped if col in columns_content}
        columns_no_content_mapper_key2 = {col: _index_mapper[val[2]] for col, val in columns_zipped if col in columns_no_content}

        def rename_columns(dfx, new_columns):
            dfx.columns = new_columns
            return dfx

        df = (
            df_raw
            .pipe(rename_columns, new_columns=columns_rename)
            .melt(id_vars=columns_no_content, value_vars=columns_content, var_name='columns')
            .assign(
                key1 = lambda dfx: dfx['columns'].replace(columns_content_mapper_key1),
                key2 = lambda dfx: dfx['columns'].replace(columns_content_mapper_key2),
                school_type = school_type,
                year = year,
                year_answer = int(year),
                year_target = int(year)-1
            )
            .rename(columns=columns_no_content_mapper_key2)
            .append(other=df)
        )
    mapper_sch_id_school_id = SchidSchoolid(c).mapper_sch_id_school_id
    def item_to_na_if_not_in_list(dfx, _target, _list):
        dfx.loc[dfx[_target].isin(_list) == False, _target] = pd.np.nan
        return dfx

    print(df.shape)

    df_save = (
        df
        .assign(
            school_id = lambda dfx: dfx['sch_id'].replace(mapper_sch_id_school_id)
        )
        .pipe(item_to_na_if_not_in_list, 'school_id', list(mapper_sch_id_school_id.values()))
        .drop('sch_id', axis=1)
        .assign(
            key_unique=lambda dfx: dfx['key1'].str.cat(dfx[['school_type', 'year']].astype(str), sep='_', na_rep='na'),
        )
        [['key1', 'key2', 'school_id', 'school_type', 'year_answer', 'year_target', 'value']]

    )
    print(df_save.shape)
    s = SchoolQes(df_save)
    s.validate_convert()
    s.save(index=False)
    s.read()
    return df_save


# def quality_check():
#     """
#     いらないよ
#     :return:
#     """
#     def get_not_all_na_col(dfx):
#         dfx_count_have_value = dfx.count(axis=0)
#         return dfx_count_have_value[dfx_count_have_value > 0].index.tolist()
#
#     info_path = './data/info/学校質問/question_school.xlsx'
#     df = pd.read_excel(info_path, sheet_name='all', skiprows=3)
#     info = pd.read_excel(info_path, sheet_name='info', skiprows=0)
#     groupby_key = ['year_answer', 'school_type', 'grade', 'subject']
#     for i in range(info.shape[0]):
#         qes_combi = info.loc[i, ['統一', '検索用文章(正規表現を用いることがある)']].values.tolist()
#         we = df.loc[df['explanation2'].str.contains(qes_combi[1]) > 0, :]
#         if we.shape[0] == 0:
#             print(qes_combi)
#             continue
#         groupby_key_use = get_not_all_na_col(we[groupby_key])
#         tmp = we.fillna('missing').groupby(groupby_key_use).size()
#         if (tmp.reset_index() == 'missing').sum().sum() > 0:
#             print(qes_combi, tmp)
#     return []

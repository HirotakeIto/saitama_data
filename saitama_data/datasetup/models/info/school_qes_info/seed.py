from functools import reduce
import pandas as pd
from saitama_data.datasetup.models.info.school_qes_info.model import SchoolQestionInfo

__path_save__ = 'data/info/学校質問/qes_info.xlsx'
__paths_all_question_info__ = [
    "data/info/school_qes/school_qes_2016_j.csv",
    "data/info/school_qes/school_qes_2016_j.csv",
    "data/info/school_qes/school_qes_2016_p.csv",
    "data/info/school_qes/school_qes_2017_j.csv",
    "data/info/school_qes/school_qes_2017_p.csv",
    "data/info/school_qes/school_qes_2018_j.csv",
    "data/info/school_qes/school_qes_2018_p.csv",
    "data/info/school_qes/school_qes_2019_j.csv",
    "data/info/school_qes/school_qes_2019_p.csv",
]
__path_question_to_sq__ = 'data/info/school_qes/sq_regex_info.csv'



def read_all_question_info():
    def get_csv_sheet(path_x: str):
        return (
            pd.read_csv(path_x, skiprows=2)
            [['key1', 'key2', 'school_type', 'year', 'grade', 'subject', 'category', 'explanation1',
           'explanation2', 'answer', 'year_answer', 'year_target']]
            .assign(
                explanation_all = lambda dfx:dfx['explanation1'] + dfx['explanation2'] + dfx['answer']
            )
        )

    def get_iterator_df_question():
        paths = __paths_all_question_info__
        for path in paths:
            print("Getting dataframe of school qestion info: {path}'s".format(path=path))
            yield get_csv_sheet(path_x=path)

    return reduce(lambda x, y: pd.concat([x, y], axis=0), get_iterator_df_question())


def read_question_to_sq():
    path = __path_question_to_sq__
    return (
        pd.read_csv(path)
        .assign(
            sq = lambda dfx: dfx['統一'],
            exp_reg = lambda dfx: dfx['検索用文章(正規表現を用いることがある)']

        )
        [['sq', 'exp_reg']]
    )


def seed():
    def get_count_ref(dfx, ref, count_col='sq'):
        return (
            dfx
            .fillna('na')
            .groupby(ref)
            [count_col]
            .transform('size')
        )

    df_qes = read_all_question_info()
    df_info = read_question_to_sq()
    ## add sq to df_qes
    df_qes['sq'] = pd.np.nan # reset
    for info in df_info.to_dict(orient='record'):
        slicing = df_qes['explanation_all'].str.contains(info['exp_reg']) == True
        if df_qes.loc[slicing, 'sq'].notna().sum() > 0:
            print('{sq}: でsqが登録されています。更新をスキップします。'.format(sq = info))
            print('すでに入力があったものは次の通りです')
            print(df_qes.loc[slicing & df_qes['sq'].notna(), ['sq', 'explanation_all']].values)
            print('今回新しく入力されるものは次の通りです')
            print(df_qes.loc[slicing & df_qes['sq'].isna(), 'explanation_all'].values)
            continue
        else:
            df_qes.loc[slicing, 'sq'] = info['sq']
    df_qes_save = (
        df_qes
        .assign(
            key_unique = lambda dfx: dfx['key1'].str.cat(dfx[['school_type', 'year']].astype(str), sep='_', na_rep='na'),
            count_sq_level_school=lambda dfx: get_count_ref(
                dfx=dfx,
                ref=['year', 'school_type', 'sq'], count_col='sq'
            ),
            count_sq_level_grade=lambda dfx: get_count_ref(
                dfx=dfx,
                ref=['year', 'school_type', 'grade', 'sq'], count_col='sq'
            ),
            count_sq_level_subject=lambda dfx: get_count_ref(
                dfx=dfx,
                ref=['year', 'school_type', 'grade', 'subject', 'sq'], count_col='sq'
            )
        )
        [[
            'key_unique', 'key1', 'key2', 'school_type', 'year', 'grade', 'subject',
            'sq', 'count_sq_level_school', 'count_sq_level_grade', 'count_sq_level_subject',
            'year_answer', 'year_target', 'category', 'explanation1', 'explanation2', 'answer', 'explanation_all',
        ]]
    )
    sqi = SchoolQestionInfo(df_qes_save)
    sqi.validate()
    sqi.save()
    writer = pd.ExcelWriter(__path_save__, mode='a')
    df_qes_save.to_excel(writer, index=False, sheet_name='qes_info')
    writer.save()
    writer.close()

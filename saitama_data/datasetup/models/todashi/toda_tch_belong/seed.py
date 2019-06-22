import re
import pandas as pd
import numpy as np
from itertools import combinations, product
from saitama_data.datasetup.models.todashi.toda_teacher_qes.model import TodaTeacherQes
from saitama_data.datasetup.models.todashi.toda_old_tch_qes.model import TodaOldTchQes
from saitama_data.datasetup.models.todashi.toda_old_tch_noncog.model import TodaOldTchNoncog
from saitama_data.datasetup.models.todashi.engineer_mixin.school_name_mixin import SchoolNameMixin
from saitama_data.datasetup.models.todashi.toda_tch_belong.model import TodaTchBelong

path_2015 = 'data/original_data/todasi/todashi_data/2016/教員調査票データ入力-1.xlsx'


def df_reset_multi_columns(dfx):
    dfx.columns = ['_'.join(x) for x in dfx.columns.get_values()]
    return dfx


def get_columns_list_from_regex(dfx, regex_str):
    return dfx.filter(regex=regex_str).columns.tolist()


def rename_df_columns_from_regex(dfx, regex_str, new_name):
    column_keys_matched = {col: new_name for col in dfx.columns.tolist() if re.match(regex_str, col)}
    if len(column_keys_matched.keys()) != 1:
        raise KeyError('Too much or less')
    return dfx.rename(columns=column_keys_matched)


def seed_from_2015():
    def get_and_melt_teacher_class(dfx):
        selector_value_vars_regex = '(' + '|'.join(
            ['{0}年{1}組'.format(x, y) for x, y in product(range(1, 7), range(1, 9))]
        ) + ')'
        selector_id_vars_regex = '(' + '|'.join(['teacher_id', 'school_name']) + ')'
        id_vars = get_columns_list_from_regex(dfx, selector_id_vars_regex)
        value_vars = get_columns_list_from_regex(dfx, selector_value_vars_regex)
        return (
            dfx
            .melt(id_vars=id_vars, value_vars=value_vars, var_name='variable', value_name='is_tantou')
            .dropna(subset=['is_tantou'])
            .assign(
                grade_class = lambda dfxx: dfxx['variable'].str.extract(selector_value_vars_regex),
                t_grade = lambda dfxx: dfxx['grade_class'].str.extract('(\d)年\d組', expand=False).astype(float),
                t_class = lambda dfxx: dfxx['grade_class'].str.extract('\d年(\d)組', expand=False).astype(float),
            )
        )

    df = pd.DataFrame()
    for sheet_name in ['中学校', '小学校']:
        adjustment = 0 if sheet_name == '小学校' else 6
        df = (
            pd.read_excel(path_2015, header=[0, 1, 2], sheet_name=sheet_name)
            .rename_axis('school_name').reset_index()  # indexに学校名が入ってきてしまう（どうしても）
            .pipe(lambda dfx: df_reset_multi_columns(dfx))
            .pipe(lambda dfx: rename_df_columns_from_regex(dfx, '個人ID', 'teacher_id'))
            .pipe(lambda dfx: rename_df_columns_from_regex(dfx, 'school_name', 'school_name'))
            .pipe(lambda dfx: get_and_melt_teacher_class(dfx))
            .assign(
                year = 2015,
                t_grade = lambda dfx: dfx['t_grade'] + adjustment
            )
            .pipe(
                lambda dfx: SchoolNameMixin.assign_sch_name_to_sch_id(
                    dfx,
                    sch_name_col='school_name',
                    sch_id_col='school_id')
            )
            .rename(columns={'t_grade': 'grade', 't_class': 'class'})
            [['teacher_id', 'school_id', 'grade', 'class', 'year']]
            .append(df)
        )
    return df


def seed_todashi_class_from_db():
    """
    TodaTeacherQes の grade1-grade9カラムを用いてデータ化するクラス
    :return:
    """
    def assign_grade(dfx):
        """
        担当学年のカラムがgrade1...grade9まであるので、それを利用しmeltする
        """
        selector_value_vars_regex = '(' + '|'.join(
            ['grade{0}'.format(x) for x in range(1, 10)]
        ) + ')'
        selector_id_vars_regex = '(' + '|'.join(['year', 'teacher_id', 'school_id']) + ')'
        id_vars = get_columns_list_from_regex(dfx, selector_id_vars_regex)
        value_vars = get_columns_list_from_regex(dfx, selector_value_vars_regex)
        return (
            dfx
            .melt(id_vars=id_vars, value_vars=value_vars, var_name='variable', value_name='class_list')
            .assign(
                grade = lambda dfxx: dfxx['variable'].str.extract('grade(\d)', expand=False).astype(float)
            )
            [id_vars + ['grade', 'class_list']]
        )

    def assign_class(dfx, class_list_col):
        """
        担当クラスは文字列で入っているので、担当クラスを表すカラムを作ったあと、meltしてデータを作る。
        """
        id_vars = get_columns_list_from_regex(dfx, regex_str="[^({0})]".format(class_list_col))
        for kumi in range(1, 10):
            dfx[kumi] = dfx[class_list_col].str.contains('{0}組'.format(kumi))
        return (
            dfx
            .melt(id_vars=id_vars, value_vars=range(1, 10), var_name='class', value_name='is_exist')
            .pipe(lambda dfxx: dfxx[dfxx['is_exist'] == True])
            .drop(['is_exist'], axis=1)
        )
    return (
        TodaTeacherQes().read().data
        .pipe(lambda dfx: assign_grade(dfx))
        .pipe(lambda dfx: assign_class(dfx, class_list_col='class_list'))
        .dropna(subset=['teacher_id'])
    )


def seed_todashi_main_class_from_db():
    """
    TodaTeacherQesのt_grade, t_classカラム
    TodaOldTchQesのt_grade, t_classカラム
    TodaOldTchNoncogのgrade, classカラムを用いててデータ化する関数
    :return:
    """
    def parser_grade(x: str):
        if type(x) != str:
            return np.nan
        try:
            return float(x)
        except ValueError:
            return np.nan

    def parser_class(x: str):
        if type(x) != str:
            return np.nan
        if x.count('組') > 0:
            return float(x[0])
        else:
            return np.nan

    def adjust_junior_school_grade(dfx):
        dfx.loc[dfx['school_id'] > 30000, 'grade'] = dfx['grade'] + 6
        return dfx

    get_grade = lambda series: series.str.split('年').apply(lambda x: parser_grade(x[0]))
    get_class = lambda series: series.str.split('年').apply(lambda x: parser_class(x[1]) if len(x) == 2 else np.nan)

    ttq = TodaTeacherQes().read()
    totn = TodaOldTchNoncog().read()
    totq = TodaOldTchQes().read()
    df1 = (
        ttq.data
        [['t_grade', 't_class', 'teacher_id', 'school_id', 'year']]
        .rename(columns={'t_grade': 'grade', 't_class': 'class'})
        .dropna(subset=['teacher_id'], how='all')
        [['teacher_id', 'school_id', 'year', 'grade', 'class']]
    )
    df2 = (
        totn.data
        .assign(
            **{'grade': lambda dfx: get_grade(dfx['t_grade_class']),
               'class': lambda dfx: get_class(dfx['t_grade_class'])}
        )
        .pipe(adjust_junior_school_grade)
        .dropna(subset=['teacher_id'], how='all')
        [['teacher_id', 'school_id', 'year', 'grade', 'class']]
    )
    df3 = (
        totq.data
        [['t_grade', 't_class', 'teacher_id', 'school_id', 'year']]
        .assign(
            t_grade=lambda dfx: pd.to_numeric(dfx['t_grade'], errors='coerce'),
            t_class=lambda dfx: pd.to_numeric(dfx['t_class'], errors='coerce')
        )
        .rename(columns={'t_grade': 'grade', 't_class': 'class'})
        .pipe(adjust_junior_school_grade)
        .dropna(subset=['teacher_id'], how='all')
        [['teacher_id', 'school_id', 'year', 'grade', 'class']]
    )
    df = (
        pd.concat([df1, df2, df3], axis=0)
        .dropna(subset=['school_id', 'grade', 'class'])
        .drop_duplicates(subset=['year', 'teacher_id'])
        .assign(is_main = 1)
    )
    return df


def seed():
    df = pd.DataFrame()
    for func_create_data in [seed_from_2015, seed_todashi_class_from_db, seed_todashi_main_class_from_db]:
        df = df.append(func_create_data())
    ttb = TodaTchBelong(data=df)
    ttb.adjust_schema().validate_convert()
    ttb.save()

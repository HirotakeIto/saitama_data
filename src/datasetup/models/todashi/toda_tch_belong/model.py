"""
めんどいから全てのファイルをここに集約
"""
from pandas_schema import Column, Schema
from pandas_schema.validation import CanConvertValidation
from src.datasetup.models.todashi.toda_teacher_qes.model import TodaTeacherQes
from src.datasetup.models.todashi.toda_old_tch_qes.model import TodaOldTchQes
from src.datasetup.models.todashi.toda_old_tch_noncog.model import TodaOldTchNoncog
from src.datasetup.models.base.schema import BaseSchema
from src.datasetup.models.mix_in.accepter_mixin import AccepterMixin
# from src.datasetup.models.util import get_schema_str

class TodaTchBelongSchema(BaseSchema):
    column_type = {
        "grade": float,
        "class": float,
        "teacher_id": float,
        "school_id": float,
        "year": int
    }
    schema = Schema([
        Column("grade", [CanConvertValidation(float)]),
        Column("class", [CanConvertValidation(float)]),
        Column("teacher_id", [CanConvertValidation(float)]),
        Column("school_id", [CanConvertValidation(float)]),
        Column("year", [CanConvertValidation(int)])
    ])
    convert_list = column_type


def seed():
    import numpy as np
    import pandas as pd
    import re
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
    get_class = lambda series: series.str.split('年').apply(lambda x: parser_class(x[1]) if len(x) ==2 else np.nan)



    ttq = TodaTeacherQes().read()
    totn = TodaOldTchNoncog().read()
    totq = TodaOldTchQes().read()
    df1 = (
        ttq.data
        [['t_grade', 't_class', 'teacher_id', 'school_id', 'year']]
        .rename(columns = {'t_grade': 'grade', 't_class': 'class'})
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
            t_grade = lambda dfx: pd.to_numeric(dfx['t_grade'], errors='coerce'),
            t_class = lambda dfx: pd.to_numeric(dfx['t_class'], errors='coerce')
        )
        .rename(columns = {'t_grade': 'grade', 't_class': 'class'})
        .pipe(adjust_junior_school_grade)
        .dropna(subset=['teacher_id'], how='all')
        [['teacher_id', 'school_id', 'year', 'grade', 'class']]
    )
    df = (
        pd.concat([df1, df2, df3], axis=0)
        .dropna(subset=['school_id', 'grade', 'class'])
        .drop_duplicates(subset=['year', 'teacher_id'])
    )
    totqs = TodaTchBelongSchema(df)
    totqs.validate()
    return df


class AdhocIOMixIn:
    def read(self):
        self.data =  seed()
        return self

    def save(self):
        raise NotImplementedError('This method must not be called')


class TodaTchBelong(AdhocIOMixIn, AccepterMixin, TodaTchBelongSchema):
    """
    ttb = TodaTchBelong().read()
    """
    pass


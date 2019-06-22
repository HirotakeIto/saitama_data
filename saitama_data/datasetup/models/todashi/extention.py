from saitama_data.datasetup.models.todashi.toda_teacher_qes.model import TodaTeacherQes
from saitama_data.datasetup.models.todashi.toda_old_tch_qes.model import TodaOldTchQes
from saitama_data.datasetup.models.todashi.toda_old_tch_noncog.model import TodaOldTchNoncog
from saitama_data.datasetup.models.mix_in.io_mixin.basic import BaseIOMixIn
import pandas as pd


class TeacherInfo(BaseIOMixIn):
    def read(self, **argv):
        check_prime_school = lambda dfx, col: pd.np.where(dfx[col] < 30000, 1, 0)
        check_math = lambda dfx, col: pd.np.where(dfx[col].str.contains('(数|算)') == True, 1, 0)
        check_kokugo = lambda dfx, col: pd.np.where(dfx[col].str.contains('国語') == True, 1, 0)
        check_eng = lambda dfx, col: pd.np.where(dfx[col].str.contains('(外国語|英)') == True, 1, 0)
        check_women = lambda dfx, col: pd.np.where(dfx[col].str.contains('女') == True, 1, 0)
        get_carrer = lambda dfx, col: dfx[col].str.extract('(\d+)(?:年目)*', expand=False).astype(float)
        df1 = (
            TodaTeacherQes().read().data
            .dropna(subset=['teacher_id'])
            [['teacher_id', 'year', 'school_id', 'age', 'sex', 'position', 'career', 'career_school', 'subject']]
            .assign(
                is_prime=lambda dfx: check_prime_school(dfx, 'school_id'),
                is_math=lambda dfx: check_math(dfx, 'subject'),
                is_kokugo=lambda dfx: check_kokugo(dfx, 'subject'),
                is_eng=lambda dfx: check_eng(dfx, 'subject'),
                career_num=lambda dfx: get_carrer(dfx, 'career'),
                career_school_num=lambda dfx: dfx['career_school'].astype(float),
                age_num = lambda dfx: dfx['age'].astype(float),
                is_women = lambda dfx: check_women(dfx, 'sex'),
            )
            [['teacher_id', 'year', 'age_num', 'is_women',
              'is_prime', 'is_math', 'is_kokugo', 'is_eng', 'career_num', 'career_school_num']]
        )
        df2 = (
            TodaOldTchQes().read().data
            .dropna(subset=['teacher_id'])
            [['teacher_id', 'year', 'school_id', 'subject']]
            .assign(
                is_prime=lambda dfx: check_prime_school(dfx, 'school_id'),
                is_math=lambda dfx: check_math(dfx, 'subject'),
                is_kokugo=lambda dfx: check_kokugo(dfx, 'subject'),
                is_eng=lambda dfx: check_eng(dfx, 'subject'),
            )
            [['teacher_id', 'year', 'is_prime', 'is_math', 'is_kokugo', 'is_eng']]
        )
        df3 = (
            TodaOldTchNoncog().read().data
            .dropna(subset=['teacher_id'])
            [['teacher_id', 'year', 'school_id', 'age', 'sex', 't_k1', 't_k2', 't_k3']]
            .assign(
                is_prime=lambda dfx: check_prime_school(dfx, 'school_id'),
                is_math=lambda dfx: check_math(dfx, 't_k3'),
                is_kokugo=lambda dfx: check_kokugo(dfx, 't_k3'),
                is_eng=lambda dfx: check_eng(dfx, 't_k3'),
                career_num=lambda dfx: get_carrer(dfx, 't_k1'),
                career_school_num=lambda dfx: get_carrer(dfx, 't_k2'),
                age_num=lambda dfx: dfx['age'].astype(float),
                is_women=lambda dfx: check_women(dfx, 'sex'),
            )
            [['teacher_id', 'year', 'age_num', 'is_women',
              'is_prime', 'is_math', 'is_kokugo', 'is_eng', 'career_num', 'career_school_num']]
        )

        def fillna_year_value(dfx, year_col, value_col):
            value = (dfx[year_col] - dfx[value_col]).max()
            return dfx.fillna({value_col: dfx[year_col] - value})

        df = (
            pd.DataFrame()
            .append(df1).append(df2).append(df3)
            .melt(id_vars=['teacher_id', 'year'])
            .dropna()  # NAカラムを排除する
            .sort_values('value', ascending=False).groupby(['teacher_id', 'year', 'variable']).first().reset_index()  # ダブっている場合値が大きい方を採用する
            .set_index(['teacher_id', 'year', 'variable'])['value'].unstack(-1).reset_index()
            # 最後特定の年度で存在しないカラムについても科目と年齢については埋め合わせるコードを書く
            .pipe(
                lambda dfx: dfx.groupby('teacher_id').apply(fillna_year_value, year_col='year', value_col='age_num').reset_index(drop=True))
            .pipe(
                lambda dfx: dfx.groupby('teacher_id').apply(fillna_year_value, year_col='year', value_col='career_num').reset_index(drop=True))
            .pipe(
                lambda dfx: dfx.groupby('teacher_id').apply(fillna_year_value, year_col='year', value_col='career_school_num').reset_index(drop=True))
        )
        self.data = df
        return self
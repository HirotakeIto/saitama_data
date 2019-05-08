import pandas as pd
import re
from src.datasetup.models.master.school_qes.schema import SchoolQesSchema
from src.datasetup.models.master.school_qes.student_qestion_info import SchoolQestionInfo
from src.datasetup.models.mix_in.io_mixin import CsvIOMixin


class SchoolQes(CsvIOMixin, SchoolQesSchema):
    """
    s = SchoolQes()
    s.fetch(['s51'])
    """
    path = './data/db/master/school_qes.csv'
    key_tidy = ['year_target', 'school_id', 'grade', 'subject']
    question_col = 'squestion'
    value_col = 'value'

    def __init__(self, data=None, **argv):
        print("""
        s74が今不完全です。今、質問文をフックに回答とquestionareを紐づけているんだけれど、
        それがうまくいってない。s74とs57を別認識したいんだけど一緒にしかできなかった。
        紐付けをこのクラスでやるべきじゃない。
        """)
        self.question_cls = SchoolQestionInfo()
        self.data = data if data is not None else self.build()

    def build(self):
        """
        tidy になっているデータ（create_dataで作成）を読み込み、SchoolQestionInfoから統一化した質問コードをmergeする
        """
        def give_search_key(dfx):
            return dfx['key1'] + '_' + dfx['year_answer'].astype(int).astype(str)  + '_' +  dfx['school_type'].replace(
                {'小学校': 'p', '中学校': 'j'})

        def print_df_shape(dfx):
            print(dfx.shape)
            return dfx

        self.read()
        df_res = (
            self.data
            .pipe(print_df_shape)
            .assign(
                search_key=lambda df: give_search_key(df),
            )
            .pipe(lambda dfx: pd.merge(dfx, self.question_cls.get_qes_info(), on='search_key', how='left'))
            .dropna(subset=['school_id'])
            .pipe(print_df_shape)
        )
        return df_res

    def fetch(self, squestion: list, is_numeric=True):
        question_col = self.question_col
        value_col = self.value_col
        value_parser = self.value_parser
        df = self.data
        df_return = (
            df
            .loc[df[question_col].isin(squestion), :]
            .assign(
                new_value = lambda dfx: value_parser(dfx[value_col]) if is_numeric is True else dfx[value_col]
            )
            .drop(value_col, axis=1)
            .rename(columns={'new_value': value_col})
        )
        return df_return

    def fetch_gather(self, squestion: list, is_numeric=True):
        """
        tidyになっているデータから、質問項目を抜き取る。
        """
        key_tidy = self.key_tidy
        question_col = self.question_col
        value_col = self.value_col
        df_return = (
            self.fetch(squestion=squestion, is_numeric=is_numeric)
            # .pipe(lambda aa: pdb.set_trace())
            .set_index(key_tidy + [question_col])[value_col].unstack(level=-1)
            .reset_index()
        )
        return df_return

    @staticmethod
    def value_parser(series: pd.Series):
        def remove_often_pattern(series_tmp):
            remove_pattern = ['-', '‐', ' ', 'o']
            for pattern in remove_pattern:
                series_tmp = series_tmp.str.replace(pattern, '')
            series_tmp = series_tmp.replace('', pd.np.nan)
            return series_tmp

        def change_numeric_only_can_change_value(series_tmp):
            slicing = (series_tmp.str.isnumeric()) & (series_tmp.isna() == False)
            series_tmp.loc[slicing] = series_tmp.loc[slicing].astype(float)
            return series_tmp

        return (
            series
            .pipe(remove_often_pattern)
            .pipe(change_numeric_only_can_change_value)
            .astype(float)
        )


class SchoolQesAccepter(SchoolQes):
    """
    sq = SchoolQesAccepter(squestion=['s1', 's4', 's33', 's36', 's69', 's70', 's71', 's50'], is_numeric=True)
    sq.how_tidy_parser(sq.data)
    sq.pdb()
    """
    def __init__(self, squestion: list, is_numeric=True, merge_key_rename_mapper = None, **argv):
        super().__init__(**argv)
        self.data = (
            self.fetch_gather(squestion=squestion, is_numeric=is_numeric)
            # .replace({'all': pd.np.nan})
        )
        self.merge_key_rename_mapper = {
            'year_target': 'year',
            'school_id': 'school_id',
            'grade': 'grade'
        } if merge_key_rename_mapper is None else merge_key_rename_mapper
        self.subject_mapper = {
            '国語': 'kokugo',
            '算数': 'math',
            '数学': 'math',
            '英語': 'eng'
        }

    @staticmethod
    def how_tidy_parser(df, squestion_pattern = 's\d'):
        """
        どのようなパターンでtidyになっているかを解析する。
        tidyになっているときは、所定のパターンでレコードが一意になっているはずなので、それを認識する
        """
        merge_key_choice_list = [
            ['school_merge', ['year_target', 'school_id']],
            ['grade_merge', ['year_target', 'school_id', 'grade']],
            ['subject_merge', ['year_target', 'school_id', 'grade', 'subject']]
        ]
        merge_name_key_squestion = []
        target_choice = [col for col in df.columns if re.match(squestion_pattern, col)]
        for merge_name, merge_key_choice in merge_key_choice_list:
            sque_count = df.groupby(merge_key_choice)[target_choice].count().max()
            tidy_sque_list = sque_count[sque_count==1].index.tolist()
            merge_name_key_squestion.append([merge_name, merge_key_choice, tidy_sque_list])
            [target_choice.remove(x) for x in tidy_sque_list]
        return merge_name_key_squestion


    @staticmethod
    def get_merge_tidy_data(df, merge_key, tidy_value_list):
        def check_unique(dfx, tidy_key):
            if dfx.groupby(tidy_key).size().max() != 1:
                raise ValueError('dont tidy')
            return dfx

        return (
            df
            [merge_key + tidy_value_list]
            .dropna(subset=tidy_value_list, how='all')
            .pipe(check_unique, tidy_key=merge_key)
        )

    def accpet(self, v):
        df = self.data
        merge_name_key_squestion = self.how_tidy_parser(df)
        for name, merge_key, squestion in merge_name_key_squestion:
            if name != 'subject_merge':
                self._accept(
                    v = v,
                    data = (
                        self
                        .get_merge_tidy_data(df=df, merge_key=merge_key, tidy_value_list=squestion)
                        .rename(columns = self.merge_key_rename_mapper)
                    ),
                    merge_key = merge_key
                )
            elif name == 'subject_merge':
                print('科目ごとに関してはめっちゃ特殊な処理を行う、、、ハードコード満載でしんどい')
                def rename_muluti_index(dfx):
                    dfx.columns = ['_'.join(x) for x in dfx.columns.get_values().tolist()]
                    return dfx

                subject_merge_key = ['year_target', 'school_id', 'grade']
                df_merge = (
                        self.get_merge_tidy_data(df=df, merge_key=merge_key, tidy_value_list=squestion)
                        .assign(
                            new_subject = lambda dfx: dfx['subject'].replace(self.subject_mapper)
                        )
                        .pivot_table(index=subject_merge_key, columns=['new_subject'], values=squestion)
                        .pipe(rename_muluti_index)
                        .reset_index()
                        .rename(columns=self.merge_key_rename_mapper)
                )
                self._accept(
                    v = v,
                    data = df_merge,
                    merge_key = subject_merge_key
                )

    @staticmethod
    def _accept(v, data, merge_key):
        print('before accept, data shape is', v.data.shape)
        v.data = pd.merge(v.data, data, on=merge_key, how='left')
        print('after accept, data shape is', v.data.shape)


    def pdb(self):
        squestion = ['s1', 's4', 's33', 's36', 's69', 's70', 's71', 's50']
        is_numeric = True
        import pdb;pdb.set_trace()
        return self


import pandas as pd
from functools import reduce
import re


class EyeData20162017:
    def read(self):
        return reduce(lambda x, y: pd.concat([x, y], axis=0), [self.read2016(), self.read2017()])

    @staticmethod
    def columns_parser(column_item: str):
        for (old, new) in (('\u3000', ''), ('\n', ''), ('、', ''), ('。', ''), ('　', ''), ('\s', '')):
            column_item = re.sub(old, new, column_item)
        return column_item

    def read_columns_mapper(self):
        columns_parser = self.columns_parser
        path1 = 'data/other/primary_low_grade/eyedata/columns.xlsx'
        columns_mapper = (
            pd.read_excel(path1, sheet_name='columns')
            .append(
                pd.DataFrame(
                    [['出席番号', 'num'], ['名前※）男女別総合肯定率順', 'id']],
                    columns=['item', 'name'])
            )
            .assign(
                item=lambda dfx: dfx['item'].apply(columns_parser)
            )
            .drop_duplicates()
            .set_index('item')
            .to_dict()['name']
        )
        return columns_mapper

    def read2016(self):
        # setting
        columns_parser = self.columns_parser
        columns_mapper = self.read_columns_mapper()
        path_list = [
            'data/other/primary_low_grade/eyedata/2016 2110000005 戸田市立笹目小学校 第1学年 ④アイ(10).xlsx',
            'data/other/primary_low_grade/eyedata/2016 2110000005 戸田市立笹目小学校 第2学年 ④アイ(20).xlsx',
            'data/other/primary_low_grade/eyedata/2016 2110000005 戸田市立笹目小学校 第3学年 ④アイ(30).xlsx',
            'data/other/primary_low_grade/eyedata/2016 2110000021 戸田市立笹目東小学校 第1学年 ④アイ(10).xlsx',
            'data/other/primary_low_grade/eyedata/2016 2110000021 戸田市立笹目東小学校 第2学年 ④アイ(20).xlsx',
            'data/other/primary_low_grade/eyedata/2016 2110000021 戸田市立笹目東小学校 第3学年 ④アイ(30).xlsx'
        ]
        def read_engineer(dfx: pd.DataFrame, columns_mapper: dict):
            def print_memo(dfx_tmp):
                print('GETTED ID is', dfx_tmp['id'].values.tolist())
                print('DUPLICATED ID is', dfx_tmp.loc[dfx_tmp.duplicated(subset=['id', 'num']), 'id'].values.tolist())
                return dfx_tmp

            dfx.columns = [columns_parser(item) for item in dfx.columns]
            return (
                dfx
                .rename(columns=columns_mapper)
                .filter(regex='^(num|id|q\d+)$', axis=1)
                .dropna(subset=['id'])
                .pipe(print_memo)
                .drop_duplicates(subset=['id', 'num'])
            )
        # start
        df = pd.DataFrame()
        for path in path_list:
            dfs_dict = pd.read_excel(path, sheet_name=None)
            sheet_name_list = [sheet for sheet in dfs_dict.keys() if sheet.count('一覧')]
            for sheet_name in sheet_name_list:
                # sheet_name = '【一覧】1_1 11'
                print(path, sheet_name)
                df = (
                    # １つのシートに２つの表があるので横につなげる
                    pd.concat(
                        [(
                            pd.read_excel(path, sheet_name=sheet_name, skiprows=range(0, 6), header=[0], skip_footer=68)
                            .pipe(read_engineer, columns_mapper=columns_mapper)
                            .set_index(['id', 'num'])
                        ), (
                            pd.read_excel(path, sheet_name=sheet_name, skiprows=range(0, 67), header=[0], skip_footer=7)
                            .pipe(read_engineer, columns_mapper=columns_mapper)
                            .set_index(['id', 'num'])
                        )],
                        axis=1
                    )
                        # シート外情報を加える
                    .assign(
                        year=2016,
                        school='笹目東'
                    )
                    .reset_index()
                    .melt(id_vars=['year', 'id', 'num'], value_name='value', var_name='key')
                    .append(df)
                )
        return df

    def read2017(self):
        # setting
        columns_parser = self.columns_parser
        columns_mapper = self.read_columns_mapper()
        # start
        def convert_multi_columns_to_single_columns(dfx: pd.DataFrame):
            def parser(s: str):
                if s.count('Unnamed') + s.count('設問') > 0:
                    return None
                if len(s) == 0:
                    return None
                return columns_parser(s)

            item_exist = lambda s: s is not None
            multicols_item_parser = lambda cols_item: filter(item_exist, map(parser, cols_item))
            # start
            dfx.columns = [''.join(multicols_item_parser(item)) for item in dfx.columns.get_values()]
            return dfx

        path1 = 'data/other/primary_low_grade/eyedata/2017 2110006005 戸田市立笹目小学校 ⑭素点データ.xlsx'
        path2 = 'data/other/primary_low_grade/eyedata/2017 2110006009 戸田市立笹目東小学校 ⑭素点データ.xlsx'
        path_list = [path1, path2]
        df = pd.DataFrame()
        for path in path_list:
            print(path)
            dfs_dict = pd.read_excel(path, sheet_name=None, header=[0, 1])
            sheet_name_list = [sheet for sheet in dfs_dict.keys()]
            for sheet in sheet_name_list:
                print(sheet)
                df = (
                    dfs_dict[sheet]
                    .pipe(convert_multi_columns_to_single_columns)
                    .rename(columns=columns_mapper)
                    .filter(regex='^(num|id|q\d+)$', axis=1)
                    .assign(
                        year=2017,
                        school='sasasa',
                        grade=1,
                    )
                    .melt(id_vars=['year', 'school', 'id', 'num'], value_name='value', var_name='key')
                    .append(df)
                )
        return df


class BasicQes20162017:
    def build(self):
        df = (
            self.read()
            .pipe(lambda dfx: self.enginner(dfx))
        )
        return df

    @staticmethod
    def read():
        ## setup
        mapper_jar_eng = {
            'index': 'id',
            '入学日': 'day_enter',
            '学年': 'grade',
            '組': 'class',
            '番号': 'num',
            '性別': 'sex',
            '生年月日': 'date',
            '児童扶養手当': 'allowance',
            '就学援助': 'attendance_support',
            '生活保護': 'public_assistance',
            'ひとり親医療': 'single',
            '障害者手帳_身体': 'handicap_physical',
            '障害者手帳_知的': 'handicap_intell',
            '障害者手帳_精神': 'handicap_mental',
            'ｂ＆ｇ_利用歴': 'history',
            'ｂ＆ｇ_利用期間': 'term',
            'こくご': 'jpn',
            'こくご_観点別正答率_国語への関心・意欲・態度': 'jpn_attitude',
            'こくご_観点別正答率_話す・聞く能力': 'jpn_speak',
            'こくご_観点別正答率_書く能力': 'jpn_writing',
            'こくご_観点別正答率_読む能力': 'jpn_reading',
            'こくご_観点別正答率_言語についての知識・理解・技能': 'jpn_skill',
            'さんすう': 'math',
            'さんすう_観点別正答率_算数への関心・意欲・態度': 'math_attitude',
            'さんすう_観点別正答率_数学的な考え方': 'math_think',
            'さんすう_観点別正答率_数量や図形についての技能': 'math_skill',
            'さんすう_観点別正答率_数量や図形についての知識・理解': 'math_understanding',
            '全受検教科_正答率\nの合計': 'sum',
            '全受検教科_正答率\nの平均': 'mean',
            '自己肯定感': 'selfesteem',
            '学級適応感': 'school_adjustment',
            '規律と他者の尊重': 'principle',
            'クラスの言葉の力': 'peer_power',
            '生活・学習習慣': 'habit',
            '身長': 'height',
            '体重': 'weight'
        }
        info_dict_list = [
            {'path': 'data/other/primary_low_grade/basicqes/H28戸田市データ.xlsx', 'year': '2016'},
            {'path': 'data/other/primary_low_grade/basicqes/H29戸田市データ.xlsx', 'year': '2017'},
            # {'path': 'data/other/primary_low_grade/eyedata/2017 2110006005 戸田市立笹目小学校 ⑭素点データ.xlsx'},
            # {'path': 'data/other/primary_low_grade/eyedata/2017 2110006009 戸田市立笹目東小学校 ⑭素点データ.xlsx'},
            # {'path': 'data/other/primary_low_grade/eyedata/2016 2110000021 戸田市立笹目東小学校 第1学年 ④アイ(10).xlsx'}
        ]
        def parser(s: str):
            if s.count('Unnamed') > 0:
                return None
            if len(s) == 0:
                return None
            return s
        item_exist = lambda s: s is not None
        multicols_item_parser = lambda cols_item: filter(item_exist, map(parser, cols_item))
        def convert_multi_columns_to_single_columns(dfx: pd.DataFrame):
            dfx.columns = ['_'.join(multicols_item_parser(item)) for item in dfx.columns.get_values()]
            return dfx
        # start
        df_summary = pd.DataFrame()
        for info_dict in info_dict_list:
            dfs_dict = pd.read_excel(info_dict['path'], sheet_name=None, header=[0, 1, 2])
            df_summary_year = pd.DataFrame()
            for [school, df] in dfs_dict.items():
                print(school)
                df_summary_year = (
                    df
                    .reset_index()
                    .pipe(lambda dfx: convert_multi_columns_to_single_columns(dfx))
                    .pipe(lambda dfx: dfx.rename(columns=mapper_jar_eng))
                    .assign(
                        year = info_dict['year'],
                        school = school
                    )
                    .append(df_summary_year)
                )
            df_summary = df_summary_year.append(df_summary)
        print(df_summary.columns)
        return df_summary

    @staticmethod
    def enginner(
            df,
            date_col='date',
            height_col='height', weight_col='weight',
            sex_col='sex', num_col = 'num'
    ):
        # date_col = 'date'
        # value_col = 'date'
        # height_col = 'height'
        # weight_col = 'weight'
        # sex_col = 'sex'
        # num_col = 'num'
        df[date_col] = pd.to_datetime(df[date_col])
        df['month'] = df[date_col].dt.month
        df['relative_age'] = pd.np.where(df['month'] <= 3, 3 - (df['month']), 15 - df['month'])
        df[height_col] = pd.to_numeric(df[height_col])
        df[weight_col] = pd.to_numeric(df[weight_col])
        df['bmi'] = df[weight_col]/((df[height_col]/100)**2)
        df[sex_col] = df[sex_col].replace({'女': 2, '男': 1})
        df['women'] = pd.np.where(df[sex_col] == 2, 1, 0)
        df[num_col] = pd.to_numeric(df[num_col])
        return df




# 先生からほめられて、うれしかったことがありますか。	q22
# せんせいからほめられて、うれしかったことがありますか。	q29
# せんせいからほめられて、うれしかったことが　ありますか。	q131

# 学校では、先生の話を、よく聞いていますか。	q54
# がっこうでは、せんせいの　はなしを　よく　きいていますか。	q9

# つらいことや、こまったことを、学校の先生にそうだんできますか。	q98
# かなしいことや　こまったことが　あったとき、せんせいに　おはなしできますか。	q63

# 先生はクラスのみんなのことを、ほめてくれますか。	q62
# あなたは、先生や家の人から、期待されているな、と感じることがありますか。（※家の人とは、今いっしょにくらしている人のことです）	q42
# 先生は、あなたの気もちを分かってくれますか。	q86
# せんせいは　あなたの　はなしを　よく　きいて　くれますか。	q69


# おうちで、１しゅうかんに　なん日くらい　べんきょうしますか。	q81
# 家で、週に何日くらいべんきょうしますか。	q104

# 学校のじゅぎょういがいに、平日（月～金）は、１日にどれくらいべんきょうをしますか。（※じゅくなどでのべんきょう時間もふくみます）	q110
# 土日や祭日など、学校が休みの日は、１日にどれくらいべんきょうをしますか。（※じゅくなどでのべんきょう時間もふくみます）	q111
# 土日や祝日など、学校が休みの日は、１日にどれくらいべんきょうをしますか。（※じゅくなどでのべんきょう時間もふくみます）	q126

# おうちでの　べんきょうじかんを、きめていますか。	q83
# １日のべんきょう時間はこれくらいと、めやすをきめていますか。	q105

def notebook2018():
    def print_unique_val(dfx, col):
        print(dfx[col].unique().tolist())
        return dfx

    def to_numeric(dfx, col_list):
        basic_mapper = {'-': '', ' ': '', "-": '', '○○': 1, '○': 2, '▼': 3, '▼▼': 4} # regexで空文字列に置換する。

        for col in col_list:
            dfx[col] = pd.to_numeric(dfx[col].replace(basic_mapper, regex=True))
        # q110
        unique_mapper_list = [
            {'col': 'q81', 'mapper': {'毎日': 1, '４～５日': 2, '２～３日': 3, '１日': 4, '0': 5}},
            {'col': 'q110', 'mapper': {'2h~': 1, '2h': 2, '1h': 3, '30分': 4, '15分': 5, 'しない': 6}},
            {'col': 'q111', 'mapper': {'2h~': 1, '2h': 2, '1h': 3, '30分': 4, '15分': 5, 'しない': 6}},
        ]
        for col_mapper in unique_mapper_list:
            col = col_mapper['col']
            mapper = col_mapper['mapper']
            if col in dfx.columns.tolist():
                print(col)
                dfx[col] = dfx[col].replace(basic_mapper, regex=True)
                dfx[col] = pd.to_numeric(dfx[col].replace(mapper, regex=True))
        return dfx


    def _engineer_somecols_to_single_val_and_reverse(dfx, cols):
        return pd.Series(
            pd.np.where(
                dfx[cols].count(axis=1)==1,
                dfx[cols].max().max() - dfx[cols].sum(axis=1, skipna=True) + 1,
                pd.np.nan),
            index=dfx.index)

    def engineer_studyday_weekday(dfx, cols=['q81', 'q104']):
        return _engineer_somecols_to_single_val_and_reverse(dfx, cols=cols)


    def engineer_studytime_holiday(dfx, cols=['q111', 'q126']):
        return _engineer_somecols_to_single_val_and_reverse(dfx, cols=cols)


    def engineer_studytime_weekday(dfx, col = 'q110'):
        return pd.Series(
            pd.np.where(
                dfx[col].notnull(), dfx[col].max() - dfx[col] + 1, pd.np.nan),
            index=dfx.index)


    def engineer_tch_praise(dfx, cols=['q22', 'q131']):
        print('q29 も必要なんだけど、データにレコードがない！')
        return _engineer_somecols_to_single_val_and_reverse(dfx, cols=cols)


    def engineer_tch_listen(dfx, cols=['q9', 'q54']):
        return _engineer_somecols_to_single_val_and_reverse(dfx, cols=cols)


    def engineer_tch_advice(dfx, cols=['q63', 'q98']):
        return _engineer_somecols_to_single_val_and_reverse(dfx, cols=cols)


    df_bq = (
        pd.read_csv('./data/db/external/primary_low_grade/basic_qes.csv')
        .assign(
            id = lambda dfx: dfx['id'].astype(float),
            year = lambda dfx: dfx['year'].astype(float),
        )
    )
    fetch_col_numeric = ['q22', 'q26', 'q91', 'q126', 'q104', 'q131', 'q63', 'q98', 'q9', 'q54']
    fetch_col_other = ['q110', 'q111', 'q81', 'q83']
    fetch_cols = fetch_col_numeric + fetch_col_other
    df_eye = (
        pd.read_csv('./data/db/external/primary_low_grade/eye_data.csv')
        .pipe(lambda dfx: dfx.loc[dfx['key'].isin(fetch_cols), :])
        .set_index(['year', 'id', 'key'])
        .pipe(print_unique_val, col='value')
        ['value']
        .unstack(-1).reset_index()
        .pipe(to_numeric, col_list=fetch_col_numeric)
        .assign(**{
            'studyday_weekday': engineer_studyday_weekday,
            'studytime_holiday': engineer_studytime_holiday,
            'studytime_weekday': engineer_studytime_weekday,
            'tch_praise': engineer_tch_praise,
            'tch_listen': engineer_tch_listen,
            'tch_advice': engineer_tch_advice,
        })
    )
    df = pd.merge(df_bq, df_eye, on=['id', 'year'], how='left')
    df.to_csv('./notebooks/Yamaguchi/RAE/data/low_grade_toda.csv')

def main():
    bq = BasicQes20162017().build()
    bq.to_csv('./data/db/external/primary_low_grade/basic_qes.csv', index=False)
    ed = EyeData20162017().read()
    ed.to_csv('./data/db/external/primary_low_grade/eye_data.csv', index=False)

#src/datasetup/models/external/primary_low_grade
# df = ed.loc[ed['key'] == 'q81', : ]
# qqq = df.set_index(['id', 'year'])['value']
# qqq.value_counts()
# qqq.unstack(-1).dropna().groupby([2016])[2017].value_counts()
#
# value_mapper = {
#     'all': {'○○': 1, '○': 2, '▼': 3, '▼▼': 4},
#     'q81': {'毎日': 1, '４～５日': 2, '２～３日': 3, '１日': 4, '0': 5}
# }
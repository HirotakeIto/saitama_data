import pandas as pd

class SchoolQestionInfo:
    """
    sqi = SchoolQestionInfo()
    sqi.data.loc[sqi.data['explanation2'].str.contains('共有した内容'), ['explanation2', 'answer']]

    """
    def __init__(self, chache_path=None):
        print("必ずしもtidyになっていない質問もあるから注意")
        self.data = self.build()

    @staticmethod
    def build():
        def check_df_is_one_value(dfx):
            if dfx.shape[0] == 0:
                raise ValueError
            else:
                return dfx

        def give_search_key(dfx):
            return dfx['key1'] + '_' + dfx['year_answer'].astype(int).astype(str)  + '_' + dfx['school_type'].replace({'小学校': 'p', '中学校': 'j'})

        info_path = './data/info/学校質問/question_school.xlsx'
        df_qes= (
            pd.read_excel(info_path, sheet_name='all', skiprows=3)
                .assign(
                search_key = lambda df: give_search_key(df),
                squestion =  pd.np.nan
            )
        )
        df_info = pd.read_excel(info_path, sheet_name='info', skiprows=0)
        qes_list = df_info['統一'].unique().tolist()
        for qes in qes_list:
            explanation = (
                df_info
                    .loc[df_info['統一']==qes, '検索用文章(正規表現を用いることがある)']
                    .pipe(check_df_is_one_value)
                    .values[0]
            )
            slicing = df_qes['explanation2'].str.contains(explanation) == True
            if slicing.sum() == 0:
                print('{0} cant find match'.format(qes))
                continue
            df_qes.loc[slicing, 'squestion'] = qes
        return df_qes

    def get_qes_info(self, cols=None):
        if cols == 'all':
            cols = self.data.columns.tolist()
        return_cols = ['squestion', 'grade', 'subject', 'search_key'] if cols is None else cols
        return_cols = list(set(return_cols + ['squestion']))
        return self.data[return_cols]

    def get_qes_info_sq(self, squestion, cols=None):
        df = self.get_qes_info(cols=cols)
        return df.loc[df['squestion'] == squestion, :]

    def check_dont_have_sq_question(self):
        return self.data.loc[self.data['squestion'].isnull(), :]

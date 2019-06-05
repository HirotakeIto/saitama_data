# from functools import reduce
# import pandas as pd
#
# __path_save = 'data/info/学校質問/qes_info.xlsx'
#
# def read_all_question():
#     def get_excel_sheet(path_x, sheet_name_x):
#         return (
#             pd.read_excel(path_x, sheet_name=sheet_name_x, skiprows=2)
#             [['key1', 'key2', 'school_type', 'year', 'grade', 'subject', 'category', 'explanation1',
#            'explanation2', 'answer', 'year_answer', 'year_target']]
#             .assign(
#                 explanation_all = lambda dfx:dfx['explanation1'] + dfx['explanation2'] + dfx['answer']
#             )
#         )
#
#     def get_iterator_df_question():
#         path_dict_list = [
#             {'path': 'data/info/学校質問/work/2016/school_qes_2016.xlsx', 'sheets': ['2016小学校解答用紙', '2016中学校解答用紙']},
#             {'path': 'data/info/学校質問/work/2017/school_qes_2017.xlsx', 'sheets': ['2017小学校解答用紙', '2017中学校解答用紙']},
#             {'path': 'data/info/学校質問/work/2018/school_qes_2018.xlsx', 'sheets': ['2018小学校解答用紙', '2018中学校解答用紙']},
#         ]
#         for path_dict in path_dict_list:
#             for sheet_name in path_dict['sheets']:
#                 print("Getting dataframe of school qestion info: {path}'s {sheet}".format(
#                     path=path_dict['path'], sheet=sheet_name))
#                 yield get_excel_sheet(path_x=path_dict['path'], sheet_name_x=sheet_name)
#
#     return reduce(lambda x, y: pd.concat([x, y], axis=0), get_iterator_df_question())
#
#
# def read_question_info():
#     path = 'data/info/学校質問/work/sq_regex.xlsx'
#     return (
#         pd.read_excel(path, sheet_name='info')
#         .assign(
#             sq = lambda dfx: dfx['統一'],
#             exp_reg = lambda dfx: dfx['検索用文章(正規表現を用いることがある)']
#
#         )
#         [['sq', 'exp_reg']]
#     )
#
#
# def create_qes_info():
#     def get_count_ref(dfx, ref, count_col='sq'):
#         return (
#             dfx
#             .fillna('na')
#             .groupby(ref)
#             [count_col]
#             .transform('size')
#         )
#
#     df_qes = read_all_question()
#     df_info = read_question_info()
#     df_qes['sq'] = pd.np.nan # reset
#     for info in df_info.to_dict(orient='record'):
#         slicing = df_qes['explanation_all'].str.contains(info['exp_reg'])
#         if df_qes.loc[slicing, 'sq'].notna().sum() > 0:
#             print('{sq}: でsqが登録されています。更新をスキップします。'.format(sq = info))
#             print('すでに入力があったものは次の通りです')
#             print(df_qes.loc[slicing & df_qes['sq'].notna(), ['sq', 'explanation_all']].values)
#             print('今回新しく入力されるものは次の通りです')
#             print(df_qes.loc[slicing & df_qes['sq'].isna(), 'explanation_all'].values)
#             continue
#         else:
#             df_qes.loc[slicing, 'sq'] = info['sq']
#     df_qes_save = (
#         df_qes
#         .assign(
#             key_unique = lambda dfx: dfx['key1'].str.cat(dfx[['school_type', 'year']].astype(str), sep='_', na_rep='na'),
#             count_sq_level_school=lambda dfx: get_count_ref(
#                 dfx=dfx,
#                 ref=['year', 'school_type', 'sq'], count_col='sq'
#             ),
#             count_sq_level_grade=lambda dfx: get_count_ref(
#                 dfx=dfx,
#                 ref=['year', 'school_type', 'grade', 'sq'], count_col='sq'
#             ),
#             count_sq_level_subject=lambda dfx: get_count_ref(
#                 dfx=dfx,
#                 ref=['year', 'school_type', 'grade', 'subject', 'sq'], count_col='sq'
#             )
#         )
#         [[
#             'key_unique', 'key1', 'key2', 'school_type', 'year', 'grade', 'subject',
#             'sq', 'count_sq_level_school', 'count_sq_level_grade', 'count_sq_level_subject',
#             'year_answer', 'year_target', 'category', 'explanation1', 'explanation2', 'answer', 'explanation_all',
#         ]]
#     )
#     path_save = __path_save
#     writer = pd.ExcelWriter(path_save, mode='a')
#     df_qes_save.to_excel(writer, index=False, sheet_name='qes_info')
#
# # df_qes.loc[df_qes['sq'].isnull(), 'explanation_all'].unique()
# # df_qes.loc[df_qes['sq'].isnull(), ['year']]
#
# # df_qes.groupby(['year', 'school_type', 'subject', 'grade', 'sq']).size().max()
# # aa = (
# #     df_qes
# #     .fillna('999')
# #     .groupby(['year', 'school_type', 'subject', 'grade', 'sq'])
# #     .transform('size')
# #     .sort_values(ascending=False)
# # )
# # df_qes.loc[df_qes.sq == 's100', 'explanation2']
# # # df_qes.loc[df_qes['sq'].isnull(), :].to_csv('data/info/学校質問/manual_list.csv', index=False, encoding='sjis')

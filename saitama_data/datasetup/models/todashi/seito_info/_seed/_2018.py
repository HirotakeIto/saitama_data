import os
import pandas as pd
import numpy as np
from typing import List

def find_all_files(directory):
    for root, dirs, files in os.walk(directory):
        yield root
        for file in files:
            yield os.path.join(root, file)

def get_csv_list(target_folder: str) -> List[str]:
    get_list = find_all_files(target_folder)
    return [g for g in get_list if g.count('.xlsx')]

def get_converted_multi_columns(df, *, to_snake_case=True):
    if to_snake_case:
        return [col[0] + '_' + col[1] for col in df.columns.values]
    else:
        return [col[0] + col[1].capitalize() for col in df.columns.values]


def data_engineer(dfx: pd.DataFrame):
    dfx.columns = get_converted_multi_columns(dfx)
    dcolumn_mapper_regex = {
        '^.*就学援助*': 'school_attendance', '^.*生活保護*': 'public_assistance',
        '^.*ひとり親*': 'single_parent', 'H30_個人番号': 'id', 'H30_出席番号': 'number',
        'H30_組': 'class'
    }
    dfx.columns = dfx.columns.to_series().replace(dcolumn_mapper_regex, regex=True)
    return (
        dfx
        .dropna(how='all').replace({'○': 1, np.nan: 0})
        .assign(year=2018)
        [['id', 'public_assistance', 'school_attendance', 'single_parent', 'year', 'class', 'number']]
    )


def main() -> pd.DataFrame:
    path_folder = "data/original_data/todasi/todashi_data/2018/個人番号管理シート(送付用)/"
    paths_target = get_csv_list(path_folder)
    path = paths_target[0]
    df_result = pd.DataFrame()
    for path in paths_target:
        dfs = pd.read_excel(path, header=[5, 6], index_col=None, sheet_name=None)
        for name, df in dfs.items():
            df_result = df_result.append(data_engineer(df))
            # print(path, df[['id', 'public_assistance', 'school_attendance', 'single_parent']].columns)
    return df_result


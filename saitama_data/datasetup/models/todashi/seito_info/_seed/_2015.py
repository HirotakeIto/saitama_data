import os
import pandas as pd
import numpy as np


def get_csv_list(target_folder: str):
    get_list = os.listdir(target_folder)
    return [g for g in get_list if g.count('.csv')]


def data_engineer(data: pd.DataFrame):
    if type(data) != pd.DataFrame:
        raise TypeError
    column_mapper = {
        '組': 'class',
        '出席番号': 'number',
        '性別': 'sex',
        '個人番号': 'id',
        '生活\n保護\n該当': 'public_assistance',
        '就学援助\n該当': 'school_attendance',
        'ひとり親\n該当': 'single_parent'
    }
    delete_col = [c for c in data.columns if c.count('Unnamed') > 0]
    data = data.drop(delete_col, axis=1)
    for d in data.columns:
        if d not in column_mapper:
            print('catch columns we can not recognize: {d}?'.format(d=d))
            raise ValueError
    data = data.rename(columns=column_mapper)
    data = data.dropna(how='all')
    data = data.replace({'○': 1, np.nan: 0})
    data['year'] = 2015  # give a year
    return data


def main() -> pd.DataFrame:
    target_folder = './data/original_data/todasi/todashi_data/todashi_addinfo_20161017/'
    target_list = get_csv_list(target_folder)
    data = pd.DataFrame()
    for target_file in target_list:
        print(target_file)
        _data = pd.read_csv(target_folder + target_file, encoding='shift-jis', header=5, index_col=None)
        _data = data_engineer(_data)
        data = pd.concat([data, _data], axis=0)
    data['year'] = 2015
    return data

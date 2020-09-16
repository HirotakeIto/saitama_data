import pandas as pd
import numpy as np

def main():
    path = "data/original_data/todasi/todashi_data/2019/SES情報(小4～中3児童生徒)提供用.xlsx"
    df = (
        pd.read_excel(path)
        .rename(columns={
            '就学援助': 'school_attendance', '生活保護': 'public_assistance',
            '児童扶養手当': 'single_parent', '(学)個人番号': 'id',  # 児童扶養手当はひとり親で受給できる
            '組': 'class', '番号': 'number'
        })
        .dropna(how='all').replace({'○': 1, np.nan: 0})
        .assign(year=2019)
        [['id', 'public_assistance', 'school_attendance', 'single_parent', 'year', 'class', 'number']]        
    )
    return df

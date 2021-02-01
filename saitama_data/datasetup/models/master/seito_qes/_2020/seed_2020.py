from saitama_data.datasetup.models.master.seito_qes.model import SeitoQes
import pandas as pd
import numpy as np
# import openpyxl

QUESTIONS = [column.name for column in SeitoQes.schema.columns if column.name in ["q" + str(i) for i in range(0, 300)]]


def _remove_anomaly(value: float, qes_type: str) -> float:
    if qes_type == "q138":
        if value in [0, 999]:
            return np.NaN
        else:
            return value
    elif qes_type in QUESTIONS:
        if value in [0, 9, 999]:
            return np.NaN
        else:
            return value
    return np.NaN


def remove_anomaly(_df: pd.DataFrame) -> pd.DataFrame:
    for question in _df.columns:
        if question in QUESTIONS:
            print(question)
            _df[question] = np.vectorize(_remove_anomaly, otypes=['float'])(_df[question], qes_type=question)
    return _df


class InfoRename():
    path = "data/info/生徒質問/R2/対応表.csv"
    df: pd.DataFrame = None

    def __init__(self):
        df: pd.DataFrame = pd.read_csv(self.path, header=[1, 2])
        df.columns = ['_'.join([x for x in column if x.count("Unnamed") == 0]) for column in df.columns.values]
        self.df = df

    def get_mapper_rename(self, grade: int) -> dict:
        if grade in [4, 5, 6]:
            column_target = '入力_小{0}'.format(grade)
        elif grade in [7, 8, 9]:
            column_target = '入力_中{0}'.format(grade - 6)
        else:
            raise KeyError
        return (
            self.df[['入力_num_Q', column_target]]
            .dropna()
            .set_index(column_target)["入力_num_Q"].to_dict()
        )


def main2020():
    mapper = {
        '個人番号': "id",
        '出席番号': "num",
        '埼玉県学校コード': "schid",
        '学年': "grade",
        '学校名': "schname",
        '市町村教育委員会コード': "city",
        '市町村教育委員会名': "cityname",
        '年度': "year",
        '性別': "sec",
        '組': "class",
        '集計対象': "taisyou"
    }
    tuples_grade_path = [
        (4, "data/original_data/R2データ/埼玉県/素データ/埼玉県_17_小4_児童生徒質問紙調査_素データ.xlsx"),
        (5, "data/original_data/R2データ/埼玉県/素データ/埼玉県_17_小5_児童生徒質問紙調査_素データ.xlsx"),
        (6, "data/original_data/R2データ/埼玉県/素データ/埼玉県_17_小6_児童生徒質問紙調査_素データ.xlsx"),
        (7, "data/original_data/R2データ/埼玉県/素データ/埼玉県_17_中1_児童生徒質問紙調査_素データ.xlsx"),
        (8, "data/original_data/R2データ/埼玉県/素データ/埼玉県_17_中2_児童生徒質問紙調査_素データ.xlsx"),
        (9, "data/original_data/R2データ/埼玉県/素データ/埼玉県_17_中3_児童生徒質問紙調査_素データ.xlsx"),
    ]
    # start
    info_rename = InfoRename()
    df = pd.DataFrame()
    for (grade, path) in tuples_grade_path:
        mapper_rename = info_rename.get_mapper_rename(grade=grade)
        df = (
            pd.read_excel(path, header=8)
            .rename(columns=mapper_rename)
            .append(df)
        )
    df_save: pd.DataFrame = (
        df
        .rename(columns=mapper)
        .pipe(lambda dfx: dfx[[x for x in dfx.columns if x in [s.name for s in SeitoQes.schema.columns]]])
        .assign(
            q138=lambda dfx: pd.to_numeric(dfx["q138"], errors="coerce")
        )
        .pipe(remove_anomaly)
    )
    # df_save.to_csv("tmp.csv", index=False)
    # df_save = pd.read_csv("tmp.csv")
    model = SeitoQes(df_save).adjust_schema().convert()
    model.validate_convert()
    return model.data

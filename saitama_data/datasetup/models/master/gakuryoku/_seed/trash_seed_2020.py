import pandas as pd
import numpy as np
import openpyxl
from saitama_data.lib.read_config import config
from typing import List, Dict
from saitama_data.datasetup.models import SeitoQes
from saitama_data.datasetup.models import Gakuryoku
from saitama_data.datasetup.models import IdMaster
from saitama_data.datasetup.models.info.classid_schoolid import SchidSchoolid

paths: List[str] = [
    config.path2020.gakuryoku_p4,
    config.path2020.gakuryoku_p5,
    config.path2020.gakuryoku_p6,
    config.path2020.gakuryoku_j1,
    config.path2020.gakuryoku_j2,
    config.path2020.gakuryoku_j3,
]


def parser(paths: List[str]):
    # gakuryoku
    df = pd.DataFrame()
    for path in paths:
        df = (
            pd.read_csv(path, encoding="sjis")
            [["stuid", "grade", "class_num", "theta_ko", "theta_su", "theta_ei"]]
            .rename(columns={"stuid": "id", "theta_ko": "kokugo_level", "theta_su": "math_level", "theta_ei": "eng_level"})
            .assign(year = 2020)
            .replace("-", pd.np.NaN)
            .append(df)
        )
    model = Gakuryoku(df).adjust_schema().convert()
    model.validate_convert()        
    model.data.to_csv('gakuryoku_2020_tmp.csv', index=False)

    # idmaster
    #  ['mst_id', 'year', 'id', 'sex', 'city_id', 'school_id', 'grade', 'class']
    mapper_sch_id_school_id = SchidSchoolid().read().mapper_sch_id_school_id
    def sch_id_to_school_id(sch_id: float):
        if sch_id in mapper_sch_id_school_id:
            return mapper_sch_id_school_id[sch_id]
        else:
            return pd.np.NaN
    
    def gender_2_to_sex(gender_2: float):
        return {"男": 1, "女": 2, "-": np.NaN}[gender_2]

    df = pd.DataFrame()
    for path in paths:
        df = (
            pd.read_csv(path, encoding="sjis")
            [["stuid", "grade", "class_num", "gender_2", "sch_id"]]
            .rename(columns={"stuid": "id", "class_num": "class"})
            .assign(year = 2020)
            .append(df)
        )
    df["school_id"] = np.vectorize(sch_id_to_school_id, otypes=['float'])(df["sch_id"])
    df["sex"] = np.vectorize(gender_2_to_sex, otypes=['float'])(df["gender_2"])
    model = IdMaster(df).adjust_schema().convert()
    model.validate_convert()        
    model.data.to_csv('id_master_2020_tmp.csv', index=False)


# from saitama_data.datasetup.models import Gakuryoku

# g = Gakuryoku().read().data
class InfoRename():
    path = "data/info/生徒質問/R2/対応表.csv"
    df: pd.DataFrame = None

    def __init__(self):
        df: pd.DataFrame = pd.read_csv(self.path, header=[1, 2])
        df.columns = ['_'.join([x for x in column if x.count("Unnamed")==0]) for column in df.columns.values]
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
    

def main():
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
            q138 = lambda dfx: pd.to_numeric(dfx["q138"], errors="coerce")
        )
    )
    df_save.to_csv("tmp.csv", index=False)
    df_save = pd.read_csv("tmp.csv")
    model = SeitoQes(df_save).adjust_schema().convert()
    model.validate_convert()
    model.save("append")

    df = SeitoQes().fetch_columns(["id", "q138", "year"]).read().data

    # pd.DataFrame([[1, 2]], columns=["a", "b"]).append(
    #     pd.DataFrame([[3, 4]], columns=["b", "a"])
    # 
    # df: pd.DataFrame = pd.read_excel("data/original_data/R2データ/埼玉県/素データ/埼玉県_17_小4_児童生徒質問紙調査_素データ.xlsx", header=8)
    # df.rename(columns=mapper_rename).columns.tolist()

    # df: pd.DataFrame = pd.read_excel("data/original_data/R2データ/埼玉県/素データ/埼玉県_17_小5_児童生徒質問紙調査_素データ.xlsx", header=8)
    # df.rename(columns=mapper_rename).columns.tolist()

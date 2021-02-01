import pandas as pd
from numpy import NaN
from collections import defaultdict
from saitama_data.lib.read_config import config
from saitama_data.datasetup.models.info.classid_schoolid import SchidSchoolid
from saitama_data.datasetup.models.master.school_qes.model import SchoolQes

path_school_prime = config.path2020.path_school_prime
path_school_junior = config.path2020.path_school_junior
path_rename_prime = config.path2020.path_rename_prime
COLUMNS_MELT_INDEX = "sch_id_input"
MAPPER_SCH_ID_TO_SCHOOL_ID = defaultdict(lambda: NaN)
MAPPER_SCH_ID_TO_SCHOOL_ID.update(SchidSchoolid().read().mapper_sch_id_school_id)
YEAR_ANSWER = 2020
YEAR_TARGET = 2019


def seed() -> pd.DataFrame:
    path_df = path_school_prime
    path_mapper = path_rename_prime
    mapper = (
        pd.read_csv(path_mapper)
        .set_index('key1')['key2'].to_dict()
    )
    columns_melt_values = list(mapper.keys())
    school_type = '小学校'
    df = (
        pd.read_csv(path_df, encoding="sjis")
        .replace("-", NaN)
        .pipe(lambda _df: _df.loc[:, _df.count() > 0])
        .melt(id_vars=COLUMNS_MELT_INDEX, value_vars=columns_melt_values, var_name='key1')
        .assign(
            key2=lambda _df: _df['key1'].apply(lambda x: mapper[x]),
            school_id=lambda _df: _df['sch_id_input'].apply(lambda x: MAPPER_SCH_ID_TO_SCHOOL_ID[x]),
            school_type=school_type,
            year_answer=YEAR_ANSWER,
            year_target=YEAR_TARGET
        )
        # .rename(columns=mapper)
    )
    s = SchoolQes(df)
    s.adjust_schema()
    s.validate_convert()
    return s.data

from pandas_schema import Column, Schema
from pandas_schema.validation import CanConvertValidation
from saitama_data.datasetup.models.base.schema import BaseSchema
from saitama_data.datasetup.models.mix_in.io_mixin import CsvIOMixin
from saitama_data.datasetup.models.mix_in.accepter_mixin import AccepterMixin

# from saitama_data.datasetup.models.util import get_schema_str


class TodaTchBelongSchema(BaseSchema):
    column_type = {
        "grade": float,
        "class": float,
        "teacher_id": float,
        "school_id": float,
        "year": float,
        'is_main': float
    }
    schema = Schema([
        Column("grade", [CanConvertValidation(float)]),
        Column("class", [CanConvertValidation(float)]),
        Column("teacher_id", [CanConvertValidation(float)]),
        Column("school_id", [CanConvertValidation(float)]),
        Column("year", [CanConvertValidation(float)]),
        Column("is_main", [CanConvertValidation(float)]),
    ])
    convert_list = column_type


class TodaTchBelong(CsvIOMixin, AccepterMixin, TodaTchBelongSchema):
    """
    ttb = TodaTchBelong().read()
    """
    path = 'data/db/todashi/toda_tch_belong.csv'

    def eda(self):
        print(
            self.data.groupby(['year', 'school_id', 'grade', 'class'])['is_main'].count()
        )
        print(
            self.data.pipe(lambda dfx: dfx[dfx['is_main'] == 1]).groupby(['year', 'school_id', 'grade', 'class'])['teacher_id'].count().value_counts()
        )

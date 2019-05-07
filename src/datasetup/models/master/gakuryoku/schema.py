from pandas_schema import Column, Schema
from pandas_schema.validation import CanConvertValidation
from src.datasetup.models.base.schema import BaseSchema
from src.datasetup.models.util import get_schema_str

class GakuryokuSchema(BaseSchema):
    column_type = {
        "eng_level": float,
        "grade": float,
        "id": float,
        "kokugo_level": float,
        "math_level": float,
        "year": float
    }
    convert_list = column_type
    # schema = Schema([
    #     Column("year", [CanConvertValidation(float)]),
    #     Column("grade", [CanConvertValidation(float)]),
    #     Column("id", [CanConvertValidation(float)]),
    #     Column("kokugo_level", [CanConvertValidation(float)]),
    #     Column("math_level", [CanConvertValidation(float)]),
    #     Column("eng_level", [CanConvertValidation(float)]),
    # ])
    schema = Schema([
        Column("year", []),
        Column("grade", []),
        Column("id", []),
        Column("kokugo_level", []),
        Column("math_level", []),
        Column("eng_level", []),
    ])
    for column in schema.columns:
        column.validations.append(CanConvertValidation(column_type[column.name]))

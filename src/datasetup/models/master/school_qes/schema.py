from src.datasetup.models.base.schema import BaseSchema
from pandas_schema import Column, Schema
from pandas_schema.validation import CanConvertValidation


class SchoolQesSchema(BaseSchema):
    column_type = {
        'key1': str,
        'key2': str,
        'school_id': float,
        'school_type': str,
        'year_answer': float,
        'year_target': float,
        'value': str
    }
    schema = Schema([
        Column('key1', [CanConvertValidation(str)]),
        Column('key2', [CanConvertValidation(str)]),
        Column('school_id', [CanConvertValidation(float)]),
        Column('school_type', [CanConvertValidation(str)]),
        Column('year_answer', [CanConvertValidation(float)]),
        Column('year_target', [CanConvertValidation(float)]),
        Column('value', [CanConvertValidation(str)]),
    ])
    convert_list = column_type

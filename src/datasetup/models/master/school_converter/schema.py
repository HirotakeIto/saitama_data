from pandas_schema import Column, Schema
from pandas_schema.validation import CanConvertValidation
from src.datasetup.models.base.schema import BaseSchema


class SchoolClassSchema(BaseSchema):
    schema = Schema([
        Column('sch_id', [CanConvertValidation(float)]),
        Column('class_id', [CanConvertValidation(float)]),
    ])
    convert_list = {'sch_id': float, 'class_id': float}


class SchidSchoolidSchema(BaseSchema):
    schema = Schema([
        Column('sch_id', [CanConvertValidation(float)]),
        Column('school_id', [CanConvertValidation(float)]),
        Column('city_id', [CanConvertValidation(float)]),
    ])
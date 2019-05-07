# from ..base import DataFrameValidater
from src.datasetup.models.base.schema import BaseSchema
from pandas_schema import Column, Schema
from pandas_schema.validation import CanConvertValidation


class GraduationRateSchema(BaseSchema):
    column_type = {
        'city_id': float,
        'rate_graduation': float,
    }
    schema = Schema([
        Column('city_id', [CanConvertValidation(float)]),
        Column('rate_graduation', [CanConvertValidation(float)])
    ])
    convert_list = column_type

# from ..base import DataFrameValidater
from saitama_data.datasetup.models.base.schema import BaseSchema
from pandas_schema import Column, Schema
from pandas_schema.validation import CanConvertValidation


class CitySchema(BaseSchema):
    column_type = {
        'city_code': float,
        'city_id': float,
        'organization_name': str,
        'city_name': str
    }
    schema = Schema([
        Column('city_code', [CanConvertValidation(float)]),
        Column('city_id', [CanConvertValidation(float)]),
        Column('organization_name', [CanConvertValidation(str)]),
        Column('city_name', [CanConvertValidation(str)]),
    ])
    convert_list = column_type


from pandas_schema import Column, Schema
from pandas_schema.validation import CanConvertValidation
from src.datasetup.models.base.schema import BaseSchema


print("# data = self.get_uniqueness(data, ['mst_id', 'year'])　みたいな制約を入れたい")
class IdMasterSchema(BaseSchema):
    schema = Schema([
        Column('mst_id', [CanConvertValidation(str)]),
        Column('year', [CanConvertValidation(float)]),
        Column('id', [CanConvertValidation(float)]),
        Column('sex', [CanConvertValidation(float)]),
        Column('city_id', [CanConvertValidation(float)]),
        Column('school_id', [CanConvertValidation(float)]),
        Column('grade', [CanConvertValidation(float)]),
        Column('class', [CanConvertValidation(float)]),
    ])
    convert_list = {'city_id': float, 'school_id': float, 'grade': float,
                    'class': float, 'sex': float, 'year': float, 'id': float, 'mst_id': str}



from pandas_schema import Column, Schema
from pandas_schema.validation import CanConvertValidation
from saitama_data.datasetup.models.mix_in.io_mixin import CsvIOMixin
from saitama_data.datasetup.models.base.schema import BaseSchema


class ClassIdSchoolIdSchema(BaseSchema):
    schema = Schema([
        Column('class_id', []),
        Column('school_id', []),
        Column('city_id', []),
    ])
    column_type = {'class_id': float, 'school_id': float, 'city_id': float}
    for col in schema.columns:
        col.validations.append(CanConvertValidation(column_type[col.name]))
    convert_list = column_type


class ClassIdSchoolId(CsvIOMixin, ClassIdSchoolIdSchema):
    path = './data/db/info/classid_schoolid.csv'
    pass
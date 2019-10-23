from pandas_schema import Column, Schema
from pandas_schema.validation import CanConvertValidation
from saitama_data.datasetup.models.mix_in.io_mixin import CsvIOMixin
from saitama_data.datasetup.models.base.schema import BaseSchema


class SchidSchoolidSchema(BaseSchema):
    schema = Schema([
        Column('sch_id', []),
        Column('school_id', []),
        Column('city_id', []),
    ])
    column_type = {'sch_id': float, 'school_id': float, 'city_id': float}
    for col in schema.columns:
        col.validations.append(CanConvertValidation(column_type[col.name]))
    convert_list = column_type


class SchidSchoolid(CsvIOMixin, SchidSchoolidSchema):
    path = './data/db/info/schid_schoolid.csv'

    @property
    def mapper_sch_id_school_id(self):
        return {x[0]: x[1] for x in self.data[['sch_id', 'school_id']].dropna().astype(int).values}

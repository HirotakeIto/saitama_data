from pandas_schema import Column, Schema
from pandas_schema.validation import CanConvertValidation

from saitama_data.datasetup.models.base.schema import BaseSchema
from saitama_data.datasetup.models.mix_in.io_mixin import CsvIOMixin


class SchoolClassSchema(BaseSchema):
    schema = Schema([
        Column('sch_id', []),
        Column('class_id', []),
    ])
    column_type = {'sch_id': float, 'class_id': float}
    for col in schema.columns:
        col.validations.append(CanConvertValidation(column_type[col.name]))
    convert_list = column_type

    def _validate(self, data):
        super()._validate(data)
        # class_id は sch_id の中に存在する下部組織という構造になっている。
        # そのため、あるclass_idにひもづくsch_idがユニークであれば良い。
        # それが年度などを跨いでも、その構造が崩れていなければおk。
        max_val = data.groupby('class_id')['sch_id'].nunique().max()
        if max_val != 1:
            string = "Error in validation: 「あるclass_idに対するユニークなsch_idの数」の最大値：{max}".format(max=max_val)
            raise ValueError(string)
        print('None')


class SchoolClass(CsvIOMixin, SchoolClassSchema):
    path = './data/db/info/school_class.csv'

    pass

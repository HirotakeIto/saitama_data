from pandas_schema import Column, Schema
from pandas_schema.validation import CanConvertValidation
from saitama_data.datasetup.models.base.schema import BaseSchema
from saitama_data.datasetup.models.mix_in.io_mixin import CsvIOMixin
from saitama_data.datasetup.models.mix_in.accepter_mixin import AccepterMixin
import pandas as pd

class SeitoInfoSchema(BaseSchema):
    column_type = {
        "id": float,
        "public_assistance": float,
        "school_attendance": float,
        "single_parent": float,
        "year": float,
        'class': float,
        'number': float,       
    }
    schema: Schema = Schema([
        Column("id", []),
        Column("public_assistance",[]),
        Column("school_attendance", []),
        Column("single_parent", []),
        Column("year", []),
        Column("class", []),
        Column("number", []),
    ])
    convert_list = column_type
    for column in schema.columns:
        column.validations.append(CanConvertValidation(convert_list[column.name]))


class SeitoInfo(CsvIOMixin, AccepterMixin, SeitoInfoSchema):
    """
    ttb = SeitoInfoSchema().read()
    """
    path = 'data/db/todashi/seito_info.csv'

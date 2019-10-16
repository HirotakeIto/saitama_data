from saitama_data.datasetup.models.mix_in.io_mixin import RdbIOMixin
from saitama_data.datasetup.models.mix_in.accepter_mixin import AccepterMixin
from pandas_schema import Column, Schema
from pandas_schema.validation import CanConvertValidation
from saitama_data.datasetup.models.base.schema import BaseSchema
from saitama_data.datasetup.models.util import get_schema_str

class SeitoQesSoseiSchema(BaseSchema):
    column_type = {
        "selfefficacy": float,
        "effort": float,
        "book": float,
        "al_math": float,
        "trad_math": float,
        "birth_month": float,
        "al_eng": float,
        "year": float,
        "execution": float,
        "theacher_attitude": float,
        "planning": float,
        "trad_kokugo": float,
        "class_attitude": float,
        "ninti": float,
        "zyunan": float,
        "al_kokugo": float,
        "no_cramschool": float,
        "birth": str,
        "resource": float,
        "dilligence": float,
        "strategy": float,
        "id": float,
        "selfcontrol": float,
        "grade": float,
        "cramschool": float 
    }
    schema = Schema([
        Column("selfefficacy", [CanConvertValidation(float)]),
        Column("effort", [CanConvertValidation(float)]),
        Column("book", [CanConvertValidation(float)]),
        Column("al_math", [CanConvertValidation(float)]),
        Column("trad_math", [CanConvertValidation(float)]),
        Column("birth_month", [CanConvertValidation(float)]),
        Column("al_eng", [CanConvertValidation(float)]),
        Column("year", [CanConvertValidation(float)]),
        Column("execution", [CanConvertValidation(float)]),
        Column("theacher_attitude", [CanConvertValidation(float)]),
        Column("planning", [CanConvertValidation(float)]),
        Column("trad_kokugo", [CanConvertValidation(float)]),
        Column("class_attitude", [CanConvertValidation(float)]),
        Column("ninti", [CanConvertValidation(float)]),
        Column("zyunan", [CanConvertValidation(float)]),
        Column("al_kokugo", [CanConvertValidation(float)]),
        Column("no_cramschool", [CanConvertValidation(float)]),
        Column("birth", [CanConvertValidation(str)]),
        Column("resource", [CanConvertValidation(float)]),
        Column("dilligence", [CanConvertValidation(float)]),
        Column("strategy", [CanConvertValidation(float)]),
        Column("id", [CanConvertValidation(float)]),
        Column("selfcontrol", [CanConvertValidation(float)]),
        Column("grade", [CanConvertValidation(float)]),
        Column("cramschool", [CanConvertValidation(float)])
    ])
    for column in schema.columns:
        column.validations.append(CanConvertValidation(column_type[column.name]))


class SeitoQesSosei(RdbIOMixin, AccepterMixin, SeitoQesSoseiSchema):
    """
    sqs = SeitoQesSosei().read()
    """
    table_name = 'seito_qes_sosei'
    schema_name = 'work'
    merge_key = ['id', 'year']

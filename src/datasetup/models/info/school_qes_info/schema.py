from src.datasetup.models.base.schema import BaseSchema
from pandas_schema import Column, Schema
from pandas_schema.validation import CanConvertValidation


class SchoolQestionInfoSchema(BaseSchema):
    column_type = {
        "key_unique": str,
        "key1": str,
        "key2": str,
        "school_type": str,
        "year": int,
        "grade": str,
        "subject": str,
        "sq": str,
        "count_sq_level_school": int,
        "count_sq_level_grade": int,
        "count_sq_level_subject": int,
        "year_answer": int,
        "year_target": int,
        "category": int,
        "explanation1": str,
        "explanation2": str,
        "answer": str,
        "explanation_all": str
    }
    schema = Schema([
        Column("key_unique", []),
        Column("key1", []),
        Column("key2", []),
        Column("school_type", []),
        Column("year", []),
        Column("grade", []),
        Column("subject", []),
        Column("sq", []),
        Column("count_sq_level_school", []),
        Column("count_sq_level_grade", []),
        Column("count_sq_level_subject", []),
        Column("year_answer", []),
        Column("year_target", []),
        Column("category", []),
        Column("explanation1", []),
        Column("explanation2", []),
        Column("answer", []),
        Column("explanation_all", [])
    ])
    for col in schema.columns:
        col.validations.append(CanConvertValidation(column_type[col.name]))
    convert_list = column_type

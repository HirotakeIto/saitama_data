from pandas_schema import Column, Schema
from pandas_schema.validation import CanConvertValidation
from src.datasetup.models.base.schema import BaseSchema
# from src.datasetup.models.util import get_schema_str

class TodaOldTchQesSchema(BaseSchema):
    column_type = {
        "school_num": float,
        "school_name": str,
        "school_id": float,
        "teacher_id": int,
        "husen": str,
        "t_grade": float,
        "t_class": str,
        "subject": str,
        "t_q1": int,
        "t_q2": int,
        "t_q3": float,
        "t_q4": int,
        "t_q5": int,
        "t_q6": float,
        "t_q7": float,
        "t_q8": float,
        "t_q9": int,
        "t_q10": float,
        "t_q11": float,
        "t_q12": int,
        "t_q13": float,
        "t_q14": float,
        "t_q15_1": float,
        "t_q15_2": float,
        "t_q15_3": float,
        "t_q15_4": float,
        "t_q15_5": float,
        "t_q16_1": float,
        "t_q16_2": float,
        "t_q16_3": float,
        "t_q17": float,
        "t_q18": float,
        "t_q19": float,
        "t_q20": float,
        "t_q21_1": float,
        "t_q21_2": float,
        "t_q21_3": float,
        "t_q22_1": float,
        "t_q22_2": float,
        "t_q22_3": float,
        "t_q23": float,
        "t_q24": float,
        "t_q25": float,
        "t_q26_1": float,
        "t_q26_2": float,
        "t_q27": int,
        "t_q28": float,
        "t_q29": int,
        "t_q30": float,
        "t_q31": float,
        "t_q32": float,
        "t_q33": float,
        "t_q34": float,
        "t_q35": float,
        "t_q36": float,
        "t_q37": float,
        "t_q38": float,
        "t_q39": float,
        "t_q40": float,
        "t_q41": int,
        "t_q42": float,
        "t_q43": int,
        "t_q44": int,
        "t_q45": float,
        "t_q46": float,
        "t_q47": int,
        "t_q48": float,
        "t_q49": float,
        "t_q50": int,
        "t_q51": float,
        "t_q52": int,
        "t_q53": float,
        "t_q54_1": int,
        "t_q54_2": float,
        "t_q55": float,
        "t_q56": float,
        "t_q57": float,
        "t_q58": float,
        "t_q59": float,
        "t_q60": float,
        "t_q61": float,
        "t_q62_1": int,
        "t_q62_2": float,
        "t_q62_3": float,
        "year": int
    }
    schema = Schema([
        Column("school_num", [CanConvertValidation(float)]),
        Column("school_name", [CanConvertValidation(str)]),
        Column("school_id", [CanConvertValidation(float)]),
        Column("teacher_id", [CanConvertValidation(int)]),
        Column("husen", [CanConvertValidation(str)]),
        Column("t_grade", [CanConvertValidation(float)]),
        Column("t_class", [CanConvertValidation(str)]),
        Column("subject", [CanConvertValidation(str)]),
        Column("t_q1", [CanConvertValidation(int)]),
        Column("t_q2", [CanConvertValidation(int)]),
        Column("t_q3", [CanConvertValidation(float)]),
        Column("t_q4", [CanConvertValidation(int)]),
        Column("t_q5", [CanConvertValidation(int)]),
        Column("t_q6", [CanConvertValidation(float)]),
        Column("t_q7", [CanConvertValidation(float)]),
        Column("t_q8", [CanConvertValidation(float)]),
        Column("t_q9", [CanConvertValidation(int)]),
        Column("t_q10", [CanConvertValidation(float)]),
        Column("t_q11", [CanConvertValidation(float)]),
        Column("t_q12", [CanConvertValidation(int)]),
        Column("t_q13", [CanConvertValidation(float)]),
        Column("t_q14", [CanConvertValidation(float)]),
        Column("t_q15_1", [CanConvertValidation(float)]),
        Column("t_q15_2", [CanConvertValidation(float)]),
        Column("t_q15_3", [CanConvertValidation(float)]),
        Column("t_q15_4", [CanConvertValidation(float)]),
        Column("t_q15_5", [CanConvertValidation(float)]),
        Column("t_q16_1", [CanConvertValidation(float)]),
        Column("t_q16_2", [CanConvertValidation(float)]),
        Column("t_q16_3", [CanConvertValidation(float)]),
        Column("t_q17", [CanConvertValidation(float)]),
        Column("t_q18", [CanConvertValidation(float)]),
        Column("t_q19", [CanConvertValidation(float)]),
        Column("t_q20", [CanConvertValidation(float)]),
        Column("t_q21_1", [CanConvertValidation(float)]),
        Column("t_q21_2", [CanConvertValidation(float)]),
        Column("t_q21_3", [CanConvertValidation(float)]),
        Column("t_q22_1", [CanConvertValidation(float)]),
        Column("t_q22_2", [CanConvertValidation(float)]),
        Column("t_q22_3", [CanConvertValidation(float)]),
        Column("t_q23", [CanConvertValidation(float)]),
        Column("t_q24", [CanConvertValidation(float)]),
        Column("t_q25", [CanConvertValidation(float)]),
        Column("t_q26_1", [CanConvertValidation(float)]),
        Column("t_q26_2", [CanConvertValidation(float)]),
        Column("t_q27", [CanConvertValidation(int)]),
        Column("t_q28", [CanConvertValidation(float)]),
        Column("t_q29", [CanConvertValidation(int)]),
        Column("t_q30", [CanConvertValidation(float)]),
        Column("t_q31", [CanConvertValidation(float)]),
        Column("t_q32", [CanConvertValidation(float)]),
        Column("t_q33", [CanConvertValidation(float)]),
        Column("t_q34", [CanConvertValidation(float)]),
        Column("t_q35", [CanConvertValidation(float)]),
        Column("t_q36", [CanConvertValidation(float)]),
        Column("t_q37", [CanConvertValidation(float)]),
        Column("t_q38", [CanConvertValidation(float)]),
        Column("t_q39", [CanConvertValidation(float)]),
        Column("t_q40", [CanConvertValidation(float)]),
        Column("t_q41", [CanConvertValidation(int)]),
        Column("t_q42", [CanConvertValidation(float)]),
        Column("t_q43", [CanConvertValidation(int)]),
        Column("t_q44", [CanConvertValidation(int)]),
        Column("t_q45", [CanConvertValidation(float)]),
        Column("t_q46", [CanConvertValidation(float)]),
        Column("t_q47", [CanConvertValidation(int)]),
        Column("t_q48", [CanConvertValidation(float)]),
        Column("t_q49", [CanConvertValidation(float)]),
        Column("t_q50", [CanConvertValidation(int)]),
        Column("t_q51", [CanConvertValidation(float)]),
        Column("t_q52", [CanConvertValidation(int)]),
        Column("t_q53", [CanConvertValidation(float)]),
        Column("t_q54_1", [CanConvertValidation(int)]),
        Column("t_q54_2", [CanConvertValidation(float)]),
        Column("t_q55", [CanConvertValidation(float)]),
        Column("t_q56", [CanConvertValidation(float)]),
        Column("t_q57", [CanConvertValidation(float)]),
        Column("t_q58", [CanConvertValidation(float)]),
        Column("t_q59", [CanConvertValidation(float)]),
        Column("t_q60", [CanConvertValidation(float)]),
        Column("t_q61", [CanConvertValidation(float)]),
        Column("t_q62_1", [CanConvertValidation(int)]),
        Column("t_q62_2", [CanConvertValidation(float)]),
        Column("t_q62_3", [CanConvertValidation(float)]),
        Column("year", [CanConvertValidation(int)])
    ])

    convert_list = column_type
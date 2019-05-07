from pandas_schema import Column, Schema
from pandas_schema.validation import CanConvertValidation
from src.datasetup.models.base.schema import BaseSchema
# from src.datasetup.models.util import get_schema_str

class TodaOldTchNoncogSchema(BaseSchema):
    """
    戸田市教員調査には
    """
    column_type = {
        "timestamp": float,
        "teacher_id": int,
        "licence": str,
        "graduation": str,
        "age": int,
        "t_grade_class": str,
        "position": str,
        "school_name": str,
        "school_num": int,
        "school_id": float,
        "sex": str,
        "t_k1": str,
        "t_k2": str,
        "t_k3": str,
        "t_k4": str,
        "t_k5": str,
        "t_k6": int,
        "t_k7": int,
        "t_k8": int,
        "t_k9": int,
        "t_k10": int,
        "t_k11": float,
        "t_k12": int,
        "t_k13": int,
        "t_k14": int,
        "t_k15": float,
        "t_k16": float,
        "t_k17": int,
        "t_k18": int,
        "t_k19": int,
        "t_k20": int,
        "t_k21": int,
        "t_k22": int,
        "t_k23": float,
        "t_k24": int,
        "t_k25": int,
        "t_k26": float,
        "t_k27": int,
        "t_k28": int,
        "t_k29": int,
        "t_k30": int,
        "t_k31": int,
        "t_k32": int,
        "t_k33": int,
        "t_k34": float,
        "t_k35": int,
        "t_k36": float,
        "t_k37": int,
        "t_k38": int,
        "t_k39": int,
        "t_k40": float,
        "t_k41": int,
        "t_k42": float,
        "t_k43": float,
        "t_k44": int,
        "t_k45": int,
        "t_k46": float,
        "t_k47": int,
        "t_k48": int,
        "t_k49": int,
        "t_k50": float,
        "t_k51": float,
        "t_k52": float,
        "t_k53": int,
        "t_k54": float,
        "t_k55": int,
        "t_k56": int,
        "t_k57": int,
        "t_k58": int,
        "t_k59": int,
        "t_k60": int,
        "t_k61": int,
        "t_k62": int,
        "t_k63": int,
        "t_k64": int,
        "t_k65": int,
        "t_k66": int,
        "t_k67": int,
        "t_k68": float,
        "t_k69": int,
        "t_k70": float,
        "t_k71": int,
        "t_k72": int,
        "t_k73": int,
        "t_k74": int,
        "t_k75": int,
        "t_k76": float,
        "t_k77": float,
        "t_k78": float,
        "t_k79": float,
        "t_k80": float,
        "t_k81": float,
        "t_k82": float,
        "t_k83": float,
        "t_k84": int,
        "t_k85": int,
        "t_k86": float,
        "t_k87": int,
        "t_k88": int,
        "t_k89": float,
        "t_k90": float,
        "t_k91": float,
        "t_k92": float,
        "t_k93": int,
        "t_k94": float,
        "t_k95": float,
        "t_k96": int,
        "t_k97": float,
        "t_k98": float,
        "t_k99": float,
        "t_k100": float,
        "t_k101": float,
        "t_k102": float,
        "t_k103": float,
        "t_k104": float,
        "t_k105": float,
        "t_k106": float,
        "t_grade": float,
        "t_class": float,
        "year": int
    }
    schema = Schema([
        Column("timestamp", [CanConvertValidation(float)]),
        Column("teacher_id", [CanConvertValidation(int)]),
        Column("licence", [CanConvertValidation(str)]),
        Column("graduation", [CanConvertValidation(str)]),
        Column("age", [CanConvertValidation(int)]),
        Column("t_grade_class", [CanConvertValidation(str)]),
        Column("position", [CanConvertValidation(str)]),
        Column("school_name", [CanConvertValidation(str)]),
        Column("school_num", [CanConvertValidation(int)]),
        Column("school_id", [CanConvertValidation(float)]),
        Column("sex", [CanConvertValidation(str)]),
        Column("t_k1", [CanConvertValidation(str)]),
        Column("t_k2", [CanConvertValidation(str)]),
        Column("t_k3", [CanConvertValidation(str)]),
        Column("t_k4", [CanConvertValidation(str)]),
        Column("t_k5", [CanConvertValidation(str)]),
        Column("t_k6", [CanConvertValidation(int)]),
        Column("t_k7", [CanConvertValidation(int)]),
        Column("t_k8", [CanConvertValidation(int)]),
        Column("t_k9", [CanConvertValidation(int)]),
        Column("t_k10", [CanConvertValidation(int)]),
        Column("t_k11", [CanConvertValidation(float)]),
        Column("t_k12", [CanConvertValidation(int)]),
        Column("t_k13", [CanConvertValidation(int)]),
        Column("t_k14", [CanConvertValidation(int)]),
        Column("t_k15", [CanConvertValidation(float)]),
        Column("t_k16", [CanConvertValidation(float)]),
        Column("t_k17", [CanConvertValidation(int)]),
        Column("t_k18", [CanConvertValidation(int)]),
        Column("t_k19", [CanConvertValidation(int)]),
        Column("t_k20", [CanConvertValidation(int)]),
        Column("t_k21", [CanConvertValidation(int)]),
        Column("t_k22", [CanConvertValidation(int)]),
        Column("t_k23", [CanConvertValidation(float)]),
        Column("t_k24", [CanConvertValidation(int)]),
        Column("t_k25", [CanConvertValidation(int)]),
        Column("t_k26", [CanConvertValidation(float)]),
        Column("t_k27", [CanConvertValidation(int)]),
        Column("t_k28", [CanConvertValidation(int)]),
        Column("t_k29", [CanConvertValidation(int)]),
        Column("t_k30", [CanConvertValidation(int)]),
        Column("t_k31", [CanConvertValidation(int)]),
        Column("t_k32", [CanConvertValidation(int)]),
        Column("t_k33", [CanConvertValidation(int)]),
        Column("t_k34", [CanConvertValidation(float)]),
        Column("t_k35", [CanConvertValidation(int)]),
        Column("t_k36", [CanConvertValidation(float)]),
        Column("t_k37", [CanConvertValidation(int)]),
        Column("t_k38", [CanConvertValidation(int)]),
        Column("t_k39", [CanConvertValidation(int)]),
        Column("t_k40", [CanConvertValidation(float)]),
        Column("t_k41", [CanConvertValidation(int)]),
        Column("t_k42", [CanConvertValidation(float)]),
        Column("t_k43", [CanConvertValidation(float)]),
        Column("t_k44", [CanConvertValidation(int)]),
        Column("t_k45", [CanConvertValidation(int)]),
        Column("t_k46", [CanConvertValidation(float)]),
        Column("t_k47", [CanConvertValidation(int)]),
        Column("t_k48", [CanConvertValidation(int)]),
        Column("t_k49", [CanConvertValidation(int)]),
        Column("t_k50", [CanConvertValidation(float)]),
        Column("t_k51", [CanConvertValidation(float)]),
        Column("t_k52", [CanConvertValidation(float)]),
        Column("t_k53", [CanConvertValidation(int)]),
        Column("t_k54", [CanConvertValidation(float)]),
        Column("t_k55", [CanConvertValidation(int)]),
        Column("t_k56", [CanConvertValidation(int)]),
        Column("t_k57", [CanConvertValidation(int)]),
        Column("t_k58", [CanConvertValidation(int)]),
        Column("t_k59", [CanConvertValidation(int)]),
        Column("t_k60", [CanConvertValidation(int)]),
        Column("t_k61", [CanConvertValidation(int)]),
        Column("t_k62", [CanConvertValidation(int)]),
        Column("t_k63", [CanConvertValidation(int)]),
        Column("t_k64", [CanConvertValidation(int)]),
        Column("t_k65", [CanConvertValidation(int)]),
        Column("t_k66", [CanConvertValidation(int)]),
        Column("t_k67", [CanConvertValidation(int)]),
        Column("t_k68", [CanConvertValidation(float)]),
        Column("t_k69", [CanConvertValidation(int)]),
        Column("t_k70", [CanConvertValidation(float)]),
        Column("t_k71", [CanConvertValidation(int)]),
        Column("t_k72", [CanConvertValidation(int)]),
        Column("t_k73", [CanConvertValidation(int)]),
        Column("t_k74", [CanConvertValidation(int)]),
        Column("t_k75", [CanConvertValidation(int)]),
        Column("t_k76", [CanConvertValidation(float)]),
        Column("t_k77", [CanConvertValidation(float)]),
        Column("t_k78", [CanConvertValidation(float)]),
        Column("t_k79", [CanConvertValidation(float)]),
        Column("t_k80", [CanConvertValidation(float)]),
        Column("t_k81", [CanConvertValidation(float)]),
        Column("t_k82", [CanConvertValidation(float)]),
        Column("t_k83", [CanConvertValidation(float)]),
        Column("t_k84", [CanConvertValidation(int)]),
        Column("t_k85", [CanConvertValidation(int)]),
        Column("t_k86", [CanConvertValidation(float)]),
        Column("t_k87", [CanConvertValidation(int)]),
        Column("t_k88", [CanConvertValidation(int)]),
        Column("t_k89", [CanConvertValidation(float)]),
        Column("t_k90", [CanConvertValidation(float)]),
        Column("t_k91", [CanConvertValidation(float)]),
        Column("t_k92", [CanConvertValidation(float)]),
        Column("t_k93", [CanConvertValidation(int)]),
        Column("t_k94", [CanConvertValidation(float)]),
        Column("t_k95", [CanConvertValidation(float)]),
        Column("t_k96", [CanConvertValidation(int)]),
        Column("t_k97", [CanConvertValidation(float)]),
        Column("t_k98", [CanConvertValidation(float)]),
        Column("t_k99", [CanConvertValidation(float)]),
        Column("t_k100", [CanConvertValidation(float)]),
        Column("t_k101", [CanConvertValidation(float)]),
        Column("t_k102", [CanConvertValidation(float)]),
        Column("t_k103", [CanConvertValidation(float)]),
        Column("t_k104", [CanConvertValidation(float)]),
        Column("t_k105", [CanConvertValidation(float)]),
        Column("t_k106", [CanConvertValidation(float)]),
        Column("t_grade", [CanConvertValidation(float)]),
        Column("t_class", [CanConvertValidation(float)]),
        Column("year", [CanConvertValidation(int)])
    ])
    convert_list = column_type

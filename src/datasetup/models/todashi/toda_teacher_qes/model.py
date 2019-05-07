from src.datasetup.models.todashi.toda_teacher_qes.schema import TodaTeacherQesSchema
from src.datasetup.models.mix_in.io_mixin import CsvIOMixin


class TodaTeacherQes(CsvIOMixin, TodaTeacherQesSchema):
    path = './data/db/todashi/toda_teacher_qes.csv'

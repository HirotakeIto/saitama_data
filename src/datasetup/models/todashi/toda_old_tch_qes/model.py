from src.datasetup.models.todashi.toda_old_tch_qes.schema import TodaOldTchQesSchema
from src.datasetup.models.mix_in.io_mixin import CsvIOMixin


class TodaOldTchQes(CsvIOMixin, TodaOldTchQesSchema):
    path = './data/db/todashi/toda_old_tch_qes.csv'
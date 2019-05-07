from src.datasetup.models.todashi.toda_old_tch_noncog.schema import TodaOldTchNoncogSchema
from src.datasetup.models.mix_in.io_mixin import CsvIOMixin


class TodaOldTchNoncog(CsvIOMixin, TodaOldTchNoncogSchema):
    path = './data/db/todashi/toda_old_tch_noncog.csv'

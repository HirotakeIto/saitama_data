from saitama_data.datasetup.models.todashi.toda_old_tch_noncog.schema import TodaOldTchNoncogSchema
from saitama_data.datasetup.models.mix_in.io_mixin import CsvIOMixin


class TodaOldTchNoncog(CsvIOMixin, TodaOldTchNoncogSchema):
    path = './data/db/todashi/toda_old_tch_noncog.csv'

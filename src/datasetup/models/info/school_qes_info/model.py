from src.datasetup.models.mix_in.io_mixin import CsvIOMixin
from .schema import SchoolQestionInfoSchema

class SchoolQestionInfo(CsvIOMixin, SchoolQestionInfoSchema):
    __doc__ = """
        学校質問紙の質問情報を格納している。
        大元のデータは、基本的に手で作成している
    """
    path = './data/db/info/school_qes_info.csv'

from .schema import GakuryokuSchema
from src.datasetup.models.mix_in.io_mixin import RdbIOMixin
from src.datasetup.models.mix_in.accepter_mixin import AccepterMixin


class Gakuryoku(RdbIOMixin, AccepterMixin, GakuryokuSchema):
    """
    gk = Gakuryoku().read(chunksize=500000)
    for r in gk.data:
        print(type(r), r.shape)
    """
    table_name = 'gakuryoku'
    schema_name = 'master'
    merge_key = ['id', 'year']

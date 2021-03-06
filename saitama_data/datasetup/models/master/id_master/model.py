# from saitama_data.datasetup.models.master.id_master.schema import IdMasterSchema
from .schema import IdMasterSchema
from saitama_data.datasetup.models.mix_in.io_mixin import RdbIOMixin
from saitama_data.datasetup.models.mix_in.accepter_mixin import AccepterMixin


class IdMaster(RdbIOMixin, AccepterMixin, IdMasterSchema):
    """
    im = IdMaster().read()
    """
    table_name = 'id_master'
    schema_name = 'master'
    merge_key = ['id', 'year']

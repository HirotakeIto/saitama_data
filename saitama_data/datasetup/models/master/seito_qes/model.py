from saitama_data.datasetup.models.mix_in.io_mixin import RdbIOMixin
from saitama_data.datasetup.models.mix_in.accepter_mixin import AccepterMixin
from saitama_data.datasetup.models.master.seito_qes.schema import SeitoQesSchema


class SeitoQes(RdbIOMixin, AccepterMixin, SeitoQesSchema):
    """
    s = SeitoQes().fetch_columns(['id', 'q1']).read()
    """
    table_name = 'seito_qes'
    schema_name = 'master'
    merge_key = ['id', 'year']
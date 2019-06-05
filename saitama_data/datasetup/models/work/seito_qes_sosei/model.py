from saitama_data.datasetup.models.mix_in.io_mixin import RdbIOMixin
from saitama_data.datasetup.models.mix_in.accepter_mixin import AccepterMixin


class SeitoQesSosei(RdbIOMixin, AccepterMixin):
    """
    sqs = SeitoQesSosei().read()
    """
    table_name = 'seito_qes_sosei'
    schema_name = 'work'
    merge_key = ['id', 'year']

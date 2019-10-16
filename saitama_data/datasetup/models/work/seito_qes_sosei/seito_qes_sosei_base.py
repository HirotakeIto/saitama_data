from saitama_data.datasetup.models import SeitoQes
from saitama_data.datasetup.models.work.seito_qes_sosei.add_value import *
from saitama_data.datasetup.models.mix_in.io_mixin import BaseIOMixIn
from saitama_data.datasetup.models.mix_in.accepter_mixin import AccepterMixin
from typing import List


class SeitoQesSoseiBase(BaseIOMixIn, AccepterMixin):
    """
    sqsb = SeitoQesSoseiBase()
    sqsb.fetch_columns(sq_col=['id', 'grade', 'year'], sosei_col=['book']).read()
    """
    def __init__(self, *argv, **kwrds):
        pass

    def read(self):
        need_col = self.need_col
        get_columns_cls = self.get_columns_cls
        ca = AddValueAccepter(get_columns_cls)
        fetch_list = list(set([x.name for x in SeitoQes.schema.columns if x.name in ca.columns_use] + need_col))
        sq = SeitoQes().fetch_columns(fetch_list=fetch_list).read()
        ca.accept(sq.data)
        self.data = sq.data[list(set(ca.columns_add + need_col))]

    def fetch_columns(self, sq_col: List[str], sosei_col: List[ValueEngineer] or List[str]):
        self.need_col = sq_col
        self.get_columns_cls = sosei_col
        return self

    def save(self):
        raise NotImplementedError


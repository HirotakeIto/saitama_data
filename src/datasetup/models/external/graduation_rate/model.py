from src.datasetup.models.external.graduation_rate.schema import GraduationRateSchema
from src.datasetup.models.external.graduation_rate.seed import get_data
from src.datasetup.models.mix_in.accepter_mixin import AccepterMixin


class AdhocIOMixIn:
    def read(self):
        self.data =  get_data()
        return self.data

    def build(self):
        self.read()
        return self


class GraduationRate(AdhocIOMixIn, AccepterMixin, GraduationRateSchema):
    merge_key = ['city_id']


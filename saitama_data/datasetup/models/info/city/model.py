from saitama_data.datasetup.models.info.city.seed import get_data
from saitama_data.datasetup.models.info.city.schema import CitySchema
from saitama_data.datasetup.models.mix_in.accepter_mixin import AccepterMixin


class AdhocIOMixIn:
    def read(self):
        self.data =  get_data()
        return self.data

    def build(self):
        self.read()
        return self


class City(AdhocIOMixIn, AccepterMixin, CitySchema):
    pass


def debug():
    # a = AdhocIOMixIn()
    # a.read()
    c = City()
    c.read()

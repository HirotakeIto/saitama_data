from saitama_data.datasetup.models.master.id_master.model import IdMaster
# from .model import IdMaster

class PrimeId(IdMaster):
    def fetch_columns(self, fetch_list=None):
        self.fetch_list = fetch_list
        return self

    def read(self, stm=None):
        if stm is None:
            stm = self.stm
        mapper = {
            'school_id': 'school_id_prime',
            'grade': 'grade_prime',
            'class': 'class_prime',
            'city_id': 'city_id_prime'
        }
        # start
        self.data = (
            self.fetch_data(stm)
            .assign(
                year_prime = lambda dfx: dfx['year']
            )
            .assign(
                year = lambda dfx: dfx['year'] + 1
            )
            .rename(columns=mapper)
            [self.fetch_list]
        )
        return self
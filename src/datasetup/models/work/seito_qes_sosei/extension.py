from .. import SeitoQesSosei
from src.datasetup.models.master.id_master.model import IdMaster
from sqlalchemy.sql import select

def plus_columns(stm, column_list):
    for col in column_list:
        stm = stm.column(col)
    return stm

class PrimeSeitoQesSosei(SeitoQesSosei):
    """
    pg = PrimeGakuryoku().fetch_columns(['mst_id', 'year', 'kokugo_level_prime']).read()
    g = Gakuryoku().fetch_columns(['id', 'year', 'kokugo_level']).read()
    """
    merge_key = ['id', 'year']
    def fetch_columns(self, fetch_list=None):
        self.fetch_list = fetch_list
        return self

    def read(self, stm=None):
        print('本当はmst_idで紐付けなきゃいけないのに、出来ていない。。。。joinしたtableってどうやって表現するんだ。。。。')
        """
        pg.stm.fromsとかかな？
        aaa = pg.stm.with_only_columns(pg.stm.froms[0].c)
        """
        # import pdb;pdb.set_trace()
        if stm is None:
            stm = self.stm
        mapper = {
            'cramschool': 'cramschool_prime',
            'book': 'book_prime',
            'birth': 'birth_prime',
            'strategy': 'strategy_prime',
            'selfcontrol': 'selfcontrol_prime',
            'selfefficacy': 'selfefficacy_prime',
            'dilligence': 'dilligence_prime'
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
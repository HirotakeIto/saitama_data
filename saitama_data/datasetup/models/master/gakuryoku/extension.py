from .. import Gakuryoku
from .. import IdMaster
# from sqlalchemy.sql import select
# from saitama_data.datasetup.models.master import IdMaster, Gakuryoku

def plus_columns(stm, column_list):
    for col in column_list:
        stm = stm.column(col)
    return stm

class PrimeGakuryoku(Gakuryoku):
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
        joinとかで構成される複数テーブルsql扱いなんとかしたい、、、
        デフォルトのテーブル名を与えて、色々なメソッドに「明示的にテーブル名を与えなければ、デフォルトテーブル名を使う」みたいにしたい。
        カラムの認識とかを、
        
            リストで与えた場合gakuryoku → id_masterの順番にやって、
            辞書で与えた場合はそれぞれ持って行きて、
            重複あったらエラーとかにすればいいでない？
        
        where_in とかも、テーブル名を与えた場合と与えてない場合で分けてやったりすればいいんでは？
        """
        """
        アイディア： fetchする際に中にyearとかが含まれていたら一個前に遡るみたいな命令かけないかな。
        """
        # import pdb;pdb.set_trace()
        if stm is None:
            stm = self.stm
        mapper = {
            'kokugo_level': 'kokugo_level_prime',
            'math_level': 'math_level_prime',
            'eng_level': 'eng_level_prime',
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
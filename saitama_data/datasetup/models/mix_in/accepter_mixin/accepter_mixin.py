import pandas as pd

class Accepter:
    def __init__(self, merge_key=None, data=None, **argv):
        self.merge_key = merge_key
        self.data = data
        for attr_name in list(argv.keys()):
            self.__setattr__(attr_name, argv[attr_name])

    def build(self):
        pass

    def read(self):
        pass

    def enginner(self):
        pass

    def accept(self, t):
        """
        t is visitor(t have `data` attribute)
        """
        # parametor
        visitor = t.data
        data = self.data
        merge_key = self.merge_key
        # accept
        visitor = pd.merge(visitor, data, on=merge_key, how='left')
        print('visit: now tabele matrix is', visitor.shape)
        t.data = visitor


class AccepterMixin:
    def build_accepter(self, merge_key=None):
        if merge_key is None:  # これはモンキーパッチかも。AccepterMixinを継承予定のクラスがmerge_keyを持っていることを前提にしているが。。。
            merge_key = self.merge_key
        return self._build_accepter(merge_key=merge_key, data=self.data)

    @staticmethod
    def _build_accepter(merge_key, data: pd.DataFrame):
        if all( x in data.columns.tolist() for x in merge_key) is False:
            raise KeyError("Please fetch merge_key")
        accepter = Accepter(merge_key=merge_key, data=data)
        return accepter
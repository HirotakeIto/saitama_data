import os
import pandas as pd
from saitama_data.datasetup.models.mix_in.io_mixin.basic import BaseIOMixIn
from saitama_data.lib.safe_path import safe_path


class CsvIOMixin(BaseIOMixIn):
    """
    path というアトリビューションがあること前提の設計
    """
    def read(self, **argv):
        self.data = pd.read_csv(filepath_or_buffer=safe_path(self.path), **argv)
        return self

    def save(self, index=False, **argv):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.data.to_csv(path_or_buf=self.path, index=index, **argv)

    def pipe(self, func, *args, **kwargs):
        self.data = self.data.pipe(func, *args, **kwargs)
        return self

    def limit(self, *argv, **kwrds):
        # 未実装
        return self

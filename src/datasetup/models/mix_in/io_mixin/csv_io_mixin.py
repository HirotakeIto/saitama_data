import os
import pandas as pd
from src.datasetup.models.mix_in.io_mixin.basic import BaseIOMixIn


class CsvIOMixin(BaseIOMixIn):
    """
    path というアトリビューションがあること前提の設計
    """
    def read(self, **argv):
        self.data = pd.read_csv(filepath_or_buffer=self.path, **argv)
        return self

    def save(self, **argv):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.data.to_csv(path_or_buf=self.path, **argv)
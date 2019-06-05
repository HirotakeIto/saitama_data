# from .model import SeitoQes
from collections import OrderedDict
from saitama_data.datasetup.models.master.seito_qes.model import SeitoQes
import pandas as pd
import pdb

def crazy_val_to_nan(series: pd.Series, lower_permitted=-pd.np.inf, upeer_permitted=pd.np.inf, ):
    def crazy_to_nan(seriesx: pd.Series):
        return seriesx.mask(~seriesx.between(lower_permitted, upeer_permitted), pd.np.nan)
        ## 以下は一緒
        # return pd.Series(pd.np.where(seriesx.between(lower_permitted, upeer_permitted), series, pd.np.nan), index=seriesx.index)
        # return seriesx.where(seriesx.between(lower_permitted, upeer_permitted), pd.np.nan)

    return (
        series
        .pipe(crazy_to_nan)
    )

def crazy_to_nan_reverse(series: pd.Series, **argv_crazy_val_to_nan):
    return (
        series
        .pipe(crazy_val_to_nan, **argv_crazy_val_to_nan)
        .pipe(lambda ser: ser.max() - ser + 1)
    )


class StudentLives(SeitoQes):
    """
    s = StudentLives().fetch_columns(['classlives', 'id', 'year']).read()
    s.data.classlives.value_counts()
    s.data.q83.value_counts()
    """

    lives_qes_info_mapper = OrderedDict([
        ('classlives', {'o': "q83", 'f': crazy_to_nan_reverse, 'argv': {'lower_permitted': 1, 'upeer_permitted': 4}}),
        ('classeconomy', {'o': 'q84', 'f': crazy_to_nan_reverse, 'argv': {'lower_permitted': 1, 'upeer_permitted': 4}}),
        ('friendrelation', {'o': 'q86', 'f': crazy_to_nan_reverse, 'argv': {'lower_permitted': 1, 'upeer_permitted': 4}}),
        ('teacherconsultation', {'o': 'q87', 'f': crazy_to_nan_reverse, 'argv': {'lower_permitted': 1, 'upeer_permitted': 4}}),
        ('homework', {'o': 'q107', 'f': crazy_to_nan_reverse, 'argv': {'lower_permitted': 1, 'upeer_permitted': 4}}),
        ('preparereview', {'o': 'q108', 'f': crazy_to_nan_reverse, 'argv': {'lower_permitted': 1, 'upeer_permitted': 4}}),
        ('originalinputweek', {'o': 'q109', 'f': crazy_to_nan_reverse, 'argv': {'lower_permitted': 1, 'upeer_permitted': 6}}),
        ('originalinputholiday', {'o': 'q110', 'f': crazy_to_nan_reverse, 'argv': {'lower_permitted': 1, 'upeer_permitted': 6}}),
        ('readingtime', {'o': 'q112', 'f': crazy_val_to_nan, 'argv': {'lower_permitted': 1, 'upeer_permitted': 5}}),
        ('gamingtime', {'o': 'q114', 'f': crazy_to_nan_reverse, 'argv': {'lower_permitted': 1, 'upeer_permitted': 6}}),
        ('gamingrule', {'o': 'q115', 'f': crazy_to_nan_reverse, 'argv': {'lower_permitted': 1, 'upeer_permitted': 2}}),
        ('telesnsuse', {'o': 'q116', 'f': crazy_to_nan_reverse, 'argv': {'lower_permitted': 1, 'upeer_permitted': 7}}),
        ('telesnsInst', {'o': 'q117', 'f': crazy_to_nan_reverse, 'argv': {'lower_permitted': 1, 'upeer_permitted': 2}}),
        ('conversationhome', {'o': 'q118', 'f': crazy_to_nan_reverse, 'argv': {'lower_permitted': 1, 'upeer_permitted': 4}}),
        ('communicationarea', {'o': 'q119', 'f': crazy_to_nan_reverse, 'argv': {'lower_permitted': 1, 'upeer_permitted': 4}}),
        ('teacheraccept', {'o': 'q208', 'f': crazy_to_nan_reverse, 'argv': {'lower_permitted': 1, 'upeer_permitted': 4}}),
        ('teacherinstruction', {'o': 'q210', 'f': crazy_to_nan_reverse, 'argv': {'lower_permitted': 1, 'upeer_permitted': 4}}),
    ])

    @property
    def lives_qes_info_mapper_from_o(self):
        return (
            OrderedDict((
                (value['o'], {'c': key, 'f': value['f'], 'argv': value['argv']})
                for key, value in self.lives_qes_info_mapper.items()
            ))
        )

    @property
    def cols_permitted(self):
        return list(self.lives_qes_info_mapper.keys()) + self.merge_key

    def __init__(self):
        super().__init__()
        self.fetch_columns()

    def fetch_columns(self, fetch_list=None):
        fetch_list_o = fetch_list if fetch_list is not None else self.cols_permitted
        if all(x in self.cols_permitted for x in fetch_list_o) is False:
            raise ValueError("Please use only permiited columns")
        super().fetch_columns(self.fetch_list_parser(fetch_list_o))
        return self

    def fetch_list_parser(self, fetch_list):
        """
        fetch_list から
        """
        fetch_list_encoded_seito_qes = [
            self.lives_qes_info_mapper[x]['o'] if x in self.lives_qes_info_mapper.keys() else x for x in fetch_list]
        return fetch_list_encoded_seito_qes

    def read(self, **argv):
        super().read()
        self.engineer()
        return self

    def engineer(self):
        for col in self.data.columns:
            if col in list(self.lives_qes_info_mapper_from_o.keys()):
                newname = self.lives_qes_info_mapper_from_o[col]['c']
                engineer_function = self.lives_qes_info_mapper_from_o[col]['f']
                # pdb.set_trace()
                argv = self.lives_qes_info_mapper_from_o[col]['argv'] \
                    if 'argv' in list(self.lives_qes_info_mapper_from_o[col].keys()) else {}
                self.data[newname] = self.data[col].pipe(engineer_function, **argv)
                self.data.drop(col, inplace=True, axis=1)


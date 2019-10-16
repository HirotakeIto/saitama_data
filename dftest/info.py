import pandas as pd
from collections import namedtuple
from dftest.lib import to_string
from saitama_data.lib.safe_path import safe_path

__VALUE_COLUMN_NAME__ = 'variable'
__VALUE_NAME__ = 'value'
__FUNC_COLUMN_NAME__ = 'func'
__NAME_COLUMN_NAME__ = 'name'


class InfosDataFrameBuilder:
    """
    from sklearn.datasets import load_iris
    df = pd.DataFrame(load_iris()['data'], columns=load_iris()['feature_names'])
    test_name = 'groupby_grade'
    df['grade'] = pd.np.random.choice(['a', 'b', 'c', 8.0], size=df.shape[0])
    df['year'] = pd.np.random.choice([2015.00, 2018], size=df.shape[0])
    ifb = InfoDataFrameBuilder(func_list=['mean'], func_list_groupby=['count'], key_groupby=['year', 'grade'])
    ifb.build(df)
    ifb.info_df
    """
    InfoMethod = namedtuple("InfoMethod", ("func", "grouped", "keys"))
    InfoMethod.__new__.__defaults__ = ('count', False, [])

    def __init__(self, func_list: list, func_list_groupby: list, key_groupby: list or str):
        self.func_list = func_list
        self.func_list_groupby = func_list_groupby
        self.key_groupby = key_groupby
        self.info_df = pd.DataFrame()

    @property
    def info_methods(self):
        methods = []
        for func in self.func_list:
            methods.append(self.InfoMethod(func=func, grouped=False, keys=None))
        for func in self.func_list_groupby:
            methods.append(self.InfoMethod(func=func, grouped=True, keys=self.key_groupby))
        return methods

    def build(self, df):
        info_df = pd.DataFrame()
        for info_method in self.info_methods:
            info_df = info_df.append(self.get_info(df=df, info_method=info_method))
        self.info_df = info_df.pipe(self.give_name)
        return self

    def get_info(self, df: pd.DataFrame, info_method: InfoMethod):
        if info_method.grouped is False:
            return self.get_info_from_df(dfx=df, func=info_method.func)
        elif info_method.grouped is True:
            return self.get_info_from_df_groupby(dfx=df, func=info_method.func, keys=info_method.keys)
        else:
            raise ValueError

    @staticmethod
    def get_info_from_df(dfx: pd.DataFrame, func, agg_kwargs={}):
        # todo: 元のやつに反映
        res = dfx.agg(func, numeric_only=True, **agg_kwargs).rename(__VALUE_NAME__).reset_index().rename(columns={'index': __VALUE_COLUMN_NAME__})
        res[__FUNC_COLUMN_NAME__] = to_string(func)
        return res

    def get_info_from_df_groupby(self, dfx: pd.DataFrame, keys, func):
        grouped_df = dfx.groupby(keys)
        result = pd.DataFrame()
        for keys_value, df in grouped_df:
            res = self.get_info_from_df(df, func=func)
            if type(keys_value) is tuple:
                for key, value in zip(keys, keys_value):
                    res[key] = value
            else:
                key = keys if type(keys) is str else keys[0]
                res[key] = keys_value
            result = result.append(res)
        return  result

    @staticmethod
    def get_name(series: pd.Series, columns: list):
        return '_'.join([to_string(series[x]) for x in columns if pd.isna(series[x]) is False])

    def give_name(self, dfx):
        use_columns_base = [__VALUE_COLUMN_NAME__, __FUNC_COLUMN_NAME__]
        not_used_columns = [__VALUE_NAME__]
        use_columns = use_columns_base + sorted([x for x in dfx.columns if x not in use_columns_base + not_used_columns])
        dfx[__NAME_COLUMN_NAME__] = dfx.apply(self.get_name, columns=use_columns, axis=1)
        return dfx


class InfoSeries(pd.Series):
    @property
    def _constructor(self):
        return InfoSeries

    @property
    def _constructor_expanddim(self):
        return InfosDataFrame

    @property
    def test_name(self):
        return self[__NAME_COLUMN_NAME__]

    @property
    def test_value(self):
        return self[__VALUE_NAME__]

    @property
    def test_func(self):
        return self[__FUNC_COLUMN_NAME__]

    @property
    def test_variable(self):
        return self[__VALUE_COLUMN_NAME__]

    @property
    def message(self):
        return 'name: {name},  value: {value}'.format(name=self.test_name, value=self.test_value)

    @classmethod
    def get_info(cls, name: str, value: str or float or int, func_name: str, variable: str, **argv):
        data = [name, value, func_name, variable] + list(argv.values())
        index = [__NAME_COLUMN_NAME__, __VALUE_NAME__, __FUNC_COLUMN_NAME__, __VALUE_COLUMN_NAME__] + list(argv.keys())
        return cls( data=data, index=index)


class InfosDataFrame(pd.DataFrame):
    """
    from sklearn.datasets import load_iris
    df = pd.DataFrame(load_iris()['data'], columns=load_iris()['feature_names'])
    test_name = 'groupby_grade'
    df['grade'] = pd.np.random.choice(['a', 'b', 'c', 8.0], size=df.shape[0])
    df['year'] = pd.np.random.choice([2015.00, 2018], size=df.shape[0])
    ifb = InfosDataFrame.read_from_df(df, func_list=['mean'], func_list_groupby=['count'], key_groupby=['year', 'grade'])
    a = ifb.get_info_from_name('petal length (cm)_mean')
    ifb.head(3)
    ifb.infos
    """
    @property
    def _constructor(self):
        return InfosDataFrame

    @property
    def _constructor_sliced(self):
        return InfoSeries

    @classmethod
    def read_from_df(cls, df, func_list: list, func_list_groupby: list, key_groupby: list or str):
        idfb = InfosDataFrameBuilder(func_list=func_list, func_list_groupby=func_list_groupby, key_groupby=key_groupby)
        idfb.build(df)
        return cls(idfb.info_df)

    @classmethod
    def read_from_csv(cls, path):
        return cls(pd.read_csv(safe_path(path)))

    @property
    def infos(self):
        return [self.iloc[i, :] for i in range(self.shape[0])]

    @property
    def names(self):
        return [x.test_name for x in self.infos]

    # あとでちゃんと実装
    # def __iter__(self):
    #     self.iter_count = 0
    #
    # def __next__(self):
    #     counter = self.iter_count
    #     if counter + 1 > len(self.infos):
    #         raise StopIterations
    #     self.iter_count = counter + 1
    #     return self.infos[counter]

    def have_name(self, name):
        return name in self[__NAME_COLUMN_NAME__].values

    def get_info_from_name(self, name):
        return self.loc[self[__NAME_COLUMN_NAME__] == name, :].iloc[0]  # 最初の1個目だけ取得する

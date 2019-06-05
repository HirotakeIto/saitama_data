from abc import ABCMeta, abstractmethod
import re
import pandas as pd
import json


class DataTester:
    """
    Data Testを行うインターフェース.
    Dataがtestモジュール持つの変だと思ったけど、やっぱ気軽にtestするためにはmixinにした方が良いね
    """

    def __init__(self, data=None, data_test_cls_list = None, test_df_info_path = '.'):
        self.data = data
        self.data_test_cls_list = data_test_cls_list if data_test_cls_list is not None else []
        self.test_df_info_path = test_df_info_path

    def save_valid(self):
        test_info_dict = self.get_test_info()
        self.write_test_info(test_info_dict)

    def get_test_info(self):
        test_info_dict = {}
        for i, data_test_cls in enumerate(self.data_test_cls_list):
            test_info_dict.update({
                'test{0}'.format(i): {
                    'clsname': data_test_cls.__class__.__name__,
                    'args': data_test_cls.init_argv,
                    'result': data_test_cls.get_df_info(self)
                }
            })
        return test_info_dict

    def test(self):
        test_info_valid_dict = self.read_test_indo()
        for key, test_info_valid in test_info_valid_dict.items():
            cls = globals()[test_info_valid['clsname']](**test_info_valid['args'])
            cls.test(self, test_info_valid['result'])

    def write_test_info(self, test_info_dict):
        with open(self.test_df_info_path, mode='w', encoding='utf8') as f:
            json.dump(test_info_dict, f, ensure_ascii=False)
        return self

    def read_test_indo(self):
        with open(self.test_df_info_path, mode='r', encoding='utf8') as f:
            res = json.load(f)
        return res


class DataTest(metaclass=ABCMeta):
    def __init__(self, **argv):
        self.init_argv = None
        if self.__class__.__name__.isupper():
            self.name = self.__class__.__name__.lower()
        else:
            self.name = re.sub(
                "([A-Z])",
                lambda x: "_" + x.group(1).lower(), self.__class__.__name__
            ).lstrip('_')
        for key, value in argv.items():
            self.__setattr__(key, value)

    def set_init_argv_from_argv(self, **argv):
        self.init_argv = argv

    @abstractmethod
    def get_df_info(self, cls, **argv):
        raise NotImplementedError

    @abstractmethod
    def test(self, cls, df_info_valid, **argv):
        raise NotImplementedError


class TestGroupInfo(DataTest):

    def __init__(self, group_cols, **argv):
        super().__init__(**argv)
        self.set_init_argv_from_argv(group_cols=group_cols, **argv)
        self.group_cols = group_cols

    def get_df_info(self, cls, **argv):
        return self._get_df_info(df=cls.data, group_cols=self.group_cols)

    def test(self, cls, df_info_valid, **argv):
        df_info_test = self.get_df_info(cls)
        return self._test(df_info_test, df_info_valid, self.group_cols)

    def _get_df_info(self, df, group_cols):
        raise NotImplementedError

    def _test(self, test_info_dict, df_info_valid, group_cols):
        df_test_info_dict = self.helper_get_df_test_info_from_dict(test_info_dict, group_cols)
        for test_info_dict_valid in df_info_valid:
            tuple_group_cols = tuple((test_info_dict_valid[col] for col in group_cols))
            for key, val_valid in test_info_dict_valid.items():
                if key not in group_cols:
                    val_test = df_test_info_dict[tuple_group_cols][key]
                    try:
                        assert val_test == val_valid
                    except AssertionError:
                        print(
                            "Column {col} &  {grouper_name} is {grouper}, "
                            "{test} is supposed, But  {valid} is made.".format(
                            col=key, grouper_name=group_cols,
                            grouper = tuple_group_cols, test=val_test, valid=val_valid))

    @staticmethod
    def helper_get_df_test_info_from_dict(test_info_dict, group_cols):
        """検索しやすいように、 group_cols > column > value の辞書を作成"""
        return (
            pd.DataFrame.from_dict(test_info_dict)
            .set_index(group_cols)
            .to_dict(orient='index')
        )


class TestGroupMean(TestGroupInfo):
    def _get_df_info(self, df, group_cols):
        return df.groupby(group_cols).mean(numeric_only=True).reset_index().fillna('na').to_dict(orient='record')


class TestGroupStd(TestGroupInfo):
    def _get_df_info(self, df, group_cols):
        return df.groupby(group_cols).std().reset_index().fillna('na').to_dict(orient='record')


class TestGroupCount(TestGroupInfo):
    def _get_df_info(self, df, group_cols):
        return df.groupby(group_cols).count().reset_index().to_dict(orient='record')


class TestGroupSize(TestGroupInfo):
    def _get_df_info(self, df, group_cols):
        return df.groupby(group_cols).size().reset_index().to_dict(orient='record')

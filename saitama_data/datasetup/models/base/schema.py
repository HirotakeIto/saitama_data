import pandas as pd
from pandas_schema import Column, Schema


class BaseSchema():
    """
    得られたデータがvalidであることを確認するためのクラス
    """
    schema = Schema([Column('id', [])])
    convert_list = {}

    def __init__(self, data=None):
        self.data = data

    def validate(self):
        self._validate(self.data)
        return self

    def convert(self):
        self.data = self._convert(self.data)
        return self

    def validate_convert(self):
        self.data = self._validate_convert(self.data)
        return self

    def adjust_schema(self):
        self.data = self._adjust_schema(self.data)
        return self

    def _validate(self, data):
        errors = self.schema.validate(data)
        for error in errors:
            print(error)
        if len(errors) != 0:
            raise ValueError('validation error')

    def _convert(self, data):
        return (
            self.convert_columns_type(data, self.convert_list)
            [[row.name for row in self.schema.columns]]  # set row order
        )

    def _validate_convert(self, data):
        self._validate(data)
        return self._convert(data)

    def _adjust_schema(self, data):
        schema_columns = [ s.name for s in self.schema.columns]
        data = self.drop(data, target_list=schema_columns)
        data = self.plus(data, target_list=schema_columns, value=pd.np.nan)
        return data

    @staticmethod
    def drop(data, target_list):
        no_need_col = []
        for col in data.columns:
            if col not in target_list:
                no_need_col.append(col)
        return data.drop(no_need_col, axis='columns')

    @staticmethod
    def plus(data, target_list, value=pd.np.nan):
        for col in target_list:
            if col not in list(data.columns):
                data[col] = value
        return data

    @staticmethod
    def convert_columns_type(data, convert_list: dict, errors='raise'):
        # for c in convert_list.keys():
        #     if convert_list[c] == float:
        #         data[c] = pd.to_numeric(data[c], errors=errors)
        #     if convert_list[c] == int:
        #         data[c] = data[c].astype(int)
        return data.astype(convert_list, errors=errors)

#
# class BaseModel(BaseIOMixIn, BaseSchema):
#     pass

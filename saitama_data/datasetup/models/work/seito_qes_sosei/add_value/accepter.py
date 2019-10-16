import pandas as pd
from typing import List
# from saitama_data.datasetup.models.work.seito_qes_sosei.add_value import ValueEngineer
from .value_engineer import ValueEngineer, get_cls_from_name, get_family_cls_list
import gc


class AddValueAccepter():
    def __init__(self, fetch_list: List[ValueEngineer or str]):
        self.value_engineers = self.get_value_engineers(fetch_list)

    def accept(self, visitor: pd.DataFrame):
        for value_engineer_cls in self.value_engineers_use:
            value_engineer = value_engineer_cls()
            print('{name} accept visitor. '.format(name=value_engineer.name))
            visitor[value_engineer.name] = value_engineer.value(visitor)
            gc.collect()
        return visitor

    @staticmethod
    def get_value_engineers(fetch_list: List[ValueEngineer or str]) ->  List[ValueEngineer]:
        value_engineers = []
        for x in fetch_list:
            if type(x) is str:
                value_engineers.append(get_cls_from_name(x))
            elif issubclass(x, ValueEngineer):
                value_engineers.append(x)
            else:
                raise TypeError
        return  value_engineers

    @property
    def value_engineers_use(self) -> List[ValueEngineer]:
        engineers: List[ValueEngineer] = []
        for engineer in self.value_engineers:
            engineers += get_family_cls_list(engineer)
        engineers_unique = []
        for x in engineers:
            if x not in engineers_unique:
                engineers_unique.append(x)
        return engineers_unique

    @property
    def columns_use(self):
        column_list: List[str] = []
        for x in self.value_engineers_use:
            column_list += x.columns_use
        return list(set(column_list))

    @property
    def columns_add(self):
        column_list: List[str] = []
        for x in self.value_engineers_use:
            column_list += [x.name]
        return list(set(column_list))

    @staticmethod
    def convert_name_to_cls(name: str) -> ValueEngineer:
        return get_cls_from_name(name)

from functools import reduce
from abc import ABCMeta, abstractmethod
import numpy as np
import pandas as pd
from typing import Callable, List
print("SeitoQesSosei：年度変更に耐えれるとは限らないから要確認")


def cramschool_type1(q111: float):
    if q111 == 1:
        return 1
    else:
        return 10 - q111


def cramschool_type2(q111: float):
    return 9 - q111


def _cramschool(q111: float, year: float):
    if year in [2015, 2016]:
        return cramschool_type1(q111)
    if year in range(2017, 2050):
        return cramschool_type2(q111)


def _no_cramschool(cramschool: float):
    return 1 if cramschool == 1 else 0


def _book(q113: float):
    return q113


def _teacher_attitude(q86: float, q87: float):
    return 6 * 2 - reduce(np.add, [q86, q87])


def _class_attitude(q83: float, q84: float, q85: float):
    return 6 * 3 - reduce(np.add, [q83, q84, q85])


def _birth(q138: float):
    if (1 <= q138) & (q138 <= 3):
        return '1_3'
    elif (4 <= q138) & (q138 <= 6):
        return '4_6'
    elif (7 <= q138) & (q138 <= 9):
        return '7_9'
    elif (10 <= q138) & (q138 <= 12):
        return '10_12'
    else:
        return np.nan


def _birth_month(q138: float):
    if (1 <= q138) & (q138 <= 12):
        return q138
    else:
        return np.nan


def _selfcontrol(q39, q40, q41, q42, q43, q44, q45, q46):
    selfcontrol_list = [q39, q40, q41, q42, q43, q44, q45, q46]
    return len(selfcontrol_list) * 6 - reduce(np.add, selfcontrol_list)


def _selfefficacy(q47, q48, q49, q50, q51, q52, q53, q54):
    selfefficacy_list = [q47, q48, q49, q50, q51, q52, q53, q54]
    return len(selfefficacy_list) * 6 - reduce(np.add, selfefficacy_list)


def _dilligence(
        q55: float, q56: float, q57: float, q58: float, q59: float, q60: float,
        q61: float, q62: float, q63: float, q64: float, q65: float, q66: float, q67: float
):
    dilligence_plut_list = [
        q55, q56, q57, q58, q59, q60, q61, q63, q64, q65, q66, q67]
    dilligence_minus_list = [q62]
    return reduce(np.add, dilligence_plut_list) \
        - reduce(np.add, dilligence_minus_list) \
        + len(dilligence_minus_list) * 6


def _zyunan(q4: float, q5: float, q6: float, q7: float):
    return 6 * 4 - reduce(np.add, [q4, q5, q6, q7])


def _planning(q12: float, q13: float, q14: float, q15: float):
    return 6 * 4 - reduce(np.add, [q12, q13, q14, q15])


def _execution(q18: float, q19: float, q20: float, q21: float):
    return 6 * 4 - reduce(np.add, [q18, q19, q20, q21])


def _resource(q24: float, q25: float, q26: float, q27: float):
    return 6 * 4 - reduce(np.add, [q24, q25, q26, q27])


def _ninti(q28: float, q29: float, q30: float, q31: float):
    return 6 * 4 - reduce(np.add, [q28, q29, q30, q31])


def _effort(q35: float, q36: float, q37: float, q38: float):
    return 6 * 2 - reduce(np.add, [q36, q38]) + reduce(np.add, [q35, q37])


def _strategy(zyunan: float, planning: float, execution: float, resource: float, ninti: float, effort: float):
    return reduce(np.add, [zyunan, planning, execution, resource, ninti, effort])


def _al_kokugo(q89: float, q92: float, q93: float):
    return len([q89, q92, q93]) * 5 - reduce(np.add, [q89, q92, q93])


def _al_math(q97: float, q100: float, q101: float):
    return len([q97, q100, q101]) * 5 - reduce(np.add, [q97, q100, q101])


def _al_eng(q105: float):
    return len([q105]) * 5 - reduce(np.add, [q105])


def _trad_kokugo(q91: float):
    return len([q91]) * 5 - reduce(np.add, [q91])


def _trad_math(q99: float):
    return len([q99]) * 5 - reduce(np.add, [q99])


def _grit():
    pass


def _hoiku():
    pass


def _future_school():
    pass

# def get_value_func(columns_use, func, mapping_func_argv):
#     return lambda dfx: value_template(dfx, columns_use, func, mapping_func_argv)


class ValueEngineer(metaclass=ABCMeta):
    name = 'ValueEngineer'.lower()
    columns_use = []

    @abstractmethod
    def value(self, *argv, **kwrds) -> pd.Series:
        pass


def vectorized_func_argv(dfx: pd.DataFrame, mapping: dict) -> dict:
    """
    np.vectrized した関数を実行する引数を作るラッパー
    :param dfx: np.vectrized した関数でcallした時に引数に用いるSeriesを持ったDataFrame
    :param mapping: 引数のマッピング
    """
    return {arg: dfx[df_column] for arg, df_column in mapping.items()}


def vectorized_func_call(dfx: pd.DataFrame, vectorized_func: Callable, mapping_func_argv: dict) -> pd.Series:
    """
    np.vectrized した関数のラッパー
    :param dfx:
    :param vectorized_func:
    :param mapping_func_argv:
    :return:
    """
    return pd.Series(vectorized_func(**vectorized_func_argv(dfx, mapping_func_argv)), index=dfx.index)


def remove_annomaly(dfx: pd.DataFrame) -> pd.DataFrame:
    return dfx.replace({0: np.nan, 9: np.nan})


def set_na(seriesx: pd.Series, na_slicing: pd.Series) -> pd.Series:
    seriesx[na_slicing] = np.nan
    return seriesx


def value_template(dfx: pd.DataFrame, columns_use: list, func: Callable, mapping_func_argv) -> pd.Series:
    slicing = ~ (dfx[columns_use].isnull().sum(axis=1) == 0)
    # import pdb;pdb.set_trace()
    return (
        dfx
        .pipe(remove_annomaly)
        .pipe(vectorized_func_call, vectorized_func=func, mapping_func_argv=mapping_func_argv)
        .pipe(set_na, na_slicing = slicing)
    )


class TemplateValueEngineer(ValueEngineer):
    func = None
    mapping_func_argv = None

    def value(self, dfx: pd.DataFrame):
        return value_template(dfx, self.columns_use, self.func, self.mapping_func_argv)


class Cramschool(TemplateValueEngineer):
    name = 'Cramschool'.lower()
    func = np.vectorize(_cramschool)
    mapping_func_argv = {'q111': 'q111', 'year': 'year'}
    columns_use = list(set(list(mapping_func_argv.values())))


class NoCramschool(TemplateValueEngineer):
    name = 'no_cramschool'
    func = np.vectorize(_no_cramschool)
    mapping_func_argv = {'cramschool': 'cramschool'}
    columns_use = list(set(list(mapping_func_argv.values())))


class Book(TemplateValueEngineer):
    name = 'Book'.lower()
    func = np.vectorize(_book)
    mapping_func_argv = {'q113': 'q113'}
    columns_use = list(set(list(mapping_func_argv.values())))


class TeacherAttitude(TemplateValueEngineer):
    name = 'theacher_attitude'
    func = np.vectorize(_teacher_attitude)
    mapping_func_argv = {'q86': 'q86', 'q87': 'q87'}
    columns_use = list(set(list(mapping_func_argv.values())))


class ClassAttitude(TemplateValueEngineer):
    name = 'class_attitude'
    func = np.vectorize(_class_attitude)
    mapping_func_argv = {'q83': 'q83', 'q84': 'q84', 'q85': 'q85'}
    columns_use = list(set(list(mapping_func_argv.values())))


class Zyunan(TemplateValueEngineer):
    name = 'Zyunan'.lower()
    func = np.vectorize(_zyunan)
    mapping_func_argv = {'q4': 'q4', 'q5': 'q5', 'q6': 'q6', 'q7': 'q7'}
    columns_use = list(set(list(mapping_func_argv.values())))


class Planning(TemplateValueEngineer):
    name = 'Planning'.lower()
    func = np.vectorize(_planning)
    mapping_func_argv = {'q12': 'q12', 'q13': 'q13', 'q14': 'q14', 'q15': 'q15'}
    columns_use = list(set(list(mapping_func_argv.values())))


class Execution(TemplateValueEngineer):
    name = 'Execution'.lower()
    func = np.vectorize(_execution)
    mapping_func_argv = {'q18': 'q18', 'q19': 'q19', 'q20': 'q20', 'q21': 'q21'}
    columns_use = list(set(list(mapping_func_argv.values())))


class Resource(TemplateValueEngineer):
    name = 'Resource'.lower()
    func = np.vectorize(_resource)
    mapping_func_argv = {'q24': 'q24', 'q25': 'q25', 'q26': 'q26', 'q27': 'q27'}
    columns_use = list(set(list(mapping_func_argv.values())))


class Ninti(TemplateValueEngineer):
    name = 'Ninti'.lower()
    func = np.vectorize(_ninti)
    mapping_func_argv = {'q28': 'q28', 'q29': 'q29', 'q30': 'q30', 'q31': 'q31'}
    columns_use = list(set(list(mapping_func_argv.values())))


class Effort(TemplateValueEngineer):
    name = 'Effort'.lower()
    func = np.vectorize(_effort)
    mapping_func_argv = {'q35': 'q35', 'q36': 'q36', 'q37': 'q37', 'q38': 'q38'}
    columns_use = list(set(list(mapping_func_argv.values())))


class Strategy(TemplateValueEngineer):
    name = 'Strategy'.lower()
    func = np.vectorize(_strategy)
    mapping_func_argv = {
        'effort': 'effort',
        'execution': 'execution',
        'ninti': 'ninti',
        'planning': 'planning',
        'resource': 'resource',
        'zyunan': 'zyunan'
    }
    columns_use = list(set(list(mapping_func_argv.values())))

    def value(self, dfx: pd.DataFrame) -> pd.Series:
        slicing = ~ (dfx[self.columns_use].isnull().sum(axis=1) == 0)
        return (
            dfx
            .replace({0: np.nan})
            .pipe(vectorized_func_call, vectorized_func=self.func, mapping_func_argv=self.mapping_func_argv)
            .pipe(set_na, na_slicing=slicing)
        )


class Birth(ValueEngineer):
    name = 'Birth'.lower()
    func = np.vectorize(_birth)
    mapping_func_argv = {'q138': 'q138'}
    columns_use = list(set(list(mapping_func_argv.values())))

    def value(self, dfx: pd.DataFrame) -> pd.Series:
        slicing = ~ (dfx[self.columns_use].isnull().sum(axis=1) == 0)
        return (
            dfx
            .replace({0: np.nan})
            .pipe(vectorized_func_call, vectorized_func=self.func, mapping_func_argv=self.mapping_func_argv)
            .pipe(set_na, na_slicing=slicing)
        )


class BirthMonth(ValueEngineer):
    name = 'birth_month'
    func = np.vectorize(_birth_month)
    mapping_func_argv = {'q138': 'q138'}
    columns_use = list(set(list(mapping_func_argv.values())))

    def value(self, dfx: pd.DataFrame) -> pd.Series:
        slicing = ~ (dfx[self.columns_use].isnull().sum(axis=1) == 0)
        return (
            dfx
            .replace({0: np.nan})
            .pipe(vectorized_func_call, vectorized_func=self.func, mapping_func_argv=self.mapping_func_argv)
            .pipe(set_na, na_slicing=slicing)
        )


class Selfcontrol(TemplateValueEngineer):
    name = 'Selfcontrol'.lower()
    func = np.vectorize(_selfcontrol)
    mapping_func_argv = {
        'q39': 'q39',
        'q40': 'q40',
        'q41': 'q41',
        'q42': 'q42',
        'q43': 'q43',
        'q44': 'q44',
        'q45': 'q45',
        'q46': 'q46'
    }
    columns_use = list(set(list(mapping_func_argv.values())))


class Selfefficacy(TemplateValueEngineer):
    name = 'Selfefficacy'.lower()
    func = np.vectorize(_selfefficacy)
    mapping_func_argv = {
        'q47': 'q47',
        'q48': 'q48',
        'q49': 'q49',
        'q50': 'q50',
        'q51': 'q51',
        'q52': 'q52',
        'q53': 'q53',
        'q54': 'q54'
    }
    columns_use = list(set(list(mapping_func_argv.values())))


class Dilligence(TemplateValueEngineer):
    name = 'Dilligence'.lower()
    func = np.vectorize(_dilligence)
    mapping_func_argv = {
        'q55': 'q55',
        'q56': 'q56',
        'q57': 'q57',
        'q58': 'q58',
        'q59': 'q59',
        'q60': 'q60',
        'q61': 'q61',
        'q62': 'q62',
        'q63': 'q63',
        'q64': 'q64',
        'q65': 'q65',
        'q66': 'q66',
        'q67': 'q67'
    }
    columns_use = list(set(list(mapping_func_argv.values())))


class AlKokugo(TemplateValueEngineer):
    name = 'al_kokugo'
    func = np.vectorize(_al_kokugo)
    mapping_func_argv = {'q89': 'q89', 'q92': 'q92', 'q93': 'q93'}
    columns_use = list(set(list(mapping_func_argv.values())))


class AlMath(TemplateValueEngineer):
    name = 'al_math'
    func = np.vectorize(_al_math)
    mapping_func_argv = {'q100': 'q100', 'q101': 'q101', 'q97': 'q97'}
    columns_use = list(set(list(mapping_func_argv.values())))


class AlEng(TemplateValueEngineer):
    name = 'al_eng'
    func = np.vectorize(_al_eng)
    mapping_func_argv = {'q105': 'q105'}
    columns_use = list(set(list(mapping_func_argv.values())))


class TradKokugo(TemplateValueEngineer):
    name = 'trad_kokugo'
    func = np.vectorize(_trad_kokugo)
    mapping_func_argv = {'q91': 'q91'}
    columns_use = list(set(list(mapping_func_argv.values())))


class TradMath(TemplateValueEngineer):
    name = 'trad_math'
    func = np.vectorize(_trad_math)
    mapping_func_argv = {'q99': 'q99'}
    columns_use = list(set(list(mapping_func_argv.values())))


def get_cls_from_name(name: str) -> ValueEngineer:
    all_engineers_cls = [
        Book, Cramschool, NoCramschool, TeacherAttitude,
        Zyunan, Planning, Execution, Resource, Ninti, Effort, Strategy,
        Birth, BirthMonth, Selfcontrol, Selfefficacy, Dilligence,
        AlKokugo, AlMath, AlEng, TradKokugo, TradMath
    ]
    for cls in all_engineers_cls:
        if cls.name ==  name:
            return cls
    raise KeyError('{name} is missing in Module'.format(name=name))


def get_family_cls_list(tgt_cls: ValueEngineer) -> List[ValueEngineer]:
    all_engineers_cls = [
        Book, Cramschool, NoCramschool, TeacherAttitude,
        Zyunan, Planning, Execution, Resource, Ninti, Effort, Strategy,
        Birth, BirthMonth, Selfcontrol, Selfefficacy, Dilligence,
        AlKokugo, AlMath, AlEng, TradKokugo, TradMath
    ]
    col_model_map = {engineer.name: engineer for engineer in all_engineers_cls}

    def search_recursive(tgt: ValueEngineer, family_cls_list=[]):
        family_cls_list = [tgt] + family_cls_list
        for column_use in tgt.columns_use:
            if column_use in list(col_model_map.keys()):
                family_cls_list = search_recursive(col_model_map[column_use], family_cls_list)
        return family_cls_list

    return search_recursive(tgt_cls)

# TODO: やっぱりインスタンス作る必要ないのに、一生懸命クラス定義するの変な気がするわ
# 別にクラスや関数じゃなくてもimportできるんだから, namedtupleとかdataclassで作りゃいいじゃん

# from collections import namedtuple, OrderedDict
# ValueEngineerBase = namedtuple('ValueEngineer2', ['name', 'func', 'mapping_func_argv', 'columns_use', 'func2'])
# Book = ValueEngineerBase(
#     name='Book'.lower(),
#     func = np.vectorize(_book),
#     mapping_func_argv = {'q113': 'q113'},
#     columns_use = {},
#     func2 = lambda x: print('100')
# )
# Book = Book._replace(columns_use = list(set(list(Book.mapping_func_argv.values()))))
# Book = Book._replace(func2 = list(set(list(Book.mapping_func_argv.values()))))

# import dataclasses  # 使わないならアンインストールしとこう TODOTODO
# @dataclasses.dataclass
# class ValueEngineerBase:
#     name : str ='Book'.lower()
#     func : Callable = lambda x: x
#     mapping_func_argv: dict = dataclasses.field(default_factory=dict)
#     columns_use : List[str] = dataclasses.field(default_factory=list)
#     value: Callable = lambda x: x

# Book = ValueEngineerBase(
#     name='Book'.lower(),
#     func = np.vectorize(_book),
#     mapping_func_argv = {'q113': 'q113'},
# )
# Book.columns_use = list(set(list(Book.mapping_func_argv.values())))
# Book.value : Callable[[pd.DataFrame], pd.Series] = lambda dfx: value_template(dfx, Book.columns_use, Book.func, Book.mapping_func_argv)

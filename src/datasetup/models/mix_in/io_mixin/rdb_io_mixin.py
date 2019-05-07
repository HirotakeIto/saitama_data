"""
read()でconnectして
dataのgetterでdataがNoneだったらstmに基づいて呼び出し。すでにあったらそのまま返すというのが良いだろういう結論に
"""
import pandas as pd
from sqlalchemy.sql import select
from sqlalchemy import Table, MetaData
from src.connect_server import return_connection
from src.datasetup.models.mix_in.io_mixin.basic import BaseIOMixIn
from src.sql.connect_postgres import to_sql
_engine_default, _conn_default = return_connection()


class RdbIOMixin(BaseIOMixIn):
    @property
    def conn(self):
        if '_conn' not in self.__dict__.keys():
            self.conn = _conn_default
        return self._conn

    @conn.setter
    def conn(self, conn):
        self._conn = conn

    @property
    def metadata(self):
        if '_metadata' not in self.__dict__.keys():
            self.metadata = MetaData(self.conn)
        return  self._metadata

    @metadata.setter
    def metadata(self, metadata):
        self._metadata = metadata

    @property
    def table(self):
        if '_table' not in self.__dict__.keys():
            self.table = Table(self.table_name, self.metadata, schema=self.schema_name, autoload=True)
        return self._table

    @table.setter
    def table(self, table):
        self._table = table

    @property
    def stm(self):
        if '_stm' not in self.__dict__.keys():
            self._stm = select([self.table])
        return self._stm

    @stm.setter
    def stm(self, stm):
        self._stm = stm

    def read(self, stm=None, **argv):
        if stm is None:
            stm = self.stm
        self.data = self.fetch_data(stm, **argv)
        return self

    def save(self, if_exists='replace', **argv):
        to_sql(df=self.data, name=self.table_name, schema=self.schema_name, engine=_engine_default, if_exists=if_exists, **argv)

    def pipe(self, func, *argv, **kwrd):
        func(self, *argv, **kwrd)
        return self

    def between(self, target: str, a: float, b: float):
        stm = self.stm
        t = self.table
        if target not in t.columns.keys():
            print('{target} is not {table} columns'.format(target=target, table=t.name))
        self.stm = stm.where(t.c[target].between(a, b))
        return self

    def where_in(self, target: str, l: list):
        stm = self.stm
        t = self.table
        if target not in t.columns.keys():
            print('{target} is not {table} columns'.format(target=target, table=t.name))
        self.stm = stm.where(t.c[target].in_(l))
        return self

    # def where_in(self, target: str, l: list):
    #     stm = self.stm
    #     if target not in stm.c.keys():
    #         print('{target} is not included in table'.format(target=target))
    #     self.stm = stm.where(stm.c[target].in_(l))
    #     return self

    def filter(self, target: str, a: object):
        if target not in self.table.columns.keys():
            print('{target} is not {table} columns'.format(target=target, table=self.table.name))
        self.stm = self.stm.where(self.table.c[target].__eq__(a))
        return self

    def fetch_columns(self, fetch_list=None):
        """
        fetch_list=False なら全部取ってくる。
        fetch_list is listならその値を取ってくる。
        :param fetch_list:
        :return:
        """
        stm = self.stm
        t = self.table
        if fetch_list is None:
            stm = stm.with_only_columns(t.columns)
        elif type(fetch_list) == list:
            get_list = []
            for i, c in enumerate(t.columns.keys()):
                if c in fetch_list:
                    get_list.append(t.columns.values()[i])
            if len(get_list) != len(fetch_list):
                print('detect some columns not in table columns')
                raise ValueError
            stm = stm.with_only_columns(get_list)
        else:
            print('fetch_list is False or list')
            raise TypeError
        self.stm = stm
        return self

    def fetch_data(self, stm: select, **argv):
        return pd.read_sql(stm, self.conn, **argv)

# def example():
#     class Example1(RdbIOMixin):
#         table_name = 'id_master'
#         schema_name = 'master'
#
#     class Example2(RdbIOMixin):
#         table_name = 'gakuryoku'
#         schema_name = 'master'
#
#     school_list = [
#         10607.0,
#         10637.0,
#         10604.0,
#         10374.0,
#         10463.0,
#         10206.0,
#         10656.0,
#         10371.0,
#         10718.0,
#         10645.0,
#         10260.0,
#         10437.0,
#         30179.0,
#         30270.0,
#         30185.0,
#         30288.0,
#         30230.0,
#         30265.0
#     ]
#
#     def todashi_extract(model2, school_list):
#         # from sqlalchemy.sql.base import _generative
#         def plus_columns(stm, column_list):
#             for col in column_list:
#                 stm = stm.column(col)
#             return stm
#
#         example1 = Example1()
#         model2.stm = (
#             model2.stm
#             .select_from(
#                 model2.table.join(
#                     example1.table,
#                     (model2.table.c.id == example1.table.c.id) & (model2.table.c.year == example1.table.c.year)
#                 )
#             )
#             .where(example1.table.c.school_id.in_(school_list))
#             # .plus_columns([example1.table.c.school_id, example1.table.c.city_id])
#             # .column(example1.table.c.school_id)
#         )
#         # chain methodにできないからmonkey patch
#         model2.stm = plus_columns(model2.stm, [example1.table.c.school_id, example1.table.c.city_id])
#
#
#     example2 = Example2().fetch_columns(['kokugo_level']).pipe(todashi_extract, school_list=school_list).read()
#     example2.data



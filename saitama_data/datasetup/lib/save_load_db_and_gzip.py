import os
import gc
import subprocess
import pandas as pd
from sqlalchemy.engine import Engine, Connection, reflection
from sqlalchemy.sql import select
from sqlalchemy import Table, MetaData
from sqlalchemy.schema import CreateSchema
from sqlalchemy.sql.elements import literal_column
from saitama_data.sql.connect_postgres import to_sql
from saitama_data.lib.safe_path import safe_path


def helper_get_tablename_to_file_path(root_dir, schema_name, table_name):
    return os.path.join(
        root_dir, schema_name, table_name, "".join([table_name, "{0}", ".csv"])
    )


class CopyData:
    def __init__(self, db_copy, schema_name: str, table_name: str, **argv):
        self.db_copy_cls_name = db_copy.__class__
        self.schema_name = schema_name
        self.table_name = table_name
        for attr_name in list(argv.keys()):
            self.__setattr__(attr_name, argv[attr_name])

    def get_table_iterator(self, **argv):
        raise NotImplementedError

    def save_table_iterator(self, **argv):
        raise NotImplementedError


class CopyDataRdbToExistsDirs(CopyData):
    def __init__(self, db_copy, schema_name: str, table_name: str, **argv):
        super().__init__(db_copy=db_copy, schema_name=schema_name, table_name=table_name, **argv)
        self.conn = db_copy.conn
        self.root_dir = db_copy.root_dir
        self.table_iterator = None

    def get_table_iterator(self):
        self.table_iterator = self.get_table_iterator_from_rdb(
            conn=self.conn,
            table_name=self.table_name,
            schema_name=self.schema_name
        )

    def save_table_iterator(self, ):
        self.save_table_iterator_in_exists_dirs(
            table_iterator=self.table_iterator,
            path_save=helper_get_tablename_to_file_path(
                root_dir=self.root_dir,
                schema_name=self.schema_name,
                table_name=self.table_name
            ),
        )

    @staticmethod
    def get_table_iterator_from_rdb(conn: Connection, table_name: str, schema_name: str, chunksize=300000, **argv):
        """
        get table iterator from table info string

        """
        return pd.read_sql(
            sql=(
                select([literal_column('*')])
                    .select_from(Table(table_name, MetaData(conn), schema=schema_name))
                # .limit(100) # debug
            ),
            con=conn,
            chunksize=chunksize,
            **argv
        )

    @staticmethod
    def save_table_iterator_in_exists_dirs(table_iterator: iter, path_save: str):
        # gzipでデータをそのまま保存する
        def save_gzip(dfx: pd.DataFrame, path):
            try:
                dfx.to_csv(path + '.gz', index=False, encoding="utf-8", compression='gzip')
            except:
                print(path)
                if os.path.exists(path + '.gz'):
                    os.remove(path + '.gz')
                dfx.to_csv(path, index=False, encoding="utf-8")
                subprocess.call(['gzip', '-9', path])
                # print(path.replace('.gzip', ''))
                # dfx.to_csv(path.replace('.gzip', ''), index=False, encoding="utf-8")
            pass

        os.makedirs(os.path.dirname(path_save), exist_ok=True)
        for i, df in enumerate(table_iterator):
            save_gzip(dfx=df, path=path_save.format(i))


class CopyDataExistsDirsToRdb(CopyData):
    def __init__(self, db_copy, schema_name: str, table_name: str, **argv):
        super().__init__(db_copy=db_copy, schema_name=schema_name, table_name=table_name, **argv)
        self.root_dir = db_copy.root_dir
        self.engine = db_copy.engine
        self.table_iterator = None

    def get_table_iterator(self):
        self.table_iterator = self.get_table_iterator_from_exists_dirs(
            root_dir=self.root_dir,
            table_name=self.table_name,
            schema_name=self.schema_name
        )

    def save_table_iterator(self):
        self.create_schema(
            engine = self.engine,
            schema_name = self.schema_name
        )
        self.save_table_iterator_in_rdb(
            table_iterator=self.table_iterator,
            engine=self.engine,
            table_name=self.table_name,
            schema_name=self.schema_name
        )

    @staticmethod
    def create_schema(engine, schema_name):
        insp = reflection.Inspector.from_engine(engine)
        schema_list = insp.get_schema_names()
        if schema_name not in  schema_list:
            engine.execute(CreateSchema(schema_name))

    @staticmethod
    def get_table_iterator_from_exists_dirs(
            root_dir: str, table_name: str, schema_name: str, ignore_files=('.DS_Store', ), **argv):

        def df_engineering_for_rdb_monkey_patch(df, tbl_name, shm_name):
            if (shm_name == 'master') & (tbl_name == 'school_qes'):
                df['value'] = df['value'].str.replace(',', '\,').str.replace('\\r\\n', '\\\\r\\\\n')
                return df
            else:
                return df

        def table_iterator_from_paths(paths, **argvs):
            for path in paths:
                try:
                    df = pd.read_csv(path, encoding='utf-8', compression='infer', **argvs)
                except OSError:
                    df = pd.read_csv(path, encoding='utf-8', **argvs)
                df = df_engineering_for_rdb_monkey_patch(df, tbl_name=table_name, shm_name=schema_name)
                yield df

        target_dir = os.path.join(root_dir, schema_name, table_name)
        table_paths = [os.path.join(target_dir, x) for x in os.listdir(target_dir) if x not in ignore_files]
        return table_iterator_from_paths(table_paths, **argv)

    @staticmethod
    def save_table_iterator_in_rdb(table_iterator: iter, engine: Engine, table_name: str, schema_name: str):
        for i, df in enumerate(table_iterator):
            if i == 0:
                to_sql(df=df, name=table_name, schema=schema_name, engine=engine, if_exists='replace')
            else:
                to_sql(df=df, name=table_name, schema=schema_name, engine=engine, if_exists='append')

class DBDumper:
    """
    from saitama_data.connect_server import return_connection
    # dump
    root_dir = './data/dump/rdb'
    engine, conn = return_connection()
    db_copy = DBDumper(copy_type='dump', root_dir=root_dir, engine=engine, conn=conn)
    db_copy.execute()
    # restore
    root_dir = './data/dump/rdb'
    engine, conn = return_connection()
    db_copy = DBDumper(copy_type='restore', root_dir=root_dir, engine=engine, conn=conn)
    db_copy.execute()
    """
    def __init__(self, copy_type: str, root_dir: str, engine: Engine, conn: Connection, **argv):
        if copy_type in ('default', 'restore', 'ExistsDirsToRdb'):
            self.mode = 'restore'
            self.copy_data_cls_name = 'CopyDataExistsDirsToRdb'
            self.copy_data_cls = CopyDataExistsDirsToRdb
        elif copy_type in ('RdbToExistsDirs', 'dump'):
            self.mode = 'dump'
            self.copy_data_cls_name = 'RdbToExistsDirs'
            self.copy_data_cls = CopyDataRdbToExistsDirs
        else:
            raise ValueError
        self.engine = engine
        self.conn = conn
        self.root_dir = root_dir
        for attr_name in list(argv.keys()):
            self.__setattr__(attr_name, argv[attr_name])

    def execute(self):
        schema_to_table = self.get_dir_schema_to_table()
        for schema_name in schema_to_table.keys():
            for table_name in schema_to_table[schema_name]:
                print(table_name)
                dfc = self.build_copy_data(schema_name, table_name)
                dfc.get_table_iterator()
                dfc.save_table_iterator()
                del dfc
                gc.collect(); gc.collect()


    def build_copy_data(self, schema_name, table_name):
        return self.copy_data_cls(self, schema_name=schema_name, table_name=table_name)

    def get_dir_schema_to_table(self):
        if self.copy_data_cls_name == 'CopyDataExistsDirsToRdb':
            return self.get_dir_schema_to_table_from_exists_dirs(root_dir=self.root_dir)
        elif self.copy_data_cls_name == 'RdbToExistsDirs':
            return self.get_dir_schema_to_table_from_engine(engine=self.engine)
        else:
            raise ValueError

    @staticmethod
    def get_dir_schema_to_table_from_engine(engine: Engine, ignore_schema=('information_schema', )):
        insp = reflection.Inspector.from_engine(engine)
        schema_list = insp.get_schema_names()
        return {s: insp.get_table_names(schema=s) for s in schema_list if s not in ignore_schema}

    @staticmethod
    def get_dir_schema_to_table_from_exists_dirs(root_dir: str, ignore_files=('.DS_Store', )):
        return {
            s: [x for x in os.listdir(os.path.join(root_dir, s)) if x not in ignore_files]
            for s in os.listdir(root_dir)
            if s not in ignore_files
        }



# def tmp():
#     from saitama_data.connect_server import return_connection
#     # 自前でスキーマ及び、テーブルを作る必要もある
#     # テーブルはpandas_schemaから作るようにしよう
#
#     root_dir = './data/dump/rdb'
#     engine, conn = return_connection()
#     # save_data_from_rdb(root_dir=root_dir, engine=engine, conn=conn)
#     table_name = 'id_master'
#     schema_name = 'master'
#     db_copy = DBDumper(copy_type='ExistsDirsToRdb', root_dir=root_dir, engine=engine, conn=conn)
#     db_copy.execute()
#     cdedr = CopyDataExistsDirsToRdb(db_copy=db_copy, table_name=table_name, schema_name=schema_name)
#     cdedr.get_table_iterator()
#     cdedr.save_table_iterator()
#
#     root_dir = './data/tmp2/rdb'
#     engine, conn = return_connection()
#     db_copy = DBDumper(copy_type='RdbToExistsDirs', root_dir=root_dir, engine=engine, conn=conn)
#     db_copy.execute()
#

    # dfx = df
    # path = './data/tmp/rdb/master/id_master/id_master{0}.csv'.format(10)
    # dfx.to_csv(path, index=False,  encoding="utf-8")
    # import gzip
    # file = gzip.open(path, mode='w')
    # gf = gzip.GzipFile(filename=path, mode='w')
    # gf.write(file)
    # path_save_dir = os.path.join(root_dir, schema_name, table_name)
    # os.makedirs(path_save_dir, exist_ok=True)
    # name = "".join([table_name, "{0}", ".gzip"])
    # path_save =  os.path.join(path_save_dir, name)
    # save_table_from_table_iterator_in_exists_dirs(path_save=path_save, table_iterator=table_iterator)

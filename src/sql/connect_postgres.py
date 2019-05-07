from io import StringIO

def to_sql(df, name, engine, schema, if_exists='fail', sep='\t', encoding='utf8', escapechar="\\"):
    """
    postgresにコピーをするメソッド

    sepで','はエラーをうむ？そんなことない？中で使っている文字列に依存したことがあった気がする
    調べた→ どこか一つでも値の中にsepで使っている文字列があったらだめ。

        以下全てこのメソッドの実行例

        * 回らない：sepは' '.　temp に' 'が入っているから

            df = data.copy()
            df['temp'] = 'yftyg,tf yghj.yguijok/jk'
            to_sql(df=df, name=tablename, engine=engine, schema=shname, if_exists='replace',sep=' ')

        * 回らない：sepは','.temp に','が入っているから

            df = data.copy()
            df['temp'] = 'yftyg,tf yghj.yguijok/jk'
            to_sql(df=df, name=tablename, engine=engine, schema=shname, if_exists='replace',sep=',')

        * 回る：postgresはescape文字列が\.sepで指定している','に'\,'とescapeしたから

        ちなみに以下の例だとtempには"a,b/ c.dj/"と入る。escape文字では特殊文字の前で初めて機能する。
        ドキュメントを読みましょう。
        http://www.postgresql.jp/document/7.3/user/sql-syntax.html#SQL-SYNTAX-CONSTANTS

            df = data.copy()
            df['temp'] = 'a\,b/ c.d\j\/'
            to_sql(df=df, name=tablename, engine=engine, schema=shname, if_exists='replace',sep=',')


    :param df: pandas.DataFrame
    :param name: string
    :param engine: sqlalqemy.engine
    :param schema: string
    :param if_exists:  {‘fail’, ‘replace’, ‘append’}, default ‘fail’
    :param sep:
    :param encoding:
    :return:
    """
    # set database
    df[:0].to_sql(name, engine, schema=schema, if_exists=if_exists, index=False)
    # write data at memory
    output = StringIO()
    df.to_csv(output, sep=sep, header=False, encoding=encoding, index=False, escapechar=escapechar)
    output.seek(0)
    # inset data by copy method in postgres
    # sql_string = 'COPY %s.%s FROM \'%s\' WITH CSV HEADER DELIMITER \',\'' % (sh, name, temp_path + name + '.csv')
    table_name = schema + '.' + name
    connection = engine.raw_connection()
    cursor = connection.cursor()
    # sql = 'COPY {0} FROM STDOUT WITH CSV DELIMITER \'{1}\''.format(table_name, sep)
    # cursor.copy_expert(sql,output)
    cursor.copy_from(output, table_name, sep=sep, null='')
    connection.commit()
    cursor.close()


# from sqlalchemy import types
# df = data.copy()
# dtyp = {c:types.VARCHAR(df[c].str.len().max())
#         for c in df.columns[df.dtypes == 'object'].tolist()}
# df.to_sql(tablename, conn, schema = shname, if_exists='append', dtype=dtyp)


# import pandas as pd
# from sklearn import datasets
#
# iris = datasets.load_iris()
# df = pd.DataFrame(iris.data, columns=iris.feature_names)
# df['target'] = iris.target_names[iris.target]
# df.to_sql('master', conn, schema='tempq')
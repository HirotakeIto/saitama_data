# System Dependencies
* Python: >= `3.6`
* PostgreSQL: >= `9.5`
* pip
* pipenv


本レポジトリではpipenvを使用してバージョンコントロールしています。

# 準備
## Python導入
Please see below document.

* [The Hitchhiker's Guide to Python: Properly Installing Python](https://docs.python-guide.org/starting/installation/)


## pipenv導入
公式の導入ページ
* https://github.com/pypa/pipenv
* https://pipenv-ja.readthedocs.io/ja/translate-ja/index.html

基本的にはpipが使えれば次のコマンドで入ります。
```
$ pip install --user pipenv
```
ただし、次のようなケースに注意してください。
> このコマンドは user installation を行い、システム全体に関わるパッケージを壊さないようにします。 インストールを行った後に pipenv が使えるようにならない場合は、 user base のバイナリディレクトリを PATH に追加する必要があります。
(https://pipenv-ja.readthedocs.io/ja/translate-ja/install.html#pragmatic-installation-of-pipenv)

## PostgreSQL
RDB（リレーションを張っていないので実質DB置き場）としてPostgreSQLを利用しています。
次のページからPostgreSQLをダウンロードしてください。

* 公式ページ：Downloads(https://www.postgresql.org/download/)

ダウンロードしたらpostgresを立ち上がっていることを確認してください。
##### 例
ユーザー名 `postgres` でポート番号 `5433` で立ち上げている場合
```
$ psql -U postgres -p 5433
```
でPostgreSQLのシェルが立ち上がる。

##### postgresのローカルサーバーが起動していない場合
```
$ /Library/PostgreSQL/9.5/bin/postmaster -D/Library/PostgreSQL/9.5/data &
```
などでサーバーをバックグランドで起動することができます。
（パスは自分の環境に合わせて適切に書き換えてください）

## そのほか

# 環境のセットアップ
## gitからソースコードのダウンロード
どこか作業をするフォルダを決めて次の様に実行してください。
```
$ git clone https://github.com/HirotakeIto/saitama_data.git
$ cd saitama_data
```
## パッケージインストール
```
pipenv install Pipfile
```


## データの入手
伊藤に連絡ください。必要なデータを提供します。
次の様なフォルダ構造のデータを入手するはずです。

-data
    |-db
    |-dump

## データベース作成
ポストグレスに適当な名前でデータベースを作成してください。
（以下のsetting fileの例だと `saitamatmp` という名前）

## setting fileの作成
`src/setting.ini` を作成する。
基本的には `src/setting_sample.ini` をそのままコピペして作って欲しいですが、
下記の部分だけ書き換えてください。

```
[connection]
application=psgr
connect_string=postgresql://[username]:[password]@localhost:[port]/[db_name]
```

例えば次の様に書くことになります。

```
[connection]
application=psgr
connect_string=postgresql://postgres:abogadoron1126@localhost:5433/saitamatmp
```

# Restore
```
$ pipenv shell
$ ipython
```
でipythonのconsoleを起動してください（別にpipenv環境の実行環境ならなんでも良いですが）。
```
from src.datasetup.lib.save_load_db_and_gzip import DBDumper
from src.connect_server import return_connection

root_dir = './data/dump/rdb'
engine, conn = return_connection()
db_copy = DBDumper(copy_type='restore', root_dir=root_dir, engine=engine, conn=conn)
db_copy.execute()
```



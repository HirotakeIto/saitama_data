# System Dependencies
* Python: >= `3.6`
* PostgreSQL: >= `9.5`
* pip
* pyenv
* pipenv


本レポジトリではpipenvを使用してバージョンコントロールしています。

# 1. 準備
まず必要な諸々のアプリケーションを導入します。

## Python導入
Please see below document.
この時64bit版を入れるようにしてください。
https://qiita.com/shibukawa/items/0daab479a2fd2cb8a0e7
* [The Hitchhiker's Guide to Python: Properly Installing Python](https://docs.python-guide.org/starting/installation/)

## pyenv
windows

## pipenv導入
公式の導入ページ
* https://github.com/pypa/pipenv
* https://pipenv-ja.readthedocs.io/ja/translate-ja/index.html

基本的にはpipが使えれば次のコマンドで入りますが実行環境に合わせて、上記サイトを必ず確認してください。
```
$ pip install --user pipenv
```
ただし、次のようなケースに注意してください。
> このコマンドは user installation を行い、システム全体に関わるパッケージを壊さないようにします。 インストールを行った後に pipenv が使えるようにならない場合は、 user base のバイナリディレクトリを PATH に追加する必要があります。
(https://pipenv-ja.readthedocs.io/ja/translate-ja/install.html#pragmatic-installation-of-pipenv)

## PostgreSQLの導入
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



# 2. 環境のセットアップ
次に環境を構築します。
基本的には1. データを入手（伊藤まで連絡）し適切に配置、2. initialファイルを作成の手順です。

## gitからソースコードのダウンロード
どこか作業をするフォルダの親ディレクトリを決めて次の様に実行してください。
仮に `path` を作業をするフォルダの親ディレクトリとします。まずはそこにカレントディレクトリを移してください。
```
cd path
```
その後、git cloneします。

```
$ git clone https://github.com/HirotakeIto/saitama_data.git
$ cd saitama_data
```
## パッケージインストール
```
pipenv install -e Pipfile
```


## データの入手
伊藤に連絡ください。必要なデータを提供します。
次の様なフォルダ構造のデータを入手するはずです。
入手したデータをこのフォルダ構造の通りに配置してください。

```
$ tree -d -L 2 ./data/dump/ ./data/db
./data/dump/
└── rdb
    ├── master
    ├── todashi
    └── work
./data/db
├── external
│   └── primary_low_grade
├── master
└── todashi
```

## データベース作成
ポストグレスに適当な名前でデータベースを作成してください。
（以下のsetting fileの例だと `saitamatmp` という名前）

## setting fileの作成
`saitama_data/setting.ini` を作成する。
基本的には `saitama_data/setting_sample.ini` をそのままコピペして作って欲しいですが、
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

# 3. Restore & Dump
## Restore
```
$ pipenv shell
$ ipython
```
でipythonのconsoleを起動してください（別にpipenv環境の実行環境ならなんでも良いですが）。
その後次のコードでデータをRDBにリストアします。
```
from saitama_data.datasetup.lib.save_load_db_and_gzip import DBDumper
from saitama_data.connect_server import return_connection

root_dir = './data/dump/rdb'
engine, conn = return_connection()
db_copy = DBDumper(copy_type='restore', root_dir=root_dir, engine=engine, conn=conn)
db_copy.execute()
```
## Dump
作ったデータは次の様にダンプすることもできます。
```
from saitama_data.datasetup.lib.save_load_db_and_gzip import DBDumper
from saitama_data.connect_server import return_connection

root_dir = './data/dump/rdb'
engine, conn = return_connection()
db_copy = DBDumper(copy_type='restore', root_dir=root_dir, engine=engine, conn=conn)
db_copy.execute()
```

# 4. データの利用
今現在準備しているのは、次のデータです。

`IdMaster, Gakuryoku, SeitoQes, SchoolQes, SeitoQesSosei, TodaTchBelong, TodaTeacherQes, TodaOldTchNoncog`

これら次の様に利用することができます。
```
In[2]: from saitama_data.datasetup.models import IdMaster
Connection to "APP: psgr,  DB:postgresql://postgres:abogadoron1126@localhost:5433/tmp2"
connecting::::::: postgresql://postgres:abogadoron1126@localhost:5433/tmp2
In[4]: idmaster = IdMaster()
In[5]: idmaster.read()
In[10]: idmaster.data.head(1)
Out[10]:
   city_id  class  grade   ...    school_id  sex    year
0  26646.0    1.0    6.0   ...      10276.0  1.0  2015.0

[1 rows x 8 columns]
```
こんな感じでデータを `panads` の `DataFrame` の形で取得することができます。

